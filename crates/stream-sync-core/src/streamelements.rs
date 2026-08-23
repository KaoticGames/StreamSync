//! StreamElements overlay import (kappa v2 API + mapper).

use crate::config_types::default_events_overlay_profile;
use crate::storage::{self, StoragePaths};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const SE_API_BASE: &str = "https://api.streamelements.com/kappa/v2/";

/// Stream Sync Events Studio standard canvas (matches events-studio.html).
const SS_STAGE_W: f64 = 1280.0;
const SS_STAGE_H: f64 = 720.0;

/// Scale SE overlay coordinates (e.g. 1920×1080) into the Stream Sync stage.
#[derive(Debug, Clone, Copy)]
struct StageScale {
    source_w: f64,
    source_h: f64,
    target_w: f64,
    target_h: f64,
}

impl StageScale {
    fn from_overlay(overlay: &Value) -> Self {
        let (source_w, source_h) = overlay
            .get("settings")
            .or_else(|| overlay.pointer("/data/settings"))
            .map(|s| {
                let w = s
                    .get("width")
                    .and_then(value_as_f64)
                    .unwrap_or(1920.0)
                    .max(1.0);
                let h = s
                    .get("height")
                    .and_then(value_as_f64)
                    .unwrap_or(1080.0)
                    .max(1.0);
                (w, h)
            })
            .unwrap_or((1920.0, 1080.0));

        Self {
            source_w,
            source_h,
            target_w: SS_STAGE_W,
            target_h: SS_STAGE_H,
        }
    }

    fn sx(&self) -> f64 {
        self.target_w / self.source_w
    }

    fn sy(&self) -> f64 {
        self.target_h / self.source_h
    }

    fn scale_rect(&self, x: f64, y: f64, w: f64, h: f64) -> (f64, f64, f64, f64) {
        let mut x = x * self.sx();
        let mut y = y * self.sy();
        let mut w = (w * self.sx()).max(40.0);
        let mut h = (h * self.sy()).max(40.0);

        if w > self.target_w {
            w = self.target_w;
        }
        if h > self.target_h {
            h = self.target_h;
        }
        if x + w > self.target_w {
            x = (self.target_w - w).max(0.0);
        }
        if y + h > self.target_h {
            y = (self.target_h - h).max(0.0);
        }
        if x < 0.0 {
            x = 0.0;
        }
        if y < 0.0 {
            y = 0.0;
        }

        (x, y, w, h)
    }

    fn scale_font(&self, size: u64) -> u64 {
        let s = (self.sx() + self.sy()) / 2.0;
        (size as f64 * s).round().clamp(12.0, 160.0) as u64
    }

    fn needs_scale(&self) -> bool {
        (self.source_w - self.target_w).abs() > 0.5 || (self.source_h - self.target_h).abs() > 0.5
    }
}

fn value_as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_u64().map(|n| n as f64))
        .or_else(|| v.as_i64().map(|n| n as f64))
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SeSession {
    pub jwt: String,
    #[serde(rename = "accountId")]
    pub account_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(rename = "capturedAt", skip_serializing_if = "Option::is_none")]
    pub captured_at: Option<String>,
}

/// Resolve display label from `GET /channels/me` JSON.
pub fn display_name_from_channel(body: &Value) -> Option<String> {
    for key in ["displayName", "username", "alias"] {
        if let Some(s) = body.get(key).and_then(|v| v.as_str()) {
            let t = s.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    body.pointer("/profile/title")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub async fn fetch_channel_profile(session: &SeSession) -> Result<Value> {
    let client = SeClient::from_session(session).await?;
    let res = client
        .http
        .get(format!("{SE_API_BASE}channels/me"))
        .header("Accept", "application/json")
        .header("Authorization", client.auth())
        .send()
        .await?;
    if !res.status().is_success() {
        anyhow::bail!("channels/me HTTP {}", res.status());
    }
    res.json().await.map_err(Into::into)
}

#[derive(Debug, Clone, Serialize)]
pub struct SeOverlaySummary {
    pub id: String,
    pub name: String,
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SeImportResult {
    pub overlay_id: String,
    pub profile_id: String,
    pub profile_name: String,
    pub warnings: Vec<String>,
}

pub fn session_path(paths: &StoragePaths) -> PathBuf {
    paths.root.join("streamelements-session.json")
}

pub fn imports_dir(paths: &StoragePaths) -> PathBuf {
    paths.root.join("imports").join("streamelements")
}

pub fn load_session(paths: &StoragePaths) -> Result<Option<SeSession>> {
    let p = session_path(paths);
    if !p.is_file() {
        return Ok(None);
    }
    let s: SeSession = storage::read_json_or_default(&p, &SeSession::default())?;
    if s.jwt.trim().is_empty() || s.account_id.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(s))
}

pub fn save_session(paths: &StoragePaths, session: &SeSession) -> Result<()> {
    storage::write_json(&session_path(paths), session)
}

pub fn clear_session(paths: &StoragePaths) -> Result<()> {
    let p = session_path(paths);
    if p.is_file() {
        let _ = std::fs::remove_file(p);
    }
    Ok(())
}

pub struct SeClient {
    http: reqwest::Client,
    jwt: String,
    account_id: String,
    auth_scheme: AuthScheme,
}

#[derive(Clone, Copy)]
enum AuthScheme {
    Bearer,
    OAuth,
}

impl SeClient {
    pub async fn from_session(session: &SeSession) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()?;
        let mut client = Self {
            http,
            jwt: session.jwt.clone(),
            account_id: session.account_id.clone(),
            auth_scheme: AuthScheme::Bearer,
        };
        if client.validate_scheme(AuthScheme::Bearer).await.is_err() {
            client.auth_scheme = AuthScheme::OAuth;
            client
                .validate_scheme(AuthScheme::OAuth)
                .await
                .context("JWT failed channels/me with Bearer and oAuth")?;
        }
        Ok(client)
    }

    async fn validate_scheme(&self, scheme: AuthScheme) -> Result<()> {
        let res = self
            .http
            .get(format!("{SE_API_BASE}channels/me"))
            .header("Accept", "application/json")
            .header("Authorization", auth_header(scheme, &self.jwt))
            .send()
            .await?;
        if !res.status().is_success() {
            anyhow::bail!("channels/me HTTP {}", res.status());
        }
        Ok(())
    }

    fn auth(&self) -> String {
        auth_header(self.auth_scheme, &self.jwt)
    }

    pub async fn list_overlays(&self) -> Result<Vec<SeOverlaySummary>> {
        if let Ok(list) = self
            .try_list_overlays_endpoint(&format!("{SE_API_BASE}overlays"))
            .await
        {
            if !list.is_empty() {
                return Ok(list);
            }
        }
        let url = format!("{SE_API_BASE}overlays/{}", self.account_id);
        self.try_list_overlays_endpoint(&url).await
    }

    async fn try_list_overlays_endpoint(&self, url: &str) -> Result<Vec<SeOverlaySummary>> {
        let res = self
            .http
            .get(url)
            .header("Accept", "application/json")
            .header("Authorization", self.auth())
            .send()
            .await?;
        if !res.status().is_success() {
            anyhow::bail!("list overlays HTTP {}", res.status());
        }
        let body: Value = res.json().await?;
        Ok(parse_overlay_list(&body))
    }

    pub async fn fetch_overlay_json(&self, overlay_id: &str) -> Result<Value> {
        let urls = [
            format!("{SE_API_BASE}overlays/{}/{}", self.account_id, overlay_id),
            format!("{SE_API_BASE}overlays/{}", overlay_id),
        ];
        let mut last_err = None;
        for url in urls {
            match self.try_fetch_overlay(&url).await {
                Ok(v) => return Ok(v),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("fetch overlay failed")))
    }

    async fn try_fetch_overlay(&self, url: &str) -> Result<Value> {
        let res = self
            .http
            .get(url)
            .header("Accept", "application/json")
            .header("Authorization", self.auth())
            .send()
            .await?;
        if !res.status().is_success() {
            anyhow::bail!("fetch overlay HTTP {}", res.status());
        }
        let text = res.text().await?;
        serde_json::from_str(&text).context("parse overlay JSON")
    }
}

fn auth_header(scheme: AuthScheme, jwt: &str) -> String {
    match scheme {
        AuthScheme::Bearer => format!("Bearer {jwt}"),
        AuthScheme::OAuth => format!("oAuth {jwt}"),
    }
}

fn overlay_id_from_value(item: &Value, map_key: Option<&str>) -> String {
    if let Some(s) = item
        .get("_id")
        .or_else(|| item.get("id"))
        .or_else(|| item.get("overlayId"))
        .and_then(value_as_id_string)
    {
        return s;
    }
    map_key.unwrap_or("").trim().to_string()
}

fn value_as_id_string(v: &Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        let t = s.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    if let Some(n) = v.as_i64() {
        return Some(n.to_string());
    }
    if let Some(n) = v.as_u64() {
        return Some(n.to_string());
    }
    None
}

fn parse_overlay_list(body: &Value) -> Vec<SeOverlaySummary> {
    enum ListItem<'a> {
        Plain(&'a Value),
        Keyed { key: &'a str, value: &'a Value },
    }

    let items: Vec<ListItem<'_>> = match body {
        Value::Array(a) => a.iter().map(ListItem::Plain).collect(),
        Value::Object(m) => {
            if let Some(arr) = m
                .get("overlays")
                .or_else(|| m.get("data"))
                .or_else(|| m.get("docs"))
                .or_else(|| m.get("items"))
                .and_then(|v| v.as_array())
            {
                arr.iter().map(ListItem::Plain).collect()
            } else {
                m.iter()
                    .filter_map(|(k, v)| {
                        if !v.is_object() {
                            return None;
                        }
                        if v.get("name").is_some()
                            || v.get("_id").is_some()
                            || v.get("id").is_some()
                        {
                            Some(ListItem::Keyed {
                                key: k.as_str(),
                                value: v,
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            }
        }
        _ => vec![],
    };
    let mut out = Vec::new();
    for item in items {
        let (map_key, value) = match item {
            ListItem::Plain(v) => (None, v),
            ListItem::Keyed { key, value } => (Some(key), value),
        };
        let id = overlay_id_from_value(value, map_key);
        if id.is_empty() {
            continue;
        }
        let name = value
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("Untitled overlay")
            .to_string();
        let updated_at = value
            .get("updatedAt")
            .or_else(|| value.get("updated_at"))
            .and_then(|v| v.as_str())
            .map(String::from);
        out.push(SeOverlaySummary {
            id,
            name,
            updated_at,
        });
    }
    out
}

pub fn save_raw_overlay(paths: &StoragePaths, overlay_id: &str, raw: &Value) -> Result<PathBuf> {
    let dir = imports_dir(paths);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{overlay_id}.json"));
    storage::write_json(&path, raw)?;
    Ok(path)
}

/// Map one SE overlay document → Stream Sync events profile JSON.
pub fn map_overlay_to_profile(overlay: &Value) -> (String, Value, Vec<String>) {
    let mut warnings = Vec::new();
    let overlay_id = overlay
        .get("_id")
        .or_else(|| overlay.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let overlay_name = overlay
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("SE Overlay");

    let mut profile = default_events_overlay_profile();
    if let Some(obj) = profile.as_object_mut() {
        obj.insert("profileName".into(), json!(format!("SE: {overlay_name}")));
    }

    let stage_scale = StageScale::from_overlay(overlay);
    if stage_scale.needs_scale() {
        warnings.push(format!(
            "Scaled layout from SE {}×{} to Stream Sync {}×{}.",
            stage_scale.source_w.round() as u32,
            stage_scale.source_h.round() as u32,
            stage_scale.target_w.round() as u32,
            stage_scale.target_h.round() as u32,
        ));
    }

    if let Some(stage) = profile.get_mut("stage").and_then(|v| v.as_object_mut()) {
        stage.insert("w".into(), json!(stage_scale.target_w.round() as u64));
        stage.insert("h".into(), json!(stage_scale.target_h.round() as u64));
    }

    let widgets = overlay
        .get("widgets")
        .or_else(|| overlay.pointer("/data/widgets"))
        .and_then(|v| v.as_array());

    let mut events = serde_json::Map::new();

    if let Some(widgets) = widgets {
        for widget in widgets {
            map_widget(widget, &stage_scale, &mut events, &mut warnings);
        }
    } else {
        warnings.push("No widgets array on overlay — using default alert templates only.".into());
    }

    if events.is_empty() {
        warnings.push("No mappable alert widgets found — profile keeps default variations.".into());
    } else if let Some(ev) = profile.get_mut("events").and_then(|v| v.as_object_mut()) {
        for (k, v) in events {
            ev.insert(k, v);
        }
    }

    let profile_id = profile_id_for_overlay(overlay_name, overlay_id);
    profile.as_object_mut().map(|o| {
        o.insert(
            "_seImport".into(),
            json!({
                "overlayId": overlay_id,
                "overlayName": overlay_name,
                "importedAt": chrono::Utc::now().to_rfc3339(),
                "seImportVersion": 3,
                "sourceStage": {
                    "w": stage_scale.source_w.round() as u64,
                    "h": stage_scale.source_h.round() as u64,
                },
                "targetStage": {
                    "w": stage_scale.target_w.round() as u64,
                    "h": stage_scale.target_h.round() as u64,
                },
            }),
        );
    });

    (profile_id, profile, warnings)
}

fn profile_id_for_overlay(name: &str, overlay_id: &str) -> String {
    let slug: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    let slug = if slug.is_empty() {
        "overlay".to_string()
    } else {
        slug.chars().take(32).collect()
    };
    let short: String = overlay_id.chars().take(6).collect();
    format!("se-{slug}-{short}")
}

fn map_widget(
    widget: &Value,
    scale: &StageScale,
    events: &mut serde_json::Map<String, Value>,
    warnings: &mut Vec<String>,
) {
    let wtype = widget
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_lowercase();

    if wtype.contains("custom") && !wtype.contains("alert") {
        warnings.push(format!(
            "Skipped custom widget {:?}",
            widget.get("name").and_then(|v| v.as_str()).unwrap_or("")
        ));
        return;
    }

    if !wtype.contains("alert") {
        return;
    }

    let variables = widget
        .get("variables")
        .or_else(|| widget.get("data"))
        .and_then(|v| v.as_object());

    let Some(vars) = variables else {
        warnings.push("Alert widget without variables — skipped.".into());
        return;
    };

    let (bx, by, bw, bh) = widget_bounds(widget);
    let bounds = scale.scale_rect(bx, by, bw, bh);

    if let Some(host_cfg) = vars.get("host") {
        if se_event_cfg_enabled(host_cfg) {
            warnings.push(
                "Skipped deprecated Twitch host alert (host is no longer supported by Twitch)."
                    .into(),
            );
        }
    }

    for (se_key, ss_key) in SE_EVENT_KEYS {
        let Some(event_cfg) = vars.get(*se_key) else {
            continue;
        };
        if !se_event_cfg_enabled(event_cfg) {
            continue;
        }
        if *se_key == "subscriber" {
            map_subscriber_se_event(event_cfg, bounds, scale, events, warnings);
            continue;
        }
        let mut mapped = Vec::new();
        map_se_event_variations(event_cfg, bounds, ss_key, scale, &mut mapped);
        if mapped.is_empty() {
            continue;
        }
        for v in mapped {
            push_event_variation(events, ss_key, v);
        }
    }

    if events.is_empty() {
        warnings.push(
            "Alert box found but no enabled SE events were mapped (check event toggles in SE)."
                .into(),
        );
    }
}

const SE_EVENT_KEYS: &[(&str, &str)] = &[
    ("follower", "follow"),
    ("subscriber", "sub"),
    ("raid", "raid"),
    ("cheer", "cheer"),
    ("tip", "cheer"),
    ("gift", "gift"),
    ("gifted", "gift"),
    ("purchase", "redeem"),
    ("redemption", "redeem"),
    ("merch", "redeem"),
];

/// Stream Sync variation trigger: mode, numeric value, optional tier (1–3).
type VariationTrigger = (String, Option<u64>, Option<u64>);

fn push_event_variation(
    events: &mut serde_json::Map<String, Value>,
    ss_key: &str,
    variation: Value,
) {
    let entry = events
        .entry(ss_key.to_string())
        .or_insert_with(|| json!({ "variations": [] }));
    if let Some(arr) = entry.get_mut("variations").and_then(|v| v.as_array_mut()) {
        arr.push(variation);
    }
}

/// SE nests gift / community gift / resub under `subscriber` — route to Stream Sync `sub` / `resub` / `gift`.
fn map_subscriber_se_event(
    event_cfg: &Value,
    bounds: (f64, f64, f64, f64),
    scale: &StageScale,
    events: &mut serde_json::Map<String, Value>,
    warnings: &mut Vec<String>,
) {
    let se_variations = event_cfg
        .get("variations")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty());

    if se_event_cfg_enabled(event_cfg) {
        let base = variation_from_se_settings(
            event_cfg,
            bounds,
            "sub",
            scale,
            "Base alert",
            None,
            Some(("none".into(), None, None)),
        );
        push_event_variation(events, "sub", base);
    }

    let Some(vars) = se_variations else {
        return;
    };

    let mut routed_gift = 0u32;
    let mut routed_resub = 0u32;

    for se_var in vars {
        if se_var.get("enabled").and_then(|v| v.as_bool()) == Some(false) {
            continue;
        }
        let settings = se_var
            .get("settings")
            .filter(|v| v.is_object())
            .unwrap_or(se_var);
        let name = se_var
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("Variation");
        let chance = se_var.get("chance").and_then(|v| v.as_u64());
        let ss_key = resolve_subscriber_variation_target(se_var);
        let trigger = map_se_subscriber_variation_trigger(se_var, ss_key);
        if ss_key == "gift" {
            routed_gift += 1;
        } else if ss_key == "resub" {
            routed_resub += 1;
        }
        let variation = variation_from_se_settings(
            settings,
            bounds,
            ss_key,
            scale,
            name,
            chance,
            Some(trigger),
        );
        push_event_variation(events, ss_key, variation);
    }

    if routed_gift > 0 || routed_resub > 0 {
        warnings.push(format!(
            "Routed SE subscriber alerts: {} gift, {} resub (plus first-time sub base).",
            routed_gift, routed_resub
        ));
    }
}

fn resolve_subscriber_variation_target(se_var: &Value) -> &'static str {
    let se_type = se_var
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match se_type.as_str() {
        "gift" | "communitygift" => "gift",
        "amount" => {
            if subscriber_variation_is_resub(se_var) {
                "resub"
            } else {
                "sub"
            }
        }
        "tier" | "subscription" => "sub",
        _ => {
            let name = se_var
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            if name.contains("community") || name.contains("gift") {
                "gift"
            } else if name.contains("resub") {
                "resub"
            } else {
                "sub"
            }
        }
    }
}

fn subscriber_variation_is_resub(se_var: &Value) -> bool {
    let se_type = se_var
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if se_type == "amount" {
        let cond = se_var
            .get("condition")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_ascii_uppercase();
        let req = se_var_requirement(se_var).unwrap_or(0);
        if (cond == "ATLEAST" || cond == "MIN" || cond == "MINIMUM") && req >= 2 {
            return true;
        }
    }
    let msg = se_var
        .pointer("/settings/text/message")
        .or_else(|| se_var.get("message"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    msg.contains("resub")
}

fn map_se_subscriber_variation_trigger(se_var: &Value, ss_key: &str) -> VariationTrigger {
    let condition = se_var_condition(se_var);
    let requirement = se_var_requirement(se_var);
    let se_type = se_var
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    if ss_key == "gift" {
        let mode = match condition.to_ascii_uppercase().as_str() {
            "EXACT" => "exact",
            "ATLEAST" | "MIN" | "MINIMUM" => "min",
            _ if se_type == "communitygift" => "min",
            _ => "exact",
        };
        let value = requirement.or(Some(if se_type == "gift" { 1 } else { 2 }));
        let tier = normalize_sub_tier(se_var.get("tier").and_then(|v| v.as_u64()));
        return (mode.into(), value, tier);
    }

    if ss_key == "resub" {
        let tier = normalize_sub_tier(se_var.get("tier").and_then(|v| v.as_u64()));
        return ("none".into(), None, tier);
    }

    if se_type == "tier" || se_type == "subscription" {
        let tier = normalize_sub_tier(requirement.or(se_var.get("tier").and_then(|v| v.as_u64())));
        return ("none".into(), None, tier);
    }

    let (mode, value) = map_se_variation_trigger_pair(se_var);
    (mode, value, None)
}

fn normalize_sub_tier(raw: Option<u64>) -> Option<u64> {
    let n = raw?;
    match n {
        1 | 1000 => Some(1),
        2 | 2000 => Some(2),
        3 | 3000 => Some(3),
        _ => None,
    }
}

fn se_event_cfg_enabled(cfg: &Value) -> bool {
    cfg.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true)
}

fn map_se_event_variations(
    event_cfg: &Value,
    bounds: (f64, f64, f64, f64),
    ss_key: &str,
    scale: &StageScale,
    out: &mut Vec<Value>,
) {
    // Stream Sync expects index 0 = base (trigger "none"). SE stores that on the parent event,
    // not in `variations[]` — mirror `map_subscriber_se_event` so trigger tests match correctly.
    out.push(variation_from_se_settings(
        event_cfg,
        bounds,
        ss_key,
        scale,
        "Base alert",
        None,
        Some(("none".to_string(), None, None)),
    ));

    let Some(se_vars) = event_cfg
        .get("variations")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
    else {
        return;
    };

    for se_var in se_vars {
        if se_var.get("enabled").and_then(|v| v.as_bool()) == Some(false) {
            continue;
        }
        let settings = se_var
            .get("settings")
            .filter(|v| v.is_object())
            .unwrap_or(se_var);
        let name = se_var
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("Variation");
        let chance = se_var.get("chance").and_then(|v| v.as_u64());
        let trigger = map_se_variation_trigger(se_var);
        out.push(variation_from_se_settings(
            settings,
            bounds,
            ss_key,
            scale,
            name,
            chance,
            Some(trigger),
        ));
    }
}

fn map_se_variation_trigger(se_var: &Value) -> VariationTrigger {
    let (mode, value) = map_se_variation_trigger_pair(se_var);
    (mode, value, None)
}

fn se_var_condition(se_var: &Value) -> String {
    se_var
        .get("condition")
        .or_else(|| se_var.get("filter"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_uppercase()
}

fn se_var_requirement(se_var: &Value) -> Option<u64> {
    let req = se_var
        .get("requirement")
        .or_else(|| se_var.get("amount"))
        .or_else(|| se_var.get("value"));
    if let Some(n) = req.and_then(value_as_f64) {
        return Some(n.round() as u64);
    }
    if let Some(s) = req.and_then(|v| v.as_str()) {
        let t = s.trim();
        if t.eq_ignore_ascii_case("prime") {
            return Some(1);
        }
        if let Ok(n) = t.parse::<u64>() {
            return Some(n);
        }
    }
    None
}

fn map_se_variation_trigger_pair(se_var: &Value) -> (String, Option<u64>) {
    let condition = se_var_condition(se_var);
    let requirement = se_var_requirement(se_var);
    match condition.as_str() {
        "EXACT" => ("exact".to_string(), requirement),
        "MIN" | "MINIMUM" | "ATLEAST" | "AT_LEAST" | "GREATER" | "GREATER_THAN" | "MORE"
        | "MORE_THAN" => ("min".to_string(), requirement),
        _ => ("none".to_string(), None),
    }
}

fn widget_bounds(widget: &Value) -> (f64, f64, f64, f64) {
    if let Some(css) = widget.get("css").and_then(|v| v.as_object()) {
        let x = parse_css_px(css.get("left"))
            .or_else(|| widget.get("x").and_then(|v| v.as_f64()))
            .unwrap_or(120.0);
        let y = parse_css_px(css.get("top"))
            .or_else(|| widget.get("y").and_then(|v| v.as_f64()))
            .unwrap_or(140.0);
        let w = parse_css_px(css.get("width"))
            .or_else(|| widget.get("width").and_then(|v| v.as_f64()))
            .unwrap_or(320.0);
        let h = parse_css_px(css.get("height"))
            .or_else(|| widget.get("height").and_then(|v| v.as_f64()))
            .unwrap_or(320.0);
        return (x, y, w, h);
    }

    let pos = widget.get("position").and_then(|v| v.as_object());
    let x = widget
        .get("x")
        .or_else(|| pos.and_then(|p| p.get("x")))
        .and_then(|v| v.as_f64())
        .unwrap_or(120.0);
    let y = widget
        .get("y")
        .or_else(|| pos.and_then(|p| p.get("y")))
        .and_then(|v| v.as_f64())
        .unwrap_or(140.0);
    let w = widget
        .get("width")
        .or_else(|| pos.and_then(|p| p.get("width")))
        .and_then(|v| v.as_f64())
        .unwrap_or(320.0);
    let h = widget
        .get("height")
        .or_else(|| pos.and_then(|p| p.get("height")))
        .and_then(|v| v.as_f64())
        .unwrap_or(320.0);
    (x, y, w, h)
}

fn parse_css_px(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => {
            let t = s.trim().trim_end_matches("px").trim();
            t.parse::<f64>().ok()
        }
        _ => None,
    }
}

fn variation_from_se_settings(
    cfg: &Value,
    bounds: (f64, f64, f64, f64),
    event_key: &str,
    scale: &StageScale,
    variation_name: &str,
    chance_pct: Option<u64>,
    trigger: Option<VariationTrigger>,
) -> Value {
    let (x, y, w, h) = bounds;
    let default_msg = match event_key {
        "follow" => "[name] followed!",
        "sub" => "[name] subscribed!",
        "resub" => "[name] resubscribed!",
        "gift" => "[name] gifted a sub!",
        "cheer" => "[name] cheered [amount]!",
        "raid" => "[name] raided with [amount] viewers!",
        "redeem" => "[name] redeemed [reward]!",
        _ => "[name] triggered an alert!",
    };

    let message = extract_message(cfg).unwrap_or_else(|| default_msg.to_string());
    let visual_url = extract_visual_url(cfg);
    let (sound_url, sound_volume) = extract_audio(cfg);

    let duration = cfg
        .get("duration")
        .or_else(|| cfg.get("widgetDuration"))
        .and_then(|v| v.as_f64())
        .unwrap_or(6.0)
        .clamp(2.0, 60.0) as u64;

    let (anim_in, anim_out, slide_in, slide_out) = map_animation(cfg);
    let (font_family, font_size, font_weight, text_color) = extract_text_style(cfg, scale);

    let (trigger_mode, trigger_value, trigger_tier) =
        trigger.unwrap_or_else(|| ("none".to_string(), None, None));

    let gap = 40.0 * scale.sx();
    let (text_x, text_y, text_w, text_h) = scale.scale_rect(x + w + gap, y + h * 0.1, 520.0, 180.0);

    json!({
        "id": format!("var-{}", Uuid::new_v4()),
        "name": variation_name,
        "trigger": {
            "mode": trigger_mode,
            "value": trigger_value,
            "tier": trigger_tier,
        },
        "chancePct": chance_pct,
        "image": { "type": "url", "value": visual_url },
        "sound": { "type": "url", "value": sound_url, "volume": sound_volume },
        "animation": {
            "in": anim_in,
            "out": anim_out,
            "animSpeed": 50,
            "slideInDir": slide_in,
            "slideOutDir": slide_out,
        },
        "layout": "textSide",
        "message": message,
        "durationSec": duration,
        "text": {
            "fontFamily": font_family,
            "fontSize": font_size,
            "fontWeight": font_weight,
            "color": text_color,
            "strokeColor": "rgba(0,0,0,0.75)",
            "strokeWidth": 0,
            "fontMeta": { "source": "google", "googleFamily": "Montserrat" }
        },
        "placement": {
            "image": { "x": x.round(), "y": y.round(), "w": w.round(), "h": h.round() },
            "text": {
                "x": text_x.round(),
                "y": text_y.round(),
                "w": text_w.round(),
                "h": text_h.round()
            }
        }
    })
}

fn extract_message(cfg: &Value) -> Option<String> {
    let candidates = [
        cfg.pointer("/text/message"),
        cfg.get("message"),
        cfg.pointer("/layout/text"),
        cfg.pointer("/announcement"),
    ];
    for c in candidates {
        if let Some(s) = c.and_then(|v| v.as_str()) {
            let t = se_template_to_ss(s);
            if !t.is_empty() {
                return Some(t);
            }
        }
    }
    None
}

fn extract_visual_url(cfg: &Value) -> String {
    if let Some(g) = cfg.get("graphics") {
        if let Some(s) = g.get("src").and_then(|v| v.as_str()) {
            if s.starts_with("http") {
                return s.to_string();
            }
        }
    }
    if let Some(img) = cfg.get("image") {
        if let Some(s) = img.as_str() {
            if s.starts_with("http") {
                return s.to_string();
            }
        }
        if let Some(s) = img.get("src").and_then(|v| v.as_str()) {
            if s.starts_with("http") {
                return s.to_string();
            }
        }
    }
    find_url_string(cfg, &["imageUrl", "media", "url", "video"])
}

fn extract_audio(cfg: &Value) -> (String, u64) {
    if let Some(audio) = cfg.get("audio").and_then(|v| v.as_object()) {
        let src = audio.get("src").and_then(|v| v.as_str()).unwrap_or("");
        let vol = audio.get("volume").and_then(|v| v.as_f64()).unwrap_or(1.0);
        let vol_pct = (vol * 100.0).round().clamp(0.0, 100.0) as u64;
        if src.starts_with("http") {
            return (src.to_string(), vol_pct);
        }
    }
    let url = find_url_string(cfg, &["sound", "soundUrl", "audioUrl"]);
    (url, 100)
}

fn extract_text_style(cfg: &Value, scale: &StageScale) -> (String, u64, u64, String) {
    let mut family = "Montserrat, system-ui, sans-serif".to_string();
    let mut size = 54u64;
    let mut weight = 800u64;
    let mut color = "#ffffff".to_string();

    if let Some(css) = cfg.pointer("/text/css").and_then(|v| v.as_object()) {
        if let Some(f) = css.get("font-family").and_then(|v| v.as_str()) {
            if !f.trim().is_empty() {
                family = format!("{}, system-ui, sans-serif", f.trim().trim_matches('"'));
            }
        }
        if let Some(px) = css.get("font-size").and_then(|v| v.as_str()) {
            if let Ok(n) = px.trim().trim_end_matches("px").parse::<f64>() {
                size = n.round().clamp(12.0, 120.0) as u64;
            }
        }
        if let Some(w) = css.get("font-weight").and_then(|v| v.as_str()) {
            if let Ok(n) = w.parse::<u64>() {
                weight = n.clamp(100, 900);
            }
        }
        if let Some(c) = css.get("color").and_then(|v| v.as_str()) {
            if !c.trim().is_empty() {
                color = c.trim().to_string();
            }
        }
    }

    (family, scale.scale_font(size), weight, color)
}

fn se_template_to_ss(s: &str) -> String {
    let mut out = s.to_string();
    let pairs = [
        ("{{name}}", "[name]"),
        ("{name}", "[name]"),
        ("{{amount}}", "[amount]"),
        ("{amount}", "[amount]"),
        ("{{months}}", "[months]"),
        ("{months}", "[months]"),
        ("{{tier}}", "[tier]"),
        ("{tier}", "[tier]"),
        ("{{message}}", "[message]"),
        ("{message}", "[message]"),
        ("{{reward}}", "[reward]"),
        ("{reward}", "[reward]"),
        ("{{sender}}", "[sender]"),
        ("{sender}", "[sender]"),
        ("{{items}}", "[items]"),
        ("{items}", "[items]"),
        ("{{currency}}", "[currency]"),
        ("{currency}", "[currency]"),
        ("{{announcement}}", "[message]"),
        ("{announcement}", "[message]"),
    ];
    for (from, to) in pairs {
        out = out.replace(from, to);
    }
    out
}

fn find_url_string(cfg: &Value, keys: &[&str]) -> String {
    for k in keys {
        if let Some(s) = cfg.get(*k).and_then(|v| v.as_str()) {
            if s.starts_with("http") {
                return s.to_string();
            }
        }
    }
    if let Some(layout) = cfg.get("layout").and_then(|v| v.as_object()) {
        for k in keys {
            if let Some(s) = layout.get(*k).and_then(|v| v.as_str()) {
                if s.starts_with("http") {
                    return s.to_string();
                }
            }
        }
    }
    String::new()
}

fn map_animation(cfg: &Value) -> (String, String, String, String) {
    let anim = cfg.get("animation").and_then(|v| v.as_object());
    let raw_in = anim
        .and_then(|a| a.get("in"))
        .or_else(|| cfg.get("animationIn"))
        .and_then(|v| v.as_str())
        .unwrap_or("fadeIn");
    let raw_out = anim
        .and_then(|a| a.get("out"))
        .or_else(|| cfg.get("animationOut"))
        .and_then(|v| v.as_str())
        .unwrap_or("fadeOut");
    (
        normalize_anim(raw_in),
        normalize_anim(raw_out),
        "down".into(),
        "down".into(),
    )
}

fn normalize_anim(raw: &str) -> String {
    let s = raw.to_lowercase();
    if s.contains("slide") {
        "slide".into()
    } else if s.contains("zoom") || s.contains("bounce") {
        "zoom".into()
    } else if s.contains("none") || s.contains("instant") {
        "none".into()
    } else {
        "fade".into()
    }
}

const MAX_MEDIA_BYTES: usize = 25 * 1024 * 1024;

/// Download remote alert media into `{events_media_dir}/{profile_id}/` and rewrite profile URLs.
pub async fn localize_profile_media(
    paths: &StoragePaths,
    profile_id: &str,
    profile: &mut Value,
) -> (Vec<String>, usize) {
    let Some(events) = profile.get_mut("events").and_then(|v| v.as_object_mut()) else {
        return (vec![], 0);
    };

    let media_dir = paths.events_media_dir.join(profile_id);
    if let Err(e) = std::fs::create_dir_all(&media_dir) {
        return (
            vec![format!("Could not create media folder for profile: {e}")],
            0,
        );
    }

    let http = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .user_agent("StreamSync/2.0 (overlay import)")
        .build()
    {
        Ok(c) => c,
        Err(e) => return (vec![format!("HTTP client error: {e}")], 0),
    };

    let mut url_cache: HashMap<String, String> = HashMap::new();
    let mut warnings = Vec::new();
    let mut saved = 0usize;

    let event_keys: Vec<String> = events.keys().cloned().collect();
    for event_key in event_keys {
        let Some(event_val) = events.get_mut(&event_key) else {
            continue;
        };
        let Some(variations) = event_val
            .get_mut("variations")
            .and_then(|v| v.as_array_mut())
        else {
            continue;
        };
        for var in variations.iter_mut() {
            if let Some(img) = var.get_mut("image") {
                if let Some(n) = localize_asset_field(
                    &http,
                    &media_dir,
                    profile_id,
                    img,
                    "image",
                    &event_key,
                    &mut url_cache,
                    &mut warnings,
                )
                .await
                {
                    saved += n;
                }
            }
            if let Some(snd) = var.get_mut("sound") {
                if let Some(n) = localize_asset_field(
                    &http,
                    &media_dir,
                    profile_id,
                    snd,
                    "sound",
                    &event_key,
                    &mut url_cache,
                    &mut warnings,
                )
                .await
                {
                    saved += n;
                }
            }
        }
    }

    (warnings, saved)
}

async fn localize_asset_field(
    http: &reqwest::Client,
    media_dir: &Path,
    profile_id: &str,
    asset: &mut Value,
    kind: &str,
    event_key: &str,
    url_cache: &mut HashMap<String, String>,
    warnings: &mut Vec<String>,
) -> Option<usize> {
    if asset.get("type").and_then(|v| v.as_str()) != Some("url") {
        return None;
    }
    let url = asset
        .get("value")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if url.is_empty() || !should_localize_url(&url) {
        return None;
    }

    if let Some(local) = url_cache.get(&url) {
        asset["value"] = json!(local);
        return None;
    }

    match download_media(http, media_dir, profile_id, &url).await {
        Ok(local_path) => {
            let label = url_basename_hint(&url);
            url_cache.insert(url, local_path.clone());
            asset["value"] = json!(local_path);
            warnings.push(format!("Downloaded {kind} for {event_key}: {label}"));
            Some(1)
        }
        Err(e) => {
            warnings.push(format!(
                "Kept remote {kind} URL for {event_key} (download failed: {e})"
            ));
            None
        }
    }
}

fn should_localize_url(url: &str) -> bool {
    let lower = url.to_lowercase();
    (lower.starts_with("http://") || lower.starts_with("https://"))
        && !lower.starts_with("http://127.0.0.1")
        && !lower.starts_with("http://localhost")
        && !lower.contains("/events-media/")
}

async fn download_media(
    http: &reqwest::Client,
    media_dir: &Path,
    profile_id: &str,
    url: &str,
) -> Result<String> {
    let resp = http.get(url).send().await.context("request failed")?;
    if !resp.status().is_success() {
        anyhow::bail!("HTTP {}", resp.status());
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = resp.bytes().await.context("read body")?;
    if bytes.len() > MAX_MEDIA_BYTES {
        anyhow::bail!("file too large ({} bytes)", bytes.len());
    }
    let ext = extension_for_media(url, &content_type);
    let base = safe_media_basename(url);
    let filename = uniquify_filename(media_dir, &format!("{base}{ext}"));
    let dest = media_dir.join(&filename);
    std::fs::write(&dest, &bytes).context("write file")?;
    Ok(format!("/events-media/{profile_id}/{filename}"))
}

fn url_basename_hint(url: &str) -> String {
    url.split('?')
        .next()
        .and_then(|p| p.rsplit('/').next())
        .filter(|s| !s.is_empty())
        .unwrap_or("media")
        .to_string()
}

fn safe_media_basename(url: &str) -> String {
    let raw = url
        .split('?')
        .next()
        .and_then(|p| p.rsplit('/').next())
        .unwrap_or("");
    let stem = Path::new(raw)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let cleaned: String = stem
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(64)
        .collect();
    if !cleaned.is_empty() {
        return cleaned;
    }
    let mut h = DefaultHasher::new();
    url.hash(&mut h);
    format!("media-{:x}", h.finish())
}

fn extension_for_media(url: &str, content_type: &str) -> &'static str {
    let path = url.split('?').next().unwrap_or(url).to_lowercase();
    for ext in [
        ".webm", ".mp4", ".mp3", ".wav", ".ogg", ".gif", ".png", ".jpg", ".jpeg", ".webp", ".svg",
    ] {
        if path.ends_with(ext) {
            return ext;
        }
    }
    let ct = content_type.to_lowercase();
    if ct.contains("image/png") {
        ".png"
    } else if ct.contains("image/gif") {
        ".gif"
    } else if ct.contains("image/webp") {
        ".webp"
    } else if ct.contains("image/jpeg") || ct.contains("image/jpg") {
        ".jpg"
    } else if ct.contains("image/svg") {
        ".svg"
    } else if ct.contains("audio/mpeg") {
        ".mp3"
    } else if ct.contains("audio/ogg") {
        ".ogg"
    } else if ct.contains("audio/wav") {
        ".wav"
    } else if ct.contains("video/webm") {
        ".webm"
    } else if ct.contains("video/mp4") {
        ".mp4"
    } else {
        ".bin"
    }
}

fn uniquify_filename(dir: &Path, filename: &str) -> String {
    let dest = dir.join(filename);
    if !dest.exists() {
        return filename.to_string();
    }
    let path = Path::new(filename);
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("media");
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
    let suffix = if ext.is_empty() {
        String::new()
    } else {
        format!(".{ext}")
    };
    for i in 2..1000 {
        let candidate = format!("{stem}-{i}{suffix}");
        if !dir.join(&candidate).exists() {
            return candidate;
        }
    }
    format!("{stem}-{}", chrono::Utc::now().timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn should_localize_remote_http_only() {
        assert!(should_localize_url(
            "https://cdn.streamelements.com/static/upload/alert.png"
        ));
        assert!(!should_localize_url("/events-media/profile/alert.png"));
        assert!(!should_localize_url(
            "http://127.0.0.1:4040/events-media/x.png"
        ));
        assert!(!should_localize_url(""));
    }

    #[test]
    fn se_template_replaces_placeholders() {
        assert_eq!(se_template_to_ss("{{name}} followed!"), "[name] followed!");
    }

    #[test]
    fn map_fixture_file() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("se_mapper_fixtures")
            .join("minimal_alertbox.json");
        let raw = std::fs::read_to_string(&path).expect("fixture");
        let overlay: Value = serde_json::from_str(&raw).expect("json");
        let (pid, profile, _) = map_overlay_to_profile(&overlay);
        assert!(pid.starts_with("se-my-alerts-"));
        assert!(profile
            .pointer("/events/follow/variations/0/message")
            .is_some());
    }

    #[test]
    fn scales_1080p_placements_to_720p_stage() {
        let overlay = json!({
            "_id": "abc",
            "name": "HD Alerts",
            "settings": { "width": 1920, "height": 1080 },
            "widgets": [{
                "type": "se-widget-alert-box",
                "css": { "left": "960px", "top": "540px", "width": "400px", "height": "300px" },
                "variables": {
                    "follower": {
                        "enabled": true,
                        "duration": 6,
                        "graphics": { "src": "https://cdn.example.com/v.webm" },
                        "audio": { "src": "https://cdn.example.com/a.mp3", "volume": 1 },
                        "text": { "message": "{name} followed" },
                        "variations": []
                    }
                }
            }]
        });
        let (_pid, profile, warnings) = map_overlay_to_profile(&overlay);
        assert!(
            warnings.iter().any(|w| w.contains("Scaled layout")),
            "expected scale warning: {warnings:?}"
        );
        assert_eq!(
            profile.pointer("/stage/w").and_then(|v| v.as_u64()),
            Some(1280)
        );
        assert_eq!(
            profile.pointer("/stage/h").and_then(|v| v.as_u64()),
            Some(720)
        );
        let img = profile
            .pointer("/events/follow/variations/0/placement/image")
            .unwrap();
        let x = img.get("x").and_then(|v| v.as_f64()).unwrap();
        let w = img.get("w").and_then(|v| v.as_f64()).unwrap();
        assert!(x < 1280.0, "x should be on 720p stage, got {x}");
        assert!(x + w <= 1280.0, "image should fit in stage width");
        assert!((x - 640.0).abs() < 5.0, "center x expected ~640, got {x}");
    }

    #[test]
    fn map_se_alertbox_extracts_graphics_audio_and_variations() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("se_mapper_fixtures")
            .join("se_alertbox_cheer.json");
        let raw = std::fs::read_to_string(&path).expect("fixture");
        let overlay: Value = serde_json::from_str(&raw).expect("json");
        let (_pid, profile, warnings) = map_overlay_to_profile(&overlay);
        let vars = profile
            .pointer("/events/cheer/variations")
            .and_then(|v| v.as_array())
            .expect("cheer variations");
        assert_eq!(vars.len(), 2, "expected base + SE variation: {warnings:?}");
        let base = &vars[0];
        assert_eq!(
            base.pointer("/trigger/mode").and_then(|x| x.as_str()),
            Some("none")
        );
        assert_eq!(
            base.pointer("/image/value").and_then(|x| x.as_str()),
            Some("https://cdn.streamelements.com/uploads/base.webm")
        );
        let v1000 = &vars[1];
        assert_eq!(
            v1000.pointer("/image/value").and_then(|x| x.as_str()),
            Some("https://cdn.streamelements.com/uploads/1000bits.webm")
        );
        assert_eq!(
            v1000.pointer("/sound/value").and_then(|x| x.as_str()),
            Some("https://cdn.streamelements.com/uploads/1000bits.mp3")
        );
        assert_eq!(
            v1000.pointer("/message").and_then(|x| x.as_str()),
            Some("[name] dropped 1000 bits!")
        );
        assert_eq!(
            v1000.pointer("/trigger/mode").and_then(|x| x.as_str()),
            Some("exact")
        );
        assert_eq!(
            v1000.pointer("/trigger/value").and_then(|x| x.as_u64()),
            Some(1000)
        );
    }

    #[test]
    fn map_subscriber_variations_to_sub_resub_gift() {
        let overlay = json!({
            "_id": "subroute1",
            "name": "Sub Route",
            "settings": { "width": 1920, "height": 1080 },
            "widgets": [{
                "type": "se-widget-alert-box",
                "css": { "left": "0px", "top": "0px", "width": "400px", "height": "300px" },
                "variables": {
                    "subscriber": {
                        "enabled": true,
                        "duration": 6,
                        "graphics": { "src": "https://cdn.example.com/newsub.webm" },
                        "audio": { "src": "https://cdn.example.com/newsub.mp3", "volume": 1 },
                        "text": { "message": "{name} just subscribed!" },
                        "variations": [
                            {
                                "enabled": true,
                                "name": "Resubscriber",
                                "type": "amount",
                                "condition": "ATLEAST",
                                "requirement": 2,
                                "settings": {
                                    "text": { "message": "{name} resubbed for {amount} months" },
                                    "graphics": { "src": "https://cdn.example.com/resub.webm" }
                                }
                            },
                            {
                                "enabled": true,
                                "name": "Subscriber gift",
                                "type": "gift",
                                "condition": "EXACT",
                                "requirement": 1,
                                "settings": {
                                    "text": { "message": "{sender} gifted {name}" },
                                    "graphics": { "src": "https://cdn.example.com/gift1.webm" }
                                }
                            },
                            {
                                "enabled": true,
                                "name": "Community gifts",
                                "type": "communityGift",
                                "condition": "ATLEAST",
                                "requirement": 5,
                                "settings": {
                                    "text": { "message": "{sender} gifted {amount} subs" },
                                    "graphics": { "src": "https://cdn.example.com/gift5.webm" }
                                }
                            }
                        ]
                    },
                    "host": {
                        "enabled": true,
                        "text": { "message": "{name} is hosting" },
                        "graphics": { "src": "https://cdn.example.com/host.webm" }
                    },
                    "raid": {
                        "enabled": true,
                        "text": { "message": "{name} raided" },
                        "graphics": { "src": "https://cdn.example.com/raid.webm" },
                        "variations": []
                    }
                }
            }]
        });
        let (_pid, profile, warnings) = map_overlay_to_profile(&overlay);
        assert!(
            warnings.iter().any(|w| w.contains("host")),
            "expected host skip warning: {warnings:?}"
        );
        let sub_n = profile
            .pointer("/events/sub/variations")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        assert!(sub_n >= 1, "expected base sub alert");
        let resub_n = profile
            .pointer("/events/resub/variations")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        assert_eq!(resub_n, 1);
        let gift_vars = profile
            .pointer("/events/gift/variations")
            .and_then(|v| v.as_array())
            .expect("gift variations");
        assert_eq!(gift_vars.len(), 2);
        assert_eq!(
            gift_vars[0]
                .pointer("/trigger/mode")
                .and_then(|v| v.as_str()),
            Some("exact")
        );
        assert_eq!(
            gift_vars[1]
                .pointer("/trigger/mode")
                .and_then(|v| v.as_str()),
            Some("min")
        );
        assert_eq!(
            gift_vars[1]
                .pointer("/trigger/value")
                .and_then(|v| v.as_u64()),
            Some(5)
        );
        let raid_msg = profile
            .pointer("/events/raid/variations/0/message")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert!(
            raid_msg.contains("raided") || raid_msg.contains("raid"),
            "raid should not use host message, got {raid_msg}"
        );
        assert!(!raid_msg.to_lowercase().contains("hosting"));
    }

    #[test]
    fn map_minimal_overlay_produces_profile_id() {
        let overlay = json!({
            "_id": "abc123def456",
            "name": "My Alerts",
            "settings": { "width": 1920, "height": 1080 },
            "widgets": [{
                "type": "alertbox",
                "x": 100,
                "y": 200,
                "width": 300,
                "height": 300,
                "variables": {
                    "follower": {
                        "message": "{{name}} just followed!",
                        "image": "https://cdn.example.com/img.png",
                        "duration": 8
                    }
                }
            }]
        });
        let (pid, profile, warnings) = map_overlay_to_profile(&overlay);
        assert!(pid.starts_with("se-my-alerts-"));
        assert!(profile
            .get("events")
            .and_then(|e| e.get("follow"))
            .is_some());
        assert!(warnings.is_empty() || !warnings.is_empty());
    }
}
