//! JSON config shapes matching the Node overlay-server.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockProfile {
    #[serde(default = "default_font_size_dock")]
    pub font_size: u32,
    #[serde(default = "default_true")]
    pub show_timestamps: bool,
    #[serde(default = "default_true")]
    pub show_badges: bool,
}

fn default_font_size_dock() -> u32 {
    13
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EventsDockEvents {
    #[serde(default = "default_true")]
    pub follow: bool,
    #[serde(default = "default_true")]
    pub sub: bool,
    #[serde(default = "default_true")]
    pub resub: bool,
    #[serde(default = "default_true")]
    pub gift: bool,
    #[serde(default = "default_true")]
    pub bits: bool,
    #[serde(default = "default_true")]
    pub raid: bool,
    #[serde(default = "default_true")]
    pub redeem: bool,
    #[serde(default = "default_true")]
    pub kicks: bool,
    #[serde(default = "default_true")]
    pub hypetrain: bool,
    #[serde(default = "default_true")]
    pub announce: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventsDockConfig {
    #[serde(default = "default_font_size_dock")]
    pub font_size: u32,
    #[serde(default = "default_true")]
    pub show_timestamps: bool,
    #[serde(default = "default_true")]
    pub show_badges: bool,
    #[serde(default)]
    pub events: EventsDockEvents,
}

impl Default for EventsDockConfig {
    fn default() -> Self {
        Self {
            font_size: 13,
            show_timestamps: true,
            show_badges: true,
            events: EventsDockEvents {
                follow: true,
                sub: true,
                resub: true,
                gift: true,
                bits: true,
                raid: true,
                redeem: true,
                kicks: true,
                hypetrain: true,
                announce: true,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockConfigFile {
    #[serde(default)]
    pub profiles: HashMap<String, DockProfile>,
    #[serde(rename = "eventsDock", default)]
    pub events_dock: Option<EventsDockConfig>,
}

impl Default for DockConfigFile {
    fn default() -> Self {
        let mut profiles = HashMap::new();
        profiles.insert(
            "chat-default".into(),
            DockProfile {
                font_size: 13,
                show_timestamps: true,
                show_badges: true,
            },
        );
        Self {
            profiles,
            events_dock: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ChatOverlayProfile {
    #[serde(default = "default_true")]
    pub show_timestamps: bool,
    #[serde(default = "default_true")]
    pub show_badges: bool,
    #[serde(default = "default_font_size_overlay")]
    pub font_size: u32,
    #[serde(default = "default_font_family")]
    pub font_family: String,
    #[serde(default, alias = "fontUrl")]
    pub local_font_url: Option<String>,
    #[serde(default)]
    pub text_rotate: f64,
    #[serde(default)]
    pub text_skew: f64,
    #[serde(default = "default_feed")]
    pub feed_direction: String,
    #[serde(default = "default_bubble")]
    pub message_style: String,
    #[serde(default = "default_bubble_radius")]
    pub bubble_radius: f64,
    #[serde(default = "default_bubble_mode")]
    pub bubble_color_mode: String,
    #[serde(default = "default_bubble_color")]
    pub bubble_color: String,
    #[serde(default)]
    pub stroke_enabled: bool,
    #[serde(default)]
    pub stroke_color: String,
    #[serde(default)]
    pub stroke_width: f64,
    #[serde(default = "default_one")]
    pub bubble_alpha: f64,
    #[serde(default = "default_bg_mode")]
    pub bg_mode: String,
    #[serde(default)]
    pub bg_color: String,
    #[serde(default)]
    pub bg_gradient: String,
    #[serde(
        default = "default_display",
        alias = "display_mode",
        deserialize_with = "deserialize_display_mode"
    )]
    pub display_mode: String,
    #[serde(
        default = "default_popup_duration",
        alias = "popup_duration",
        deserialize_with = "deserialize_popup_duration"
    )]
    pub popup_duration: u32,
    #[serde(default = "default_popup_exit")]
    pub popup_exit_style: String,
    #[serde(rename = "profileName", default)]
    pub profile_name: Option<String>,
    #[serde(rename = "displayName", default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

fn default_font_size_overlay() -> u32 {
    18
}
fn default_font_family() -> String {
    "system-ui".into()
}
fn default_feed() -> String {
    "up-down".into()
}
fn default_bubble() -> String {
    "bubble".into()
}
fn default_bubble_radius() -> f64 {
    18.0
}
fn default_bubble_mode() -> String {
    "fixed".into()
}
fn default_bubble_color() -> String {
    "rgba(15, 23, 42, 0.85)".into()
}
fn default_one() -> f64 {
    1.0
}
fn default_bg_mode() -> String {
    "transparent".into()
}
fn default_display() -> String {
    "solid".into()
}

/// `solid` unless the value is exactly `popup` (case-insensitive).
pub fn normalize_display_mode(raw: &str) -> String {
    if raw.trim().eq_ignore_ascii_case("popup") {
        "popup".into()
    } else {
        "solid".into()
    }
}

pub fn normalize_popup_duration(raw: u32) -> u32 {
    if raw == 0 {
        8
    } else {
        raw
    }
}

fn default_popup_duration() -> u32 {
    8
}
fn default_popup_exit() -> String {
    "fade".into()
}

fn deserialize_display_mode<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    Ok(normalize_display_mode(&raw))
}

fn deserialize_popup_duration<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = u32::deserialize(deserializer)?;
    Ok(normalize_popup_duration(raw))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverlayConfigFile {
    #[serde(default)]
    pub version: Option<u32>,
    #[serde(default)]
    pub profiles: HashMap<String, ChatOverlayProfile>,
}

impl Default for OverlayConfigFile {
    fn default() -> Self {
        let mut profiles = HashMap::new();
        profiles.insert("chat-default".into(), ChatOverlayProfile::default());
        Self {
            version: Some(1),
            profiles,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventsOverlayConfigFile {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub profiles: HashMap<String, Value>,
}

/// Full events overlay profile JSON (`stage`, `events`, etc.) — stored opaquely like Node.
pub fn default_events_overlay_profile() -> Value {
    let base_variation = |message: &str| {
        json!({
            "id": format!("{:x}", chrono::Utc::now().timestamp_millis()),
            "name": "Base Alert",
            "image": { "type": "url", "value": "" },
            "sound": { "type": "url", "value": "", "volume": 100 },
            "animation": {
                "in": "fade",
                "out": "fade",
                "animSpeed": 50,
                "slideInDir": "down",
                "slideOutDir": "down"
            },
            "layout": "textSide",
            "message": message,
            "durationSec": 6,
            "text": {
                "fontFamily": "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
                "fontSize": 54,
                "fontWeight": 800,
                "color": "#ffffff",
                "strokeColor": "rgba(0,0,0,0.75)",
                "strokeWidth": 0,
                "fontMeta": { "source": "google", "googleFamily": "Montserrat" }
            },
            "placement": {
                "image": { "x": 120, "y": 140, "w": 320, "h": 320 },
                "text": { "x": 500, "y": 170, "w": 520, "h": 180 }
            }
        })
    };
    let event = |msg: &str| json!({ "variations": [base_variation(msg)] });
    json!({
        "version": 1,
        "stage": { "w": 1280, "h": 720, "grid": true, "zoom": 1 },
        "events": {
            "follow": event("[name] followed!"),
            "sub": event("[name] subscribed!"),
            "resub": event("[name] resubscribed!"),
            "gift": event("[name] gifted a sub!"),
            "cheer": event("[name] cheered [amount]!"),
            "raid": event("[name] raided with [amount] viewers!"),
            "redeem": event("[name] redeemed [reward]!")
        }
    })
}

/// Rust previously saved only `{ "variations": {} }` — treat that as missing config.
pub fn events_profile_has_alerts(profile: &Value) -> bool {
    profile
        .get("events")
        .and_then(|e| e.as_object())
        .map(|o| !o.is_empty())
        .unwrap_or(false)
}

pub fn resolve_events_overlay_profile(stored: Option<Value>) -> Value {
    match stored {
        Some(p) if events_profile_has_alerts(&p) => p,
        _ => default_events_overlay_profile(),
    }
}

/// Known events overlay event keys (studio / runtime picker).
pub const EVENTS_OVERLAY_EVENT_KEYS: &[&str] =
    &["follow", "sub", "resub", "gift", "cheer", "raid", "redeem"];

/// Validate a profile before persisting a POST `/api/events/overlay-config` write.
///
/// Fail-closed on malformed trigger / chance / duration / placement fields so the
/// variation picker never sees garbage. Unknown presentation fields are left alone.
pub fn validate_events_overlay_profile_write(config: &Value) -> Result<(), String> {
    let Some(obj) = config.as_object() else {
        return Err("config must be a JSON object".into());
    };
    if let Some(events) = obj.get("events") {
        validate_events_map(events)?;
    }
    Ok(())
}

fn validate_events_map(events: &Value) -> Result<(), String> {
    let Some(map) = events.as_object() else {
        return Err("config.events must be an object".into());
    };
    for (key, event_cfg) in map {
        if !EVENTS_OVERLAY_EVENT_KEYS.contains(&key.as_str()) {
            // Existing studio / SE profiles may carry extra keys. Persist them;
            // do not fail the whole write.
            continue;
        }
        validate_event_config(key, event_cfg)?;
    }
    Ok(())
}

fn validate_event_config(event_key: &str, event_cfg: &Value) -> Result<(), String> {
    let Some(obj) = event_cfg.as_object() else {
        return Err(format!("events.{event_key} must be an object"));
    };
    let Some(variations) = obj.get("variations") else {
        return Err(format!("events.{event_key}.variations is required"));
    };
    let Some(arr) = variations.as_array() else {
        return Err(format!("events.{event_key}.variations must be an array"));
    };
    for (i, var) in arr.iter().enumerate() {
        validate_variation(event_key, i, var)?;
    }
    Ok(())
}

fn validate_variation(event_key: &str, index: usize, var: &Value) -> Result<(), String> {
    let ctx = format!("events.{event_key}.variations[{index}]");
    let Some(obj) = var.as_object() else {
        return Err(format!("{ctx} must be an object"));
    };
    if let Some(trigger) = obj.get("trigger") {
        validate_trigger(&ctx, trigger)?;
    }
    if let Some(chance) = obj.get("chancePct").or_else(|| obj.get("chance")) {
        validate_chance(&ctx, chance)?;
    }
    if let Some(duration) = obj.get("durationSec") {
        validate_duration_sec(&ctx, duration)?;
    }
    if let Some(placement) = obj.get("placement") {
        validate_placement(&ctx, placement)?;
    }
    Ok(())
}

fn validate_trigger(ctx: &str, trigger: &Value) -> Result<(), String> {
    let Some(obj) = trigger.as_object() else {
        return Err(format!("{ctx}.trigger must be an object"));
    };
    let mode = match obj.get("mode") {
        None | Some(Value::Null) => "none",
        Some(Value::String(s)) => s.as_str(),
        Some(_) => return Err(format!("{ctx}.trigger.mode must be a string")),
    };
    match mode {
        "none" | "exact" | "min" => {}
        other => {
            return Err(format!(
                "{ctx}.trigger.mode must be none|exact|min (got {other})"
            ));
        }
    }
    if mode == "exact" || mode == "min" {
        let Some(value) = obj.get("value") else {
            return Err(format!(
                "{ctx}.trigger.value is required when mode is {mode}"
            ));
        };
        if coerce_trigger_threshold(value).is_none() {
            return Err(format!(
                "{ctx}.trigger.value must be a finite number or numeric string when mode is {mode}"
            ));
        }
    }
    if let Some(tier) = obj.get("tier") {
        if !tier.is_null() && normalize_trigger_tier(tier).is_none() {
            return Err(format!(
                "{ctx}.trigger.tier must be 1|2|3 or Twitch 1000|2000|3000"
            ));
        }
    }
    Ok(())
}

/// Coerce finite numbers and numeric strings; reject objects/arrays/empty/non-finite.
fn coerce_trigger_threshold(val: &Value) -> Option<f64> {
    match val {
        Value::Number(n) => n.as_f64().filter(|f| f.is_finite()),
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return None;
            }
            let n: f64 = trimmed.parse().ok()?;
            n.is_finite().then_some(n)
        }
        _ => None,
    }
}

fn normalize_trigger_tier(val: &Value) -> Option<u8> {
    let n = match val {
        Value::Number(num) => num.as_f64()?,
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return None;
            }
            trimmed.parse::<f64>().ok()?
        }
        _ => return None,
    };
    if !n.is_finite() || n.fract() != 0.0 {
        return None;
    }
    match n as i64 {
        1 | 1000 => Some(1),
        2 | 2000 => Some(2),
        3 | 3000 => Some(3),
        _ => None,
    }
}

fn validate_chance(ctx: &str, chance: &Value) -> Result<(), String> {
    if chance.is_null() {
        return Ok(());
    }
    let Some(n) = chance.as_f64().filter(|f| f.is_finite()) else {
        return Err(format!("{ctx}.chance/chancePct must be a finite number"));
    };
    if !(0.0..=100.0).contains(&n) {
        return Err(format!(
            "{ctx}.chance/chancePct must be between 0 and 100 (got {n})"
        ));
    }
    Ok(())
}

fn validate_duration_sec(ctx: &str, duration: &Value) -> Result<(), String> {
    if duration.is_null() {
        return Ok(());
    }
    let Some(n) = duration.as_f64().filter(|f| f.is_finite()) else {
        return Err(format!("{ctx}.durationSec must be a finite number"));
    };
    if n < 0.0 {
        return Err(format!("{ctx}.durationSec must be non-negative (got {n})"));
    }
    Ok(())
}

fn validate_placement(ctx: &str, placement: &Value) -> Result<(), String> {
    let Some(obj) = placement.as_object() else {
        return Err(format!("{ctx}.placement must be an object"));
    };
    for key in ["image", "text"] {
        if let Some(rect) = obj.get(key) {
            validate_placement_rect(&format!("{ctx}.placement.{key}"), rect)?;
        }
    }
    Ok(())
}

fn validate_placement_rect(ctx: &str, rect: &Value) -> Result<(), String> {
    let Some(obj) = rect.as_object() else {
        return Err(format!("{ctx} must be an object"));
    };
    for key in ["x", "y", "w", "h"] {
        match obj.get(key) {
            None => return Err(format!("{ctx}.{key} is required")),
            Some(v) => {
                if v.as_f64().filter(|f| f.is_finite()).is_none() {
                    return Err(format!("{ctx}.{key} must be a finite number"));
                }
            }
        }
    }
    Ok(())
}

impl Default for EventsOverlayConfigFile {
    fn default() -> Self {
        let mut profiles = HashMap::new();
        profiles.insert("default".into(), default_events_overlay_profile());
        Self {
            version: 1,
            profiles,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TwitchTokenFile {
    #[serde(rename = "accessToken", default)]
    pub access_token: Option<String>,
    #[serde(rename = "refreshToken", default)]
    pub refresh_token: Option<String>,
    #[serde(rename = "expiresIn", default)]
    pub expires_in: Option<i64>,
    #[serde(rename = "obtainmentTimestamp", default)]
    pub obtainment_timestamp: Option<i64>,
    #[serde(default)]
    pub login: Option<String>,
    #[serde(rename = "userId", default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
}

/// Persisted Syndicate connection-key (takeover) session. Not mixed into personal tokens.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DelegatedSessionFile {
    /// Monotonic session generation — stale workers must not mutate a newer session.
    #[serde(default)]
    pub generation: u64,
    pub connection_key: String,
    pub client_id: String,
    pub access_token: String,
    pub channel_login: String,
    pub channel_twitch_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    /// ISO-8601 Twitch access token expiry (from Syndicate exchange).
    pub twitch_expires_at: String,
    /// ISO-8601 connection key expiry.
    #[serde(default)]
    pub connection_expires_at: Option<String>,
    #[serde(default)]
    pub kick_id: Option<String>,
    #[serde(default)]
    pub kick_login: Option<String>,
    #[serde(default)]
    pub kick_access_token: Option<String>,
    #[serde(default)]
    pub kick_refresh_token: Option<String>,
    #[serde(default)]
    pub kick_expires_at: Option<String>,
    #[serde(default)]
    pub kick_scopes: Vec<String>,
}

/// Personal Kick OAuth / live Kick session for docks and chat send.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KickTokenFile {
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub kick_id: Option<String>,
    #[serde(default)]
    pub login: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
    #[serde(default)]
    pub feed_ticket: Option<String>,
}

impl KickTokenFile {
    pub fn is_linked(&self) -> bool {
        self.access_token.as_ref().is_some_and(|s| !s.is_empty())
            && self.kick_id.as_ref().is_some_and(|s| !s.is_empty())
    }
}

/// Which saved Twitch identity is driving IRC / EventSub right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TwitchActiveMode {
    #[default]
    Local,
    Delegated,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TwitchActiveModeFile {
    #[serde(default)]
    pub mode: TwitchActiveMode,
}

#[cfg(test)]
mod events_overlay_write_validation_tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn cheer_variation(trigger: Value, extras: Value) -> Value {
        let mut v = json!({
            "id": "v1",
            "name": "Test",
            "trigger": trigger,
            "durationSec": 6,
            "message": "[name] cheered!",
            "placement": {
                "image": { "x": 1, "y": 2, "w": 3, "h": 4 },
                "text": { "x": 5, "y": 6, "w": 7, "h": 8 }
            }
        });
        if let (Some(obj), Some(extra)) = (v.as_object_mut(), extras.as_object()) {
            for (k, val) in extra {
                obj.insert(k.clone(), val.clone());
            }
        }
        v
    }

    fn profile_with_cheer_variation(variation: Value) -> Value {
        json!({
            "version": 1,
            "stage": { "w": 1280, "h": 720 },
            "events": {
                "cheer": { "variations": [variation] }
            }
        })
    }

    /// Mirrors POST handler: validate then insert; on Err leave map unchanged.
    fn try_store_profile(
        store: &mut HashMap<String, Value>,
        profile_id: &str,
        config: Value,
    ) -> Result<(), String> {
        validate_events_overlay_profile_write(&config)?;
        store.insert(profile_id.to_string(), config);
        Ok(())
    }

    #[test]
    fn valid_default_shaped_profile_accepted() {
        let profile = default_events_overlay_profile();
        assert!(validate_events_overlay_profile_write(&profile).is_ok());
    }

    #[test]
    fn invalid_trigger_value_rejected_and_not_stored() {
        let mut store = HashMap::new();
        store.insert("keep".into(), json!({ "version": 1 }));

        let bad = profile_with_cheer_variation(cheer_variation(
            json!({ "mode": "exact", "value": "nope" }),
            json!({}),
        ));
        let err = try_store_profile(&mut store, "bad", bad).expect_err("must reject");
        assert!(err.contains("trigger.value"), "unexpected error: {err}");
        assert!(!store.contains_key("bad"));
        assert!(store.contains_key("keep"));
    }

    #[test]
    fn invalid_duration_rejected() {
        let bad = profile_with_cheer_variation(cheer_variation(
            json!({ "mode": "none", "value": null }),
            json!({ "durationSec": -1 }),
        ));
        let err = validate_events_overlay_profile_write(&bad).expect_err("must reject");
        assert!(err.contains("durationSec"), "unexpected error: {err}");
    }

    #[test]
    fn unknown_presentation_field_round_trips() {
        let mut profile = default_events_overlay_profile();
        profile
            .pointer_mut("/events/cheer/variations/0")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(
                "customShaderPreset".into(),
                json!({ "glow": true, "intensity": 0.42 }),
            );

        validate_events_overlay_profile_write(&profile).expect("valid");

        let mut store = HashMap::new();
        try_store_profile(&mut store, "p1", profile.clone()).expect("store");
        let stored = store.get("p1").expect("stored");
        assert_eq!(
            stored.pointer("/events/cheer/variations/0/customShaderPreset"),
            Some(&json!({ "glow": true, "intensity": 0.42 }))
        );
        // Presentation fields from the default shape remain intact.
        assert!(stored
            .pointer("/events/cheer/variations/0/animation")
            .is_some());
        assert!(stored
            .pointer("/events/cheer/variations/0/text/fontMeta")
            .is_some());
    }

    #[test]
    fn unknown_event_key_does_not_fail_write() {
        let ok = json!({
            "events": {
                "follow": { "variations": [] },
                "mystery": { "variations": [] }
            }
        });
        assert!(validate_events_overlay_profile_write(&ok).is_ok());
    }

    #[test]
    fn studio_default_variation_shape_is_accepted() {
        let profile = json!({
            "version": 1,
            "stage": { "w": 1280, "h": 720, "grid": true, "zoom": 1 },
            "events": {
                "follow": { "variations": [{
                    "id": "base",
                    "name": "Base Alert",
                    "trigger": { "mode": "none", "value": null, "tier": null },
                    "chancePct": null,
                    "durationSec": 6,
                    "placement": {
                        "image": { "x": 120, "y": 140, "w": 320, "h": 320 },
                        "text": { "x": 500, "y": 170, "w": 520, "h": 180 }
                    }
                }]}
            }
        });
        assert!(validate_events_overlay_profile_write(&profile).is_ok());
    }

    #[test]
    fn exact_mode_accepts_numeric_string_value() {
        let ok = profile_with_cheer_variation(cheer_variation(
            json!({ "mode": "exact", "value": "100" }),
            json!({}),
        ));
        assert!(validate_events_overlay_profile_write(&ok).is_ok());
    }

    #[test]
    fn twitch_tier_codes_accepted() {
        let ok = profile_with_cheer_variation(cheer_variation(
            json!({ "mode": "none", "tier": 2000 }),
            json!({}),
        ));
        assert!(validate_events_overlay_profile_write(&ok).is_ok());
    }
}
