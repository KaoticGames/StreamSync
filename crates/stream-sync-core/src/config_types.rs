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
