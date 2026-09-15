//! Platform-aware Events Studio live test alert construction and validation.

use crate::broadcast::make_platform_dock_event;
use crate::kick;
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestPlatform {
    Twitch,
    Kick,
}

impl TestPlatform {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Twitch => "twitch",
            Self::Kick => "kick",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestAlertError {
    PlatformRequired,
    NoPlatformConnected,
    UnknownPlatform,
    PlatformNotConnected,
    UnsupportedEventForPlatform,
}

impl TestAlertError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::PlatformRequired => "platform_required",
            Self::NoPlatformConnected => "no_platform_connected",
            Self::UnknownPlatform => "unknown_platform",
            Self::PlatformNotConnected => "platform_not_connected",
            Self::UnsupportedEventForPlatform => "unsupported_event_for_platform",
        }
    }
}

pub fn resolve_platform(
    platform: Option<&str>,
    twitch_connected: bool,
    kick_connected: bool,
) -> Result<TestPlatform, TestAlertError> {
    if let Some(raw) = platform {
        let p = raw.trim().to_ascii_lowercase();
        let resolved = match p.as_str() {
            "twitch" => TestPlatform::Twitch,
            "kick" => TestPlatform::Kick,
            _ => return Err(TestAlertError::UnknownPlatform),
        };
        let connected = match resolved {
            TestPlatform::Twitch => twitch_connected,
            TestPlatform::Kick => kick_connected,
        };
        if !connected {
            return Err(TestAlertError::PlatformNotConnected);
        }
        return Ok(resolved);
    }

    match (twitch_connected, kick_connected) {
        (true, false) => Ok(TestPlatform::Twitch),
        (false, true) => Ok(TestPlatform::Kick),
        (true, true) => Err(TestAlertError::PlatformRequired),
        (false, false) => Err(TestAlertError::NoPlatformConnected),
    }
}

pub fn assert_event_supported(
    platform: TestPlatform,
    event_type: &str,
) -> Result<(), TestAlertError> {
    let et = event_type.trim().to_ascii_lowercase();
    let supported = match platform {
        TestPlatform::Twitch => matches!(
            et.as_str(),
            "follow" | "sub" | "resub" | "gift" | "cheer" | "raid" | "redeem"
        ),
        TestPlatform::Kick => matches!(et.as_str(), "follow" | "sub" | "gift" | "kicks" | "redeem"),
    };
    if supported {
        Ok(())
    } else {
        Err(TestAlertError::UnsupportedEventForPlatform)
    }
}

pub fn overlay_event_type(platform: TestPlatform, event_type: &str) -> String {
    let et = event_type.trim().to_ascii_lowercase();
    if platform == TestPlatform::Kick && et == "kicks" {
        return "cheer".into();
    }
    et
}

pub fn dock_only(_platform: TestPlatform, event_type: &str) -> bool {
    event_type.trim().eq_ignore_ascii_case("redeem")
}

pub fn build_test_alert_payload(
    platform: TestPlatform,
    event_type: &str,
    variables: &Value,
    variation_id: Option<&str>,
    sound_volume: Option<&Value>,
) -> Value {
    let overlay_et = overlay_event_type(platform, event_type);
    let alert_vars = alert_variables(platform, event_type, variables);
    let mut alert = json!({
        "type": "event-alert",
        "platform": platform.as_str(),
        "eventType": overlay_et,
        "data": { "variables": alert_vars },
    });
    if let Some(vid) = variation_id.filter(|s| !s.is_empty()) {
        alert["variationId"] = json!(vid);
    }
    if let Some(sv) = sound_volume {
        if !sv.is_null() {
            alert["soundVolume"] = sv.clone();
        }
    }
    alert
}

pub fn build_test_dock_event(platform: TestPlatform, event_type: &str, variables: &Value) -> Value {
    let et = event_type.trim().to_ascii_lowercase();
    let name = variables
        .get("name")
        .or_else(|| variables.get("user"))
        .and_then(|v| v.as_str())
        .unwrap_or("Someone");

    match platform {
        TestPlatform::Twitch => {
            let detail = twitch_dock_detail(&et, name, variables);
            let dock_type = if et == "cheer" || et == "bits" {
                "bits"
            } else {
                et.as_str()
            };
            make_platform_dock_event("twitch", dock_type, &detail, Some(event_type), None)
        }
        TestPlatform::Kick => {
            if et == "kicks" {
                let amount = variables
                    .get("amount")
                    .or_else(|| variables.get("bits"))
                    .cloned()
                    .unwrap_or(json!(0));
                let mapped = kick::map_kick_kicks(name, &amount);
                return make_platform_dock_event(
                    "kick",
                    mapped.dock_event_type,
                    &mapped.dock_detail,
                    Some(mapped.dock_label),
                    None,
                );
            }
            let (dock_type, detail, label) = kick_dock_parts(&et, name, variables);
            make_platform_dock_event("kick", dock_type, &detail, Some(label), None)
        }
    }
}

fn alert_variables(platform: TestPlatform, event_type: &str, variables: &Value) -> Value {
    let et = event_type.trim().to_ascii_lowercase();
    if platform != TestPlatform::Kick {
        return variables.clone();
    }

    let name = variables
        .get("name")
        .or_else(|| variables.get("user"))
        .and_then(|v| v.as_str())
        .unwrap_or("Someone");

    match et.as_str() {
        "follow" => json!({ "name": name }),
        "sub" => json!({
            "name": name,
            "amount": variables.get("amount").cloned().unwrap_or(json!("1000")),
        }),
        "gift" => json!({
            "name": name,
            "amount": variables.get("amount").cloned().unwrap_or(json!(1)),
        }),
        "kicks" => {
            let amount = variables
                .get("amount")
                .or_else(|| variables.get("bits"))
                .cloned()
                .unwrap_or(json!(0));
            kick::map_kick_kicks(name, &amount).variables
        }
        _ => variables.clone(),
    }
}

fn twitch_dock_detail(et: &str, name: &str, variables: &Value) -> String {
    match et {
        "follow" => format!("{name} followed"),
        "sub" => crate::twitch::format_sub_dock_detail(
            name,
            variables
                .get("tier")
                .or(variables.get("amount"))
                .unwrap_or(&Value::Null),
        ),
        "resub" => crate::twitch::format_resub_dock_detail(
            name,
            variables.get("months").unwrap_or(&Value::Null),
            variables
                .get("tier")
                .or(variables.get("amount"))
                .unwrap_or(&Value::Null),
            variables
                .get("input")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
        ),
        "gift" => crate::twitch::format_gift_dock_detail(
            name,
            variables.get("amount").unwrap_or(&Value::Null),
            variables.get("tier").unwrap_or(&Value::Null),
            variables
                .get("recipient")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
        ),
        "cheer" | "bits" => format!(
            "{name} cheered {}{}",
            variables
                .get("amount")
                .or(variables.get("bits"))
                .map(|v| v.to_string())
                .unwrap_or_default(),
            variables
                .get("input")
                .map(|i| format!(": {i}"))
                .unwrap_or_default()
        ),
        "raid" => format!(
            "{name} raided{}",
            variables
                .get("amount")
                .or(variables.get("raiders"))
                .map(|v| format!(" with {v}"))
                .unwrap_or_default()
        ),
        "redeem" => format!(
            "{} — {}{}",
            variables
                .get("reward")
                .map(|v| v.to_string())
                .unwrap_or_else(|| "Redeem".into()),
            name,
            variables
                .get("input")
                .map(|i| format!(": {i}"))
                .unwrap_or_default()
        ),
        _ => format!("{name} triggered {et}"),
    }
}

fn kick_dock_parts(
    et: &str,
    name: &str,
    variables: &Value,
) -> (&'static str, String, &'static str) {
    match et {
        "follow" => ("follow", format!("{name} followed"), "Follow"),
        "sub" => ("sub", format!("{name} subscribed"), "Sub"),
        "gift" => {
            let count = variables.get("amount").cloned().unwrap_or(json!(1));
            ("gift", format!("{name} gifted {count}"), "Gift")
        }
        "redeem" => {
            let title = variables
                .get("reward")
                .and_then(|v| v.as_str())
                .unwrap_or("Reward");
            let input = variables
                .get("input")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let mut detail = format!("{title} — {name}");
            if !input.is_empty() {
                detail.push_str(&format!(": {input}"));
            }
            ("redeem", detail, "Reward")
        }
        _ => ("follow", format!("{name} triggered {et}"), "Event"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_platform_defaults_to_single_connected_platform() {
        assert_eq!(
            resolve_platform(None, true, false).unwrap(),
            TestPlatform::Twitch
        );
        assert_eq!(
            resolve_platform(None, false, true).unwrap(),
            TestPlatform::Kick
        );
    }

    #[test]
    fn resolve_platform_requires_explicit_choice_when_both_connected() {
        assert_eq!(
            resolve_platform(None, true, true).unwrap_err(),
            TestAlertError::PlatformRequired
        );
    }

    #[test]
    fn resolve_platform_rejects_when_neither_connected() {
        assert_eq!(
            resolve_platform(None, false, false).unwrap_err(),
            TestAlertError::NoPlatformConnected
        );
    }

    #[test]
    fn resolve_platform_parses_known_platforms() {
        assert_eq!(
            resolve_platform(Some("kick"), false, true).unwrap(),
            TestPlatform::Kick
        );
        assert_eq!(
            resolve_platform(Some("KICK"), true, true).unwrap(),
            TestPlatform::Kick
        );
    }

    #[test]
    fn resolve_platform_rejects_unknown_platform() {
        assert_eq!(
            resolve_platform(Some("youtube"), true, true).unwrap_err(),
            TestAlertError::UnknownPlatform
        );
    }

    #[test]
    fn resolve_platform_rejects_disconnected_platform() {
        assert_eq!(
            resolve_platform(Some("kick"), true, false).unwrap_err(),
            TestAlertError::PlatformNotConnected
        );
    }

    #[test]
    fn event_supported_allows_platform_specific_events() {
        assert!(assert_event_supported(TestPlatform::Twitch, "raid").is_ok());
        assert!(assert_event_supported(TestPlatform::Kick, "kicks").is_ok());
        assert_eq!(
            assert_event_supported(TestPlatform::Twitch, "bits").unwrap_err(),
            TestAlertError::UnsupportedEventForPlatform
        );
        assert_eq!(
            assert_event_supported(TestPlatform::Kick, "raid").unwrap_err(),
            TestAlertError::UnsupportedEventForPlatform
        );
        assert_eq!(
            assert_event_supported(TestPlatform::Kick, "cheer").unwrap_err(),
            TestAlertError::UnsupportedEventForPlatform
        );
        assert_eq!(
            assert_event_supported(TestPlatform::Twitch, "kicks").unwrap_err(),
            TestAlertError::UnsupportedEventForPlatform
        );
        assert!(dock_only(TestPlatform::Twitch, "redeem"));
        assert!(dock_only(TestPlatform::Kick, "redeem"));
    }

    #[test]
    fn build_test_alert_payload_matches_kick_overlay_shapes() {
        let sub = build_test_alert_payload(
            TestPlatform::Kick,
            "sub",
            &json!({
                "name": "alice",
                "amount": "1000",
                "tier": "3000",
                "input": "not emitted by Kick"
            }),
            None,
            None,
        );
        assert_eq!(
            sub["data"]["variables"],
            json!({ "name": "alice", "amount": "1000" })
        );

        let gift = build_test_alert_payload(
            TestPlatform::Kick,
            "gift",
            &json!({ "name": "alice", "amount": 2, "tier": "1000" }),
            None,
            None,
        );
        assert_eq!(
            gift["data"]["variables"],
            json!({ "name": "alice", "amount": 2 })
        );

        let follow = build_test_alert_payload(
            TestPlatform::Kick,
            "follow",
            &json!({ "name": "alice", "input": "not emitted by Kick" }),
            None,
            None,
        );
        assert_eq!(follow["data"]["variables"], json!({ "name": "alice" }));
    }

    #[test]
    fn build_test_dock_event_uses_platform_specific_shapes() {
        let kick_dock = build_test_dock_event(
            TestPlatform::Kick,
            "kicks",
            &json!({ "name": "carol", "amount": 50 }),
        );
        assert_eq!(kick_dock["platform"], "kick");
        assert_eq!(kick_dock["eventType"], "kicks");
        assert_eq!(kick_dock["detail"], "carol gifted 50 Kicks");

        let twitch_dock = build_test_dock_event(
            TestPlatform::Twitch,
            "cheer",
            &json!({ "name": "bob", "amount": 100 }),
        );
        assert_eq!(twitch_dock["platform"], "twitch");
        assert_eq!(twitch_dock["eventType"], "bits");
    }
}
