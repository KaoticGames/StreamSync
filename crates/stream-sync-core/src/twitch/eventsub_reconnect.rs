//! Twitch EventSub websocket reconnect handoff (Task 7.2).
//!
//! On `session_reconnect`, connect to Twitch's `reconnect_url`. Subscriptions
//! migrate; do not create a second subscription set. Keep accepting
//! notifications from the old socket until the replacement sends `session_welcome`.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EventSubDecision {
    Subscribe { session_id: String },
    Connect { url: String },
}

#[derive(Debug, Default)]
pub(crate) struct EventSubReconnectState {
    subscribed_session: Option<String>,
    handoff_url: Option<String>,
    replacement_welcome: bool,
}

impl EventSubReconnectState {
    pub(crate) fn on_control_message(
        &mut self,
        message_type: &str,
        session_id: Option<&str>,
        reconnect_url: Option<&str>,
    ) -> Vec<EventSubDecision> {
        match message_type {
            "session_welcome" => {
                let Some(id) = session_id.map(str::trim).filter(|s| !s.is_empty()) else {
                    return Vec::new();
                };
                if self.handoff_url.is_some() {
                    self.replacement_welcome = true;
                    self.handoff_url = None;
                    return Vec::new();
                }
                if self.subscribed_session.is_none() {
                    self.subscribed_session = Some(id.to_string());
                    vec![EventSubDecision::Subscribe {
                        session_id: id.to_string(),
                    }]
                } else {
                    Vec::new()
                }
            }
            "session_reconnect" => {
                let Some(url) = reconnect_url.map(str::trim).filter(|s| !s.is_empty()) else {
                    return Vec::new();
                };
                self.handoff_url = Some(url.to_string());
                vec![EventSubDecision::Connect {
                    url: url.to_string(),
                }]
            }
            _ => Vec::new(),
        }
    }

    pub(crate) fn accept_old_socket_notification(&self) -> bool {
        !self.replacement_welcome
    }

    #[cfg(test)]
    pub(crate) fn subscribe_count_session(&self) -> Option<&str> {
        self.subscribed_session.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn welcome(id: &str) -> (&'static str, Option<&str>, Option<&str>) {
        ("session_welcome", Some(id), None)
    }

    fn reconnect(url: &str) -> (&'static str, Option<&str>, Option<&str>) {
        ("session_reconnect", None, Some(url))
    }

    #[test]
    fn eventsub_session_welcome_subscribes_once() {
        let mut state = EventSubReconnectState::default();
        let (ty, id, url) = welcome("session-a");
        let first = state.on_control_message(ty, id, url);
        assert_eq!(
            first,
            vec![EventSubDecision::Subscribe {
                session_id: "session-a".into()
            }]
        );
        let second = state.on_control_message(ty, id, url);
        assert!(second.is_empty());
        assert_eq!(state.subscribe_count_session(), Some("session-a"));
    }

    #[test]
    fn eventsub_session_reconnect_uses_reconnect_url_without_resubscribe() {
        let mut state = EventSubReconnectState::default();
        let (ty, id, url) = welcome("session-a");
        let _ = state.on_control_message(ty, id, url);
        let reconnect_url = "wss://eventsub.wss.twitch.tv/ws?session=abc";
        let (ty, id, url) = reconnect(reconnect_url);
        let actions = state.on_control_message(ty, id, url);
        assert_eq!(
            actions,
            vec![EventSubDecision::Connect {
                url: reconnect_url.into()
            }]
        );
        assert_eq!(state.subscribe_count_session(), Some("session-a"));
        assert!(state.accept_old_socket_notification());
    }

    #[test]
    fn eventsub_handoff_keeps_old_notifications_until_new_welcome_without_duplicate_subscribe() {
        let mut state = EventSubReconnectState::default();
        let (ty, id, url) = welcome("session-a");
        assert_eq!(state.on_control_message(ty, id, url).len(), 1);

        let reconnect_url = "wss://eventsub.wss.twitch.tv/ws?session=handoff";
        let (ty, id, url) = reconnect(reconnect_url);
        let actions = state.on_control_message(ty, id, url);
        assert!(matches!(
            actions.as_slice(),
            [EventSubDecision::Connect { url }] if url == reconnect_url
        ));
        assert!(
            state.accept_old_socket_notification(),
            "old socket notifications must still be accepted during handoff"
        );

        let replacement = state.on_control_message("session_welcome", Some("session-b"), None);
        assert!(
            replacement.is_empty(),
            "replacement welcome must not create a second subscription set"
        );
        assert_eq!(state.subscribe_count_session(), Some("session-a"));
        assert!(
            !state.accept_old_socket_notification(),
            "old socket closes after replacement welcome"
        );
    }
}
