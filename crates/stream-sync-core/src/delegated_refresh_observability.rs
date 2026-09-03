//! Test-visible counters for delegated-refresh external side effects (pre-grant / post-revoke).

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
static IRC_JOINS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static EVENTSUB_CONNECTS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static KICK_SSE_CONNECTS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static DELEGATED_CHAT_FANOUT: AtomicUsize = AtomicUsize::new(0);

pub fn record_irc_join() {
    #[cfg(test)]
    IRC_JOINS.fetch_add(1, Ordering::SeqCst);
}

pub fn record_eventsub_connect() {
    #[cfg(test)]
    EVENTSUB_CONNECTS.fetch_add(1, Ordering::SeqCst);
}

pub fn record_kick_sse_connect() {
    #[cfg(test)]
    KICK_SSE_CONNECTS.fetch_add(1, Ordering::SeqCst);
}

pub fn record_delegated_chat_fanout() {
    #[cfg(test)]
    DELEGATED_CHAT_FANOUT.fetch_add(1, Ordering::SeqCst);
}

#[cfg(test)]
pub fn reset_side_effect_counters() {
    IRC_JOINS.store(0, Ordering::SeqCst);
    EVENTSUB_CONNECTS.store(0, Ordering::SeqCst);
    KICK_SSE_CONNECTS.store(0, Ordering::SeqCst);
    DELEGATED_CHAT_FANOUT.store(0, Ordering::SeqCst);
}

#[cfg(test)]
pub fn irc_join_count() -> usize {
    IRC_JOINS.load(Ordering::SeqCst)
}

#[cfg(test)]
pub fn eventsub_connect_count() -> usize {
    EVENTSUB_CONNECTS.load(Ordering::SeqCst)
}

#[cfg(test)]
pub fn kick_sse_connect_count() -> usize {
    KICK_SSE_CONNECTS.load(Ordering::SeqCst)
}

#[cfg(test)]
pub fn delegated_chat_fanout_count() -> usize {
    DELEGATED_CHAT_FANOUT.load(Ordering::SeqCst)
}

#[cfg(test)]
pub fn assert_zero_pre_grant_side_effects() {
    assert_eq!(
        irc_join_count(),
        0,
        "stale refresh must not IRC-join before grant"
    );
    assert_eq!(
        eventsub_connect_count(),
        0,
        "stale refresh must not EventSub-connect before grant"
    );
    assert_eq!(
        kick_sse_connect_count(),
        0,
        "stale refresh must not Kick-SSE-connect before grant"
    );
    assert_eq!(
        delegated_chat_fanout_count(),
        0,
        "stale refresh must not fan out delegated chat before grant"
    );
}
