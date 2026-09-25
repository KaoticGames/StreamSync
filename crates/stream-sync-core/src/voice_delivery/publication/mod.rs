//! Publication execution and crash/restart recovery (Phase 0C slices 10–11).

mod error;
mod publish;
mod recover;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests;
