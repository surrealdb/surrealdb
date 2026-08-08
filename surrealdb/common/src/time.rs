//! Timers, over whichever clock the target actually has.
//!
//! Tokio's timers read the clock through `std::time`, which has no
//! implementation on `wasm32-unknown-unknown` and panics with "time not
//! implemented on this platform" the first time one is polled.
//!
//! Excluding tokio's `time` feature for a wasm target does not keep it out of
//! the build: `surrealdb-core` omits it from its wasm dependency block, but
//! `surrealdb-datastore` requests it unconditionally and cargo unifies features
//! across the graph. So `tokio::time` compiles for wasm and traps at runtime
//! rather than failing the build, and a per-crate feature list cannot prevent
//! that.
//!
//! `wasmtimer` provides the same API over the JavaScript clock. Everything that
//! needs to sleep, tick, or bound a future by time takes it from here, so the
//! choice is made once rather than at each call site — and off wasm these are
//! `tokio::time`'s own items, so behaviour is unchanged.
//!
//! [`std::time::Duration`] is a plain value with no clock behind it and is used
//! directly. For wall-clock timestamps rather than timers, use
//! [`web_time`](https://docs.rs/web-time).

#[cfg(not(target_family = "wasm"))]
pub use tokio::time::{
	Instant, Interval, MissedTickBehavior, interval, sleep, timeout, timeout_at,
};
#[cfg(target_family = "wasm")]
pub use wasmtimer::std::Instant;
#[cfg(target_family = "wasm")]
pub use wasmtimer::tokio::{Interval, MissedTickBehavior, interval, sleep, timeout, timeout_at};
