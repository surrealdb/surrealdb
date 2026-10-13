//! Helpers for the function families.
//!
//! The pure ones descended with the families they serve. `http` is the
//! exception that stayed: it builds requests through the engine's client, so it
//! belongs beside the client.

#[cfg(feature = "http")]
pub mod http;
