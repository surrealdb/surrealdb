//! The names of the HTTP headers SurrealDB's HTTP and WebSocket endpoints read
//! and write.
//!
//! These are part of the wire contract between the server and every client, so
//! the server and the SDKs take them from here rather than each spelling them.
//! Header names are case-insensitive on the wire; these are the lowercase forms
//! `http::HeaderName::from_static` requires.

/// The session to run a request on.
pub const ID: &str = "surreal-id";
/// The namespace to select.
pub const NS: &str = "surreal-ns";
/// The database to select.
pub const DB: &str = "surreal-db";
/// The namespace a basic-auth user is defined on.
pub const AUTH_NS: &str = "surreal-auth-ns";
/// The database a basic-auth user is defined on.
pub const AUTH_DB: &str = "surreal-auth-db";
/// The server's version, on every response.
pub const VERSION: &str = "surreal-version";
