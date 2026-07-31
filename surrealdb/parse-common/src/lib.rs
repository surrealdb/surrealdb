//! Shared parsing routines for SurrealQL literal values.
//!
//! This crate implements parsing for the textual formats of literal values which occur in
//! multiple SurrealQL dialects and configuration formats: durations, datetimes, uuids, bytes
//! strings, and the escape sequences of stringly formatted SurrealQL structures.
//!
//! All functions take the bare content string, i.e. with any surrounding quotes and prefixes
//! (`d"`, `u"`, etc.) already stripped. Errors carry a message and a byte span relative to that
//! input string. When the input was produced by unescaping an escaped source string,
//! [`unescaped_to_escaped_offset`] maps error offsets back to offsets in the escaped source.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

use std::fmt;
use std::ops::Range;

mod bytes;
mod datetime;
mod duration;
mod escape;
mod uuid;

pub use crate::bytes::bytes;
pub use crate::datetime::datetime;
pub use crate::duration::duration;
pub use crate::escape::{unescape, unescape_cow, unescaped_to_escaped_offset};
pub use crate::uuid::uuid;

/// An error produced while parsing a literal value.
///
/// Carries a human readable message and the byte range within the input string where the error
/// occurred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
	pub span: Range<usize>,
	pub message: String,
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.message)
	}
}

impl std::error::Error for Error {}
