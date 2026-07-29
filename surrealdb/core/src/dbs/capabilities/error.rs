//! Refusals raised by the [`Capabilities`](super::Capabilities) gates.
//!
//! Every variant means the same thing: the operation itself was well formed and
//! the engine could have performed it, but the configured capabilities forbid
//! it. Nothing here reports a broken query, a broken document or a broken
//! store, and nothing here depends on how far the operation had got.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::{Error as TypesError, NotAllowedError};

/// An operation refused by the configured capabilities.
///
/// Which variants are reachable depends on the feature set, and the two HTTP
/// ones are reachable under opposite ones: `HttpDisabled` needs a build with no
/// outbound client, `NetTargetNotAllowed` needs a build with one. No single
/// `cfg` describes that, hence the blanket allow.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code, reason = "the HTTP gates are reachable under opposite feature sets")]
pub(crate) enum Error {
	/// The build or the configuration has no outbound HTTP at all, so the
	/// `http::*` functions are absent rather than merely restricted.
	#[error("Remote HTTP request functions are not enabled")]
	HttpDisabled,

	/// Embedded scripting functions are switched off.
	#[error("Scripting functions are not allowed")]
	ScriptingNotAllowed,

	/// The named function is outside the allowed function set.
	#[error("Function '{0}' is not allowed to be executed")]
	FunctionNotAllowed(String),

	/// The outbound network target is outside the allowed network set.
	///
	/// Raised both for the host named in a URL and for each address that host
	/// resolves to, so a permitted name cannot smuggle in a denied address.
	/// Every construction site sits behind an outbound HTTP client, so the
	/// variant is unreachable in a build with no client compiled in.
	#[error("Access to network target '{0}' is not allowed")]
	NetTargetNotAllowed(String),
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			Error::ScriptingNotAllowed => {
				TypesError::not_allowed(message, NotAllowedError::Scripting)
			}
			Error::FunctionNotAllowed(name) => TypesError::not_allowed(
				message,
				NotAllowedError::Function {
					name,
				},
			),
			Error::NetTargetNotAllowed(name) => TypesError::not_allowed(
				message,
				NotAllowedError::Target {
					name,
				},
			),
			// Not a refusal a client can act on: the feature is absent from this
			// build, so no capability change would make the call succeed.
			Error::HttpDisabled => internal_todo(message),
		}
	}
}
