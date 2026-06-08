//! Auth context extraction and subject binding for MCP sessions.
//!
//! When running behind SurrealDB's `SurrealAuth` middleware, the authenticated
//! `Session` is placed into request extensions. This module:
//!
//! - extracts that `Session` during MCP session initialization ([`extract_session_from_parts`]);
//! - records a [`BoundSubject`] fingerprint of the initialising session so subsequent requests on
//!   the same MCP session id can be re-verified ([`BoundSubject::from_session`],
//!   [`McpService::verify_request_subject`]).
//!
//! Re-verifying every inbound request closes the session-hijack vector
//! described in the MCP security best-practices document: possession of an
//! `mcp-session-id` alone must not let an attacker present a different
//! credential and impersonate the original user.

use rmcp::service::RequestContext;
use rmcp::{ErrorData as McpError, RoleServer};
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::Level;

/// Extract the authenticated `Session` from HTTP request parts in the
/// extensions.
///
/// The rmcp `RequestContext` stores `http::request::Parts` in its extensions.
/// The SurrealDB auth middleware places the `Session` into the Parts'
/// extensions.
pub(crate) fn extract_session_from_parts(parts: &http::request::Parts) -> Option<Session> {
	parts.extensions.get::<Session>().cloned()
}

/// Stable fingerprint of the authenticated subject behind an MCP session.
///
/// Captured once at `initialize` time and re-checked on every subsequent
/// MCP request. Two `BoundSubject`s compare equal iff they describe the
/// same authentication level *and* the same actor identity. Anonymous
/// (`Level::No`) bound subjects are treated specially by [`check_subject`]:
/// when the handshake bound an anonymous caller, follow-up requests may
/// remain anonymous (an anonymous handshake stays anonymous). When the
/// handshake bound an authenticated caller, follow-up requests on
/// networked transports must present the same authenticated subject —
/// missing, anonymous, or different credentials are rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundSubject {
	level: Level,
	identity: String,
}

impl BoundSubject {
	/// Compute the fingerprint from the authenticated `Session.au`.
	pub(crate) fn from_session(session: &Session) -> Self {
		Self {
			level: session.au.level().clone(),
			identity: session.au.id().to_string(),
		}
	}

	/// Whether this subject represents an unauthenticated caller.
	pub(crate) fn is_anonymous(&self) -> bool {
		matches!(self.level, Level::No)
	}

	/// Stable identity string suitable for audit logs. For non-anonymous
	/// subjects this is `"<level>::<id>"` so both the actor and its scope
	/// are visible at a glance; anonymous subjects render as
	/// `"anonymous"`.
	pub(crate) fn audit_label(&self) -> String {
		if self.is_anonymous() {
			"anonymous".to_string()
		} else {
			format!("{}::{}", self.level, self.identity)
		}
	}
}

/// Pull the request's [`http::request::Parts`] out of an rmcp
/// [`RequestContext`] and extract the authenticated `Session`. Returns
/// `None` when no parts are attached (the stdio transport path) or when
/// no session is in the parts' extensions (a misconfigured proxy).
pub(crate) fn incoming_subject(ctx: &RequestContext<RoleServer>) -> Option<BoundSubject> {
	let parts = ctx.extensions.get::<http::request::Parts>()?;
	let session = extract_session_from_parts(parts)?;
	Some(BoundSubject::from_session(&session))
}

/// Compare an incoming request's subject against a previously bound
/// subject and reject the request when they disagree, or when the
/// caller has dropped credentials on a session that was bound to an
/// authenticated subject.
///
/// This is the strict per-request check used on networked transports.
/// It is called from [`crate::service::McpService::verify_request_subject`]
/// only for non-stdio transports; stdio short-circuits before reaching
/// here because it has no per-request credential channel and no
/// session-hijack vector. See `verify_request_subject` for the
/// transport discriminator.
///
/// Outcomes:
///
/// - Bound subject is anonymous: any incoming subject is accepted (anonymous handshake stays
///   anonymous).
/// - Bound subject is authenticated, incoming is missing or anonymous: reject with
///   [`McpError::invalid_params`]. Possession of the session id alone must not let a caller drop
///   credentials and keep running under the bound subject.
/// - Bound subject is authenticated, incoming is authenticated and matches: accept.
/// - Bound subject is authenticated, incoming is authenticated but different: reject with
///   [`McpError::invalid_params`] so the caller sees a protocol-level failure rather than a leaked
///   tool result.
///
/// Mismatches also emit a `tracing::warn!` on the general
/// `surrealdb::mcp` target so operators who have not yet wired up
/// audit-target forwarding still see the rejection in their server
/// logs. The canonical, machine-parseable record continues to land
/// on `surrealdb::mcp::audit` via the dispatcher.
pub(crate) fn check_subject(
	bound: &BoundSubject,
	incoming: Option<BoundSubject>,
) -> Result<(), McpError> {
	match incoming {
		None if !bound.is_anonymous() => {
			Err(McpError::invalid_params("Credentials required for this MCP session", None))
		}
		Some(new) if new.is_anonymous() && !bound.is_anonymous() => {
			Err(McpError::invalid_params("Credentials required for this MCP session", None))
		}
		Some(new) if !new.is_anonymous() && new != *bound => {
			tracing::warn!(
				target: "surrealdb::mcp",
				bound = %bound.audit_label(),
				incoming = %new.audit_label(),
				"rejecting MCP request: credentials do not match bound subject"
			);
			Err(McpError::invalid_params(
				"Credentials do not match the MCP session: re-initialize to bind a new session",
				None,
			))
		}
		_ => Ok(()),
	}
}

#[cfg(test)]
mod tests {
	use http::Request;
	use surrealdb_core::iam::{Auth, Role};

	use super::*;

	fn parts_with<F: FnOnce(&mut http::request::Parts)>(mutate: F) -> http::request::Parts {
		let req = Request::builder().uri("/").body(()).expect("build request");
		let (mut parts, _) = req.into_parts();
		mutate(&mut parts);
		parts
	}

	#[test]
	fn extracts_session_when_present() {
		let parts = parts_with(|p| {
			p.extensions.insert(Session::owner().with_ns("x").with_db("y"));
		});
		let session = extract_session_from_parts(&parts).expect("session should be extracted");
		assert_eq!(session.ns.as_deref(), Some("x"));
		assert_eq!(session.db.as_deref(), Some("y"));
	}

	#[test]
	fn returns_none_when_absent() {
		let parts = parts_with(|_| {});
		assert!(extract_session_from_parts(&parts).is_none());
	}

	#[test]
	fn missing_or_anonymous_is_rejected_for_authenticated_bound_subject() {
		let session = Session {
			au: std::sync::Arc::new(Auth::for_db(Role::Editor, "ns", "db")),
			ns: Some("ns".into()),
			db: Some("db".into()),
			..Session::default()
		};
		let bound = BoundSubject::from_session(&session);
		assert!(check_subject(&bound, None).is_err());
		let anon = BoundSubject::from_session(&Session::default());
		assert!(check_subject(&bound, Some(anon)).is_err());
	}

	#[test]
	fn anonymous_bound_subject_accepts_missing_or_anonymous() {
		let bound = BoundSubject::from_session(&Session::default());
		assert!(check_subject(&bound, None).is_ok());
		let anon = BoundSubject::from_session(&Session::default());
		assert!(check_subject(&bound, Some(anon)).is_ok());
	}

	#[test]
	fn mismatch_is_rejected() {
		let alice = BoundSubject {
			level: Level::Database("ns".into(), "db".into()),
			identity: "alice".into(),
		};
		let bob = BoundSubject {
			level: Level::Database("ns".into(), "db".into()),
			identity: "bob".into(),
		};
		assert!(check_subject(&alice, Some(bob)).is_err());
	}

	#[test]
	fn same_identity_is_accepted() {
		let s = Session {
			au: std::sync::Arc::new(Auth::for_db(Role::Editor, "ns", "db")),
			..Session::default()
		};
		let bound = BoundSubject::from_session(&s);
		let again = BoundSubject::from_session(&s);
		assert!(check_subject(&bound, Some(again)).is_ok());
	}

	#[test]
	fn audit_label_distinguishes_anonymous_and_authenticated() {
		let anon = BoundSubject::from_session(&Session::default());
		assert_eq!(anon.audit_label(), "anonymous");
		let auth = BoundSubject {
			level: Level::Root,
			identity: "root".into(),
		};
		assert!(auth.audit_label().contains("root"));
	}
}
