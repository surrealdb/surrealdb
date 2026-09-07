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
use surrealdb_core::dbs::{Session, SessionData};
use surrealdb_iam::Level;

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
///
/// The identity comes from `Auth::id()`, which for access-method tokens names
/// the *access method* rather than the individual token holder, so two callers
/// authenticated by the same `DEFINE ACCESS` share a fingerprint. Roles are not
/// part of it either. The check therefore binds a session to an access path, not
/// to a principal.
///
/// SECURITY: the access method's `CONTEXT` payload is part of the fingerprint,
/// for the same reason `AuthPrincipalSnapshot` counts it as part of the auth
/// principal. `McpService` pins the handshake `Session` in `BoundHandshake` and
/// serves every later tool call from it, so nothing else in the MCP path ever
/// re-reads the payload. Without it here, a grant revoked in the table would be
/// recomputed (correctly, and emptily) by each request's HTTP verification and
/// then thrown away in favour of the pinned session, leaving the caller reading
/// rows their grant no longer covers until `DURATION FOR SESSION` elapsed.
/// Including it makes the mismatch a rejection, and the caller re-initializes.
///
/// The corollary: a `CONTEXT` clause that is not deterministic for a settled
/// identity — one reading `time::now()`, say — recomputes differently on every
/// request and so rejects every call after the handshake. Such a clause is
/// already unusable, because `AuthPrincipalSnapshot` would tear the session's
/// LIVE subscriptions down on every re-authentication for the same reason; MCP
/// simply surfaces it sooner and louder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundSubject {
	level: Level,
	identity: String,
	/// The access method's `CONTEXT` payload, compared by [`SessionData`]'s own
	/// order-insensitive equality so a rescan that reorders an unchanged grant
	/// list does not read as a different subject.
	data: Option<SessionData>,
}

impl BoundSubject {
	/// Compute the fingerprint from the authenticated `Session.au`, plus the
	/// `CONTEXT` payload the session was authenticated with.
	pub(crate) fn from_session(session: &Session) -> Self {
		Self {
			level: session.au.level().clone(),
			identity: session.au.id().to_string(),
			data: session.data.clone(),
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
/// - Bound subject is anonymous, incoming is missing or anonymous: accepted (an anonymous handshake
///   stays anonymous).
/// - Bound subject is anonymous, incoming is authenticated: rejected. The session would still serve
///   the call as anonymous, so the caller must re-initialize to bind the identity it now wants to
///   act under rather than have it silently ignored.
/// - Bound subject is authenticated, incoming is missing or anonymous: reject with
///   [`McpError::invalid_params`]. Possession of the session id alone must not let a caller drop
///   credentials and keep running under the bound subject.
/// - Bound subject is authenticated, incoming is authenticated and matches: accept.
/// - Bound subject is authenticated, incoming is authenticated but different: reject with
///   [`McpError::invalid_params`] so the caller sees a protocol-level failure rather than a leaked
///   tool result. "Different" includes the same identity presenting a changed `CONTEXT` payload —
///   that is a revoked or altered grant, and the pinned handshake session would otherwise keep
///   serving under the old one. See [`BoundSubject`].
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
	use surrealdb_iam::{Auth, Role};

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
			data: None,
		};
		let bob = BoundSubject {
			level: Level::Database("ns".into(), "db".into()),
			identity: "bob".into(),
			data: None,
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

	/// A revoked grant changes the `CONTEXT` payload the request is
	/// authenticated with. The handshake session is pinned, so the only place
	/// that can notice is this fingerprint.
	#[test]
	fn a_changed_context_payload_is_a_different_subject() {
		use surrealdb_core::dbs::SessionData;
		use surrealdb_types::{Array, Value as PV};

		fn session_with(orgs: &[&str]) -> Session {
			Session {
				au: std::sync::Arc::new(Auth::for_db(Role::Editor, "ns", "db")),
				ns: Some("ns".into()),
				db: Some("db".into()),
				data: Some(SessionData::new(PV::Array(Array::from(
					orgs.iter().map(|o| PV::String((*o).to_owned())).collect::<Vec<_>>(),
				)))),
				..Session::default()
			}
		}

		let bound = BoundSubject::from_session(&session_with(&["acme"]));

		// The grant is gone: the request must not be served from the pinned
		// handshake session.
		let revoked = BoundSubject::from_session(&session_with(&[]));
		assert!(check_subject(&bound, Some(revoked)).is_err());

		// A different grant set is equally a different subject.
		let moved = BoundSubject::from_session(&session_with(&["globex"]));
		assert!(check_subject(&bound, Some(moved)).is_err());

		// An unchanged payload is accepted, including when the scan that
		// produced it returned the same grants in a different order.
		let same = BoundSubject::from_session(&session_with(&["acme"]));
		assert!(check_subject(&bound, Some(same)).is_ok());

		let two = BoundSubject::from_session(&session_with(&["acme", "globex"]));
		let reordered = BoundSubject::from_session(&session_with(&["globex", "acme"]));
		assert!(check_subject(&two, Some(reordered)).is_ok());
	}

	#[test]
	fn audit_label_distinguishes_anonymous_and_authenticated() {
		let anon = BoundSubject::from_session(&Session::default());
		assert_eq!(anon.audit_label(), "anonymous");
		let auth = BoundSubject {
			level: Level::Root,
			identity: "root".into(),
			data: None,
		};
		assert!(auth.audit_label().contains("root"));
	}
}
