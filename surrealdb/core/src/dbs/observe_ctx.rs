//! Core-side glue building observability event context from a [`Session`].
//!
//! The observability event types live in the leaf `surrealdb-observe` crate,
//! which cannot depend on [`Session`] (a `dbs` type). These `From<&Session>`
//! conversions therefore live here, in core, next to the type they read.
//!
//! All three conversions apply the same record-access collapsing rule for the
//! `user` field: anonymous sessions map to `None`, record-access principals to
//! a fixed `<record>` sentinel (record ids are unbounded and must never widen
//! metric-label cardinality), and everything else to the actor id.

use std::net::IpAddr;

use super::Session;
use crate::observe::{HttpRequestEventCtx, NetworkBytesEventCtx, TenantIdentity};

/// Collapse a session's authenticated principal into a bounded `user` label.
fn session_user(sess: &Session) -> Option<String> {
	if sess.au.is_anon() {
		None
	} else if sess.au.is_record() {
		Some("<record>".to_owned())
	} else {
		Some(sess.au.id().to_owned())
	}
}

/// Parse the session's stored client IP, if it parses cleanly. Proxy headers
/// can include trailing port specifiers; a parse failure leaves the field
/// `None` rather than corrupting it.
fn session_client_ip(sess: &Session) -> Option<IpAddr> {
	sess.ip.as_deref().and_then(|raw| raw.parse::<IpAddr>().ok())
}

impl From<&Session> for TenantIdentity {
	/// Build a [`TenantIdentity`] from an authenticated [`Session`].
	fn from(sess: &Session) -> Self {
		Self {
			namespace: sess.ns.clone(),
			database: sess.db.clone(),
			user: session_user(sess),
			session_id: sess.id,
			client_ip: session_client_ip(sess),
		}
	}
}

impl From<&Session> for NetworkBytesEventCtx {
	/// Build a ctx from an authenticated [`Session`].
	fn from(sess: &Session) -> Self {
		Self {
			namespace: sess.ns.clone(),
			database: sess.db.clone(),
			user: session_user(sess),
		}
	}
}

impl From<&Session> for HttpRequestEventCtx {
	/// Build a ctx from an authenticated [`Session`], carrying the session id
	/// and client IP alongside the tenant fields.
	fn from(sess: &Session) -> Self {
		Self {
			namespace: sess.ns.clone(),
			database: sess.db.clone(),
			user: session_user(sess),
			session_id: sess.id,
			client_ip: session_client_ip(sess),
		}
	}
}

#[cfg(test)]
mod network_ctx_from_session {
	use std::sync::Arc;

	use crate::dbs::Session;
	use crate::iam::{Auth, Role};
	use crate::observe::NetworkBytesEventCtx;

	#[test]
	fn root_user_populates_user() {
		let sess = Session {
			au: Arc::new(Auth::for_root(Role::Owner)),
			..Session::default()
		}
		.with_ns("acme")
		.with_db("prod");
		let ctx = NetworkBytesEventCtx::from(&sess);
		assert_eq!(ctx.namespace.as_deref(), Some("acme"));
		assert_eq!(ctx.database.as_deref(), Some("prod"));
		// `Auth::for_root` sets the actor id to `system_auth`.
		assert_eq!(ctx.user.as_deref(), Some("system_auth"));
	}

	#[test]
	fn record_principal_collapses_to_sentinel() {
		// Use a record-shaped principal id; the helper MUST NOT
		// surface it verbatim because record ids are unbounded.
		let sess = Session {
			au: Arc::new(Auth::for_record("user:abc123".to_owned(), "acme", "prod", "web")),
			..Session::default()
		};
		let ctx = NetworkBytesEventCtx::from(&sess);
		assert_eq!(ctx.user.as_deref(), Some("<record>"));
		assert!(
			!ctx.user.as_ref().unwrap().contains("abc123"),
			"record id leaked into ctx.user: {:?}",
			ctx.user
		);
	}

	#[test]
	fn anonymous_session_leaves_user_none() {
		let sess = Session::default();
		let ctx = NetworkBytesEventCtx::from(&sess);
		assert!(ctx.user.is_none());
	}

	#[test]
	fn ns_db_carry_through_verbatim() {
		let sess = Session::default().with_ns("a-ns").with_db("a-db");
		let ctx = NetworkBytesEventCtx::from(&sess);
		assert_eq!(ctx.namespace.as_deref(), Some("a-ns"));
		assert_eq!(ctx.database.as_deref(), Some("a-db"));
	}
}

#[cfg(test)]
mod tenant_identity_projections {
	//! Coverage for the `TenantIdentity::to_{rpc,auth}_ctx`
	//! projections used by the RPC dispatch site.
	//!
	//! These projections preserve the `From<&Session>` collapsing rule
	//! (anonymous -> `None`, record-access -> `<record>` sentinel, else ->
	//! actor id) so per-tenant dimensional dashboards and audit destinations
	//! never receive raw record-access principal ids and never see synthetic
	//! empty-string users for unauthenticated traffic.

	use std::sync::Arc;

	use crate::dbs::Session;
	use crate::iam::{Auth, Role};
	use crate::observe::TenantIdentity;

	#[test]
	fn rpc_ctx_collapses_record_principal() {
		let sess = Session {
			au: Arc::new(Auth::for_record("user:abc123".to_owned(), "acme", "prod", "web")),
			..Session::default()
		};
		let identity = TenantIdentity::from(&sess);
		let rpc = identity.to_rpc_ctx();
		assert_eq!(rpc.user.as_deref(), Some("<record>"));
		assert!(
			!rpc.user.as_ref().unwrap().contains("abc123"),
			"record id leaked into rpc ctx.user: {:?}",
			rpc.user
		);
	}

	#[test]
	fn auth_ctx_collapses_record_principal() {
		let sess = Session {
			au: Arc::new(Auth::for_record("user:abc123".to_owned(), "acme", "prod", "web")),
			..Session::default()
		};
		let identity = TenantIdentity::from(&sess);
		let auth = identity.to_auth_ctx();
		assert_eq!(auth.user.as_deref(), Some("<record>"));
		assert!(
			!auth.user.as_ref().unwrap().contains("abc123"),
			"record id leaked into auth ctx.user: {:?}",
			auth.user
		);
	}

	#[test]
	fn projections_drop_user_for_anonymous() {
		let sess = Session::default();
		let identity = TenantIdentity::from(&sess);
		let rpc = identity.to_rpc_ctx();
		let auth = identity.to_auth_ctx();
		assert!(rpc.user.is_none(), "anonymous session must not surface a user label");
		assert!(auth.user.is_none(), "anonymous session must not surface a user label");
	}

	#[test]
	fn projections_carry_root_user_id() {
		let sess = Session {
			au: Arc::new(Auth::for_root(Role::Owner)),
			..Session::default()
		}
		.with_ns("acme")
		.with_db("prod");
		let identity = TenantIdentity::from(&sess);
		let rpc = identity.to_rpc_ctx();
		let auth = identity.to_auth_ctx();
		assert_eq!(rpc.namespace.as_deref(), Some("acme"));
		assert_eq!(rpc.database.as_deref(), Some("prod"));
		assert_eq!(rpc.user.as_deref(), Some("system_auth"));
		assert_eq!(auth.user.as_deref(), Some("system_auth"));
	}
}
