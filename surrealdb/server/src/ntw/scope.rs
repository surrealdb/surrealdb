//! Scoped routes: the namespace, database and access method named in the URL
//! path.
//!
//! A request to a scoped path reaches the same handler as the unscoped one,
//! with the scope taken from the path instead of the request headers or body:
//!
//! ```text
//! POST /ns/foo/db/bar/sql             → POST /sql                  + PathScope { ns: foo, db: bar }
//! GET  /ns/foo/rpc                    → GET  /rpc                  + PathScope { ns: foo }
//! GET  /ns/foo/db/bar/key/person      → GET  /key/person           + PathScope { ns: foo, db: bar }
//! ANY  /ns/foo/db/bar/api/a/b         → ANY  /api/foo/bar/a/b      + PathScope { ns: foo, db: bar }
//! GET  /ns/foo/db/bar/bucket/img/a    → GET  /bucket/img/a         + PathScope { ns: foo, db: bar }
//! GET  /ns/foo/bucket/img/a           → GET  /bucket/img/a         + PathScope { ns: foo }
//! POST /ns/foo/db/bar/signin          → POST /signin               + PathScope { ns: foo, db: bar }
//! POST /ns/foo/db/bar/ac/users/signup → POST /signup               + PathScope { ns: foo, db: bar, ac: users }
//! POST /ac/admin/signin               → POST /signin               + PathScope { ac: admin }
//! ```
//!
//! [`mount`] rewrites the URI before the request enters the router, so every
//! layer that matches on the path (readiness gate, auth rate limiter, the
//! credential-exchange checks in the auth layer, metrics) sees the unscoped
//! route. The scope travels on as a [`PathScope`] request extension:
//!
//! - The auth layer selects the scope's namespace and database, reconciles them with the headers
//!   and the credentials, and refuses credentials outside them (see [`PathScope::admits`]). The
//!   scope is the session's starting selection: `USE` may still select another namespace or
//!   database.
//! - `signin` and `signup` complete their credentials' `NS`, `DB` and `AC` from it (see
//!   [`credentials`]).
//! - The bucket routes take the level of the bucket they serve from it.
//!
//! `ns`, `db` and `ac` are reserved path segments. A method outside the scoped
//! sets, an access method on a route other than `signin` or `signup`, an empty
//! or malformed segment, or a scoped `api` path without a database leaves the
//! URI unchanged, and the router answers 404.

use std::convert::Infallible;
use std::task::{Context, Poll};

use axum::Router;
use axum::extract::Request;
use axum::response::Response;
use futures_util::future::BoxFuture;
use http::Uri;
use http::uri::PathAndQuery;
use surrealdb_core::dbs::{Session, complete_credential};
use surrealdb_iam::Level;
use surrealdb_types::Variables;
use tower::{Service, ServiceExt};

use crate::ntw::error::Error as NetError;

/// Routes that run in the session's namespace and database, and so accept a
/// namespace and database scope.
const SESSION_METHODS: &[&str] =
	&["sql", "rpc", "import", "export", "key", "ml", "gql", "graphql", "mcp", "api", "bucket"];

/// Routes that exchange credentials, and so accept a namespace, database and
/// access method scope. Their sub-routes (`/signin/cookie`, `/signup/cookie`)
/// follow the same rule.
///
/// `signout` and `refresh` are absent: they act on the credential a request
/// carries, which already names its own namespace, database and access method.
const CREDENTIAL_METHODS: &[&str] = &["signin", "signup"];

/// The namespace, database and access method a request names in its URL path.
/// Inserted into the request extensions by [`mount`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PathScope {
	pub(crate) ns: Option<String>,
	pub(crate) db: Option<String>,
	pub(crate) ac: Option<String>,
}

impl PathScope {
	/// Whether the request's session belongs to this scope: its selected
	/// namespace and database are the ones the path names, and its principal
	/// belongs to that tenant. Root and anonymous principals carry no tenant.
	/// A namespace principal must belong to the scope's namespace; a database or
	/// record principal must also belong to its database when the path names
	/// one. A scope that names no namespace (`/ac/{ac}/…`) admits any session.
	///
	/// This is checked once, when the request is authenticated. The scope is
	/// the session's starting selection, not a confinement: a `USE` the request
	/// or connection runs later may select another namespace or database, as it
	/// may under the `surreal-ns` and `surreal-db` headers.
	pub(crate) fn admits(&self, session: &Session) -> bool {
		let Some(ns) = self.ns.as_deref() else {
			return true;
		};
		let db = self.db.as_deref();
		let selects = session.ns.as_deref() == Some(ns)
			&& db.is_none_or(|db| session.db.as_deref() == Some(db));
		let confines = match session.au.level() {
			Level::Root | Level::No => true,
			Level::Namespace(level_ns) => level_ns == ns,
			Level::Database(level_ns, level_db) | Level::Record(level_ns, level_db, _) => {
				level_ns == ns && db.is_none_or(|db| level_db == db)
			}
		};
		selects && confines
	}
}

/// Completes `signin` or `signup` credentials from the request's scope.
///
/// The URL fixes the level the credentials act at. Each level the scope names
/// (`NS`, `DB`, `AC`) is set where the credentials leave it out, and refused
/// where they name it differently. A namespace or database the scope leaves
/// out must also be absent from the credentials: `/ac/admin/signin` signs in
/// to a root access method and `/ns/foo/signin` at the namespace, and a body
/// that adds `NS` or `DB` would move the signin to a lower level. Without an
/// `ac` in the path, the credentials may still name an access method at the
/// level the path fixes. Every refusal is a [`NetError::ScopeConflict`].
pub(crate) fn credentials(
	scope: Option<&PathScope>,
	vars: impl Into<Variables>,
) -> Result<Variables, NetError> {
	let mut vars = vars.into();
	if let Some(scope) = scope {
		for (upper, lower, value, level) in [
			("NS", "ns", scope.ns.as_deref(), "namespace"),
			("DB", "db", scope.db.as_deref(), "database"),
		] {
			if value.is_none() && (vars.get(upper).is_some() || vars.get(lower).is_some()) {
				return Err(NetError::ScopeConflict(level));
			}
			complete_credential(&mut vars, upper, lower, value, level)
				.map_err(NetError::ScopeConflict)?;
		}
		complete_credential(&mut vars, "AC", "ac", scope.ac.as_deref(), "access method")
			.map_err(NetError::ScopeConflict)?;
	}
	Ok(vars)
}

/// The path prefixes that introduce a scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Prefix {
	/// `/ns/{ns}[/db/{db}][/ac/{ac}]/…`
	Ns,
	/// `/ac/{ac}/…`, an access method defined on the root.
	Ac,
}

impl Prefix {
	fn path(self) -> &'static str {
		match self {
			Prefix::Ns => "/ns",
			Prefix::Ac => "/ac",
		}
	}
}

/// Adds the scoped forms of the scoped routes to a fully built router.
///
/// The scoped requests are dispatched to a clone of `router` taken before the
/// prefixes are added, so a rewritten request runs through the same middleware
/// as an unscoped one and cannot be scoped twice.
///
/// `nest_service` strips the prefix and records no path parameters, so the
/// handlers' `Path` extractors see only the parameters of their own route.
pub(super) fn mount(router: Router) -> Router {
	let scoped = |prefix| ScopedRoutes {
		prefix,
		routes: router.clone(),
	};
	let (ns, ac) = (scoped(Prefix::Ns), scoped(Prefix::Ac));
	router.nest_service(Prefix::Ns.path(), ns).nest_service(Prefix::Ac.path(), ac)
}

#[derive(Clone)]
struct ScopedRoutes {
	prefix: Prefix,
	routes: Router,
}

impl Service<Request> for ScopedRoutes {
	type Response = Response;
	type Error = Infallible;
	type Future = BoxFuture<'static, Result<Response, Infallible>>;

	fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn call(&mut self, mut request: Request) -> Self::Future {
		let routes = self.routes.clone();
		// The nest has stripped the prefix from the path.
		let path = request.uri().path();
		let target = match rewrite(self.prefix, path) {
			Some((scope, path)) => {
				request.extensions_mut().insert(scope);
				path
			}
			None => format!("{}{path}", self.prefix.path()),
		};
		let target = match request.uri().query() {
			Some(query) => format!("{target}?{query}"),
			None => target,
		};
		// Only the path and query change; the scheme and authority an HTTP/2
		// request carries are kept. A target built from a valid URI path, and
		// from segments that were valid in it, is a valid path. A failure leaves
		// the stripped URI in place, which matches no route.
		let mut parts = request.uri().clone().into_parts();
		if let Ok(path_and_query) = target.parse::<PathAndQuery>() {
			parts.path_and_query = Some(path_and_query);
			if let Ok(uri) = Uri::from_parts(parts) {
				*request.uri_mut() = uri;
			}
		}
		Box::pin(routes.oneshot(request))
	}
}

/// Splits a path that follows `prefix` into its scope and the route it
/// addresses. Returns `None` when the path names no scoped route.
fn rewrite(prefix: Prefix, path: &str) -> Option<(PathScope, String)> {
	let rest = path.strip_prefix('/')?;
	let (ns_raw, db_raw, ac_raw, rest) = match prefix {
		Prefix::Ns => {
			let (ns, rest) = rest.split_once('/')?;
			let (db, rest) = match rest.strip_prefix("db/") {
				Some(rest) => rest.split_once('/').map(|(db, rest)| (Some(db), rest))?,
				None => (None, rest),
			};
			let (ac, rest) = match rest.strip_prefix("ac/") {
				Some(rest) => rest.split_once('/').map(|(ac, rest)| (Some(ac), rest))?,
				None => (None, rest),
			};
			(Some(ns), db, ac, rest)
		}
		Prefix::Ac => {
			let (ac, rest) = rest.split_once('/')?;
			(None, None, Some(ac), rest)
		}
	};
	let method = rest.split('/').next()?;
	let allowed = CREDENTIAL_METHODS.contains(&method)
		|| (ac_raw.is_none() && SESSION_METHODS.contains(&method));
	if !allowed {
		return None;
	}
	let decode_level = |raw: Option<&str>| match raw {
		Some(raw) => decode(raw).map(Some),
		None => Some(None),
	};
	let scope = PathScope {
		ns: decode_level(ns_raw)?,
		db: decode_level(db_raw)?,
		ac: decode_level(ac_raw)?,
	};
	// `/api` already carries the namespace and database as its first two path
	// segments. The scoped form maps onto that route; mapping it onto
	// `/api/{path}` would read the path's own first segments as the scope.
	let path = match rest.strip_prefix("api") {
		Some(tail) if method == "api" => format!("/api/{}/{}{tail}", ns_raw?, db_raw?),
		_ => format!("/{rest}"),
	};
	Some((scope, path))
}

/// Percent-decodes one path segment. Returns `None` for an empty segment, a
/// malformed escape, or bytes that are not UTF-8.
fn decode(segment: &str) -> Option<String> {
	if segment.is_empty() {
		return None;
	}
	let bytes = segment.as_bytes();
	let mut out = Vec::with_capacity(bytes.len());
	let mut i = 0;
	while i < bytes.len() {
		if bytes[i] == b'%' {
			let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
			out.push(u8::from_str_radix(hex, 16).ok()?);
			i += 3;
		} else {
			out.push(bytes[i]);
			i += 1;
		}
	}
	String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
	use surrealdb_iam::Role;
	use surrealdb_types::Value;

	use super::*;

	fn scope(ns: Option<&str>, db: Option<&str>, ac: Option<&str>) -> PathScope {
		PathScope {
			ns: ns.map(str::to_owned),
			db: db.map(str::to_owned),
			ac: ac.map(str::to_owned),
		}
	}

	fn ns(path: &str) -> Option<(PathScope, String)> {
		rewrite(Prefix::Ns, path)
	}

	#[test]
	fn rewrites_namespace_and_database() {
		assert_eq!(
			ns("/foo/db/bar/sql"),
			Some((scope(Some("foo"), Some("bar"), None), "/sql".into()))
		);
		assert_eq!(
			ns("/foo/db/bar/key/person/tobie"),
			Some((scope(Some("foo"), Some("bar"), None), "/key/person/tobie".into()))
		);
	}

	#[test]
	fn rewrites_namespace_only() {
		assert_eq!(ns("/foo/rpc"), Some((scope(Some("foo"), None, None), "/rpc".into())));
	}

	#[test]
	fn rewrites_credential_routes() {
		assert_eq!(
			ns("/foo/db/bar/signin"),
			Some((scope(Some("foo"), Some("bar"), None), "/signin".into()))
		);
		assert_eq!(
			ns("/foo/db/bar/ac/users/signup/cookie"),
			Some((scope(Some("foo"), Some("bar"), Some("users")), "/signup/cookie".into()))
		);
		assert_eq!(
			ns("/foo/ac/admins/signin"),
			Some((scope(Some("foo"), None, Some("admins")), "/signin".into()))
		);
		assert_eq!(
			rewrite(Prefix::Ac, "/admins/signin"),
			Some((scope(None, None, Some("admins")), "/signin".into()))
		);
	}

	#[test]
	fn access_method_scopes_only_credential_routes() {
		assert_eq!(ns("/foo/db/bar/ac/users/sql"), None);
		assert_eq!(ns("/foo/db/bar/ac/users/rpc"), None);
		assert_eq!(rewrite(Prefix::Ac, "/admins/sql"), None);
		// Credential routes that act on a credential the request already carries.
		assert_eq!(ns("/foo/db/bar/signout"), None);
		assert_eq!(ns("/foo/db/bar/refresh/cookie"), None);
	}

	#[test]
	fn api_maps_onto_the_existing_route() {
		assert_eq!(
			ns("/foo/db/bar/api/a/b"),
			Some((scope(Some("foo"), Some("bar"), None), "/api/foo/bar/a/b".into()))
		);
		// The existing route keeps the raw segments; its `Path` extractor decodes them.
		assert_eq!(
			ns("/a%2Fb/db/c%20d/api/x"),
			Some((scope(Some("a/b"), Some("c d"), None), "/api/a%2Fb/c%20d/x".into()))
		);
		// `/api` needs a database.
		assert_eq!(ns("/foo/api/x"), None);
	}

	#[test]
	fn bucket_is_scoped_at_every_level() {
		assert_eq!(
			ns("/foo/db/bar/bucket/images/a/b.png"),
			Some((scope(Some("foo"), Some("bar"), None), "/bucket/images/a/b.png".into()))
		);
		assert_eq!(
			ns("/foo/bucket/images/a/b.png"),
			Some((scope(Some("foo"), None, None), "/bucket/images/a/b.png".into()))
		);
		// The listing routes, with and without the trailing slash.
		assert_eq!(
			ns("/foo/db/bar/bucket/images"),
			Some((scope(Some("foo"), Some("bar"), None), "/bucket/images".into()))
		);
		assert_eq!(
			ns("/foo/bucket/images/"),
			Some((scope(Some("foo"), None, None), "/bucket/images/".into()))
		);
		// The bucket and key keep their raw segments; the route's `Path`
		// extractor decodes them.
		assert_eq!(
			ns("/a%2Fb/db/c%20d/bucket/img/x%3Ay"),
			Some((scope(Some("a/b"), Some("c d"), None), "/bucket/img/x%3Ay".into()))
		);
		// No access method, and no access-method-only scope.
		assert_eq!(ns("/foo/db/bar/ac/users/bucket/images/x"), None);
		assert_eq!(rewrite(Prefix::Ac, "/admins/bucket/images/x"), None);
		assert_eq!(ns("/foo/db/bar/bucketx/images/x"), None);
	}

	#[test]
	fn decodes_segments() {
		assert_eq!(
			ns("/a%2Fb/db/%F0%9F%A6%80/sql"),
			Some((scope(Some("a/b"), Some("🦀"), None), "/sql".into()))
		);
	}

	#[test]
	fn refuses_unscoped_and_malformed_paths() {
		// Root-only routes.
		assert_eq!(ns("/foo/health"), None);
		assert_eq!(ns("/foo/db/bar/version"), None);
		// No method.
		assert_eq!(ns("/foo"), None);
		assert_eq!(ns("/foo/db/bar"), None);
		assert_eq!(ns("/foo/db"), None);
		assert_eq!(ns("/foo/db/bar/ac/users"), None);
		// Empty and malformed segments.
		assert_eq!(ns("//sql"), None);
		assert_eq!(ns("/foo/db//sql"), None);
		assert_eq!(ns("/foo/ac//signin"), None);
		assert_eq!(ns("/fo%2/sql"), None);
		assert_eq!(ns("/%FF/sql"), None);
	}

	#[test]
	fn method_prefix_is_not_a_method() {
		assert_eq!(ns("/foo/sqlx"), None);
		assert_eq!(ns("/foo/db/bar/apix/a"), None);
		assert_eq!(ns("/foo/signinx"), None);
	}

	#[test]
	fn admits_sessions_that_start_in_the_scope() {
		let both = scope(Some("foo"), Some("bar"), None);
		let session = |level| Session::for_level(level, Role::Viewer).with_ns("foo").with_db("bar");
		assert!(both.admits(&session(Level::Root)));
		assert!(both.admits(&session(Level::Database("foo".into(), "bar".into()))));
		assert!(!both.admits(&session(Level::Database("foo".into(), "other".into()))));
		assert!(!both.admits(&session(Level::Namespace("other".into()))));
		assert!(!both.admits(&Session::owner().with_ns("foo").with_db("other")));
		// Without a database in the path, any database in the namespace.
		let ns = scope(Some("foo"), None, None);
		assert!(
			ns.admits(&session(Level::Database("foo".into(), "other".into())).with_db("other"))
		);
		// A root access-method scope names no tenant.
		assert!(scope(None, None, Some("admins")).admits(&Session::default()));
	}

	#[test]
	fn credentials_are_completed_from_the_scope() {
		let full = scope(Some("foo"), Some("bar"), Some("users"));
		let mut given = Variables::new();
		given.insert("user", "tobie");
		let vars = credentials(Some(&full), given).expect("completed");
		assert_eq!(vars.get("NS"), Some(&Value::String("foo".into())));
		assert_eq!(vars.get("DB"), Some(&Value::String("bar".into())));
		assert_eq!(vars.get("AC"), Some(&Value::String("users".into())));

		// A level the credentials name in either case must match.
		let mut given = Variables::new();
		given.insert("ns", "foo");
		given.insert("DB", "bar");
		assert!(credentials(Some(&full), given).is_ok());
		let mut given = Variables::new();
		given.insert("db", "other");
		assert!(matches!(
			credentials(Some(&full), given),
			Err(NetError::ScopeConflict("database"))
		));

		// Both aliases must match.
		let mut given = Variables::new();
		given.insert("NS", "foo");
		given.insert("ns", "other");
		assert!(matches!(
			credentials(Some(&full), given),
			Err(NetError::ScopeConflict("namespace"))
		));

		// A level the path leaves out cannot be added by the credentials.
		let root = scope(None, None, Some("admin"));
		let mut given = Variables::new();
		given.insert("NS", "foo");
		given.insert("DB", "bar");
		assert!(matches!(
			credentials(Some(&root), given),
			Err(NetError::ScopeConflict("namespace"))
		));
		let ns_only = scope(Some("foo"), None, Some("admin"));
		let mut given = Variables::new();
		given.insert("db", "bar");
		assert!(matches!(
			credentials(Some(&ns_only), given),
			Err(NetError::ScopeConflict("database"))
		));
		// An access method may be named at the level the path fixes.
		let db_only = scope(Some("foo"), Some("bar"), None);
		let mut given = Variables::new();
		given.insert("AC", "users");
		assert!(credentials(Some(&db_only), given).is_ok());

		// No scope leaves the credentials alone.
		let vars = credentials(None, Variables::new()).expect("unchanged");
		assert_eq!(vars.get("NS"), None);
	}
}
