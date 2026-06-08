//! Helpers for deciding whether an incoming `/metrics` request should see
//! operator-level metrics.
//!
//! The server's global auth layer is lenient: if no `Authorization` header is
//! supplied, it inserts an anonymous [`Session`] and forwards the request.
//! That means the `/metrics` handler can look at the session and decide the
//! appropriate view without needing to re-run auth logic.
//!
//! # Why only `is_root`?
//!
//! The richer metrics view is an operator capability. Tying it to `Level::Root`
//! keeps the surface small and matches existing operator workflows (the same
//! credentials used to run `surreal sql --user root` etc.). Namespace and
//! database users are explicitly excluded because they represent tenants in a
//! multi-tenant deployment.

use std::convert::Infallible;

use axum::extract::FromRequestParts;
use http::request::Parts;
use surrealdb_core::dbs::Session;

/// Marker extractor that is `true` when the request carries a root-level
/// authenticated session.
#[derive(Clone, Copy, Debug)]
pub struct OperatorAuth(pub bool);

impl<S> FromRequestParts<S> for OperatorAuth
where
	S: Send + Sync,
{
	type Rejection = Infallible;

	async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
		let is_operator =
			parts.extensions.get::<Session>().map(|s| s.au.is_root()).unwrap_or(false);
		Ok(OperatorAuth(is_operator))
	}
}
