use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Result, bail};
use axum::body::Body;
use axum::response::IntoResponse;
use axum::{Extension, RequestPartsExt};
use axum_extra::TypedHeader;
use axum_extra::headers::authorization::{Basic, Bearer};
use axum_extra::headers::{Authorization, Origin};
use futures_util::future::BoxFuture;
use http::header::{AUTHORIZATION, COOKIE, ORIGIN, SET_COOKIE};
use http::request::Parts;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use hyper::{Request, Response};
use surrealdb_catalog::providers::{AuthorisationProvider, DatabaseProvider};
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::is_expired_token_error;
use surrealdb_core::iam::verify::{basic, token};
use surrealdb_core::kvs::Datastore;
use surrealdb_datastore::TransactionType;
use surrealdb_observe::HttpRequestEventCtx;
use surrealdb_rpc::Token;
use surrealdb_types::SurrealValue;
use tower::{Layer, Service};
use uuid::Uuid;

use super::AppState;
use super::client_ip::ExtractClientIP;
use super::headers::{
	SurrealAuthDatabase, SurrealAuthNamespace, SurrealDatabase, SurrealId, SurrealNamespace,
	parse_typed_header,
};
use super::scope::PathScope;
use crate::ntw::error::Error as NetError;

/// Tower layer applying [`SurrealAuthService`] to a wrapped service.
///
/// Mounted between the outer HTTP metrics layer and the route handlers in
/// [`crate::ntw::SurrealRouter`]. Replaces `tower_http`'s
/// `AsyncRequireAuthorizationLayer<SurrealAuth>` so the service can both
/// authenticate the request and stamp the resulting [`HttpRequestEventCtx`]
/// onto the response before it propagates back through the metrics layer.
#[derive(Clone, Copy)]
pub(super) struct SurrealAuthLayer;

impl<S> Layer<S> for SurrealAuthLayer {
	type Service = SurrealAuthService<S>;

	fn layer(&self, inner: S) -> Self::Service {
		SurrealAuthService {
			inner,
		}
	}
}

/// Authentication middleware running [`check_auth`] on every request and
/// short-circuiting failures with a 401 response.
///
/// On success, the authenticated [`Session`] is inserted into the request
/// extensions for downstream handlers and the derived
/// [`HttpRequestEventCtx`] is inserted into the response extensions on the
/// way out so the outer HTTP metrics tower layer can attribute the
/// [`HttpRequestEvent`] and any [`NetworkBytesEvent`]s per
/// `(namespace, database, user, session_id, client_ip)`. Auth-failed and
/// anonymous requests carry no ctx; the metrics layer falls back to
/// [`HttpRequestEventCtx::default`] in that case.
#[derive(Clone)]
pub(super) struct SurrealAuthService<S> {
	inner: S,
}

impl<S> Service<Request<Body>> for SurrealAuthService<S>
where
	S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
	S::Future: Send + 'static,
	S::Error: Send + 'static,
{
	type Response = Response<Body>;
	type Error = S::Error;
	type Future = BoxFuture<'static, Result<Response<Body>, S::Error>>;

	fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		self.inner.poll_ready(cx)
	}

	fn call(&mut self, request: Request<Body>) -> Self::Future {
		// Standard tower idiom: the service captured by the future must be
		// the one that has been `poll_ready`'d, not the placeholder we
		// leave behind in `self`.
		let clone = self.inner.clone();
		let mut inner = std::mem::replace(&mut self.inner, clone);
		Box::pin(async move {
			let (mut parts, body) = request.into_parts();
			// A presigned bucket URL is its own credential. The browser's
			// ambient ones are not read, so they cannot refuse it.
			let use_credentials = !super::bucket::presents_signature(&parts.uri);
			let mut checked = check_auth(&mut parts, use_credentials).await;
			// The credential-exchange routes are how a client replaces an
			// expired token, so an expired bearer token on one of them is
			// ignored and the request is authenticated as a fresh, anonymous
			// session. Any other failure, and every other route, is refused.
			if is_credential_exchange(&parts)
				&& let Err(err) = &checked
				&& is_expired_token_error(err)
			{
				checked = check_auth(&mut parts, false).await;
			}
			match checked {
				Ok((sess, clear_cookie)) => {
					// Build the per-tenant ctx from the authenticated
					// session before we hand the request down. We stash
					// it on the response on the way out so the outer
					// metrics layer can attribute the per-request
					// telemetry (HttpRequestEvent + NetworkBytesEvent)
					// without reaching back into request state.
					let ctx = HttpRequestEventCtx::from(&sess);
					parts.extensions.insert(sess);
					let mut response = inner.call(Request::from_parts(parts, body)).await?;
					response.extensions_mut().insert(ctx);
					// A cookie the session could not be built from is cleared,
					// unless the handler already set one (a fresh sign-in).
					if clear_cookie && !sets_session_cookie(response.headers()) {
						response.headers_mut().append(SET_COOKIE, clear_session_cookie());
					}
					Ok(response)
				}
				Err(err) => {
					// A scope the path and a header name differently is a malformed
					// request, not a failed authentication.
					if let Some(&NetError::ScopeConflict(level)) = err.downcast_ref::<NetError>() {
						debug!("Request scope rejected: {err}");
						return Ok(NetError::ScopeConflict(level).into_response());
					}
					debug!("Request authentication failed: {err}");
					let unauthorized = Response::builder()
						.status(StatusCode::UNAUTHORIZED)
						.body(Body::from("There was a problem with authentication"))
						.unwrap_or_else(|_| {
							let mut resp = Response::new(Body::empty());
							*resp.status_mut() = StatusCode::UNAUTHORIZED;
							resp
						});
					Ok(unauthorized)
				}
			}
		})
	}
}

/// Whether the request is to a route that exchanges credentials for a token.
fn is_credential_exchange(parts: &Parts) -> bool {
	parts.method == http::Method::POST
		&& matches!(
			parts.uri.path(),
			"/signin" | "/signup" | "/signin/cookie" | "/signup/cookie" | "/refresh/cookie"
		)
}

/// Whether the request is to the route that ends a cookie session.
fn is_signout(parts: &Parts) -> bool {
	parts.method == http::Method::POST && parts.uri.path() == "/signout"
}

/// Authenticates the request from its headers. With `use_credentials` false
/// neither the `Authorization` header nor the session cookie is read, and the
/// session is the anonymous one the `Surreal-*` headers select.
///
/// The flag returned with the session is whether the request carried a session
/// cookie that must be cleared on the response.
async fn check_auth(parts: &mut Parts, use_credentials: bool) -> Result<(Session, bool)> {
	let or = match parts.extract::<TypedHeader<Origin>>().await {
		Ok(or) => {
			if !or.is_null() {
				Some(or.to_string())
			} else {
				None
			}
		}
		_ => None,
	};

	// Extract the session id from the headers or generate a new one.
	let id = match parse_typed_header::<SurrealId>(parts.extract::<TypedHeader<SurrealId>>().await)?
	{
		Some(id) => {
			// Attempt to parse the request id as a UUID.
			match Uuid::try_parse(&id) {
				// The specified request id was a valid UUID.
				Ok(id) => Some(id),
				// The specified request id was not a valid UUID.
				Err(_) => bail!(NetError::Request),
			}
		}
		// No request id was specified, create a new id.
		None => Some(Uuid::new_v4()),
	};

	// Extract the namespace from the headers.
	let ns = parse_typed_header::<SurrealNamespace>(
		parts.extract::<TypedHeader<SurrealNamespace>>().await,
	)?;

	// Extract the database from the headers.
	let db = parse_typed_header::<SurrealDatabase>(
		parts.extract::<TypedHeader<SurrealDatabase>>().await,
	)?;

	// A scope in the URL path selects the namespace and database. A header that
	// names a different one is refused rather than overridden.
	let scope = parts.extensions.get::<PathScope>().cloned();
	let ns = resolve_scope("namespace", scope.as_ref().and_then(|s| s.ns.clone()), ns)?;
	let db = resolve_scope("database", scope.as_ref().and_then(|s| s.db.clone()), db)?;

	// Extract the authentication namespace and database from the headers.
	let auth_ns = parse_typed_header::<SurrealAuthNamespace>(
		parts.extract::<TypedHeader<SurrealAuthNamespace>>().await,
	)?;
	let auth_db = parse_typed_header::<SurrealAuthDatabase>(
		parts.extract::<TypedHeader<SurrealAuthDatabase>>().await,
	)?;

	let Extension(state) = parts.extract::<Extension<AppState>>().await.map_err(|err| {
		tracing::error!("Error extracting the app state: {:?}", err);
		NetError::InvalidAuth
	})?;

	let kvs = &state.datastore;

	let cookie_policy = parts.extensions.get::<CookiePolicy>().cloned();

	let ExtractClientIP(ip) =
		parts.extract_with_state(&state).await.unwrap_or(ExtractClientIP(None));

	// Create session
	let mut session = Session {
		ip,
		or,
		id,
		ns,
		db,
		..Session::default()
	};

	// If Basic authentication data was supplied
	if use_credentials && let Ok(au) = parts.extract::<TypedHeader<Authorization<Basic>>>().await {
		basic(
			kvs,
			&mut session,
			au.username(),
			au.password(),
			auth_ns.as_deref(),
			auth_db.as_deref(),
		)
		.await?;
	};

	// If Token authentication data was supplied
	if use_credentials && let Ok(au) = parts.extract::<TypedHeader<Authorization<Bearer>>>().await {
		token(kvs, &mut session, au.token()).await?;
	};

	// Session cookie. Contract:
	// - The cookie is read only when cookie authentication is enabled, no `Authorization` header is
	//   present, and the request passes the browser origin gate. A cookie the gate does not admit
	//   is ignored, never an error: the session stays the anonymous one the `Surreal-*` headers
	//   select.
	// - A cookie whose token fails verification for any reason (expired, revoked access method, bad
	//   signature) also yields the anonymous session and flags the cookie for clearing. Unlike a
	//   bad bearer token this is not a 401: the cookie is `HttpOnly`, so scripts cannot remove it,
	//   and refusing every request would lock the user out until it expires.
	// - The credential-exchange routes and `/signout` never read the cookie, so a dead cookie
	//   cannot block the request that replaces or clears it.
	// It sits before the check below so that applies to cookie sessions
	// exactly as it does to bearer ones.
	let mut clear_cookie = false;
	if use_credentials
		&& let Some(policy) = cookie_policy.as_ref()
		&& policy.enabled
		&& !is_credential_exchange(parts)
		&& !is_signout(parts)
		&& !parts.headers.contains_key(AUTHORIZATION)
		&& let Some(value) = cookie_value(&parts.headers, SESSION_COOKIE)
	{
		match browser_origin_allowed(&parts.headers, &policy.origins) {
			Gate::Allowed => {
				if token(kvs, &mut session, value).await.is_err() {
					debug!("Ignoring the session cookie: its token failed verification");
					clear_cookie = true;
				}
			}
			Gate::Denied => {
				debug!("Ignoring the session cookie: the request origin is not allowed");
			}
			Gate::NotBrowser => {
				debug!("Ignoring the session cookie: the request carries no browser origin");
			}
		}
	}

	// Credentials must belong to the namespace and database the URL path names.
	// A token selects its own namespace and database, and Basic authentication
	// against `surreal-auth-ns`/`surreal-auth-db` yields a principal of another
	// tenant; either way a mismatch is refused, because the path states where
	// the request starts. The path does not confine the session: a `USE` may
	// select another namespace or database afterwards.
	if let Some(scope) = &scope
		&& !scope.admits(&session)
	{
		bail!(NetError::InvalidAuth);
	}

	// SECURITY (GHSA-2v9j): the `Surreal-NS`/`Surreal-DB` headers select the
	// session's namespace/database independently of the authenticated level —
	// Basic auth verifies credentials against `Surreal-Auth-NS`/`Surreal-Auth-DB`,
	// and a token carries its own ns/db. A Database- or Namespace-level principal
	// must not be able to point the selected context at a foreign tenant and
	// query it without a `USE`, so reject a context the authenticated level is
	// not permitted to access. Root and anonymous (`Level::No`) principals are
	// unrestricted by `can_access_ns_db`; the check only applies once both a
	// namespace and database are selected.
	if let (Some(ns), Some(db)) = (session.ns.as_deref(), session.db.as_deref())
		&& !session.au.can_access_ns_db(ns, db)
	{
		bail!(NetError::InvalidAuth);
	}

	Ok((session, clear_cookie))
}

/// Chooses the namespace or database for a request from its URL path and its
/// header. Either one alone is used; both must agree.
fn resolve_scope(
	level: &'static str,
	path: Option<String>,
	header: Option<String>,
) -> Result<Option<String>> {
	match (path, header) {
		(Some(path), Some(header)) if path != header => bail!(NetError::ScopeConflict(level)),
		(Some(path), _) => Ok(Some(path)),
		(None, header) => Ok(header),
	}
}

/// The name of the cookie carrying the access token.
const SESSION_COOKIE: &str = "surreal_token";

/// The name of the cookie carrying the refresh token.
const REFRESH_COOKIE: &str = "surreal_refresh";

/// The cookie attributes shared by issuing and clearing the session cookie.
const COOKIE_ATTRIBUTES: &str = "HttpOnly; Secure; SameSite=Strict; Path=/";

/// Cookie authentication settings, shared with the auth layer and the
/// credential handlers as a request extension.
///
/// `origins` is the CORS allowlist, held once and cloned by reference count
/// per request. Cookie authentication is only ever enabled together with a
/// non-empty allowlist.
#[derive(Clone, Debug)]
pub(super) struct CookiePolicy {
	pub enabled: bool,
	pub origins: Arc<[HeaderValue]>,
}

/// The verdict of the browser origin gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Gate {
	/// The request comes from the server's own origin or an allowlisted one.
	Allowed,
	/// The request comes from a browser context outside the allowlist.
	Denied,
	/// The request carries no browser provenance headers.
	NotBrowser,
}

/// Decides whether a cookie credential may be used for, or issued to, a request.
///
/// This is the origin policy the CORS layer applies, enforced on the server
/// side so a request that bypasses preflight (a "simple" cross-site `POST`, a
/// WebSocket upgrade) cannot ride on the cookie:
/// - `Sec-Fetch-Site: same-origin` is allowed.
/// - Otherwise a non-null `Origin` is allowed only when it exactly matches an allowlist entry; a
///   sibling subdomain or other scheme/port does not match.
/// - Otherwise a request that carries `Sec-Fetch-Site` but no usable `Origin` is a browser context
///   and is denied.
/// - A request with neither header is not from a browser.
pub(super) fn browser_origin_allowed(headers: &HeaderMap, allow_origin: &[HeaderValue]) -> Gate {
	let fetch_site = headers.get(&SEC_FETCH_SITE);
	if fetch_site.is_some_and(|v| v.as_bytes() == b"same-origin") {
		return Gate::Allowed;
	}
	match headers.get(ORIGIN) {
		Some(origin) if origin.as_bytes() != b"null" => {
			if allow_origin.iter().any(|o| o == origin) {
				Gate::Allowed
			} else {
				Gate::Denied
			}
		}
		_ if fetch_site.is_some() => Gate::Denied,
		_ => Gate::NotBrowser,
	}
}

static SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");

/// The cookies that carry server-issued credentials. They are removed from any
/// request headers handed to user-defined code, so an `HttpOnly` credential
/// can never be echoed back into a response body a script can read.
const CREDENTIAL_COOKIES: &[&str] = &[SESSION_COOKIE, REFRESH_COOKIE];

/// Removes the credential cookies from the request's `Cookie` headers, keeping
/// every other cookie in its original order. A `Cookie` header left with no
/// cookies is removed.
pub(super) fn strip_credential_cookies(headers: &mut HeaderMap) {
	if !headers.contains_key(COOKIE) {
		return;
	}
	let kept: Vec<HeaderValue> = headers
		.get_all(COOKIE)
		.iter()
		.filter_map(|v| {
			let Ok(text) = v.to_str() else {
				// Not valid text, so not a cookie this server parses or issued
				return Some(v.clone());
			};
			let rest: Vec<&str> = text
				.split(';')
				.map(str::trim)
				.filter(|pair| {
					let name = pair.split_once('=').map_or(*pair, |(n, _)| n).trim();
					!pair.is_empty() && !CREDENTIAL_COOKIES.contains(&name)
				})
				.collect();
			if rest.is_empty() {
				None
			} else {
				HeaderValue::from_str(&rest.join("; ")).ok()
			}
		})
		.collect();
	headers.remove(COOKIE);
	for value in kept {
		headers.append(COOKIE, value);
	}
}

/// The value of the cookie `name` in the request's `Cookie` headers.
///
/// The name must match exactly (`surreal_token2` is a different cookie). The
/// first occurrence wins, an empty value counts as absent, and a value wrapped
/// in double quotes is unquoted.
fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
	headers
		.get_all(COOKIE)
		.iter()
		.filter_map(|v| v.to_str().ok())
		.flat_map(|v| v.split(';'))
		.filter_map(|pair| pair.trim().split_once('='))
		.find(|(n, _)| n.trim() == name)
		.map(|(_, v)| {
			let v = v.trim();
			v.strip_prefix('"').and_then(|v| v.strip_suffix('"')).unwrap_or(v)
		})
		.filter(|v| !v.is_empty())
}

/// Requires cookie authentication to be on and the browser origin gate to
/// allow the request. With the feature off the cookie routes do not exist.
pub(super) fn require_allowed_origin(
	policy: &CookiePolicy,
	headers: &HeaderMap,
	path: &str,
) -> Result<(), NetError> {
	if !policy.enabled {
		return Err(NetError::NotFound(path.to_string()));
	}
	if browser_origin_allowed(headers, &policy.origins) == Gate::Allowed {
		Ok(())
	} else {
		Err(NetError::ForbiddenOrigin)
	}
}

/// The `Set-Cookie` value that stores the access token of a new session.
///
/// `Max-Age` is the time until the earlier of the access token's own `exp`
/// claim and the session's expiry. The session's expiry alone is not enough:
/// it is the session duration, which can outlive the token. A cookie is never
/// issued without a bound, so a token with no `exp` on a session with no
/// expiry is refused.
pub(super) fn session_cookie(token: &Token, session: &Session) -> Result<HeaderValue, NetError> {
	let access = match token {
		Token::Access(token) => token,
		Token::WithRefresh {
			access,
			..
		} => access,
	};
	let exp = match (token_exp(access), session.exp) {
		(Some(a), Some(b)) => a.min(b),
		(Some(e), None) | (None, Some(e)) => e,
		(None, None) => return Err(NetError::CookieUnbounded),
	};
	let now = web_time::SystemTime::now()
		.duration_since(web_time::UNIX_EPOCH)
		.map(|d| d.as_secs() as i64)
		.unwrap_or_default();
	let max_age = exp.saturating_sub(now).max(0);
	HeaderValue::try_from(format!(
		"{SESSION_COOKIE}={access}; {COOKIE_ATTRIBUTES}; Max-Age={max_age}"
	))
	.map_err(|_| NetError::CookieUnbounded)
}

/// The `Set-Cookie` value that removes the session cookie.
pub(super) fn clear_session_cookie() -> HeaderValue {
	HeaderValue::from_static("surreal_token=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0")
}

/// Whether the response already carries a `Set-Cookie` for the session cookie itself. Other
/// cookies in `Set-Cookie` do not count: a cookie name must match exactly, so `surreal_token2=`
/// is not the session cookie.
fn sets_session_cookie(headers: &HeaderMap) -> bool {
	headers.get_all(SET_COOKIE).iter().any(|value| {
		value
			.to_str()
			.ok()
			.and_then(|v| v.strip_prefix(SESSION_COOKIE))
			.is_some_and(|rest| rest.starts_with('='))
	})
}

/// The refresh cookie's payload: which record access the refresh key belongs
/// to, and the key itself. Carried as unpadded base64url of its compact JSON so
/// renewal and revocation need no request body.
///
/// Every field is client-controlled when the cookie comes back in, so it is
/// untrusted input: core verifies the key against the stored grant before
/// anything is rotated or revoked, and a decoded value proves nothing.
#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct RefreshCookie {
	pub ns: String,
	pub db: String,
	pub ac: String,
	pub rt: String,
}

impl RefreshCookie {
	fn encode(&self) -> Option<String> {
		serde_json::to_vec(self).ok().map(|json| base64url_encode(&json))
	}

	fn decode(value: &str) -> Option<Self> {
		serde_json::from_slice(&base64url_decode(value)?).ok()
	}

	/// The refresh cookie of the request, if it carries one that decodes.
	pub(super) fn from_headers(headers: &HeaderMap) -> Option<Self> {
		Self::decode(cookie_value(headers, REFRESH_COOKIE)?)
	}
}

/// The `Set-Cookie` value that stores a refresh token.
///
/// `Max-Age` is the lifetime of the refresh grant, so the cookie never outlives
/// the key it carries. The value is only ever read by `POST /refresh/cookie`
/// and `POST /signout`; the auth layer does not read it.
pub(super) fn refresh_cookie(scope: &RefreshCookie, max_age: u64) -> Result<HeaderValue, NetError> {
	let value = scope.encode().ok_or(NetError::CookieUnbounded)?;
	HeaderValue::try_from(format!(
		"{REFRESH_COOKIE}={value}; {COOKIE_ATTRIBUTES}; Max-Age={max_age}"
	))
	.map_err(|_| NetError::CookieUnbounded)
}

/// The `Set-Cookie` value that removes the refresh cookie.
pub(super) fn clear_refresh_cookie() -> HeaderValue {
	HeaderValue::from_static(
		"surreal_refresh=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
	)
}

/// The lifetime of the refresh grants a record access issues, in seconds,
/// read from the catalog definition.
///
/// This is the duration `create_refresh_token_record` gives each grant, so it
/// is the `Max-Age` of the refresh cookie. `None` when the access method is
/// not defined or its grants do not expire; no refresh cookie is issued then,
/// because a refresh cookie is never issued without a bound.
async fn refresh_grant_lifetime(kvs: &Datastore, ns: &str, db: &str, ac: &str) -> Option<u64> {
	let tx = kvs.transaction(TransactionType::Read).await.ok()?;
	let def = async {
		let db_def = tx.get_db_by_name(ns, db, None).await.ok().flatten()?;
		tx.get_db_access(db_def.namespace_id, db_def.database_id, ac, None).await.ok().flatten()
	}
	.await;
	let _ = tx.cancel().await;
	let secs = def?.grant_duration?.as_secs();
	(secs > 0).then_some(secs)
}

/// The `Set-Cookie` headers for a successful sign-in, sign-up or refresh: the
/// access cookie, plus the refresh cookie when the token carries a refresh
/// token and the access method's grants expire.
///
/// Both are appended, never inserted, so both survive. `scope` names the
/// record access the refresh token was issued for.
pub(super) async fn token_cookies(
	kvs: &Datastore,
	token: &Token,
	session: &Session,
	scope: Option<(&str, &str, &str)>,
) -> Result<HeaderMap, NetError> {
	let mut out = HeaderMap::new();
	out.append(SET_COOKIE, session_cookie(token, session)?);
	if let Token::WithRefresh {
		refresh,
		..
	} = token
		&& let Some((ns, db, ac)) = scope
	{
		if let Some(max_age) = refresh_grant_lifetime(kvs, ns, db, ac).await {
			let cookie = RefreshCookie {
				ns: ns.to_string(),
				db: db.to_string(),
				ac: ac.to_string(),
				rt: refresh.clone(),
			};
			out.append(SET_COOKIE, refresh_cookie(&cookie, max_age)?);
		} else {
			debug!("Not issuing a refresh cookie: the access method's grants do not expire");
		}
	}
	Ok(out)
}

/// The `(ns, db, ac)` a session was authenticated into, when it names all three.
pub(super) fn session_scope(session: &Session) -> Option<(&str, &str, &str)> {
	Some((session.ns.as_deref()?, session.db.as_deref()?, session.ac.as_deref()?))
}

/// The `exp` claim of a server-issued token, read without verification.
fn token_exp(token: &str) -> Option<i64> {
	let payload = token.split('.').nth(1)?;
	let bytes = base64url_decode(payload)?;
	serde_json::from_slice::<serde_json::Value>(&bytes).ok()?.get("exp")?.as_i64()
}

/// Encodes unpadded base64url.
fn base64url_encode(input: &[u8]) -> String {
	const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
	let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
	for chunk in input.chunks(3) {
		let n =
			chunk.iter().enumerate().fold(0u32, |n, (i, b)| n | (u32::from(*b) << (16 - 8 * i)));
		let chars = chunk.len() + 1;
		for i in 0..chars {
			out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
		}
	}
	out
}

/// Decodes unpadded base64url.
fn base64url_decode(input: &str) -> Option<Vec<u8>> {
	let mut out = Vec::with_capacity(input.len() * 3 / 4);
	let (mut acc, mut bits) = (0u32, 0u32);
	for c in input.bytes() {
		let v = match c {
			b'A'..=b'Z' => c - b'A',
			b'a'..=b'z' => c - b'a' + 26,
			b'0'..=b'9' => c - b'0' + 52,
			b'-' => 62,
			b'_' => 63,
			_ => return None,
		};
		acc = (acc << 6) | u32::from(v);
		bits += 6;
		if bits >= 8 {
			bits -= 8;
			out.push((acc >> bits) as u8);
			acc &= (1 << bits) - 1;
		}
	}
	Some(out)
}

/// The acknowledgement returned instead of a token when the token is issued as
/// a cookie. It carries no credential in any format.
#[derive(SurrealValue)]
struct CookieSuccess {
	code: u16,
	details: String,
}

/// The response body for a successful sign-in or sign-up whose token went into
/// the session cookie. JSON, CBOR and flatbuffers carry `{code, details}` only;
/// plain text is empty. A refresh token travels only in its own cookie.
pub(super) fn cookie_output(
	accept: Option<&super::headers::Accept>,
) -> Result<super::output::Output, NetError> {
	use super::headers::Accept;
	use super::output::Output;
	let ack = || CookieSuccess {
		code: 200,
		details: String::from("Authentication succeeded"),
	};
	match accept {
		Some(Accept::ApplicationJson) => {
			Ok(Output::json_other(&ack().into_value().into_json_value()))
		}
		Some(Accept::ApplicationCbor) => Ok(Output::cbor(ack().into_value())),
		Some(Accept::TextPlain) => Ok(Output::Text(String::new())),
		Some(Accept::ApplicationFlatbuffers) => Ok(Output::flatbuffers(&ack().into_value())),
		None => Ok(Output::None),
		_ => Err(NetError::InvalidType),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn origins() -> Vec<HeaderValue> {
		vec![HeaderValue::from_static("http://app.test")]
	}

	fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
		let mut map = HeaderMap::new();
		for (name, value) in pairs {
			map.append(HeaderName::from_static(name), HeaderValue::from_static(value));
		}
		map
	}

	#[test]
	fn gate_same_origin_is_allowed() {
		let h = headers(&[("sec-fetch-site", "same-origin")]);
		assert_eq!(browser_origin_allowed(&h, &origins()), Gate::Allowed);
		// Even with an empty allowlist: the server's own origin needs no entry.
		assert_eq!(browser_origin_allowed(&h, &[]), Gate::Allowed);
	}

	#[test]
	fn gate_matches_origin_exactly() {
		let allowed = headers(&[("origin", "http://app.test")]);
		assert_eq!(browser_origin_allowed(&allowed, &origins()), Gate::Allowed);
		for foreign in [
			"http://evil.test",
			"http://sub.app.test",
			"https://app.test",
			"http://app.test:8080",
			"http://app.test/",
		] {
			let mut h = HeaderMap::new();
			h.insert(ORIGIN, HeaderValue::from_str(foreign).unwrap());
			assert_eq!(browser_origin_allowed(&h, &origins()), Gate::Denied, "{foreign}");
		}
	}

	#[test]
	fn gate_foreign_origin_beats_same_site() {
		let h = headers(&[("origin", "http://evil.test"), ("sec-fetch-site", "cross-site")]);
		assert_eq!(browser_origin_allowed(&h, &origins()), Gate::Denied);
	}

	#[test]
	fn gate_fetch_site_without_origin_is_denied() {
		for site in ["same-site", "cross-site", "none"] {
			let mut h = HeaderMap::new();
			h.insert(SEC_FETCH_SITE.clone(), HeaderValue::from_str(site).unwrap());
			assert_eq!(browser_origin_allowed(&h, &origins()), Gate::Denied, "{site}");
		}
		// A null origin is not usable either.
		let h = headers(&[("origin", "null"), ("sec-fetch-site", "cross-site")]);
		assert_eq!(browser_origin_allowed(&h, &origins()), Gate::Denied);
	}

	#[test]
	fn gate_without_browser_headers_is_not_a_browser() {
		assert_eq!(browser_origin_allowed(&HeaderMap::new(), &origins()), Gate::NotBrowser);
		let h = headers(&[("origin", "null")]);
		assert_eq!(browser_origin_allowed(&h, &origins()), Gate::NotBrowser);
	}

	fn cookie_headers(values: &[&str]) -> HeaderMap {
		let mut map = HeaderMap::new();
		for v in values {
			map.append(COOKIE, HeaderValue::from_str(v).unwrap());
		}
		map
	}

	#[test]
	fn cookie_is_found_among_several() {
		let h = cookie_headers(&["a=1; surreal_token=abc.def; b=2"]);
		assert_eq!(cookie_value(&h, SESSION_COOKIE), Some("abc.def"));
		let h = cookie_headers(&["a=1", "surreal_token=xyz"]);
		assert_eq!(cookie_value(&h, SESSION_COOKIE), Some("xyz"));
	}

	#[test]
	fn cookie_name_must_match_exactly() {
		for header in [
			"surreal_token2=abc",
			"xsurreal_token=abc",
			"surreal_tokens=abc",
			"surreal_token_old=abc; other=1",
		] {
			let h = cookie_headers(&[header]);
			assert_eq!(cookie_value(&h, SESSION_COOKIE), None, "{header}");
		}
		let h = cookie_headers(&["surreal_token2=bad; surreal_token=good"]);
		assert_eq!(cookie_value(&h, SESSION_COOKIE), Some("good"));
	}

	#[test]
	fn strip_credential_cookies_keeps_other_cookies() {
		let mut h = headers(&[
			("cookie", "a=1; surreal_token=secret; surreal_token2=x"),
			("cookie", "surreal_token=other"),
			("authorization", "Bearer keep"),
		]);
		strip_credential_cookies(&mut h);
		let cookies: Vec<_> = h.get_all(COOKIE).iter().map(|v| v.to_str().unwrap()).collect();
		assert_eq!(cookies, ["a=1; surreal_token2=x"]);
		assert!(h.contains_key(AUTHORIZATION));
		let mut only = headers(&[("cookie", "surreal_token=secret")]);
		strip_credential_cookies(&mut only);
		assert!(!only.contains_key(COOKIE));
	}

	#[test]
	fn cookie_value_handles_quotes_and_whitespace() {
		let h = cookie_headers(&["  surreal_token = \"abc\"  ;other=1"]);
		assert_eq!(cookie_value(&h, SESSION_COOKIE), Some("abc"));
		let h = cookie_headers(&["surreal_token="]);
		assert_eq!(cookie_value(&h, SESSION_COOKIE), None);
		let h = cookie_headers(&["surreal_token"]);
		assert_eq!(cookie_value(&h, SESSION_COOKIE), None);
		assert_eq!(cookie_value(&HeaderMap::new(), SESSION_COOKIE), None);
	}

	#[test]
	fn token_exp_reads_the_payload_claim() {
		// {"alg":"HS256","typ":"JWT"}.{"exp":1700000000,"ns":"n"}.sig
		let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDAwMDAwMDAsIm5zIjoibiJ9.sig";
		assert_eq!(token_exp(token), Some(1_700_000_000));
		assert_eq!(token_exp("not-a-jwt"), None);
		assert_eq!(token_exp("a.!!!.c"), None);
	}

	#[test]
	fn session_cookie_carries_every_attribute_and_a_max_age() {
		let token = Token::Access("eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjQxMDI0NDQ4MDB9.sig".to_string());
		let cookie = session_cookie(&token, &Session::default()).unwrap();
		let cookie = cookie.to_str().unwrap();
		assert!(cookie.starts_with("surreal_token=eyJ"));
		for attr in ["HttpOnly", "Secure", "SameSite=Strict", "Path=/", "Max-Age="] {
			assert!(cookie.contains(attr), "{cookie}");
		}
		// An unbounded token on an unbounded session is refused.
		let unbounded = Token::Access("eyJhbGciOiJIUzI1NiJ9.e30.sig".to_string());
		assert!(session_cookie(&unbounded, &Session::default()).is_err());
	}

	#[test]
	fn base64url_round_trips() {
		for input in
			[&b""[..], b"f", b"fo", b"foo", b"foob", b"fooba", b"foobar", &[0xfb, 0xff, 0xfe]]
		{
			let encoded = base64url_encode(input);
			assert!(!encoded.contains('='), "{encoded}");
			assert_eq!(base64url_decode(&encoded).unwrap(), input);
		}
		assert_eq!(base64url_encode(b"foobar"), "Zm9vYmFy");
		assert_eq!(base64url_encode(&[0xfb, 0xff, 0xfe]), "-__-");
	}

	#[test]
	fn refresh_cookie_value_round_trips() {
		let cookie = RefreshCookie {
			ns: "n".into(),
			db: "d".into(),
			ac: "a".into(),
			rt: "sdb-refresh-id-key".into(),
		};
		let value = cookie.encode().unwrap();
		let h = cookie_headers(&[&format!("x=1; {REFRESH_COOKIE}={value}")]);
		let back = RefreshCookie::from_headers(&h).unwrap();
		assert_eq!((back.ns.as_str(), back.db.as_str()), ("n", "d"));
		assert_eq!((back.ac.as_str(), back.rt.as_str()), ("a", "sdb-refresh-id-key"));
		// Not base64url, or not the expected JSON, is absent rather than an error.
		assert!(RefreshCookie::decode("!!!").is_none());
		assert!(RefreshCookie::decode(&base64url_encode(b"{\"ns\":\"n\"}")).is_none());
		assert!(RefreshCookie::decode(&base64url_encode(b"not json")).is_none());
	}

	#[test]
	fn refresh_cookie_carries_every_attribute() {
		let scope = RefreshCookie {
			ns: "n".into(),
			db: "d".into(),
			ac: "a".into(),
			rt: "k".into(),
		};
		let cookie = refresh_cookie(&scope, 3600).unwrap();
		let cookie = cookie.to_str().unwrap();
		assert!(cookie.starts_with("surreal_refresh="), "{cookie}");
		for attr in ["HttpOnly", "Secure", "SameSite=Strict", "Path=/", "Max-Age=3600"] {
			assert!(cookie.split(';').any(|p| p.trim() == attr), "{attr} missing in {cookie}");
		}
		assert_eq!(
			clear_refresh_cookie().to_str().unwrap(),
			"surreal_refresh=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0"
		);
	}

	#[test]
	fn strip_credential_cookies_removes_the_refresh_cookie() {
		let mut h = headers(&[("cookie", "a=1; surreal_refresh=secret; surreal_token=t")]);
		strip_credential_cookies(&mut h);
		let cookies: Vec<_> = h.get_all(COOKIE).iter().map(|v| v.to_str().unwrap()).collect();
		assert_eq!(cookies, ["a=1"]);
	}
}
