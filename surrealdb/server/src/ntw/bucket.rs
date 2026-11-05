//! The bucket routes: redeeming a presigned URL, and serving a bucket to an
//! ordinary session or a guest.
//!
//! The URL names the level the bucket is defined at, as every scoped route
//! does (see `ntw/scope.rs`):
//!
//! ```text
//! /bucket/{name}/{*path}                    a bucket on the root
//! /ns/{ns}/bucket/{name}/{*path}            a bucket on a namespace
//! /ns/{ns}/db/{db}/bucket/{name}/{*path}    a bucket on a database
//! ```
//!
//! The scoped forms reach these routes as `/bucket/{name}/…` with a
//! [`PathScope`], which is the only source of the level: the `surreal-ns` and
//! `surreal-db` headers never move a request to another bucket.
//!
//! - `GET | HEAD | PUT | DELETE …/bucket/{name}/{*path}` reads, writes and removes one object, as
//!   `file::get`, `file::put` and `file::delete` do.
//! - `POST …/bucket/{name}/{*path}` copies or renames it; the body names the action and the target.
//! - `GET …/bucket/{name}/` lists the bucket, taking `file::list`'s options as query parameters.
//!
//! Every request is judged by the bucket's `PERMISSIONS` clause, as the
//! function it mirrors is. Structured bodies and answers are JSON or CBOR, by
//! `Content-Type` and `Accept`.
//!
//! The level is in the path rather than in headers because a presigned URL
//! arrives from a browser with no session to carry it. The path is the
//! resource identifier; a session, when there is one, is only a credential.
//! A request that presents a signature is authenticated without its ambient
//! credentials at all (see [`presents_signature`]).
//!
//! Every decision about whether a request is allowed lives in
//! `surrealdb_core::buc::access`. This module owns HTTP and nothing else: it does
//! not hold the signing secret, and it computes no signature.

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::get;
use tower_http::limit::RequestBodyLimitLayer;

use crate::cnf::HTTP_MAX_BUCKET_BODY_SIZE;
#[cfg(doc)]
use crate::ntw::scope::PathScope;

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/bucket/{name}", get(implementation::list_objects))
		.route("/bucket/{name}/", get(implementation::list_objects))
		.route(
			"/bucket/{name}/{*path}",
			get(implementation::get_object)
				.head(implementation::head_object)
				.put(implementation::put_object)
				.delete(implementation::delete_object)
				.post(implementation::transfer_object),
		)
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_BUCKET_BODY_SIZE))
}

/// Whether a bucket request presents a presigned-URL signature, whole or
/// partial: a query parameter named `exp` or `sig`.
///
/// Such a request is authorised by its signature, and the auth layer
/// authenticates it without reading its `Authorization` header or session
/// cookie. A credential the browser attaches to every request (a cookie for
/// another tenant, an expired token) would otherwise refuse a URL that needs
/// no credential at all. The handler then refuses a signature it cannot
/// verify rather than falling back to the session.
pub(super) fn presents_signature(uri: &http::Uri) -> bool {
	uri.path().starts_with("/bucket/")
		&& uri.query().is_some_and(|query| {
			query.split('&').any(|pair| {
				let key = pair.split_once('=').map_or(pair, |(key, _)| key);
				matches!(key, "exp" | "sig")
			})
		})
}

mod implementation {
	use std::ops::Range;
	use std::sync::Arc;
	use std::sync::atomic::{AtomicBool, Ordering};

	use axum::Extension;
	use axum::body::Body;
	use axum::extract::{Path, Query, RawQuery, Request};
	use axum::response::{IntoResponse, Response};
	use axum_extra::TypedHeader;
	use axum_extra::typed_header::TypedHeaderRejection;
	use futures::TryStreamExt;
	use http::header::{
		ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_SECURITY_POLICY, CONTENT_TYPE, ETAG,
		IF_NONE_MATCH, LAST_MODIFIED, RANGE, X_CONTENT_TYPE_OPTIONS,
	};
	use http::{HeaderMap, StatusCode};
	use serde::Deserialize;
	use surrealdb_core::buc::BucketOperation;
	use surrealdb_core::buc::access::{
		BucketAccess, BucketAccessError, BucketAuth, BucketLocation, SignedQuery, TransferOutcome,
	};
	use surrealdb_core::buc::store::{
		ByteRange, ListEntry, ListOptions, NO_CONTENT_TYPE, ObjectMeta, normalise_content_type,
	};
	use surrealdb_core::dbs::Session;
	use surrealdb_rpc::capabilities::RouteTarget;
	use surrealdb_types::{Object as PublicObject, SurrealValue, Value as PublicValue};

	use crate::ntw::AppState;
	use crate::ntw::error::{Error as NetError, ResponseError};
	use crate::ntw::headers::{Accept, ContentType};
	use crate::ntw::scope::PathScope;

	/// The level a request's URL names: the scope it was rewritten from, or
	/// the root for an unscoped `/bucket/…` path.
	fn location(scope: Option<&PathScope>) -> BucketLocation<'_> {
		match scope.map(|s| (s.ns.as_deref(), s.db.as_deref())) {
			Some((Some(ns), Some(db))) => BucketLocation::Database(ns, db),
			Some((Some(ns), None)) => BucketLocation::Namespace(ns),
			_ => BucketLocation::Root,
		}
	}

	/// What a handler on this route can fail with.
	///
	/// A plain status is needed alongside the usual error mapping because three
	/// of the outcomes here are decided by HTTP semantics rather than by a
	/// fault: `410` for a URL that ran out, `403` for one that does not
	/// authorise this request, and `401` for a signature this build cannot
	/// reconstruct.
	pub enum RouteError {
		Status(StatusCode),
		Failed(ResponseError),
	}

	impl IntoResponse for RouteError {
		fn into_response(self) -> Response {
			match self {
				RouteError::Status(status) => status.into_response(),
				RouteError::Failed(e) => e.into_response(),
			}
		}
	}

	impl From<ResponseError> for RouteError {
		fn from(e: ResponseError) -> Self {
			RouteError::Failed(e)
		}
	}

	impl From<NetError> for RouteError {
		fn from(e: NetError) -> Self {
			RouteError::Failed(e.into())
		}
	}

	impl From<http::Error> for RouteError {
		fn from(e: http::Error) -> Self {
			RouteError::Failed(ResponseError(e.into()))
		}
	}

	/// Resolve and authorise, shared by every method on this route.
	///
	/// The order is the security-relevant part and is enforced in
	/// `buc::access::bucket_access`: resolve the bucket, authenticate, check
	/// expiry, then normalise the key.
	async fn access(
		state: &AppState,
		session: &Session,
		scope: Option<&PathScope>,
		(name, key): (String, String),
		query: Option<String>,
		op: BucketOperation,
		target: Option<&str>,
	) -> Result<BucketAccess, RouteError> {
		let ds = &state.datastore;

		if !ds.allows_http_route(&RouteTarget::Bucket) {
			warn!(
				"Capabilities denied HTTP route request attempt, target: '{}'",
				&RouteTarget::Bucket
			);
			return Err(NetError::ForbiddenRoute(RouteTarget::Bucket.to_string()).into());
		}

		// A signature wins when both are present. Presenting one is a statement
		// that this request is the presigned one, and silently preferring an
		// ambient session would serve a different set of permissions than the
		// URL was minted under.
		let auth = match parse_signed_query(query.as_deref()) {
			Ok(Some(signed)) => BucketAuth::Signature(signed),
			Ok(None) => BucketAuth::Session,
			Err(()) => return Err(RouteError::Status(StatusCode::UNAUTHORIZED)),
		};

		ds.bucket_access(session, location(scope), &name, &key, op, target, auth)
			.await
			.map_err(access_error)
	}

	/// The status an access refusal is reported with.
	fn access_error(e: BucketAccessError) -> RouteError {
		match e {
			// `410 Gone`, not `403`: the URL was genuine and simply ran out, so
			// the remedy is to mint a new one rather than to retry this one.
			BucketAccessError::Expired => RouteError::Status(StatusCode::GONE),
			// `403`, not `401`: a `401` obliges a `WWW-Authenticate` challenge,
			// and there is none to offer — a signed URL cannot be retried with
			// different credentials.
			BucketAccessError::Denied(_) => RouteError::Status(StatusCode::FORBIDDEN),
			// The bucket's backend cannot keep the media type the request
			// names. Refused rather than dropped, so the caller never believes
			// a type was stored.
			BucketAccessError::UnsupportedMediaType(_) => {
				RouteError::Status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
			}
			BucketAccessError::Other(e) => RouteError::Failed(ResponseError(e)),
		}
	}

	/// Read the signed parameters out of a raw query string.
	///
	/// `Ok(None)` means the request presented no signature at all and should be
	/// authenticated as a session. `Err(())` means it presented one that this
	/// build cannot reconstruct — an unknown parameter, a malformed number, a
	/// missing field — and the request is refused rather than retried as an
	/// unsigned one. Falling back would let a caller strip a signature it could
	/// not satisfy and be judged by a different rule.
	fn parse_signed_query(query: Option<&str>) -> Result<Option<SignedQuery>, ()> {
		let Some(query) = query.filter(|q| !q.is_empty()) else {
			return Ok(None);
		};
		let mut ct = None;
		let mut exp = None;
		let mut max = None;
		let mut ow = None;
		let mut sig = None;

		// Split rather than percent-decode. Every value the signature covers is
		// an integer, `false`, or base64url, all of which are in the unreserved
		// set and so are never encoded, except `ct`, which is passed on still
		// encoded and decoded beside the signature check. A value that did
		// arrive encoded otherwise fails to parse here, or fails to verify as a
		// signature — closed either way.
		for pair in query.split('&') {
			let Some((k, v)) = pair.split_once('=') else {
				return Err(());
			};
			match k {
				"ct" if ct.is_none() => ct = Some(v.to_owned()),
				"exp" if exp.is_none() => exp = Some(v.parse::<i64>().map_err(|_| ())?),
				"max" if max.is_none() => max = Some(v.parse::<u64>().map_err(|_| ())?),
				"ow" if ow.is_none() && v == "false" => ow = Some(false),
				"sig" if sig.is_none() => sig = Some(v.to_owned()),
				// An unknown, repeated, or non-canonical parameter is not
				// ignored: the signature covers the whole query string, so one
				// that verifies cannot carry any. Skipping it here would check
				// a different request than the one that arrived.
				_ => return Err(()),
			}
		}

		match (sig, exp) {
			(Some(sig), Some(exp)) => Ok(Some(SignedQuery {
				exp,
				max,
				overwrite: ow.unwrap_or(true),
				content_type: ct,
				sig,
			})),
			// A partial signature is a signature. Treating it as unsigned would
			// let a caller drop `sig` and be judged by the session rule instead.
			_ => Err(()),
		}
	}

	/// `Range: bytes=...`, for the single-range forms a media player sends.
	///
	/// A multi-range request is answered as if unranged rather than with a
	/// `multipart/byteranges` body: no browser sends one for media, and
	/// returning the whole object is a valid response to any range request.
	fn parse_range(headers: &HeaderMap) -> Option<ByteRange> {
		let raw = headers.get(RANGE)?.to_str().ok()?;
		let spec = raw.strip_prefix("bytes=")?.trim();
		if spec.contains(',') {
			return None;
		}

		let (start, end) = spec.split_once('-')?;
		match (start.trim(), end.trim()) {
			("", suffix) => Some(ByteRange::Suffix(suffix.parse().ok()?)),
			(start, "") => Some(ByteRange::Offset(start.parse().ok()?)),
			// HTTP ranges are inclusive at both ends; `ByteRange` is half-open.
			(start, end) => {
				let end: u64 = end.parse().ok()?;
				Some(ByteRange::Bounded(start.parse().ok()?..end.checked_add(1)?))
			}
		}
	}

	/// Headers every response about an object carries.
	///
	/// `nosniff` and a `sandbox` policy are sent whatever the object's type.
	/// Anyone allowed to upload chooses the type, and a guest or the holder of
	/// a presigned URL can be: without them, a stored `text/html` or
	/// `image/svg+xml` would run its script on the database's own origin, with
	/// the cookies of everyone signed in to it.
	fn object_headers(meta: &ObjectMeta) -> Vec<(http::HeaderName, String)> {
		let mut headers = vec![
			(ACCEPT_RANGES, "bytes".to_string()),
			(LAST_MODIFIED, meta.updated.format("%a, %d %b %Y %H:%M:%S GMT").to_string()),
			(X_CONTENT_TYPE_OPTIONS, "nosniff".to_string()),
			(CONTENT_SECURITY_POLICY, "sandbox".to_string()),
		];
		if let Some(etag) = &meta.etag {
			headers.push((ETAG, etag.clone()));
		}
		headers
	}

	/// The `Content-Type` an object is served with: the one stored with it, or
	/// `application/octet-stream`. Never guessed from the key.
	fn served_type(meta: &ObjectMeta) -> &str {
		meta.content_type.as_deref().unwrap_or(NO_CONTENT_TYPE)
	}

	/// Whether the client already holds this exact version.
	///
	/// Weak comparison, which is what `If-None-Match` requires: the local store
	/// emits a `W/`-prefixed validator, and rejecting it here would make every
	/// revalidation against a `file://` bucket a full re-transfer.
	fn is_fresh(headers: &HeaderMap, meta: &ObjectMeta) -> bool {
		let Some(presented) = headers.get(IF_NONE_MATCH).and_then(|v| v.to_str().ok()) else {
			return false;
		};

		// `*` means "if the object exists at all", which is what a client sends
		// when it holds a copy but no validator. Reaching here at all means it
		// does exist.
		if presented.trim() == "*" {
			return true;
		}

		let Some(etag) = meta.etag.as_deref() else {
			return false;
		};
		let strip = |t: &str| t.trim().trim_start_matches("W/").trim_matches('"').to_string();
		let ours = strip(etag);
		presented.split(',').any(|candidate| strip(candidate) == ours)
	}

	/// `304`, carrying the validators a client needs to revalidate again.
	fn not_modified(meta: &ObjectMeta) -> Result<Response, http::Error> {
		let mut res = Response::builder().status(StatusCode::NOT_MODIFIED);
		for (name, value) in object_headers(meta) {
			res = res.header(name, value);
		}
		res.body(Body::empty())
	}

	pub async fn get_object(
		Extension(state): Extension<AppState>,
		Extension(session): Extension<Session>,
		scope: Option<Extension<PathScope>>,
		Path(path): Path<(String, String)>,
		RawQuery(query): RawQuery,
		headers: HeaderMap,
	) -> Result<Response, RouteError> {
		let access =
			access(&state, &session, scope.as_deref(), path, query, BucketOperation::Get, None)
				.await?;

		// A conditional request is answered from metadata, before any object is
		// opened. Opening first would issue a real backend GET whose body is
		// then dropped -- billed on a cloud bucket, and booked as an abandoned
		// transfer by the metered store, on every cache revalidation.
		if headers.contains_key(IF_NONE_MATCH)
			&& let Some(meta) = access.head().await.map_err(ResponseError)?
			&& is_fresh(&headers, &meta)
		{
			return Ok(not_modified(&meta)?);
		}

		let requested = parse_range(&headers);
		let ranged = requested.is_some();

		let Some(stream) = access.get(requested).await.map_err(ResponseError)? else {
			// A range that cannot be satisfied is reported apart from a missing
			// object: the object may well exist, and a client that retries
			// without the header would get it.
			let status = if ranged && access.exists().await.map_err(ResponseError)? {
				StatusCode::RANGE_NOT_SATISFIABLE
			} else {
				StatusCode::NOT_FOUND
			};
			return Ok(status.into_response());
		};

		let Range {
			start,
			end,
		} = stream.range;
		let partial = start != 0 || end != stream.meta.size;

		let mut res = Response::builder().status(if partial {
			StatusCode::PARTIAL_CONTENT
		} else {
			StatusCode::OK
		});
		for (name, value) in object_headers(&stream.meta) {
			res = res.header(name, value);
		}
		res = res
			.header(CONTENT_TYPE, served_type(&stream.meta))
			.header(CONTENT_LENGTH, (end - start).to_string());
		if partial {
			res = res.header(
				CONTENT_RANGE,
				format!("bytes {start}-{}/{}", end.saturating_sub(1), stream.meta.size),
			);
		}

		let body = Body::from_stream(stream.stream.map_err(std::io::Error::other));
		Ok(res.body(body)?)
	}

	pub async fn head_object(
		Extension(state): Extension<AppState>,
		Extension(session): Extension<Session>,
		scope: Option<Extension<PathScope>>,
		Path(path): Path<(String, String)>,
		RawQuery(query): RawQuery,
		headers: HeaderMap,
	) -> Result<Response, RouteError> {
		let access =
			access(&state, &session, scope.as_deref(), path, query, BucketOperation::Head, None)
				.await?;

		let Some(meta) = access.head().await.map_err(ResponseError)? else {
			return Ok(StatusCode::NOT_FOUND.into_response());
		};

		// A `HEAD` is revalidated on the same terms as a `GET`, so a client can
		// check freshness without asking for a body it may not need.
		if is_fresh(&headers, &meta) {
			return Ok(not_modified(&meta)?);
		}

		let mut res = Response::builder().status(StatusCode::OK);
		for (name, value) in object_headers(&meta) {
			res = res.header(name, value);
		}
		Ok(res
			.header(CONTENT_LENGTH, meta.size.to_string())
			.header(CONTENT_TYPE, served_type(&meta))
			.body(Body::empty())?)
	}

	pub async fn put_object(
		Extension(state): Extension<AppState>,
		Extension(session): Extension<Session>,
		scope: Option<Extension<PathScope>>,
		Path(path): Path<(String, String)>,
		RawQuery(query): RawQuery,
		request: Request,
	) -> Result<Response, RouteError> {
		// The media type to store, read before anything else so a malformed
		// one is refused without authorising or reading the body. A missing
		// header and `application/octet-stream` both mean none.
		let declared = match request.headers().get(CONTENT_TYPE) {
			None => None,
			Some(v) => Some(
				v.to_str().map_err(|_| RouteError::Status(StatusCode::BAD_REQUEST))?.to_owned(),
			),
		};
		let content_type = normalise_content_type(declared)
			.map_err(|_| RouteError::Status(StatusCode::BAD_REQUEST))?;

		// `If-None-Match: *` asks for a write that never replaces an object,
		// decided as part of the commit. Any other value names a version, and
		// a conditional write against one is not supported.
		let if_absent = match request.headers().get(IF_NONE_MATCH).map(|v| v.to_str()) {
			None => false,
			Some(Ok(v)) if v.trim() == "*" => true,
			Some(_) => return Err(RouteError::Status(StatusCode::BAD_REQUEST)),
		};

		let access =
			access(&state, &session, scope.as_deref(), path, query, BucketOperation::Put, None)
				.await?;

		// The signature stops `max` being edited; this counter is what enforces
		// it. Erroring the stream is how the limit is applied, because
		// `put_streamed` treats a stream error as an abort and leaves no object
		// behind — so a breached limit cannot leave a truncated upload at the
		// key.
		//
		// Whether the limit was the cause is recorded here rather than read back
		// out of the error text. The message travels through the store, a
		// backend's own formatting and `ObjectStoreFailure` before it returns,
		// so matching on it would make the status depend on wording; and the
		// transport ceiling below produces a different message for the same
		// condition.
		let too_large = Arc::new(AtomicBool::new(false));

		// Both ceilings are counted here, in one place. A signed `max` bounds
		// what the URL allows and the node's transport limit bounds what any
		// upload may be; whichever is lower is the one that applies, and the
		// caller gets the same status either way. `RequestBodyLimitLayer`
		// stays as the backstop for a body that never reaches this stream.
		let limit =
			access.grant.max.unwrap_or(u64::MAX).min(*crate::cnf::HTTP_MAX_BUCKET_BODY_SIZE as u64);
		let mut seen = 0u64;
		let body = request.into_body().into_data_stream().map_err(|e| e.to_string()).and_then({
			let too_large = Arc::clone(&too_large);
			move |chunk| {
				seen += chunk.len() as u64;
				let over = seen > limit;
				let too_large = Arc::clone(&too_large);
				async move {
					if over {
						too_large.store(true, Ordering::Relaxed);
						return Err("The upload exceeds the limit this URL allows".to_string());
					}
					Ok(chunk)
				}
			}
		});

		match access.put(Box::pin(body), content_type, if_absent).await {
			Ok(Some(_)) => Ok(StatusCode::CREATED.into_response()),
			// The URL carried `overwrite: false`, or the request sent
			// `If-None-Match: *`, and the key was taken. Decided as part of the
			// commit, not by a check before it, so two redemptions of one
			// write-once URL cannot both succeed.
			Ok(None) => Ok(StatusCode::PRECONDITION_FAILED.into_response()),
			// An oversized body is the caller's fault, not the store's, and it
			// is the one stream failure with a status of its own.
			Err(_) if too_large.load(Ordering::Relaxed) => {
				Ok(StatusCode::PAYLOAD_TOO_LARGE.into_response())
			}
			// A type the signature does not name, or one the backend cannot
			// keep, is refused before the body is read.
			Err(e) => Err(access_error(e)),
		}
	}

	pub async fn delete_object(
		Extension(state): Extension<AppState>,
		Extension(session): Extension<Session>,
		scope: Option<Extension<PathScope>>,
		Path(path): Path<(String, String)>,
		RawQuery(query): RawQuery,
	) -> Result<Response, RouteError> {
		let access =
			access(&state, &session, scope.as_deref(), path, query, BucketOperation::Delete, None)
				.await?;
		access.delete().await.map_err(ResponseError)?;
		Ok(StatusCode::NO_CONTENT.into_response())
	}

	/// The largest copy or rename request body accepted. It holds an action, a
	/// key and a flag.
	const TRANSFER_BODY_LIMIT: usize = 64 * 1024;

	/// How deeply a copy or rename request body may nest. It has one level.
	const TRANSFER_BODY_DEPTH: usize = 8;

	/// `POST` on an object: copy or rename it within the bucket, as
	/// `file::copy`, `file::copy_if_not_exists`, `file::rename` and
	/// `file::rename_if_not_exists` do.
	///
	/// ```json
	/// { "action": "copy", "target": "/archive/a.png", "overwrite": false }
	/// ```
	///
	/// `overwrite` defaults to `true`, as `file::copy` and `file::rename` do.
	/// `201` when the object was copied or moved, `404` when there is nothing
	/// at the source, `412` when the target is taken and `overwrite` is
	/// `false`. A presigned URL cannot authorise either action.
	pub async fn transfer_object(
		Extension(state): Extension<AppState>,
		Extension(session): Extension<Session>,
		scope: Option<Extension<PathScope>>,
		Path(path): Path<(String, String)>,
		RawQuery(query): RawQuery,
		content_type: Result<TypedHeader<ContentType>, TypedHeaderRejection>,
		request: Request,
	) -> Result<Response, RouteError> {
		// A missing header means JSON. One that names any other format, or
		// none this server knows, is refused before the body is read.
		let content_type = match content_type {
			Ok(TypedHeader(content_type)) => Some(content_type),
			Err(e) if e.is_missing() => None,
			Err(_) => return Err(RouteError::Status(StatusCode::UNSUPPORTED_MEDIA_TYPE)),
		};
		let bytes = axum::body::to_bytes(request.into_body(), TRANSFER_BODY_LIMIT)
			.await
			.map_err(|_| RouteError::Status(StatusCode::PAYLOAD_TOO_LARGE))?;
		let body = match content_type {
			None | Some(ContentType::ApplicationJson) => {
				surrealdb_core::rpc::format::json::decode(&bytes, TRANSFER_BODY_DEPTH)
			}
			Some(ContentType::ApplicationCbor) => {
				surrealdb_core::rpc::format::cbor::decode(&bytes, TRANSFER_BODY_DEPTH)
			}
			Some(ContentType::ApplicationCbor2) => {
				surrealdb_core::rpc::format::cbor::decode_v2(&bytes, TRANSFER_BODY_DEPTH)
			}
			Some(_) => return Err(RouteError::Status(StatusCode::UNSUPPORTED_MEDIA_TYPE)),
		}
		.map_err(|_| RouteError::Status(StatusCode::BAD_REQUEST))?;
		let request = TransferRequest::from_value(body)
			.map_err(|_| RouteError::Status(StatusCode::BAD_REQUEST))?;

		let (op, rename) = match request.action.as_str() {
			"copy" => (BucketOperation::Copy, false),
			"rename" => (BucketOperation::Rename, true),
			_ => return Err(RouteError::Status(StatusCode::BAD_REQUEST)),
		};
		let access =
			access(&state, &session, scope.as_deref(), path, query, op, Some(&request.target))
				.await?;
		let outcome = if rename {
			access.rename(request.overwrite).await
		} else {
			access.copy(request.overwrite).await
		}
		.map_err(ResponseError)?;
		Ok(match outcome {
			TransferOutcome::Done => StatusCode::CREATED,
			TransferOutcome::SourceMissing => StatusCode::NOT_FOUND,
			TransferOutcome::TargetExists => StatusCode::PRECONDITION_FAILED,
		}
		.into_response())
	}

	/// The body of a copy or rename.
	#[derive(SurrealValue)]
	#[surreal(crate = "surrealdb_types")]
	struct TransferRequest {
		action: String,
		target: String,
		#[surreal(default = "default_overwrite")]
		overwrite: bool,
	}

	fn default_overwrite() -> bool {
		true
	}

	/// `file::list`'s options, as query parameters. An unknown parameter is
	/// refused rather than ignored, as a misspelled option is by the function.
	#[derive(Deserialize)]
	#[serde(deny_unknown_fields)]
	pub struct ListQuery {
		prefix: Option<String>,
		delimiter: Option<String>,
		start: Option<String>,
		limit: Option<usize>,
	}

	/// `GET` on a bucket: list it, as `file::list` does.
	///
	/// Each entry is `{ key, size, updated, etag? }` for an object, or
	/// `{ prefix }` for a group under `delimiter=/`. A listing is never
	/// presigned, and is refused for guests and record users, as the function
	/// is.
	pub async fn list_objects(
		Extension(state): Extension<AppState>,
		Extension(session): Extension<Session>,
		scope: Option<Extension<PathScope>>,
		Path(name): Path<String>,
		query: Result<Query<ListQuery>, axum::extract::rejection::QueryRejection>,
		accept: Option<TypedHeader<Accept>>,
	) -> Result<Response, RouteError> {
		let Query(query) = query.map_err(|_| RouteError::Status(StatusCode::BAD_REQUEST))?;
		let opts = ListOptions::from_parts(
			query.prefix,
			query.delimiter.as_deref(),
			query.start,
			query.limit,
		)
		.map_err(|_| RouteError::Status(StatusCode::BAD_REQUEST))?;

		// Answered before the listing, so an unanswerable request reads nothing.
		let accept = accept.map(|TypedHeader(accept)| accept);
		if !matches!(
			accept,
			None | Some(
				Accept::ApplicationJson | Accept::ApplicationCbor | Accept::ApplicationCbor2
			)
		) {
			return Err(RouteError::Status(StatusCode::NOT_ACCEPTABLE));
		}

		let path = (name, "/".to_owned());
		let access =
			access(&state, &session, scope.as_deref(), path, None, BucketOperation::List, None)
				.await?;
		let entries = access.list(&opts).await.map_err(ResponseError)?;
		let value = PublicValue::Array(entries.into_iter().map(listed).collect::<Vec<_>>().into());

		let (bytes, format) = match accept {
			Some(Accept::ApplicationCbor) => {
				(surrealdb_core::rpc::format::cbor::encode(value), Accept::ApplicationCbor)
			}
			Some(Accept::ApplicationCbor2) => {
				(surrealdb_core::rpc::format::cbor::encode_v2(value), Accept::ApplicationCbor2)
			}
			_ => (surrealdb_core::rpc::format::json::encode(value), Accept::ApplicationJson),
		};
		let bytes = bytes.map_err(ResponseError)?;
		Ok(([(CONTENT_TYPE, http::HeaderValue::from(format))], bytes).into_response())
	}

	/// One listing entry, as the HTTP API reports it: a plain key rather than a
	/// SurrealQL file value, which a client outside SurrealQL cannot use.
	fn listed(entry: ListEntry) -> PublicValue {
		let mut obj = PublicObject::new();
		match entry {
			ListEntry::Object(meta) => {
				obj.insert("key".to_owned(), meta.key.to_string().into_value());
				obj.insert("size".to_owned(), meta.size.into_value());
				obj.insert(
					"updated".to_owned(),
					PublicValue::Datetime(surrealdb_types::Datetime::from(meta.updated)),
				);
				if let Some(etag) = meta.etag {
					obj.insert("etag".to_owned(), etag.into_value());
				}
			}
			ListEntry::Prefix(prefix) => {
				obj.insert("prefix".to_owned(), prefix.into_value());
			}
		}
		PublicValue::Object(obj)
	}

	#[cfg(test)]
	mod tests {
		use super::parse_signed_query;

		#[test]
		fn parse_signed_query_handles_absent_and_valid_queries() {
			assert!(matches!(parse_signed_query(None), Ok(None)));
			assert!(matches!(parse_signed_query(Some("")), Ok(None)));

			let signed =
				parse_signed_query(Some("exp=100&max=2048&ow=false&sig=abc")).unwrap().unwrap();
			assert_eq!(signed.exp, 100);
			assert_eq!(signed.max, Some(2048));
			assert!(!signed.overwrite);
			assert_eq!(signed.sig, "abc");
		}

		#[test]
		fn parse_signed_query_rejects_partial_duplicate_and_non_canonical_params() {
			assert!(parse_signed_query(Some("max=2048")).is_err());
			assert!(parse_signed_query(Some("ow=false")).is_err());
			assert!(parse_signed_query(Some("exp=100&ow=true&sig=abc")).is_err());
			assert!(parse_signed_query(Some("exp=100&exp=100&sig=abc")).is_err());
			assert!(parse_signed_query(Some("exp=100&ow=false&ow=false&sig=abc")).is_err());
			// `ct` is passed on still percent-encoded; core decodes it beside
			// the signature check.
			let signed =
				parse_signed_query(Some("ct=image%2Fpng&exp=100&sig=abc")).unwrap().unwrap();
			assert_eq!(signed.content_type.as_deref(), Some("image%2Fpng"));
			assert!(parse_signed_query(Some("ct=a&ct=b&exp=100&sig=abc")).is_err());
			// A parameter this build does not sign is refused, not ignored.
			assert!(parse_signed_query(Some("exp=100&kid=1&sig=abc")).is_err());
		}

		#[test]
		fn presigned_bucket_queries_are_recognised() {
			let signed = |uri: &str| crate::ntw::bucket::presents_signature(&uri.parse().unwrap());
			assert!(signed("/bucket/img/a?exp=1&sig=abc"));
			assert!(signed("/bucket/img/a?ct=text%2Fplain&exp=1&sig=abc"));
			// A partial signature is still a signature.
			assert!(signed("/bucket/img/a?sig"));
			assert!(signed("/bucket/img/a?exp=1"));
			assert!(!signed("/bucket/img/a"));
			assert!(!signed("/bucket/img/?prefix=a&limit=10"));
			assert!(!signed("/bucket/img/a?signature=x&expires=1"));
			// Only the bucket routes.
			assert!(!signed("/sql?exp=1&sig=abc"));
		}
	}
}
