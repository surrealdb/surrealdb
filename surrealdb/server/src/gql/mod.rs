//! HTTP service layer for GraphQL.
//!
//! Implements an Axum [`Service`] that handles incoming GraphQL HTTP requests.
//! The service:
//!
//! 1. Checks that the GraphQL HTTP route is allowed by the datastore's capabilities.
//! 2. Validates that the session specifies a namespace and database.
//! 3. Retrieves (or generates) the GraphQL schema via [`GraphQLSchemaCache`].
//! 4. Injects the [`Datastore`](surrealdb_core::kvs::Datastore) and [`Session`] into the
//!    `async_graphql` request context so resolvers can access them.
//! 5. Executes the request -- either as a batch request or as a streaming `multipart/mixed`
//!    response, depending on the `Accept` header.

use std::convert::Infallible;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_graphql::http::is_accept_multipart_mixed;
use async_graphql::parser::types::OperationType;
use async_graphql::{BatchRequest, Executor, Request as GraphQLInnerRequest, ServerError};
use async_graphql_axum::rejection::GraphQLRejection;
use async_graphql_axum::{GraphQLBatchRequest, GraphQLRequest, GraphQLResponse};
use axum::BoxError;
use axum::body::{Body, HttpBody};
use axum::extract::FromRequest;
use axum::http::{Request as HttpRequest, Response as HttpResponse, StatusCode};
use axum::response::IntoResponse;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use http::header::{CONTENT_TYPE, HeaderValue};
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_core::gql::cache::GraphQLSchemaCache;
use surrealdb_core::observe::Outcome;
use tower_service::Service;
use web_time::Instant;

use crate::ntw::error::Error as NetError;

/// Resolve the operation-type label for the `surrealdb.graphql.operation`
/// metric attribute.
///
/// Mutates `req` because `parsed_query()` lazily caches the AST inside the
/// request; the request is otherwise unchanged.
fn graphql_operation_type_label(req: &mut GraphQLInnerRequest) -> &'static str {
	let operation_name = req.operation_name.clone();
	let Ok(doc) = req.parsed_query() else {
		return "unknown";
	};
	let op = match operation_name.as_deref() {
		Some(selected) => doc.operations.iter().find_map(|(name, op)| {
			if name.is_some_and(|n| n.as_str() == selected) {
				Some(op.node.ty)
			} else {
				None
			}
		}),
		None => doc.operations.iter().next().map(|(_, op)| op.node.ty),
	};
	match op {
		Some(OperationType::Query) => "query",
		Some(OperationType::Mutation) => "mutation",
		Some(OperationType::Subscription) => "subscription",
		None => "unknown",
	}
}

/// Axum service that handles GraphQL HTTP requests.
///
/// Each instance holds a [`GraphQLSchemaCache`] that is shared across all
/// requests handled by this service.  The cache is cheap to clone (backed
/// by `Arc<RwLock<...>>`).
#[derive(Clone)]
pub struct GraphQLService {
	cache: GraphQLSchemaCache,
}

impl GraphQLService {
	/// Create a new GraphQL HTTP service with an empty schema cache.
	pub fn new() -> Self {
		GraphQLService {
			cache: GraphQLSchemaCache::default(),
		}
	}

	/// Return a clone of the underlying schema cache (cheap -- backed by `Arc`).
	pub(crate) fn cache(&self) -> GraphQLSchemaCache {
		self.cache.clone()
	}
}

impl<B> Service<HttpRequest<B>> for GraphQLService
where
	B: HttpBody<Data = Bytes> + Send + 'static,
	B::Data: Into<Bytes>,
	B::Error: Into<BoxError>,
{
	type Response = HttpResponse<Body>;
	type Error = Infallible;
	type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

	fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn call(&mut self, req: HttpRequest<B>) -> Self::Future {
		let cache = self.cache.clone();
		let req = req.map(Body::new);

		Box::pin(async move {
			let state = req
				.extensions()
				.get::<crate::ntw::AppState>()
				.expect("state extractor should always succeed");

			let datastore = &state.datastore;
			let metrics_observer = state.metrics_observer.clone();
			let started_at = Instant::now();

			// Check if capabilities allow querying the requested HTTP route
			if !datastore.allows_http_route(&RouteTarget::GraphQL) {
				warn!(
					"Capabilities denied HTTP route request attempt, target: '{}'",
					&RouteTarget::GraphQL
				);
				return Ok(
					NetError::ForbiddenRoute(RouteTarget::GraphQL.to_string()).into_response()
				);
			}

			let session =
				req.extensions().get::<Session>().expect("session extractor should always succeed");

			let Some(_ns) = session.ns.as_ref() else {
				return Ok(graphql_error_response(
					"No namespace specified. Set the `surreal-ns` header on the request.",
				));
			};
			let Some(_db) = session.db.as_ref() else {
				return Ok(graphql_error_response(
					"No database specified. Set the `surreal-db` header on the request.",
				));
			};

			let schema = match cache.get_schema(datastore, session).await {
				Ok(e) => e,
				Err(e) => {
					info!(?e, "error generating schema");
					return Ok(graphql_error_response(&format!("{e}")));
				}
			};

			// Clone Arc's before moving req (needed for GraphQL context)
			let datastore_ctx = Arc::clone(datastore);
			let session_ctx = std::sync::Arc::new(session.clone());

			let is_accept_multipart_mixed = req
				.headers()
				.get("accept")
				.and_then(|value| value.to_str().ok())
				.map(is_accept_multipart_mixed)
				.unwrap_or_default();

			// Snapshot tenant ctx for the metric. Cheap clones (Option<String>).
			let metric_ns = session_ctx.ns.clone();
			let metric_db = session_ctx.db.clone();
			let metric_user = if session_ctx.au.is_anon() {
				None
			} else if session_ctx.au.is_record() {
				Some("<record>".to_owned())
			} else {
				Some(session_ctx.au.id().to_owned())
			};

			if is_accept_multipart_mixed {
				let gql_req = match GraphQLRequest::<GraphQLRejection>::from_request(req, &()).await
				{
					Ok(r) => r,
					Err(err) => return Ok(err.into_response()),
				};
				let mut req_with_data = gql_req.into_inner().data(datastore_ctx).data(session_ctx);
				let op_label = graphql_operation_type_label(&mut req_with_data);
				if request_is_subscription(&mut req_with_data) {
					// A subscription request that arrives over HTTP /
					// multipart-mixed instead of the WebSocket transport
					// is unsupported. We do NOT record a metric for this
					// rejection path -- real subscriptions land on
					// `serve_graphql_ws` (see `ntw::gql`) and never
					// reach this `Service::call`, so an
					// `operation_type="subscription"` series here would
					// only show misuse and would be missing the
					// successful subscription traffic that operators
					// actually want to track. Connection-level WS
					// volume is observable via the HTTP layer's
					// `/graphql` GET upgrade requests on
					// `surrealdb.http.request*`.
					let response = async_graphql::Response::from_errors(vec![ServerError::new(
						"Subscriptions require WebSocket transport on GET /graphql",
						None,
					)]);
					Ok(as_application_json(GraphQLResponse::from(response).into_response()))
				} else {
					let response = schema.execute(req_with_data).await;
					if let Some(observer) = metrics_observer.as_ref() {
						let outcome = if response.is_err() {
							Outcome::Error
						} else {
							Outcome::Success
						};
						let error_class = if response.is_err() {
							Some(surrealdb_core::observe::error_class::CLIENT)
						} else {
							None
						};
						observer.record_graphql_operation(
							op_label,
							outcome,
							error_class,
							started_at.elapsed(),
							metric_ns.as_deref(),
							metric_db.as_deref(),
							metric_user.as_deref(),
						);
					}
					Ok(as_application_json(GraphQLResponse::from(response).into_response()))
				}
			} else {
				let gql_req =
					match GraphQLBatchRequest::<GraphQLRejection>::from_request(req, &()).await {
						Ok(r) => r,
						Err(err) => return Ok(err.into_response()),
					};
				// Most GraphQL POST traffic is a single operation wrapped in
				// the batch envelope. Inspect the request *before* attaching
				// per-request data so the metric carries the proper
				// `operation_type` (`query` / `mutation` / `subscription`) on
				// the common path; only true multi-operation batches collapse
				// to `"batch"`.
				let mut batch_req = gql_req.into_inner();
				let op_label = match &mut batch_req {
					BatchRequest::Single(req) => graphql_operation_type_label(req),
					BatchRequest::Batch(_) => "batch",
				};
				let req_with_data = batch_req.data(datastore_ctx).data(session_ctx);
				let response = schema.execute_batch(req_with_data).await;
				if let Some(observer) = metrics_observer.as_ref() {
					// Batch responses can carry per-operation results. We fold
					// them into a single counter increment with `outcome` set
					// to `error` if any sub-response errored. Per-operation
					// duration breakdowns can be reconstructed from the
					// histogram once the SDK exposes per-op results to us.
					let any_err = !response.is_ok();
					let outcome = if any_err {
						Outcome::Error
					} else {
						Outcome::Success
					};
					let error_class = if any_err {
						Some(surrealdb_core::observe::error_class::CLIENT)
					} else {
						None
					};
					observer.record_graphql_operation(
						op_label,
						outcome,
						error_class,
						started_at.elapsed(),
						metric_ns.as_deref(),
						metric_db.as_deref(),
						metric_user.as_deref(),
					);
				}
				Ok(as_application_json(GraphQLResponse(response).into_response()))
			}
		})
	}
}

/// Check whether `req` represents a GraphQL subscription operation.
///
/// Takes `&mut` because `parsed_query()` lazily parses and caches the AST
/// inside the request. The request is not otherwise modified.
fn request_is_subscription(req: &mut GraphQLInnerRequest) -> bool {
	let operation_name = req.operation_name.clone();
	let Ok(doc) = req.parsed_query() else {
		return false;
	};

	match operation_name.as_deref() {
		Some(selected) => doc.operations.iter().any(|(name, op)| {
			name.is_some_and(|n| n.as_str() == selected)
				&& matches!(op.node.ty, OperationType::Subscription)
		}),
		None => doc
			.operations
			.iter()
			.next()
			.is_some_and(|(_, op)| matches!(op.node.ty, OperationType::Subscription)),
	}
}

fn as_application_json(mut response: HttpResponse<Body>) -> HttpResponse<Body> {
	response.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
	response
}

/// Build a spec-compliant GraphQL error response (HTTP 400, `application/json`,
/// body `{"data": null, "errors": [{"message": "..."}]}`).
///
/// Used for pre-execution failures such as missing `surreal-ns` / `surreal-db`
/// headers or a schema-generation error.  Plain-text 400s break GraphQL
/// clients (Postman, GraphiQL, urql) which expect the `errors` shape when
/// they fail to parse the body, often surfacing the user-facing message
/// "Received an invalid GraphQL response".
fn graphql_error_response(message: &str) -> HttpResponse<Body> {
	let body = serde_json::json!({
		"data": null,
		"errors": [{ "message": message }],
	})
	.to_string();
	let mut response = HttpResponse::new(Body::from(body));
	*response.status_mut() = StatusCode::BAD_REQUEST;
	response.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
	response
}
