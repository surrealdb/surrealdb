//! MCP (Model Context Protocol) endpoint at `/mcp`.
//!
//! Mounts rmcp's `StreamableHttpService` which handles the full MCP protocol
//! over Streamable HTTP (POST for requests, GET for SSE streams, session
//! management via `MCP-Session-Id` header).

use std::sync::Arc;

use axum::Router;
use axum::extract::Extension;
use axum::response::IntoResponse;
use axum::routing::post;
use surrealdb_core::dbs::capabilities::RouteTarget;
use tokio::sync::OnceCell;

use super::AppState;
use crate::rpc::RpcState;

type McpHttpService = surrealdb_mcp::service::McpHttpService;
type SharedMcpService = Arc<OnceCell<McpHttpService>>;

pub fn router() -> Router<Arc<RpcState>> {
	let mcp_cell: SharedMcpService = Arc::new(OnceCell::new());
	let mcp_post = mcp_cell.clone();
	let mcp_get = mcp_cell.clone();
	let mcp_del = mcp_cell;

	Router::new().route(
		"/mcp",
		post(move |state: Extension<AppState>, req: axum::extract::Request| {
			handle_mcp(mcp_post, state, req)
		})
		.get(move |state: Extension<AppState>, req: axum::extract::Request| {
			handle_mcp(mcp_get, state, req)
		})
		.delete(move |state: Extension<AppState>, req: axum::extract::Request| {
			handle_mcp(mcp_del, state, req)
		}),
	)
}

async fn handle_mcp(
	mcp_cell: SharedMcpService,
	Extension(state): Extension<AppState>,
	req: axum::extract::Request,
) -> impl IntoResponse {
	let db = &state.datastore;

	if !db.allows_http_route(&RouteTarget::Mcp) {
		tracing::warn!("Capabilities denied HTTP route request attempt, target: 'mcp'");
		return forbidden_response();
	}

	let service = mcp_cell
		.get_or_init(|| async { surrealdb_mcp::service::create_http_service(db.clone()) })
		.await;

	match tower_service::Service::call(&mut service.clone(), req).await {
		Ok(resp) => resp.into_response(),
		Err(e) => {
			tracing::error!(target: "surrealdb::mcp", error = %e, "MCP request handling failed");
			internal_error_response()
		}
	}
}

fn forbidden_response() -> axum::response::Response {
	axum::response::Response::builder()
		.status(403)
		.body(axum::body::Body::from("Forbidden"))
		.unwrap_or_else(|e| {
			tracing::error!(target: "surrealdb::mcp", error = %e, "Failed to build 403 response");
			axum::response::Response::new(axum::body::Body::from("Forbidden"))
		})
}

fn internal_error_response() -> axum::response::Response {
	axum::response::Response::builder()
		.status(500)
		.body(axum::body::Body::from("Internal Server Error"))
		.unwrap_or_else(|e| {
			tracing::error!(target: "surrealdb::mcp", error = %e, "Failed to build 500 response");
			axum::response::Response::new(axum::body::Body::from("Internal Server Error"))
		})
}
