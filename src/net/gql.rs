use std::collections::BTreeMap;
use std::vec;

use crate::dbs::DB;
use crate::err;
use axum::debug_handler;
use axum::routing::get;
use axum::routing::post;
use axum::Extension;
use axum::Json;
use axum::Router;
use http::StatusCode;
use http_body::Body as HttpBody;
use serde::de::IntoDeserializer;
use serde::Deserialize;
use serde::Serialize;
use surrealdb::dbs::Session;
use surrealdb::error;
use surrealdb::gql;
use surrealdb::sql;
use tower_http::request_id::RequestId;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/gql/schema", get(schema_handler)).route("/gql", post(handler))
}

async fn schema_handler() -> String {
	let schema =
		gql::get_schema(DB.get().unwrap(), "test".to_string(), "test".to_string()).await.unwrap();

	schema.to_string()
}

#[derive(Deserialize)]
struct GQLQuery {
	query: String,
	variables: Option<BTreeMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
enum GQLResponse {
	#[serde(rename = "data")]
	Data(serde_json::Value),
	#[serde(rename = "errors")]
	Errors(String),
}

#[debug_handler]
async fn handler(
	Extension(sess): Extension<Session>,
	// payload: String,
	Json(query): Json<GQLQuery>,
	// Extension(req_id): Extension<RequestId>,
) -> (StatusCode, Json<GQLResponse>) {
	// info!(query.query)
	// info!(payload);
	// let query = r#"query {
	// 	person
	// }
	// "#;
	// let variables: Option<BTreeMap<String, surrealdb::sql::Value>> =
	// 	query.variables.map(|v| v.iter().map(|(k, v)| (k.to_owned(), sql::Value::json)).collect());
	let res = DB
		.get()
		.unwrap()
		.execute_gql(&query.query, &sess.with_ns("test").with_db("test"), None)
		.await
		.unwrap();

	let mut response = vec![];
	for r in res {
		match r.result {
			Ok(v) => response.push(v.into_json()),
			Err(e) => {
				return (
					StatusCode::INTERNAL_SERVER_ERROR,
					Json(GQLResponse::Errors(format!("{}", e))),
				)
			}
		}
	}

	(StatusCode::OK, Json(GQLResponse::Data(response.into())))
}

// async fn handler(
// 	ws: WebSocketUpgrade,
// 	Extension(sess): Extension<Session>,
// 	Extension(req_id): Extension<RequestId>,
// ) -> impl IntoResponse {
// 	ws
// 		// Set the maximum frame size
// 		.max_frame_size(*cnf::WEBSOCKET_MAX_FRAME_SIZE)
// 		// Set the maximum message size
// 		.max_message_size(*cnf::WEBSOCKET_MAX_MESSAGE_SIZE)
// 		// Set the potential WebSocket protocol formats
// 		.protocols(["surrealql-binary", "json", "cbor", "messagepack"])
// 		// Handle the WebSocket upgrade and process messages
// 		.on_upgrade(move |socket| handle_socket(socket, sess, req_id))
// }

// async fn handle_socket(ws: WebSocket, sess: Session, req_id: RequestId) {
// 	// Create a new connection instance
// 	let rpc = Connection::new(sess);
// 	// Update the WebSocket ID with the Request ID
// 	if let Ok(Ok(req_id)) = req_id.header_value().to_str().map(Uuid::parse_str) {
// 		// If the ID couldn't be updated, ignore the error and keep the default ID
// 		let _ = rpc.write().await.update_ws_id(req_id).await;
// 	}
// 	// Serve the socket connection requests
// 	Connection::serve(rpc, ws).await;
// }
