use tracing::{field, Span};
use uuid::Uuid;

pub fn span_for_request(ws_id: &Uuid) -> Span {
	let span = tracing::info_span!(
		parent: None,
		// Dynamic span names need to be 'recorded', can't be used on the macro
		"rpc/call",
		otel.name = field::Empty,
		otel.kind = "server",

		// To be populated by the request handler when the method is known
		rpc.method = field::Empty,
		rpc.service = "surrealdb",
		rpc.request.format = field::Empty,
		rpc.response.format = field::Empty,

		// SurrealDB custom fields
		ws.id = %ws_id,

		// Fields for error reporting
		otel.status_code = field::Empty,
		otel.status_message = field::Empty,
	);

	span
}
