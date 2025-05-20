use tracing::{Span, field};
use uuid::Uuid;

pub fn span_for_request(ws_id: &Uuid) -> Span {
	let span = tracing::debug_span!(
		// Dynamic span names need to be 'recorded', can't be used on the macro. Use a static name here and overwrite later on
		"rpc/call",
		otel.kind = "server",
		otel.name = field::Empty,
		otel.status_code = field::Empty,
		otel.status_message = field::Empty,

		// SurrealDB custom fields
		ws.id = %ws_id,

		// To be populated by the request handler when the method is known
		rpc.method = field::Empty,
		rpc.service = "surrealdb",
		rpc.request_id = field::Empty,
		rpc.error_code = field::Empty,
		rpc.error_message = field::Empty,
	);

	span
}
