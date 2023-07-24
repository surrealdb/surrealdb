use tracing::{field, Span};
use uuid::Uuid;

pub fn span_for_request(method: &str, ws_id: &Uuid) -> Span {
	let span = tracing::info_span!(
		parent: None,
		// Dynamic span names need to be 'recorded', can't be used on the macro
		"rpc/request",
		otel.name = field::Empty,
		otel.kind = "server",

		// OTEL fields (see https://github.com/open-telemetry/opentelemetry-specification/blob/v1.23.0/specification/trace/semantic_conventions/rpc.md)
		rpc.service = "surrealdb",
		rpc.method = %method,

		network.transport = "tcp",
		server.address = field::Empty,
		server.port = field::Empty,
		server.socket.address = field::Empty,
		server.socket.port = field::Empty,

		// SurrealDB custom fields
		ws.id = %ws_id,
	);

	span.record("otel.name", format!("surrealdb.rpc/{}", method));

	// TODO: populate the rest of the fields

	span
}
