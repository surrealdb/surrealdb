//! Bridge between MCP logging and SurrealDB's tracing infrastructure.

use rmcp::model::{LoggingLevel, LoggingMessageNotificationParam};

/// Convert an MCP log level to a tracing level.
fn to_tracing_level(level: &LoggingLevel) -> tracing::Level {
	match level {
		LoggingLevel::Debug => tracing::Level::DEBUG,
		LoggingLevel::Info => tracing::Level::INFO,
		LoggingLevel::Warning => tracing::Level::WARN,
		LoggingLevel::Error => tracing::Level::ERROR,
		_ => tracing::Level::TRACE,
	}
}

/// Emit an MCP log message through the tracing infrastructure.
/// Available for future integration with MCP logging notifications.
#[expect(dead_code, reason = "reserved for MCP logging notification handler")]
pub fn emit_log(notification: &LoggingMessageNotificationParam) {
	let level = to_tracing_level(&notification.level);
	let logger = notification.logger.as_deref().unwrap_or("mcp");
	let data = &notification.data;

	match level {
		tracing::Level::ERROR => {
			tracing::error!(target: "surrealdb::mcp", logger = logger, "{data}")
		}
		tracing::Level::WARN => tracing::warn!(target: "surrealdb::mcp", logger = logger, "{data}"),
		tracing::Level::INFO => tracing::info!(target: "surrealdb::mcp", logger = logger, "{data}"),
		tracing::Level::DEBUG => {
			tracing::debug!(target: "surrealdb::mcp", logger = logger, "{data}")
		}
		_ => tracing::trace!(target: "surrealdb::mcp", logger = logger, "{data}"),
	}
}
