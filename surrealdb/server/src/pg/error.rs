use std::fmt;

use surrealdb_types::Error as TypesError;

/// A wire-level error: a SQLSTATE code plus a human-readable message,
/// rendered to the client as an ErrorResponse.
#[derive(Debug)]
pub(super) struct PgError {
	pub(super) code: &'static str,
	pub(super) message: String,
	/// FATAL errors terminate the connection (startup/auth failures,
	/// protocol violations); ERROR errors leave the session usable.
	pub(super) fatal: bool,
}

impl fmt::Display for PgError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}: {}", self.code, self.message)
	}
}

impl std::error::Error for PgError {}

impl PgError {
	fn new(code: &'static str, message: impl Into<String>) -> Self {
		Self {
			code,
			message: message.into(),
			fatal: false,
		}
	}

	/// Mark this error as connection-terminating.
	pub(super) fn fatal(mut self) -> Self {
		self.fatal = true;
		self
	}

	pub(super) fn severity(&self) -> &'static str {
		if self.fatal {
			"FATAL"
		} else {
			"ERROR"
		}
	}

	/// `53300 too_many_connections`
	pub(super) fn too_many_connections() -> Self {
		Self::new("53300", "sorry, too many clients already").fatal()
	}

	/// `57P05`-style authentication timeout.
	pub(super) fn auth_timeout() -> Self {
		Self::new("57P05", "terminating connection due to authentication timeout").fatal()
	}

	/// `08P01 protocol_violation`
	pub(super) fn protocol(message: impl Into<String>) -> Self {
		Self::new("08P01", message)
	}

	/// `22P02 invalid_text_representation` — a client-supplied value could not
	/// be parsed as its declared type.
	pub(super) fn invalid_text(message: impl Into<String>) -> Self {
		Self::new("22P02", message)
	}

	/// `0A000 feature_not_supported`
	pub(super) fn feature_not_supported(message: impl Into<String>) -> Self {
		Self::new("0A000", message)
	}

	/// `28P01 invalid_password`
	pub(super) fn invalid_password(message: impl Into<String>) -> Self {
		Self::new("28P01", message)
	}

	/// `42501 insufficient_privilege`
	pub(super) fn insufficient_privilege(message: impl Into<String>) -> Self {
		Self::new("42501", message)
	}

	/// `57P03 cannot_connect_now`
	pub(super) fn cannot_connect_now(message: impl Into<String>) -> Self {
		Self::new("57P03", message)
	}

	/// `42601 syntax_error`
	pub(super) fn syntax(message: impl Into<String>) -> Self {
		Self::new("42601", message)
	}

	/// `25P02 in_failed_sql_transaction`
	pub(super) fn in_failed_transaction(message: impl Into<String>) -> Self {
		Self::new("25P02", message)
	}

	/// `42704 undefined_object` — e.g. an unknown configuration parameter.
	pub(super) fn undefined_object(message: impl Into<String>) -> Self {
		Self::new("42704", message)
	}

	/// `26000 invalid_sql_statement_name`
	pub(super) fn invalid_statement(name: &str) -> Self {
		Self::new("26000", format!("prepared statement \"{name}\" does not exist"))
	}

	/// `42P05 duplicate_prepared_statement`
	pub(super) fn duplicate_statement(name: &str) -> Self {
		Self::new("42P05", format!("prepared statement \"{name}\" already exists"))
	}

	/// `34000 invalid_cursor_name`
	pub(super) fn invalid_cursor(name: &str) -> Self {
		Self::new("34000", format!("portal \"{name}\" does not exist"))
	}

	/// `54000 program_limit_exceeded`
	pub(super) fn too_many_prepared(kind: &str) -> Self {
		Self::new("54000", format!("too many {kind} on this connection"))
	}

	/// `XX000 internal_error`
	pub(super) fn internal(message: impl Into<String>) -> Self {
		Self::new("XX000", message)
	}
}

impl From<&TypesError> for PgError {
	fn from(err: &TypesError) -> Self {
		let code = match err.kind_str() {
			"Auth" => "28000",
			"Validation" => "42601",
			"NotAllowed" => "42501",
			"Thrown" => "P0001",
			"NotFound" => "42704",
			_ => "XX000",
		};
		Self::new(code, err.message())
	}
}
