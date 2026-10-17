use std::fmt;

use surrealdb_types::{
	AlreadyExistsError, Error as TypesError, ErrorDetails, NotFoundError, QueryError,
	ValidationError,
};

/// Optional structured fields for an ErrorResponse or NoticeResponse.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct PgErrorFields {
	pub(super) severity_override: Option<&'static str>,
	pub(super) detail: Option<String>,
	pub(super) hint: Option<String>,
	pub(super) position: Option<u32>,
	pub(super) internal_position: Option<u32>,
	pub(super) internal_query: Option<String>,
	pub(super) where_ctx: Option<String>,
	pub(super) schema_name: Option<String>,
	pub(super) table_name: Option<String>,
	pub(super) column_name: Option<String>,
	pub(super) data_type_name: Option<String>,
	pub(super) constraint_name: Option<String>,
	pub(super) file: Option<&'static str>,
	pub(super) line: Option<u32>,
	pub(super) routine: Option<&'static str>,
}

/// A wire-level error: a SQLSTATE code plus a human-readable message,
/// rendered to the client as an ErrorResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgError {
	pub(super) code: &'static str,
	pub(super) message: String,
	/// FATAL errors terminate the connection (startup/auth failures,
	/// protocol violations); ERROR errors leave the session usable.
	pub(super) fatal: bool,
	pub(super) fields: Option<Box<PgErrorFields>>,
}

impl fmt::Display for PgError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}: {}", self.code, self.message)
	}
}

impl std::error::Error for PgError {}

impl PgError {
	pub(super) fn new(code: &'static str, message: impl Into<String>) -> Self {
		Self {
			code,
			message: message.into(),
			fatal: false,
			fields: None,
		}
	}

	fn fields_mut(&mut self) -> &mut PgErrorFields {
		self.fields.get_or_insert_with(Box::default)
	}

	/// Mark this error as connection-terminating.
	pub(super) fn fatal(mut self) -> Self {
		self.fatal = true;
		self
	}

	pub(super) fn severity(&self) -> &'static str {
		if let Some(fields) = &self.fields
			&& let Some(s) = fields.severity_override
		{
			s
		} else if self.fatal {
			"FATAL"
		} else {
			"ERROR"
		}
	}

	#[allow(dead_code)]
	pub(super) fn with_severity(mut self, severity: &'static str) -> Self {
		self.fields_mut().severity_override = Some(severity);
		self
	}

	pub(super) fn with_detail(mut self, detail: impl Into<String>) -> Self {
		self.fields_mut().detail = Some(detail.into());
		self
	}

	#[allow(dead_code)]
	pub(super) fn with_hint(mut self, hint: impl Into<String>) -> Self {
		self.fields_mut().hint = Some(hint.into());
		self
	}

	pub(super) fn with_position(mut self, position: u32) -> Self {
		self.fields_mut().position = Some(position);
		self
	}

	#[allow(dead_code)]
	pub(super) fn with_internal_position(mut self, pos: u32) -> Self {
		self.fields_mut().internal_position = Some(pos);
		self
	}

	#[allow(dead_code)]
	pub(super) fn with_internal_query(mut self, query: impl Into<String>) -> Self {
		self.fields_mut().internal_query = Some(query.into());
		self
	}

	#[allow(dead_code)]
	pub(super) fn with_where(mut self, where_ctx: impl Into<String>) -> Self {
		self.fields_mut().where_ctx = Some(where_ctx.into());
		self
	}

	#[allow(dead_code)]
	pub(super) fn with_schema(mut self, schema: impl Into<String>) -> Self {
		self.fields_mut().schema_name = Some(schema.into());
		self
	}

	pub(super) fn with_table(mut self, table: impl Into<String>) -> Self {
		self.fields_mut().table_name = Some(table.into());
		self
	}

	#[allow(dead_code)]
	pub(super) fn with_column(mut self, column: impl Into<String>) -> Self {
		self.fields_mut().column_name = Some(column.into());
		self
	}

	#[allow(dead_code)]
	pub(super) fn with_data_type(mut self, data_type: impl Into<String>) -> Self {
		self.fields_mut().data_type_name = Some(data_type.into());
		self
	}

	pub(super) fn with_constraint(mut self, constraint: impl Into<String>) -> Self {
		self.fields_mut().constraint_name = Some(constraint.into());
		self
	}

	#[allow(dead_code)]
	pub(super) fn with_file_line_routine(
		mut self,
		file: &'static str,
		line: u32,
		routine: &'static str,
	) -> Self {
		let fields = self.fields_mut();
		fields.file = Some(file);
		fields.line = Some(line);
		fields.routine = Some(routine);
		self
	}

	pub(super) fn detail(&self) -> Option<&str> {
		self.fields.as_ref().and_then(|f| f.detail.as_deref())
	}

	pub(super) fn hint(&self) -> Option<&str> {
		self.fields.as_ref().and_then(|f| f.hint.as_deref())
	}

	pub(super) fn position(&self) -> Option<u32> {
		self.fields.as_ref().and_then(|f| f.position)
	}

	pub(super) fn internal_position(&self) -> Option<u32> {
		self.fields.as_ref().and_then(|f| f.internal_position)
	}

	pub(super) fn internal_query(&self) -> Option<&str> {
		self.fields.as_ref().and_then(|f| f.internal_query.as_deref())
	}

	pub(super) fn where_ctx(&self) -> Option<&str> {
		self.fields.as_ref().and_then(|f| f.where_ctx.as_deref())
	}

	pub(super) fn schema_name(&self) -> Option<&str> {
		self.fields.as_ref().and_then(|f| f.schema_name.as_deref())
	}

	pub(super) fn table_name(&self) -> Option<&str> {
		self.fields.as_ref().and_then(|f| f.table_name.as_deref())
	}

	pub(super) fn column_name(&self) -> Option<&str> {
		self.fields.as_ref().and_then(|f| f.column_name.as_deref())
	}

	pub(super) fn data_type_name(&self) -> Option<&str> {
		self.fields.as_ref().and_then(|f| f.data_type_name.as_deref())
	}

	pub(super) fn constraint_name(&self) -> Option<&str> {
		self.fields.as_ref().and_then(|f| f.constraint_name.as_deref())
	}

	pub(super) fn file(&self) -> Option<&'static str> {
		self.fields.as_ref().and_then(|f| f.file)
	}

	pub(super) fn line(&self) -> Option<u32> {
		self.fields.as_ref().and_then(|f| f.line)
	}

	pub(super) fn routine(&self) -> Option<&'static str> {
		self.fields.as_ref().and_then(|f| f.routine)
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

	/// `25P01 no_active_sql_transaction`
	pub(super) fn no_active_transaction(message: impl Into<String>) -> Self {
		Self::new("25P01", message)
	}

	/// `3B001 invalid_savepoint_specification`
	pub(super) fn invalid_savepoint(message: impl Into<String>) -> Self {
		Self::new("3B001", message)
	}

	/// `42704 undefined_object` — e.g. an unknown configuration parameter.
	pub(super) fn undefined_object(message: impl Into<String>) -> Self {
		Self::new("42704", message)
	}

	/// `55P02 cant_change_runtime_param`
	pub(super) fn cant_change_runtime_param(message: impl Into<String>) -> Self {
		Self::new("55P02", message)
	}

	/// `22023 invalid_parameter_value`
	pub(super) fn invalid_parameter_value(message: impl Into<String>) -> Self {
		Self::new("22023", message)
	}

	/// `42P01 undefined_table`
	pub(super) fn undefined_table(message: impl Into<String>) -> Self {
		Self::new("42P01", message)
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

	/// `3D000 invalid_catalog_name`
	pub(super) fn invalid_catalog_name(message: impl Into<String>) -> Self {
		Self::new("3D000", message)
	}

	/// `XX000 internal_error`
	pub(super) fn internal(message: impl Into<String>) -> Self {
		Self::new("XX000", message)
	}

	/// `23505 unique_violation`
	/// `23505 unique_violation`
	#[allow(dead_code)]
	pub(super) fn already_exists(message: impl Into<String>) -> Self {
		Self::new("23505", message)
	}

	/// `42P07 duplicate_table`
	#[allow(dead_code)]
	pub(super) fn duplicate_table(message: impl Into<String>) -> Self {
		Self::new("42P07", message)
	}

	/// `57014 query_canceled`
	#[allow(dead_code)]
	pub(super) fn query_canceled(message: impl Into<String>) -> Self {
		Self::new("57014", message)
	}

	/// `40001 serialization_failure`
	#[allow(dead_code)]
	pub(super) fn serialization_failure(message: impl Into<String>) -> Self {
		Self::new("40001", message)
	}

	/// `23502 not_null_violation`
	#[allow(dead_code)]
	pub(super) fn not_null_violation(message: impl Into<String>) -> Self {
		Self::new("23502", message)
	}

	/// `23514 check_violation`
	#[allow(dead_code)]
	pub(super) fn check_violation(message: impl Into<String>) -> Self {
		Self::new("23514", message)
	}

	/// `23503 foreign_key_violation`
	#[allow(dead_code)]
	pub(super) fn foreign_key_violation(message: impl Into<String>) -> Self {
		Self::new("23503", message)
	}

	/// `22012 division_by_zero`
	#[allow(dead_code)]
	pub(super) fn division_by_zero(message: impl Into<String>) -> Self {
		Self::new("22012", message)
	}

	/// `42703 undefined_column`
	#[allow(dead_code)]
	pub(super) fn undefined_column(message: impl Into<String>) -> Self {
		Self::new("42703", message)
	}
}

impl From<&TypesError> for PgError {
	fn from(err: &TypesError) -> Self {
		let msg = err.message();
		let mut table_name: Option<String> = None;
		let constraint_name: Option<String> = None;
		let mut detail: Option<String> = None;

		let code = match err.details() {
			ErrorDetails::AlreadyExists(opt) => match opt {
				Some(AlreadyExistsError::Table {
					name,
				}) => {
					table_name = Some(name.clone());
					detail = Some(format!("relation \"{name}\" already exists"));
					"42P07"
				}
				Some(AlreadyExistsError::Record {
					id,
				}) => {
					detail = Some(format!("Key (id)=({id}) already exists."));
					"23505"
				}
				Some(
					AlreadyExistsError::Database {
						..
					}
					| AlreadyExistsError::Namespace {
						..
					},
				) => "42P04",
				_ => {
					let lower = msg.to_ascii_lowercase();
					if lower.contains("table") || lower.contains("relation") {
						"42P07"
					} else {
						"23505"
					}
				}
			},
			ErrorDetails::Query(opt) => match opt {
				Some(
					QueryError::TimedOut {
						..
					}
					| QueryError::Cancelled,
				) => "57014",
				Some(QueryError::TransactionConflict) => "40001",
				_ => {
					let lower = msg.to_ascii_lowercase();
					if lower.contains("timed out")
						|| lower.contains("timeout")
						|| lower.contains("cancel")
					{
						"57014"
					} else if lower.contains("conflict") || lower.contains("serialization") {
						"40001"
					} else {
						"XX000"
					}
				}
			},
			ErrorDetails::NotFound(opt) => match opt {
				Some(NotFoundError::Table {
					name,
				}) => {
					table_name = Some(name.clone());
					"42P01"
				}
				Some(
					NotFoundError::Database {
						..
					}
					| NotFoundError::Namespace {
						..
					},
				) => "3D000",
				_ => {
					let lower = msg.to_ascii_lowercase();
					if lower.contains("database") {
						"3D000"
					} else if lower.contains("table") {
						"42P01"
					} else {
						"42704"
					}
				}
			},
			ErrorDetails::Validation(opt) => {
				let lower = msg.to_ascii_lowercase();
				if lower.contains("division by zero") || lower.contains("divide by zero") {
					"22012"
				} else if lower.contains("not null") {
					"23502"
				} else if lower.contains("check constraint") || lower.contains("violates check") {
					"23514"
				} else if lower.contains("foreign key") {
					"23503"
				} else if let Some(ValidationError::InvalidParameter {
					..
				}) = opt
				{
					"22023"
				} else {
					"42601"
				}
			}
			ErrorDetails::NotAllowed(_) => "42501",
			ErrorDetails::Thrown => "P0001",
			ErrorDetails::Configuration(_) => "0A000",
			ErrorDetails::Serialization(_) => "40001",
			_ => {
				let lower = msg.to_ascii_lowercase();
				if lower.contains("division by zero") || lower.contains("divide by zero") {
					"22012"
				} else if lower.contains("not null") {
					"23502"
				} else if lower.contains("check constraint") || lower.contains("violates check") {
					"23514"
				} else if lower.contains("foreign key") {
					"23503"
				} else if lower.contains("already exists") || lower.contains("duplicate key") {
					if lower.contains("table") || lower.contains("relation") {
						"42P07"
					} else {
						"23505"
					}
				} else if lower.contains("does not exist") && lower.contains("column") {
					"42703"
				} else if lower.contains("timed out")
					|| lower.contains("timeout")
					|| lower.contains("canceled")
					|| lower.contains("cancelled")
				{
					"57014"
				} else if lower.contains("transaction conflict")
					|| lower.contains("serialization")
					|| lower.contains("conflict")
				{
					"40001"
				} else if lower.contains("database") {
					"3D000"
				} else if lower.contains("auth") || lower.contains("password") {
					"28000"
				} else {
					"XX000"
				}
			}
		};

		let mut pg_err = Self::new(code, msg);
		if let Some(t) = table_name {
			pg_err = pg_err.with_table(t);
		}
		if let Some(n) = constraint_name {
			pg_err = pg_err.with_constraint(n);
		}
		if let Some(d) = detail {
			pg_err = pg_err.with_detail(d);
		}
		pg_err
	}
}

impl From<TypesError> for PgError {
	fn from(err: TypesError) -> Self {
		Self::from(&err)
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;

	#[test]
	fn test_already_exists_table_mapping() {
		let err = TypesError::already_exists(
			"table already exists".to_string(),
			Some(AlreadyExistsError::Table {
				name: "users".to_string(),
			}),
		);
		let pg_err = PgError::from(&err);
		assert_eq!(pg_err.code, "42P07");
		assert_eq!(pg_err.table_name(), Some("users"));
		assert_eq!(pg_err.detail(), Some("relation \"users\" already exists"));
	}

	#[test]
	fn test_already_exists_record_mapping() {
		let err = TypesError::already_exists(
			"record already exists".to_string(),
			Some(AlreadyExistsError::Record {
				id: "users:1".to_string(),
			}),
		);
		let pg_err = PgError::from(&err);
		assert_eq!(pg_err.code, "23505");
		assert_eq!(pg_err.detail(), Some("Key (id)=(users:1) already exists."));
	}

	#[test]
	fn test_query_timeout_and_cancel_mapping() {
		let timeout_err = TypesError::query(
			"timed out".to_string(),
			Some(QueryError::TimedOut {
				duration: Duration::from_secs(5),
			}),
		);
		assert_eq!(PgError::from(&timeout_err).code, "57014");

		let cancel_err =
			TypesError::query("query canceled".to_string(), Some(QueryError::Cancelled));
		assert_eq!(PgError::from(&cancel_err).code, "57014");
	}

	#[test]
	fn test_transaction_conflict_mapping() {
		let conflict_err = TypesError::query(
			"conflict detected".to_string(),
			Some(QueryError::TransactionConflict),
		);
		assert_eq!(PgError::from(&conflict_err).code, "40001");
	}

	#[test]
	fn test_constraint_and_division_by_zero_mappings() {
		let div_err = TypesError::validation("division by zero occurred".to_string(), None);
		assert_eq!(PgError::from(&div_err).code, "22012");

		let not_null_err =
			TypesError::validation("value violates not null constraint".to_string(), None);
		assert_eq!(PgError::from(&not_null_err).code, "23502");

		let check_err = TypesError::validation("violates check constraint".to_string(), None);
		assert_eq!(PgError::from(&check_err).code, "23514");

		let fk_err = TypesError::validation("violates foreign key constraint".to_string(), None);
		assert_eq!(PgError::from(&fk_err).code, "23503");
	}

	#[test]
	fn test_builder_methods() {
		let err = PgError::check_violation("check violation")
			.with_detail("f > 0 violated")
			.with_hint("Check values")
			.with_position(42)
			.with_schema("public")
			.with_table("t")
			.with_column("c")
			.with_data_type("int4")
			.with_constraint("chk_c_positive");

		assert_eq!(err.code, "23514");
		assert_eq!(err.detail(), Some("f > 0 violated"));
		assert_eq!(err.hint(), Some("Check values"));
		assert_eq!(err.position(), Some(42));
		assert_eq!(err.schema_name(), Some("public"));
		assert_eq!(err.table_name(), Some("t"));
		assert_eq!(err.column_name(), Some("c"));
		assert_eq!(err.data_type_name(), Some("int4"));
		assert_eq!(err.constraint_name(), Some("chk_c_positive"));
	}
}
