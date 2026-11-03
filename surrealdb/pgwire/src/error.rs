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
	#[cfg(feature = "listener")]
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

/// The planner's own message in the text of a planner error.
///
/// The datastore reports a planner failure as a validation error whose message is
/// `<dialect> <statement> lowering error: <the planner's message>`, where the dialect is
/// `PostgreSQL` or `MySQL` and the statement is one upper-case word. A prepared statement that the
/// planner refuses at Parse carries the planner's message alone. Any other text is returned
/// whole, so a message that only holds `lowering error: ` is not read as a planner error.
fn planner_message(message: &str) -> &str {
	match message.split_once(" lowering error: ") {
		Some((head, reason)) if is_lowering_head(head) => reason,
		_ => message,
	}
}

fn is_lowering_head(head: &str) -> bool {
	let mut words = head.split(' ');
	match (words.next(), words.next(), words.next()) {
		(Some("PostgreSQL" | "MySQL"), Some(statement), None) => {
			!statement.is_empty() && statement.bytes().all(|byte| byte.is_ascii_uppercase())
		}
		_ => false,
	}
}

/// How the planner words the errors that name something from the client's statement, with the
/// code it chose for each (`PlanError::pg_sqlstate`): the text before the name, the text after it,
/// and the SQLSTATE. A message is one of these when it starts and ends with that text, so what the
/// name holds does not decide which error it is.
const NAMED_PLANNER_ERRORS: &[(&str, &str, &str)] = &[
	("schema or table \"", "\" does not exist", "42P01"),
	("missing FROM-clause entry for table \"", "\"", "42P01"),
	("column \"", "\" does not exist", "42703"),
	("column reference \"", "\" is ambiguous", "42702"),
	("GROUP BY position ", " is not in select list", "42P10"),
	("ORDER BY position ", " is not in select list", "42P10"),
	("for SELECT DISTINCT, ORDER BY expressions must appear in select list", "", "42P10"),
	(
		"column \"",
		"\" must appear in the GROUP BY clause or be used in an aggregate function",
		"42803",
	),
	("aggregate functions are not allowed in ", "", "42803"),
	("window functions are not allowed in ", "", "42P20"),
	("cannot override PARTITION BY clause of window \"", "\"", "42P20"),
	("cannot override ORDER BY clause of window \"", "\"", "42P20"),
	("cannot copy window \"", "\" because it has a frame clause", "42P20"),
	("function \"", "\" does not exist", "42883"),
	("function \"", " were provided", "42883"),
	("function \"", "\" must be called with an OVER clause", "42809"),
	("unrecognized configuration parameter \"", "\"", "42704"),
	("operator class \"", "\"", "42704"),
	("data type ", "\"", "42704"),
	("extension \"", "\" is not available", "58P01"),
	("type \"", "\" does not exist", "42704"),
	("constraint \"", "\" already exists", "42710"),
	("enum label \"", "\" used more than once", "42710"),
	("column \"", "\" specified more than once", "42701"),
];

/// The SQLSTATE of a refusal of the engine to define, alter or remove a type, which a domain, a
/// composite type and an enum all are:
/// - a type whose name is taken is `42710`, and one that does not exist is `42704`;
/// - a type that a column, a function or another type still names is `2BP01`;
/// - an enum value added that the type already holds is `42710`, one renamed that it does not hold
///   is `22023`, and a value edit of a type that is no enum is `42809`;
/// - `DROP DOMAIN` of a type that is no domain, and `DROP TYPE` of a domain, are `42809`;
/// - an enum past the number of labels a type can hold is `54000`.
///
/// Any other message gives `None`. As for [`engine_sqlstate`], a message is read by the text it
/// starts and ends with, which the client's own names do not choose.
fn user_type_sqlstate(message: &str) -> Option<&'static str> {
	let refusal = message.strip_prefix("Invalid statement: ").unwrap_or(message);
	if refusal.starts_with("Cannot change the type '")
		&& refusal.contains(" holds a value it does not admit: ")
	{
		// NULL or NONE ends the refusal only when it is the whole value held. Refused by the
		// type's body it is a NOT NULL violation; refused by one of the type's checks it is a
		// CHECK violation, as any other value is.
		let absent =
			refusal.ends_with(" but found `NULL`") || refusal.ends_with(" but found `NONE`");
		let checked =
			refusal.contains(" failed)` but found `") || refusal.contains(" failed with: ");
		return Some(if absent && !checked {
			"23502"
		} else {
			"23514"
		});
	}
	if refusal.starts_with("The type '") {
		if refusal.contains("' already has the value '") {
			return Some("42710");
		}
		if refusal.contains("' has no value '") {
			return Some("22023");
		}
		if refusal.ends_with(" is not a union of string labels, so it has no values to alter") {
			return Some("42809");
		}
		if refusal.ends_with("' is not a domain; remove it with DROP TYPE")
			|| refusal.ends_with("' is a domain; remove it with DROP DOMAIN")
		{
			return Some("42809");
		}
		if refusal.contains(" a union holds at most ") {
			return Some("54000");
		}
	}
	if message.starts_with("The type '") && message.ends_with("' already exists") {
		Some("42710")
	} else if message.starts_with("The type '") && message.ends_with("' does not exist") {
		Some("42704")
	} else if message.starts_with("Invalid statement: Cannot remove the type '") {
		Some("2BP01")
	} else {
		None
	}
}

/// The SQLSTATE of a refusal by the SQL planner, which is the code the planner chose for it
/// (`PlanError::pg_sqlstate`). A feature the planner does not lower is `0A000`, a text it cannot
/// read as a value of the type it must have is `22P02`, an operator that has no definition for
/// its operand types is `42883`, a value beyond the range of its type is `22003` and a
/// constraint the table does not have is `42704`; a table, column, function or position the
/// statement names wrongly has the code of that kind of error ([`NAMED_PLANNER_ERRORS`]). Any
/// other message gives `None`.
fn planner_sqlstate(message: &str) -> Option<&'static str> {
	let reason = planner_message(message);
	if unsupported_construct(message).is_some() {
		Some("0A000")
	} else if reason.starts_with("invalid input syntax for type: ") {
		Some("22P02")
	} else if reason.starts_with("operator does not exist: ") {
		Some("42883")
	} else if is_out_of_range(reason) {
		Some("22003")
	} else if reason.starts_with("constraint \"") && reason.ends_with("\" does not exist") {
		Some("42704")
	} else if reason.starts_with("invalid parameter value: ") {
		Some("22023")
	} else if reason.starts_with("data exception: ") {
		Some("22000")
	} else if reason.starts_with("program limit exceeded: ") {
		Some("54000")
	} else {
		NAMED_PLANNER_ERRORS
			.iter()
			.find(|(before, after, _)| {
				reason.strip_prefix(before).is_some_and(|rest| rest.ends_with(after))
			})
			.map(|(_, _, code)| *code)
	}
}

/// Whether `reason` is the planner's refusal of a value beyond the range of its type: an integer
/// type's `<type> out of range`, `numeric field overflow`, a float's `value out of range:
/// overflow` or `underflow`, and a text constant's `value "<text>" is out of range for type
/// <type>`.
fn is_out_of_range(reason: &str) -> bool {
	matches!(
		reason,
		"smallint out of range"
			| "integer out of range"
			| "bigint out of range"
			| "numeric field overflow"
			| "value out of range: overflow"
			| "value out of range: underflow"
	) || reason
		.strip_prefix("value \"")
		.and_then(|rest| rest.rsplit_once("\" is out of range for type "))
		.is_some_and(|(_, type_name)| matches!(type_name, "smallint" | "integer" | "bigint"))
}

/// The kind of construct the planner refused as unsupported, from the text of a planner error:
/// `unsupported expression` or `unsupported statement`, and `None` for any other message.
///
/// The planner's message goes on to describe the construct from the client's statement, so the
/// kind alone is what a counter or a log line may carry.
pub(super) fn unsupported_construct(message: &str) -> Option<&'static str> {
	let reason = planner_message(message);
	["unsupported expression", "unsupported statement"]
		.into_iter()
		.find(|kind| reason.strip_prefix(kind).is_some_and(|rest| rest.starts_with(": ")))
}

/// The SQLSTATE of a write the engine refuses for a constraint of the table. The engine words these
/// refusals as text that no kind of `ErrorDetails` tells apart from other failures:
/// - a UNIQUE index that already holds the key, whether the message names the record and value in
///   conflict or withholds them from a caller who may not read them, is `23505`;
/// - a NULL, or no value, for a field that admits neither is `23502`;
/// - any other value that is not of the type of its field is `42804`. The engine refuses a value of
///   another type even when its text would read as the field's type, so the refusal is a datatype
///   mismatch and not an invalid text representation;
/// - a vector function over vectors of different dimensions is `22000`, as `different vector
///   dimensions` is in pgvector.
///
/// Any other message gives `None`.
///
/// A message is read by the text the engine starts or ends it with, never by a word anywhere in
/// it: the rest of a message quotes the client's own values, which must not choose the code.
/// The end of the engine's refusal of a value by the assertion a `NOT NULL` column lowers to.
const NOT_NULL_ASSERTION_TAIL: &str =
	" but field must conform to: $value != NONE AND $value != NULL";

fn engine_sqlstate(message: &str) -> Option<&'static str> {
	if message.starts_with("Database index `") && message.contains("` already contains ") {
		Some("23505")
	} else if message.starts_with("Couldn't coerce value for field `") {
		// NULL or NONE ends the message only when it is the whole value written to the field. An
		// element of an array or an object is followed by where in the field it was found, and is
		// a value of the wrong type for that element.
		if message.ends_with(" but found `NULL`") || message.ends_with(" but found `NONE`") {
			Some("23502")
		} else if message.contains(" failed)` but found `") || message.contains(" failed with: ") {
			// A value of a named type that one of the type's checks refuses: a domain's CHECK.
			Some("23514")
		} else {
			Some("42804")
		}
	} else if message.starts_with("Found NULL for field `")
		&& message.ends_with(NOT_NULL_ASSERTION_TAIL)
	{
		// A NULL a `NOT NULL` column's assertion refuses, over a type that admits NULL.
		Some("23502")
	} else if message.starts_with("Could not cast into `") {
		// Text written to a typed column that does not read as the column's type.
		Some("42804")
	} else if message.starts_with("Incorrect arguments for function vector::")
		&& message.ends_with("must be of the same dimension.")
	{
		// A distance between vectors of different dimensions.
		Some("22000")
	} else {
		sql_type_sqlstate(message)
	}
}

/// The SQLSTATE of a value that does not fit its field's declared SQL type. The reason opens the
/// message (`DocError::FieldSqlType`), before the client's field name and declared type, so only
/// its start is read. Any other message gives `None`.
fn sql_type_sqlstate(message: &str) -> Option<&'static str> {
	if message.starts_with("value too long for type ")
		|| message.starts_with("bit string too long for type ")
	{
		Some("22001")
	} else if message.starts_with("bit string length ") {
		Some("22026")
	} else if message.starts_with("invalid binary digit in a bit string: ") {
		Some("22P02")
	} else if message.starts_with("numeric field overflow ") {
		Some("22003")
	} else if message.starts_with("date/time field value out of range") {
		Some("22008")
	} else if message.starts_with("JSON cannot hold a ") {
		Some("22032")
	} else {
		None
	}
}

/// The SQLSTATE of an error a query raised with `THROW`: the code PostgreSQL gives the error a SQL
/// function lowering throws, from the text after the engine's own prefix, and `P0001`
/// (`raise_exception`) for any other.
fn thrown_sqlstate(message: &str) -> &'static str {
	let Some(thrown) = message.strip_prefix("An error occurred: ") else {
		return "P0001";
	};
	match thrown {
		"negative substring length not allowed" => "22011",
		"division by zero" => "22012",
		"cannot take logarithm of zero" | "cannot take logarithm of a negative number" => "2201E",
		"field position must not be zero" => "22023",
		unit if unit.starts_with("unit \"") && unit.contains("\" not recognized for type ") => {
			"22023"
		}
		_ => "P0001",
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
				if let Some(code) = planner_sqlstate(msg) {
					code
				} else if let Some(code) = engine_sqlstate(msg) {
					code
				} else if lower.contains("division by zero") || lower.contains("divide by zero") {
					"22012"
				} else if lower.contains("not null")
					|| (lower.contains("couldn't coerce value for field")
						&& lower.ends_with("but found `null`"))
				{
					// A NULL written to a field whose type refuses it.
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
			// A foreign key column's assertion throws the message PostgreSQL words for the
			// violation.
			ErrorDetails::Thrown if msg.contains("violates foreign key constraint") => "23503",
			ErrorDetails::Thrown => thrown_sqlstate(msg),
			ErrorDetails::Configuration(_) => "0A000",
			ErrorDetails::Serialization(_) => "40001",
			ErrorDetails::Connection(_) | ErrorDetails::Internal | ErrorDetails::Context | _ => {
				let lower = msg.to_ascii_lowercase();
				if let Some(code) = engine_sqlstate(msg) {
					code
				} else if lower.contains("division by zero") || lower.contains("divide by zero") {
					"22012"
				} else if lower.contains("not null") {
					"23502"
				} else if lower.contains("check constraint") || lower.contains("violates check") {
					"23514"
				} else if lower.contains("foreign key")
					|| lower.contains("with an on delete reject clause")
				{
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

		// The engine words its refusals of a user type as invalid statements, whatever kind of
		// error carries them.
		let code = user_type_sqlstate(msg).unwrap_or(code);
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
	fn a_negative_substring_count_is_22011_and_another_throw_is_p0001() {
		let negative = TypesError::thrown(
			"An error occurred: negative substring length not allowed".to_string(),
		);
		assert_eq!(PgError::from(&negative).code, "22011");
		let other = TypesError::thrown("An error occurred: boom".to_string());
		assert_eq!(PgError::from(&other).code, "P0001");
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
		let null_to_link = TypesError::validation(
			"Couldn't coerce value for field `dept` of `emp:1`: Expected `record<dept>` but found `NULL`"
				.to_string(),
			None,
		);
		assert_eq!(PgError::from(&null_to_link).code, "23502");

		let check_err = TypesError::validation("violates check constraint".to_string(), None);
		assert_eq!(PgError::from(&check_err).code, "23514");

		let fk_err = TypesError::validation("violates foreign key constraint".to_string(), None);
		assert_eq!(PgError::from(&fk_err).code, "23503");
	}

	#[test]
	fn test_foreign_key_violations_map_to_23503() {
		// The assertion of a foreign key column throws the message PostgreSQL words.
		let thrown = TypesError::thrown(
			"An error occurred: insert or update on table \"p\" violates foreign key constraint \"p_a_fkey\""
				.to_string(),
		);
		assert_eq!(PgError::from(&thrown).code, "23503");
		let other = TypesError::thrown("An error occurred: boom".to_string());
		assert_eq!(PgError::from(&other).code, "P0001");
		// A delete that a reference refuses.
		let reject = TypesError::internal(
			"Cannot delete `u:1` as it is referenced by `p:1` with an ON DELETE REJECT clause"
				.to_string(),
		);
		assert_eq!(PgError::from(&reject).code, "23503");
	}

	#[test]
	fn test_planner_refusals_keep_the_code_the_planner_chose() {
		use surrealdb_sqlplan::PlanError;

		for plan in [
			PlanError::UnsupportedExpression("the @> containment operator".to_string()),
			PlanError::UnsupportedStatement("MERGE".to_string()),
			PlanError::InvalidTextRepresentation("malformed array literal: \"{a,\"".to_string()),
			PlanError::UndefinedOperator("jsonb -> bigint".to_string()),
			PlanError::NumericValueOutOfRange,
		] {
			for statement in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
				let message = format!("PostgreSQL {statement} lowering error: {plan}");
				let err = TypesError::validation(message.clone(), None);
				assert_eq!(PgError::from(&err).code, plan.pg_sqlstate(), "{message}");
			}
		}
	}

	/// One planner error of every kind that a planner path raises, with the SQLSTATE PostgreSQL
	/// 17 reports for the equivalent error. The `match` has no wildcard, so a new kind does not
	/// compile until it is placed here or among those no planner path raises.
	fn planner_errors_with_postgres_sqlstates() -> Vec<(surrealdb_sqlplan::PlanError, &'static str)>
	{
		use surrealdb_sqlplan::PlanError;

		let name = || "t".to_string();
		let all = vec![
			PlanError::UndefinedTable(name()),
			PlanError::UndefinedColumn(name()),
			PlanError::AmbiguousColumn(name()),
			PlanError::MissingFromClauseEntry(name()),
			PlanError::InvalidGroupByPosition(9),
			PlanError::InvalidOrderByPosition(9),
			PlanError::DistinctOrderByNotSelected,
			PlanError::ColumnNotInGroupBy(name()),
			PlanError::AggregateNotAllowed("WHERE".to_string()),
			PlanError::WindowNotAllowed("WHERE clause".to_string()),
			PlanError::WindowPartitionOverride(name()),
			PlanError::WindowOrderOverride(name()),
			PlanError::WindowFrameCopy(name()),
			PlanError::UndefinedFunction(name()),
			PlanError::UndefinedOperator("boolean + integer".to_string()),
			PlanError::FunctionArgumentCountMismatch {
				name: name(),
				expected: "at most 1".to_string(),
				got: 2,
			},
			PlanError::WindowFunctionWithoutOver(name()),
			PlanError::SubqueryMustHaveAlias,
			PlanError::DivisionByZero,
			PlanError::NumericValueOutOfRange,
			PlanError::ValueOutOfRange("smallint out of range".to_string()),
			PlanError::ValueOutOfRange("numeric field overflow".to_string()),
			PlanError::ValueOutOfRange("value out of range: overflow".to_string()),
			PlanError::ValueOutOfRange(
				"value \"100000\" is out of range for type smallint".to_string(),
			),
			PlanError::InvalidTextRepresentation("integer: \"a\"".to_string()),
			PlanError::UnsupportedStatement("MERGE".to_string()),
			PlanError::UnsupportedExpression("Row".to_string()),
			PlanError::UndefinedObject(name()),
			PlanError::UndefinedConstraint {
				constraint: name(),
				table: name(),
			},
			PlanError::InvalidParameterValue("value 1 out of bounds for option \"m\"".to_string()),
			PlanError::DataException("expected 3 dimensions, not 2".to_string()),
			PlanError::ProgramLimitExceeded(
				"column cannot have more than 2000 dimensions for hnsw index".to_string(),
			),
			PlanError::UndefinedOperatorClass {
				name: name(),
				method: "hnsw".to_string(),
			},
			PlanError::NoDefaultOperatorClass {
				type_name: "vector".to_string(),
				method: "hnsw".to_string(),
			},
			PlanError::ExtensionNotAvailable(name()),
			PlanError::UndefinedType(name()),
			PlanError::UndefinedDomainConstraint {
				constraint: name(),
				domain: name(),
			},
			PlanError::DuplicateDomainConstraint {
				constraint: name(),
				domain: name(),
			},
			PlanError::DuplicateEnumLabel(name()),
			PlanError::DuplicateColumn(name()),
			PlanError::Custom("each query must have the same number of columns".to_string()),
		];
		all.into_iter()
			.filter_map(|plan| {
				let code = match &plan {
					PlanError::UndefinedTable(_) | PlanError::MissingFromClauseEntry(_) => "42P01",
					PlanError::UndefinedColumn(_) => "42703",
					PlanError::AmbiguousColumn(_) => "42702",
					PlanError::InvalidGroupByPosition(_)
					| PlanError::InvalidOrderByPosition(_)
					| PlanError::DistinctOrderByNotSelected => "42P10",
					PlanError::ColumnNotInGroupBy(_) | PlanError::AggregateNotAllowed(_) => "42803",
					PlanError::WindowNotAllowed(_)
					| PlanError::WindowPartitionOverride(_)
					| PlanError::WindowOrderOverride(_)
					| PlanError::WindowFrameCopy(_) => "42P20",
					PlanError::UndefinedFunction(_)
					| PlanError::UndefinedOperator(_)
					| PlanError::FunctionArgumentCountMismatch {
						..
					} => "42883",
					PlanError::WindowFunctionWithoutOver(_) => "42809",
					PlanError::SubqueryMustHaveAlias => "42601",
					PlanError::DivisionByZero => "22012",
					PlanError::NumericValueOutOfRange | PlanError::ValueOutOfRange(_) => "22003",
					PlanError::InvalidTextRepresentation(_) => "22P02",
					PlanError::UnsupportedStatement(_) | PlanError::UnsupportedExpression(_) => {
						"0A000"
					}
					PlanError::UndefinedObject(_)
					| PlanError::UndefinedType(_)
					| PlanError::UndefinedDomainConstraint {
						..
					}
					| PlanError::UndefinedConstraint {
						..
					}
					| PlanError::UndefinedOperatorClass {
						..
					}
					| PlanError::NoDefaultOperatorClass {
						..
					} => "42704",
					PlanError::InvalidParameterValue(_) => "22023",
					PlanError::DatatypeMismatch(_) => "42804",
					PlanError::DataException(_) => "22000",
					PlanError::ProgramLimitExceeded(_) => "54000",
					PlanError::ExtensionNotAvailable(_) => "58P01",
					PlanError::DuplicateDomainConstraint {
						..
					}
					| PlanError::DuplicateEnumLabel(_) => "42710",
					PlanError::DuplicateColumn(_) => "42701",
					// A planning error of no kind of its own is a syntax error.
					PlanError::Custom(_) => "42601",
					// No planner path raises these: the connection reports the state itself.
					PlanError::ParameterOutOfRange(_)
					| PlanError::NoActiveTransaction
					| PlanError::InFailedTransaction
					| PlanError::SavepointDoesNotExist(_)
					| PlanError::ActiveTransactionAlreadyInProgress => return None,
				};
				Some((plan, code))
			})
			.collect()
	}

	#[test]
	fn test_every_planner_error_is_reported_under_the_sqlstate_of_its_kind() {
		for (plan, code) in planner_errors_with_postgres_sqlstates() {
			// `Custom` has no code of its own: the planner reports it as a generic error and the
			// wire, which has nothing more to go by, as a syntax error.
			if !matches!(plan, surrealdb_sqlplan::PlanError::Custom(_)) {
				assert_eq!(plan.pg_sqlstate(), code, "{plan}");
			}
			// A prepared statement is refused with the message alone at Parse, and with the
			// statement it belongs to when it is lowered.
			for head in
				["PostgreSQL SELECT lowering error: ", "PostgreSQL INSERT lowering error: ", ""]
			{
				let message = format!("{head}{plan}");
				let err = TypesError::validation(message.clone(), None);
				assert_eq!(PgError::from(&err).code, code, "{message}");
			}
		}
	}

	#[test]
	fn test_a_name_in_a_planner_error_does_not_choose_its_code() {
		for (message, code) in [
			("column \"function \"x\" does not exist\" does not exist", "42703"),
			("function \"column \"x\" does not exist\" does not exist", "42883"),
			("schema or table \"division by zero\" does not exist", "42P01"),
			("function \"not null\" does not exist", "42883"),
			("column \"foreign key\" does not exist", "42703"),
			("column reference \"already contains\" is ambiguous", "42702"),
		] {
			let err = TypesError::validation(
				format!("PostgreSQL SELECT lowering error: {message}"),
				None,
			);
			assert_eq!(PgError::from(&err).code, code, "{message}");
		}
	}

	#[test]
	fn test_a_constraint_the_engine_refuses_has_the_sqlstate_of_its_kind() {
		// A conflict that names the record and value, and one that withholds them from a caller who
		// may not read them, are both unique violations: the word "Database" in the message does
		// not make either a missing database.
		let unique = TypesError::internal(
			"Database index `t_a_pk` already contains [1, 2], with record `t:x`".to_string(),
		);
		let hidden = TypesError::validation(
			"Database index `t_a_uniq` already contains a conflicting value in another record"
				.to_string(),
			ValidationError::InvalidRequest,
		);
		for err in [&unique, &hidden] {
			let pg = PgError::from(err);
			assert_eq!(pg.code, "23505", "{}", err.message());
			// The client is told what the engine tells it, and nothing from the row.
			assert_eq!(pg.message, err.message());
			assert_eq!(pg.detail(), None);
		}

		for (message, code) in [
			// vectors of different dimensions
			(
				"Incorrect arguments for function vector::distance::euclidean(). The two vectors must be of the same dimension.",
				"22000",
			),
			// NULL, or no value, for a field that admits neither
			(
				"Couldn't coerce value for field `name` of `t:1`: Expected `string` but found `NULL`",
				"23502",
			),
			(
				"Couldn't coerce value for field `name` of `t:1`: Expected `string` but found `NONE`",
				"23502",
			),
			// a value of another type
			(
				"Couldn't coerce value for field `n` of `t:1`: Expected `none | null | int` but found `'abc'`",
				"42804",
			),
			(
				"Couldn't coerce value for field `flag` of `t:1`: Expected `none | null | bool` but found `5`",
				"42804",
			),
			// a NULL element of a field that is an array: PostgreSQL stores it, the field refuses
			// it as a value of another type
			(
				"Couldn't coerce value for field `tags` of `t:1`: Expected `string` but found `NULL` when coercing element at index 1 of `array<string>`",
				"42804",
			),
		] {
			let err = TypesError::validation(message.to_string(), ValidationError::InvalidRequest);
			let pg = PgError::from(&err);
			assert_eq!(pg.code, code, "{message}");
			assert_eq!(pg.message, message);
			assert_eq!(pg.detail(), None);
		}
	}

	#[test]
	fn test_a_value_in_the_message_does_not_choose_the_sqlstate() {
		// Each message quotes a value the client wrote; the words in it are not the engine's.
		for value in [
			"division by zero",
			"divide by zero",
			"not null",
			"check constraint",
			"foreign key",
			"already exists",
			"already contains",
			"Database index `x` already contains 1",
			"timed out",
			"conflict",
			"database",
			"Couldn't coerce value for field `x` of `y`: Expected `int` but found `NULL`",
			"a but found `NULL`",
		] {
			let message = format!(
				"Couldn't coerce value for field `n` of `t:1`: Expected `none | null | int` but found `'{value}'`"
			);
			for err in [
				TypesError::validation(message.clone(), ValidationError::InvalidRequest),
				TypesError::internal(message.clone()),
			] {
				assert_eq!(PgError::from(&err).code, "42804", "{message}");
			}
		}
	}

	#[test]
	fn test_a_message_that_is_not_a_planner_refusal_is_a_syntax_error() {
		for message in [
			"PostgreSQL parse error: unsupported expression: x",
			"Parse error: invalid input syntax for type: x",
			"the value lowering error: unsupported expression: x",
			"PostgreSQL SELECT lowering error: unsupported-expression: x",
			"postgresql SELECT lowering error: unsupported expression: x",
			"PostgreSQL select lowering error: unsupported expression: x",
			"PostgreSQL SELECT FROM lowering error: unsupported expression: x",
			"table \"PostgreSQL SELECT lowering error: unsupported expression: x\" is wrong",
			"expected unsupported expression: x",
		] {
			let err = TypesError::validation(message.to_string(), None);
			assert_eq!(PgError::from(&err).code, "42601", "{message}");
		}
	}

	#[test]
	fn test_a_planner_refusal_is_read_with_or_without_its_statement_prefix() {
		for message in [
			"unsupported expression: ArrayConstructor { elements: [] }",
			"PostgreSQL SELECT lowering error: unsupported expression: Row { fields: [] }",
			"MySQL REPLACE lowering error: unsupported expression: x",
		] {
			assert_eq!(planner_sqlstate(message), Some("0A000"), "{message}");
			assert_eq!(unsupported_construct(message), Some("unsupported expression"), "{message}");
		}
		assert_eq!(
			unsupported_construct("PostgreSQL DDL lowering error: unsupported statement: MERGE"),
			Some("unsupported statement")
		);
		let malformed = "PostgreSQL SELECT lowering error: invalid input syntax for type: x";
		assert_eq!(planner_sqlstate(malformed), Some("22P02"));
		assert_eq!(unsupported_construct(malformed), None);
		let overflow = "PostgreSQL SELECT lowering error: bigint out of range";
		assert_eq!(planner_sqlstate(overflow), Some("22003"));
		assert_eq!(planner_sqlstate("bigint out of range"), Some("22003"));
		assert_eq!(planner_sqlstate("parameter $1 out of range"), None);
		assert_eq!(unsupported_construct(overflow), None);
	}

	#[test]
	fn test_user_type_refusals_have_postgres_codes() {
		for (message, code) in [
			("The type 'posint' already exists", "42710"),
			("The type 'posint' does not exist", "42704"),
			(
				"Invalid statement: Cannot remove the type 'posint': the field 'n' on the table 't' has a type that names it",
				"2BP01",
			),
			("The type 'mood' already has the value 'sad'", "42710"),
			("The type 'mood' has no value 'meh'; its values are 'sad', 'happy'", "22023"),
			(
				"The type 'posint' is not a union of string labels, so it has no values to alter",
				"42809",
			),
			("The type 'mood' is not a domain; remove it with DROP TYPE", "42809"),
			("The type 'd' is a domain; remove it with DROP DOMAIN", "42809"),
			(
				"Invalid statement: The type 'mood' lists 1024 labels; a union holds at most 1023",
				"54000",
			),
			(
				"The type 'mood' cannot take another value: a union holds at most 1023 values over its life",
				"54000",
			),
			(
				"Invalid statement: Cannot change the type 'amount': the field 'a' on the table 'ledger' holds a value it does not admit: Expected `amount (constraint small failed)` but found `500i32`",
				"23514",
			),
			(
				"Invalid statement: Cannot change the type 'maybe': the field 'm' on the table 'ledger' holds a value it does not admit: Expected `maybe (i32)` but found `NULL`",
				"23502",
			),
			(
				"Invalid statement: Cannot change the type 'jdom': the field 'j' on the table 't' holds a value it does not admit: Expected `jdom (constraint c failed)` but found `NULL`",
				"23514",
			),
		] {
			assert_eq!(user_type_sqlstate(message), Some(code), "{message}");
		}
		assert_eq!(
			engine_sqlstate(
				"Couldn't coerce value for field `n` of `t:1`: Expected `posint (constraint positive failed)` but found `-1`"
			),
			Some("23514")
		);
		assert_eq!(user_type_sqlstate("Invalid statement: something else"), None);
		for (message, code) in [
			("PostgreSQL DDL lowering error: type \"d\" does not exist", "42704"),
			(
				"PostgreSQL DDL lowering error: constraint \"c\" for domain \"d\" already exists",
				"42710",
			),
			(
				"PostgreSQL DDL lowering error: constraint \"c\" of domain \"d\" does not exist",
				"42704",
			),
			("PostgreSQL DDL lowering error: column \"a\" specified more than once", "42701"),
		] {
			assert_eq!(planner_sqlstate(message), Some(code), "{message}");
		}
	}

	#[test]
	fn test_the_construct_of_a_refusal_carries_none_of_the_statement() {
		for message in [
			"unsupported expression: ArrayConstructor { elements: [Literal { lit: Integer(918273645) }] }",
			"PostgreSQL SELECT lowering error: unsupported expression: Row { 918273645 }",
		] {
			let construct = unsupported_construct(message).expect("a planner refusal");
			assert!(!construct.contains("918273645"), "{construct}");
		}
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
