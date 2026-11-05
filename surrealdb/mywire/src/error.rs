//! The MySQL error number and SQLSTATE an error is sent under.
//!
//! A client chooses what to do with a failed statement from the number and SQLSTATE of the ERR
//! packet, not from its text: it ignores a duplicate entry, creates a missing table, retries a
//! deadlock and reports a syntax error. An error is therefore sent under the number MySQL gives
//! the same condition, and its message is sent as the engine wrote it.
//!
//! The number is read from, in order:
//!
//! 1. the kind the engine gave the error (a missing table, a duplicate record, a timeout);
//! 2. for a validation or internal error, which says no more than that, the form of its message.
//!    Only text the engine or the planner writes selects a number: the start of the message and,
//!    where the form has a fixed ending, its end. A value, a record id or a name the client wrote
//!    lies between the two, or at the end of a form that has no fixed ending, so it cannot choose
//!    the number;
//! 3. `ER_UNKNOWN_ERROR`, for an error that has no number of its own.
//!
//! The numbers of the planner's errors are in one table, `plan_number`, which covers every kind of
//! `PlanError`. The checks made before a statement runs hold the error as a value and use it
//! directly; a statement the datastore refused arrives as text, whose form is read back to a kind
//! and numbered by the same table.

use surrealdb_sqlplan::PlanError;
use surrealdb_types::{
	AlreadyExistsError, Error as TypesError, ErrorDetails, NotFoundError, QueryError,
	ValidationError,
};

use crate::msg::ErrPacket;

/// An error number and its SQLSTATE.
type Number = (u16, &'static [u8; 5]);

const ER_BAD_DB_ERROR: Number = (1049, b"42000");
const ER_BAD_FIELD_ERROR: Number = (1054, b"42S22");
const ER_BAD_NULL_ERROR: Number = (1048, b"23000");
const ER_DATA_OUT_OF_RANGE: Number = (1690, b"22003");
const ER_DATA_TOO_LONG: Number = (1406, b"22001");
const ER_INVALID_JSON_TEXT: Number = (3140, b"22032");
const ER_DB_CREATE_EXISTS: Number = (1007, b"HY000");
const ER_DERIVED_MUST_HAVE_ALIAS: Number = (1248, b"42000");
const ER_DIVISION_BY_ZERO: Number = (1365, b"22012");
const ER_DUP_ENTRY: Number = (1062, b"23000");
const ER_DUP_FIELDNAME: Number = (1060, b"42S21");
const ER_FIELD_IN_ORDER_NOT_SELECT: Number = (3065, b"HY000");
const ER_INVALID_GROUP_FUNC_USE: Number = (1111, b"HY000");
const ER_LOCK_DEADLOCK: Number = (1213, b"40001");
const ER_NON_UNIQ_ERROR: Number = (1052, b"23000");
const ER_NOT_SUPPORTED_YET: Number = (1235, b"42000");
const ER_NO_DEFAULT_FOR_FIELD: Number = (1364, b"HY000");
const ER_NO_SUCH_TABLE: Number = (1146, b"42S02");
const ER_PARSE_ERROR: Number = (1064, b"42000");
const ER_QUERY_INTERRUPTED: Number = (1317, b"70100");
const ER_QUERY_TIMEOUT: Number = (3024, b"HY000");
const ER_SIGNAL_EXCEPTION: Number = (1644, b"45000");
const ER_SP_DOES_NOT_EXIST: Number = (1305, b"42000");
const ER_TABLEACCESS_DENIED_ERROR: Number = (1142, b"42000");
const ER_TABLE_EXISTS_ERROR: Number = (1050, b"42S01");
const ER_TRUNCATED_WRONG_VALUE_FOR_FIELD: Number = (1366, b"HY000");
const ER_UNKNOWN_ERROR: Number = (1105, b"HY000");
const ER_CANT_DROP_FIELD_OR_KEY: Number = (1091, b"42000");
const ER_UNKNOWN_SYSTEM_VARIABLE: Number = (1193, b"HY000");
const ER_WINDOW_INVALID_WINDOW_FUNC_USE: Number = (3593, b"HY000");
const ER_WINDOW_NO_CHILD_PARTITIONING: Number = (3581, b"HY000");
const ER_WINDOW_NO_INHERIT_FRAME: Number = (3582, b"HY000");
const ER_WINDOW_NO_REDEFINE_ORDER_BY: Number = (3583, b"HY000");
const ER_WRONG_FIELD_WITH_GROUP: Number = (1055, b"42000");
const ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT: Number = (1582, b"42000");

impl From<&TypesError> for ErrPacket {
	fn from(err: &TypesError) -> Self {
		let (code, state) = engine_number(err);
		Self::general(code, state, err.message())
	}
}

/// A refusal by the planner held as a value, as the checks made before a statement runs hold it.
/// It is numbered by its kind, not by its message.
impl From<&PlanError> for ErrPacket {
	fn from(err: &PlanError) -> Self {
		let (code, state) = plan_number(err);
		Self::general(code, state, err.to_string())
	}
}

/// The number of each kind of planner error. No other table gives a `PlanError` a number: the form
/// of a planner message is read back to its kind and numbered here.
///
/// The match has no wildcard arm, so a kind added to `PlanError` does not build until it has a
/// number. A kind that MySQL has no number for is `ER_UNKNOWN_ERROR`.
fn plan_number(err: &PlanError) -> Number {
	match err {
		PlanError::UndefinedTable(_) => ER_NO_SUCH_TABLE,
		PlanError::UndefinedColumn(_)
		| PlanError::MissingFromClauseEntry(_)
		| PlanError::InvalidGroupByPosition(_)
		| PlanError::InvalidOrderByPosition(_) => ER_BAD_FIELD_ERROR,
		PlanError::AmbiguousColumn(_) => ER_NON_UNIQ_ERROR,
		PlanError::DistinctOrderByNotSelected => ER_FIELD_IN_ORDER_NOT_SELECT,
		PlanError::ColumnNotInGroupBy(_) => ER_WRONG_FIELD_WITH_GROUP,
		PlanError::AggregateNotAllowed(_) => ER_INVALID_GROUP_FUNC_USE,
		PlanError::WindowNotAllowed(_) => ER_WINDOW_INVALID_WINDOW_FUNC_USE,
		PlanError::WindowPartitionOverride(_) => ER_WINDOW_NO_CHILD_PARTITIONING,
		PlanError::WindowOrderOverride(_) => ER_WINDOW_NO_REDEFINE_ORDER_BY,
		PlanError::WindowFrameCopy(_) => ER_WINDOW_NO_INHERIT_FRAME,
		PlanError::UndefinedFunction(_) => ER_SP_DOES_NOT_EXIST,
		PlanError::FunctionArgumentCountMismatch {
			..
		} => ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
		// A window function with no OVER clause is a syntax error to MySQL.
		PlanError::WindowFunctionWithoutOver(_) => ER_PARSE_ERROR,
		PlanError::SubqueryMustHaveAlias => ER_DERIVED_MUST_HAVE_ALIAS,
		PlanError::DivisionByZero => ER_DIVISION_BY_ZERO,
		PlanError::NumericValueOutOfRange => ER_DATA_OUT_OF_RANGE,
		PlanError::InvalidTextRepresentation(_) => ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
		PlanError::UnsupportedStatement(_) | PlanError::UnsupportedExpression(_) => {
			ER_NOT_SUPPORTED_YET
		}
		PlanError::UndefinedObject(_) => ER_UNKNOWN_SYSTEM_VARIABLE,
		PlanError::UndefinedConstraint {
			..
		} => ER_CANT_DROP_FIELD_OR_KEY,
		PlanError::DuplicateColumn(_) => ER_DUP_FIELDNAME,
		// Domains and composite types are PostgreSQL's; MySQL has no number for them.
		PlanError::UndefinedType(_)
		| PlanError::UndefinedDomainConstraint {
			..
		}
		| PlanError::DuplicateDomainConstraint {
			..
		}
		| PlanError::DuplicateEnumLabel(_)
		| PlanError::ValueOutOfRange(_)
		| PlanError::UndefinedOperator(_)
		| PlanError::ParameterOutOfRange(_)
		| PlanError::NoActiveTransaction
		| PlanError::InFailedTransaction
		| PlanError::SavepointDoesNotExist(_)
		| PlanError::ActiveTransactionAlreadyInProgress
		| PlanError::InvalidParameterValue(_)
		| PlanError::DatatypeMismatch(_)
		| PlanError::DataException(_)
		| PlanError::ProgramLimitExceeded(_)
		| PlanError::UndefinedOperatorClass {
			..
		}
		| PlanError::NoDefaultOperatorClass {
			..
		}
		| PlanError::ExtensionNotAvailable(_)
		| PlanError::Custom(_) => ER_UNKNOWN_ERROR,
	}
}

fn engine_number(err: &TypesError) -> Number {
	match err.details() {
		ErrorDetails::AlreadyExists(Some(AlreadyExistsError::Table {
			..
		})) => ER_TABLE_EXISTS_ERROR,
		ErrorDetails::AlreadyExists(Some(
			AlreadyExistsError::Database {
				..
			}
			| AlreadyExistsError::Namespace {
				..
			},
		)) => ER_DB_CREATE_EXISTS,
		// A record, or a key the storage layer holds already.
		ErrorDetails::AlreadyExists(_) => ER_DUP_ENTRY,
		ErrorDetails::NotFound(Some(NotFoundError::Table {
			..
		})) => ER_NO_SUCH_TABLE,
		ErrorDetails::NotFound(Some(
			NotFoundError::Database {
				..
			}
			| NotFoundError::Namespace {
				..
			},
		)) => ER_BAD_DB_ERROR,
		ErrorDetails::NotAllowed(_) => ER_TABLEACCESS_DENIED_ERROR,
		ErrorDetails::Query(Some(QueryError::TimedOut {
			..
		})) => ER_QUERY_TIMEOUT,
		ErrorDetails::Query(Some(QueryError::Cancelled)) => ER_QUERY_INTERRUPTED,
		ErrorDetails::Query(Some(QueryError::TransactionConflict)) => ER_LOCK_DEADLOCK,
		ErrorDetails::Configuration(_) => ER_NOT_SUPPORTED_YET,
		ErrorDetails::Thrown => ER_SIGNAL_EXCEPTION,
		ErrorDetails::Validation(Some(ValidationError::Parse)) => ER_PARSE_ERROR,
		// The engine reports most statement failures without a kind of their own: a planner
		// refusal and a parse error of the MySQL front end are validation errors, a schema
		// violation and a unique-index conflict are internal ones.
		ErrorDetails::Validation(_) | ErrorDetails::Internal => {
			message_number(err.message()).unwrap_or(ER_UNKNOWN_ERROR)
		}
		ErrorDetails::NotFound(_)
		| ErrorDetails::Query(_)
		| ErrorDetails::Serialization(_)
		| ErrorDetails::Connection(_)
		| ErrorDetails::Context => ER_UNKNOWN_ERROR,
		// The kinds are not exhaustive: a kind added later has no number until it is given one.
		_ => ER_UNKNOWN_ERROR,
	}
}

/// The number of an error with no kind of its own, from the form of its message.
///
/// The specific forms are read before the generic ones. A form is recognised only at the start of
/// the message (and at its end, where it has one), where the engine's own text is.
fn message_number(message: &str) -> Option<Number> {
	// Each parser begins the message of its syntax error with its own name: the MySQL and the
	// PostgreSQL front ends and the SurrealQL parser, which a connection can switch to.
	if ["MySQL parse error: ", "PostgreSQL parse error: ", "Parse error: "]
		.iter()
		.any(|start| message.starts_with(start))
	{
		Some(ER_PARSE_ERROR)
	} else if let Some(reason) = lowering_reason(message) {
		planner_number(reason)
	} else if is_unique_violation(message) {
		Some(ER_DUP_ENTRY)
	} else if message.starts_with("The field '") && message.ends_with("' already exists") {
		// A table that defines a column twice.
		Some(ER_DUP_FIELDNAME)
	} else if message.starts_with("Found field '") {
		// A write to a field a SCHEMAFULL table does not define.
		Some(ER_BAD_FIELD_ERROR)
	} else if message.starts_with("Found NULL for field `")
		&& message.ends_with(" but field must conform to: $value != NONE AND $value != NULL")
	{
		// A NULL a `NOT NULL` column's assertion refuses, over a type that admits NULL.
		Some(ER_BAD_NULL_ERROR)
	} else if message.starts_with("Could not cast into `") {
		// Text written to a typed column that does not read as the column's type.
		Some(ER_TRUNCATED_WRONG_VALUE_FOR_FIELD)
	} else if message.starts_with("Couldn't coerce value for field `") {
		// The value is the last thing in the message, so a NULL or a NONE is told from a string
		// that spells it (which ends in a quote) and from an element of an array (which is
		// followed by the element's position).
		if message.ends_with(" but found `NULL`") {
			Some(ER_BAD_NULL_ERROR)
		} else if message.ends_with(" but found `NONE`") {
			Some(ER_NO_DEFAULT_FOR_FIELD)
		} else {
			Some(ER_TRUNCATED_WRONG_VALUE_FOR_FIELD)
		}
	} else if message.starts_with("Data too long for column '")
		|| message.starts_with("value too long for type ")
	{
		// A value that a declared SQL type's length refuses.
		Some(ER_DATA_TOO_LONG)
	} else if message.starts_with("numeric field overflow ") {
		Some(ER_DATA_OUT_OF_RANGE)
	} else if message.starts_with("JSON cannot hold a ") {
		Some(ER_INVALID_JSON_TEXT)
	} else if message.starts_with("bit string ")
		|| message.starts_with("invalid binary digit in a bit string: ")
	{
		Some(ER_TRUNCATED_WRONG_VALUE_FOR_FIELD)
	} else {
		None
	}
}

/// The planner's own message in the message of a refused statement.
///
/// The datastore reports a statement the planner refuses as a validation error whose message is
/// `<dialect> <statement> lowering error: <the planner's message>`, where the dialect is `MySQL`
/// or `PostgreSQL` and the statement is one upper-case word, and some statements repeat
/// `lowering error: ` before the planner's message. Any other text gives `None`, so a message that
/// only holds `lowering error: ` is not read as a refusal by the planner.
fn lowering_reason(message: &str) -> Option<&str> {
	let head = message.strip_prefix("MySQL ").or_else(|| message.strip_prefix("PostgreSQL "))?;
	let (statement, reason) = head.split_once(" lowering error: ")?;
	if statement.is_empty() || !statement.bytes().all(|byte| byte.is_ascii_uppercase()) {
		return None;
	}
	Some(reason.strip_prefix("lowering error: ").unwrap_or(reason))
}

/// Whether `message` is the refusal of a write that a UNIQUE index would hold twice:
/// ``Database index `<name>` already contains <value>, with record `<record>` ``, or the same with
/// the value and record withheld from a caller who may not read them.
///
/// The value is client text, so the form is read from the index name on: a name with a backtick,
/// or any other message about an index, is not taken for one.
fn is_unique_violation(message: &str) -> bool {
	let Some((_, rest)) =
		message.strip_prefix("Database index `").and_then(|tail| tail.split_once('`'))
	else {
		return false;
	};
	rest == " already contains a conflicting value in another record"
		|| (rest.starts_with(" already contains ")
			&& rest.contains(", with record `")
			&& rest.ends_with('`'))
}

/// The messages of the planner's errors that are one fixed sentence, and the error each is.
static PLANNER_SENTENCES: [(&str, PlanError); 3] = [
	("division by zero", PlanError::DivisionByZero),
	("bigint out of range", PlanError::NumericValueOutOfRange),
	("subquery in FROM must have an alias", PlanError::SubqueryMustHaveAlias),
];

/// The forms of the planner's messages that carry a name or a number: the text a message starts
/// with, the text it ends with, and the error that has the form, its name or number left empty.
/// Whatever lies between is what the statement gave. An error that has no form here is one MySQL
/// has no number for.
static PLANNER_FORMS: &[(&str, &str, PlanError)] = &[
	("schema or table \"", "\" does not exist", PlanError::UndefinedTable(String::new())),
	("column \"", "\" does not exist", PlanError::UndefinedColumn(String::new())),
	("column \"", "\" specified more than once", PlanError::DuplicateColumn(String::new())),
	("column reference \"", "\" is ambiguous", PlanError::AmbiguousColumn(String::new())),
	(
		"missing FROM-clause entry for table \"",
		"\"",
		PlanError::MissingFromClauseEntry(String::new()),
	),
	("GROUP BY position ", " is not in select list", PlanError::InvalidGroupByPosition(0)),
	("ORDER BY position ", " is not in select list", PlanError::InvalidOrderByPosition(0)),
	(
		"for SELECT DISTINCT, ORDER BY expressions must appear in select list",
		"",
		PlanError::DistinctOrderByNotSelected,
	),
	(
		"column \"",
		"\" must appear in the GROUP BY clause or be used in an aggregate function",
		PlanError::ColumnNotInGroupBy(String::new()),
	),
	("aggregate functions are not allowed in ", "", PlanError::AggregateNotAllowed(String::new())),
	("window functions are not allowed in ", "", PlanError::WindowNotAllowed(String::new())),
	(
		"cannot override PARTITION BY clause of window \"",
		"\"",
		PlanError::WindowPartitionOverride(String::new()),
	),
	(
		"cannot override ORDER BY clause of window \"",
		"\"",
		PlanError::WindowOrderOverride(String::new()),
	),
	(
		"cannot copy window \"",
		"\" because it has a frame clause",
		PlanError::WindowFrameCopy(String::new()),
	),
	("function \"", "\" does not exist", PlanError::UndefinedFunction(String::new())),
	(
		"function \"",
		" were provided",
		PlanError::FunctionArgumentCountMismatch {
			name: String::new(),
			expected: String::new(),
			got: 0,
		},
	),
	(
		"function \"",
		"\" must be called with an OVER clause",
		PlanError::WindowFunctionWithoutOver(String::new()),
	),
	("invalid input syntax for type: ", "", PlanError::InvalidTextRepresentation(String::new())),
	("unsupported statement: ", "", PlanError::UnsupportedStatement(String::new())),
	("unsupported expression: ", "", PlanError::UnsupportedExpression(String::new())),
	("unrecognized configuration parameter \"", "\"", PlanError::UndefinedObject(String::new())),
	(
		"constraint \"",
		"\" does not exist",
		PlanError::UndefinedConstraint {
			constraint: String::new(),
			table: String::new(),
		},
	),
];

/// The kind of planner error that `reason` is the message of, or `None` when no form matches, as
/// for an error that MySQL has no number for.
///
/// The datastore reports a refusal by the planner as text, so the kind is read back from the form
/// of the message. The first form that matches is the kind, and `plan_number` gives the number.
fn planner_kind(reason: &str) -> Option<&'static PlanError> {
	if let Some((_, kind)) = PLANNER_SENTENCES.iter().find(|(sentence, _)| reason == *sentence) {
		return Some(kind);
	}
	PLANNER_FORMS.iter().find_map(|(start, end, kind)| {
		let middle = reason.strip_prefix(start)?.strip_suffix(end)?;
		// A domain's constraint is named in the same frame as a relation's; only the noun
		// between the names tells them apart.
		if matches!(kind, PlanError::UndefinedConstraint { .. })
			&& !middle.contains("\" of relation \"")
		{
			return None;
		}
		Some(kind)
	})
}

/// The number of a planner message (`PlanError`'s text), or `None` when no form matches.
fn planner_number(reason: &str) -> Option<Number> {
	planner_kind(reason).map(plan_number)
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;

	fn packet(err: &TypesError) -> (u16, [u8; 5]) {
		let pkt = ErrPacket::from(err);
		(pkt.error_code, pkt.sql_state)
	}

	/// What the engine sends for a statement it refuses without a kind of its own.
	fn untyped(message: &str) -> (u16, [u8; 5]) {
		packet(&TypesError::internal(message.to_string()))
	}

	fn number(number: Number) -> (u16, [u8; 5]) {
		(number.0, *number.1)
	}

	#[test]
	fn the_kind_of_an_error_gives_its_number() {
		let not_found = |details| TypesError::not_found("m".to_string(), Some(details));
		let exists = |details| TypesError::already_exists("m".to_string(), Some(details));
		let query = |details| TypesError::query("m".to_string(), Some(details));
		let name = || "t".to_string();
		let cases = [
			(
				not_found(NotFoundError::Table {
					name: name(),
				}),
				(1146, *b"42S02"),
			),
			(
				not_found(NotFoundError::Database {
					name: name(),
				}),
				(1049, *b"42000"),
			),
			(
				not_found(NotFoundError::Namespace {
					name: name(),
				}),
				(1049, *b"42000"),
			),
			(
				exists(AlreadyExistsError::Table {
					name: name(),
				}),
				(1050, *b"42S01"),
			),
			(
				exists(AlreadyExistsError::Database {
					name: name(),
				}),
				(1007, *b"HY000"),
			),
			(
				exists(AlreadyExistsError::Record {
					id: "t:1".to_string(),
				}),
				(1062, *b"23000"),
			),
			(TypesError::already_exists("m".to_string(), None), (1062, *b"23000")),
			(
				query(QueryError::TimedOut {
					duration: Duration::from_secs(1),
				}),
				(3024, *b"HY000"),
			),
			(query(QueryError::Cancelled), (1317, *b"70100")),
			(query(QueryError::TransactionConflict), (1213, *b"40001")),
			(TypesError::not_allowed("m".to_string(), None), (1142, *b"42000")),
			(TypesError::configuration("m".to_string(), None), (1235, *b"42000")),
			(TypesError::thrown("m".to_string()), (1644, *b"45000")),
			(
				TypesError::validation("m".to_string(), Some(ValidationError::Parse)),
				(1064, *b"42000"),
			),
			(TypesError::internal("m".to_string()), (1105, *b"HY000")),
			(TypesError::validation("m".to_string(), None), (1105, *b"HY000")),
			(TypesError::query("m".to_string(), None), (1105, *b"HY000")),
		];
		for (err, expected) in cases {
			assert_eq!(packet(&err), expected, "{err:?}");
		}
	}

	#[test]
	fn the_message_is_sent_as_the_engine_wrote_it() {
		let message = "Database record `t:1` already exists";
		let err = TypesError::already_exists(message.to_string(), None);
		assert_eq!(ErrPacket::from(&err).error_message, message);
	}

	#[test]
	fn an_error_of_no_kind_is_read_from_the_form_of_its_message() {
		let cases = [
			("MySQL parse error: Unexpected token: found SELEC", (1064, *b"42000")),
			(
				"Database index `t_a_uniq` already contains 'a@x', with record `t:1`",
				(1062, *b"23000"),
			),
			(
				"Database index `t_a_uniq` already contains a conflicting value in another record",
				(1062, *b"23000"),
			),
			(
				"Couldn't coerce value for field `name` of `t:3`: Expected `string` but found \
				 `NULL`",
				(1048, *b"23000"),
			),
			(
				"Couldn't coerce value for field `name` of `t:4`: Expected `string` but found \
				 `NONE`",
				(1364, *b"HY000"),
			),
			(
				"Couldn't coerce value for field `age` of `t:5`: Expected `none | null | int` but \
				 found `'abc'`",
				(1366, *b"HY000"),
			),
			(
				"Couldn't coerce value for field `a` of `t:1`: Expected `int` but found `NULL` when \
				 coercing element at index 1 of `array<int>`",
				(1366, *b"HY000"),
			),
			("Found field 'nosuch', but no such field exists for table 't'", (1054, *b"42S22")),
			("The field 'id' already exists", (1060, *b"42S21")),
			("MySQL SELECT lowering error: lowering error: bigint out of range", (1690, *b"22003")),
			("MySQL INSERT lowering error: division by zero", (1365, *b"22012")),
			("PostgreSQL parse error: at 0..5: unexpected token", (1064, *b"42000")),
			("Parse error: Unexpected end of file, expected an expression", (1064, *b"42000")),
			("PostgreSQL SELECT lowering error: division by zero", (1365, *b"22012")),
			(
				"MySQL SELECT lowering error: unsupported expression: window functions",
				(1235, *b"42000"),
			),
		];
		for (message, expected) in cases {
			assert_eq!(untyped(message), expected, "{message}");
			assert_eq!(
				packet(&TypesError::validation(message.to_string(), None)),
				expected,
				"{message}"
			);
		}
	}

	#[test]
	fn an_unrecognised_message_is_an_unknown_error() {
		for message in [
			"something broke",
			"MySQL SELECT lowering error: lowering error: planning error: each query of a set \
			 operation must have the same number of columns",
			"MySQL SELECT lowering error: lowering error: planning error: invalid regular \
			 expression: regex parse error",
			"Failed to translate MySQL: Unsupported MySQL feature: DDL statements",
			"Database index `t_a_uniq` is currently building",
			"Database index `t_a_uniq` on table `t` failed to build, so it cannot enforce UNIQUE",
			"Found -1 but the LIMIT clause must evaluate to a positive integer",
			"The field 'id' does not exist",
			"The field 'a' already exists in table 't'",
			"The index 'i1' already exists",
		] {
			assert_eq!(untyped(message), (1105, *b"HY000"), "{message}");
		}
	}

	/// A value, a record id or a name that the client wrote must not choose the number: each
	/// message here is one the engine writes for a value of the wrong type, with the phrase of
	/// another error in the client's text.
	#[test]
	fn text_the_client_wrote_does_not_choose_the_number() {
		let phrases = [
			"division by zero",
			"divide by zero",
			"already contains",
			"duplicate key",
			"timed out",
			"timeout",
			"conflict",
			"transaction conflict",
			"parse error",
			"unexpected token",
			"does not exist",
			"not found in function registry",
			"table",
			"cannot be null",
		];
		for phrase in phrases {
			// The value of a field of the wrong type.
			let value = format!(
				"Couldn't coerce value for field `age` of `t:1`: Expected `none | null | int` but \
				 found `'{phrase}'`"
			);
			assert_eq!(untyped(&value), (1366, *b"HY000"), "{value}");
			// The id of the record the value was written to.
			let id = format!(
				"Couldn't coerce value for field `age` of `t:`{phrase}``: Expected `int` but \
				 found `'x'`"
			);
			assert_eq!(untyped(&id), (1366, *b"HY000"), "{id}");
			// The same text where an index conflict, a missing table or a refusal would be.
			for message in [
				format!("Found value `{phrase}`"),
				format!("something {phrase} happened"),
				format!("Expected `int` but found `'{phrase}'`"),
			] {
				assert_eq!(untyped(&message), (1105, *b"HY000"), "{message}");
			}
		}

		// A string that spells NULL or NONE is a string, and a NULL element is not a NULL field.
		for spelled in ["NULL", "NONE"] {
			let message = format!(
				"Couldn't coerce value for field `name` of `t:1`: Expected `string` but found \
				 `'{spelled}'`"
			);
			assert_eq!(untyped(&message), (1366, *b"HY000"), "{message}");
		}

		// A table or index name that carries the form of another error.
		let message = "Database index `a` already contains `b` is currently building";
		assert_eq!(untyped(message), (1105, *b"HY000"), "{message}");
		let message = "Database index `a` already contains `b` on table `t` failed to build";
		assert_eq!(untyped(message), (1105, *b"HY000"), "{message}");
		// A message that only holds the words of a planner error is not one.
		for message in [
			"Couldn't coerce value for field `a` of `t:1`: Expected `int` but found `'x'` \
			 lowering error: bigint out of range",
			"MySQL parse error ... MySQL SELECT lowering error: bigint out of range",
			"MySQL SELECT\n lowering error: bigint out of range",
			"MySQL select lowering error: bigint out of range",
			"MySQL  lowering error: bigint out of range",
			"bigint out of range",
		] {
			assert_ne!(untyped(message), (1690, *b"22003"), "{message}");
		}
	}

	#[test]
	fn a_kind_is_not_overridden_by_the_text_of_its_message() {
		let err = TypesError::not_found(
			"The table 'division by zero' does not exist".to_string(),
			Some(NotFoundError::Table {
				name: "division by zero".to_string(),
			}),
		);
		assert_eq!(packet(&err), (1146, *b"42S02"));
		let err = TypesError::not_allowed("Database index `a` already contains".to_string(), None);
		assert_eq!(packet(&err), (1142, *b"42000"));
		// Only a validation or an internal error is read from its message.
		let err = TypesError::query("MySQL parse error: x".to_string(), None);
		assert_eq!(packet(&err), (1105, *b"HY000"));
	}

	/// One planner error of every kind, with the number MySQL 8.4 gives the same condition, written
	/// out here apart from `plan_number` so that a changed number fails a test.
	///
	/// The match is over every variant, so a kind added to `PlanError` is placed here before the
	/// tests build. A kind of the PostgreSQL front end that MySQL has no number for is
	/// `ER_UNKNOWN_ERROR`.
	fn planner_cases() -> Vec<(PlanError, Number)> {
		let name = || "x".to_string();
		let cases = vec![
			PlanError::UndefinedTable(name()),
			PlanError::UndefinedColumn(name()),
			PlanError::AmbiguousColumn(name()),
			PlanError::MissingFromClauseEntry(name()),
			PlanError::InvalidGroupByPosition(9),
			PlanError::InvalidOrderByPosition(9),
			PlanError::ColumnNotInGroupBy(name()),
			PlanError::AggregateNotAllowed("WHERE clause".to_string()),
			PlanError::WindowNotAllowed("WHERE clause".to_string()),
			PlanError::UndefinedFunction(name()),
			PlanError::UndefinedOperator("jsonb ? text".to_string()),
			PlanError::FunctionArgumentCountMismatch {
				name: name(),
				expected: "at most 1".to_string(),
				got: 2,
			},
			PlanError::WindowFunctionWithoutOver(name()),
			PlanError::SubqueryMustHaveAlias,
			PlanError::ParameterOutOfRange(3),
			PlanError::DivisionByZero,
			PlanError::NumericValueOutOfRange,
			PlanError::InvalidTextRepresentation("integer: \"x\"".to_string()),
			PlanError::UnsupportedStatement("MERGE".to_string()),
			PlanError::UnsupportedExpression("window functions".to_string()),
			PlanError::NoActiveTransaction,
			PlanError::InFailedTransaction,
			PlanError::SavepointDoesNotExist(name()),
			PlanError::ActiveTransactionAlreadyInProgress,
			PlanError::UndefinedObject(name()),
			PlanError::UndefinedConstraint {
				constraint: name(),
				table: name(),
			},
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
			PlanError::ValueOutOfRange("smallint out of range".to_string()),
			PlanError::DuplicateColumn(name()),
			PlanError::InvalidParameterValue("m".to_string()),
			PlanError::DataException("expected 3 dimensions, not 2".to_string()),
			PlanError::ProgramLimitExceeded("dimensions".to_string()),
			PlanError::UndefinedOperatorClass {
				name: name(),
				method: "hnsw".to_string(),
			},
			PlanError::NoDefaultOperatorClass {
				type_name: "vector".to_string(),
				method: "hnsw".to_string(),
			},
			PlanError::ExtensionNotAvailable(name()),
			PlanError::DistinctOrderByNotSelected,
			PlanError::WindowPartitionOverride(name()),
			PlanError::WindowOrderOverride(name()),
			PlanError::WindowFrameCopy(name()),
			PlanError::Custom("something".to_string()),
		];
		cases
			.into_iter()
			.map(|err| {
				let expected = match &err {
					PlanError::UndefinedTable(_) => ER_NO_SUCH_TABLE,
					PlanError::UndefinedColumn(_)
					| PlanError::MissingFromClauseEntry(_)
					| PlanError::InvalidGroupByPosition(_)
					| PlanError::InvalidOrderByPosition(_) => ER_BAD_FIELD_ERROR,
					PlanError::AmbiguousColumn(_) => ER_NON_UNIQ_ERROR,
					PlanError::DistinctOrderByNotSelected => ER_FIELD_IN_ORDER_NOT_SELECT,
					PlanError::ColumnNotInGroupBy(_) => ER_WRONG_FIELD_WITH_GROUP,
					PlanError::AggregateNotAllowed(_) => ER_INVALID_GROUP_FUNC_USE,
					PlanError::WindowNotAllowed(_) => ER_WINDOW_INVALID_WINDOW_FUNC_USE,
					PlanError::WindowPartitionOverride(_) => ER_WINDOW_NO_CHILD_PARTITIONING,
					PlanError::WindowOrderOverride(_) => ER_WINDOW_NO_REDEFINE_ORDER_BY,
					PlanError::WindowFrameCopy(_) => ER_WINDOW_NO_INHERIT_FRAME,
					PlanError::UndefinedFunction(_) => ER_SP_DOES_NOT_EXIST,
					PlanError::FunctionArgumentCountMismatch {
						..
					} => ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
					PlanError::WindowFunctionWithoutOver(_) => ER_PARSE_ERROR,
					PlanError::SubqueryMustHaveAlias => ER_DERIVED_MUST_HAVE_ALIAS,
					PlanError::DivisionByZero => ER_DIVISION_BY_ZERO,
					PlanError::NumericValueOutOfRange => ER_DATA_OUT_OF_RANGE,
					PlanError::InvalidTextRepresentation(_) => ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
					PlanError::UnsupportedStatement(_) | PlanError::UnsupportedExpression(_) => {
						ER_NOT_SUPPORTED_YET
					}
					PlanError::UndefinedObject(_) => ER_UNKNOWN_SYSTEM_VARIABLE,
					PlanError::UndefinedConstraint {
						..
					} => ER_CANT_DROP_FIELD_OR_KEY,
					PlanError::DuplicateColumn(_) => ER_DUP_FIELDNAME,
					PlanError::UndefinedType(_)
					| PlanError::UndefinedDomainConstraint {
						..
					}
					| PlanError::DuplicateDomainConstraint {
						..
					}
					| PlanError::DuplicateEnumLabel(_)
					| PlanError::ValueOutOfRange(_)
					| PlanError::UndefinedOperator(_)
					| PlanError::ParameterOutOfRange(_)
					| PlanError::NoActiveTransaction
					| PlanError::InFailedTransaction
					| PlanError::SavepointDoesNotExist(_)
					| PlanError::ActiveTransactionAlreadyInProgress
					| PlanError::InvalidParameterValue(_)
					| PlanError::DatatypeMismatch(_)
					| PlanError::DataException(_)
					| PlanError::ProgramLimitExceeded(_)
					| PlanError::UndefinedOperatorClass {
						..
					}
					| PlanError::NoDefaultOperatorClass {
						..
					}
					| PlanError::ExtensionNotAvailable(_)
					| PlanError::Custom(_) => ER_UNKNOWN_ERROR,
				};
				(err, expected)
			})
			.collect()
	}

	#[test]
	fn a_planner_error_gets_the_number_of_its_kind_however_it_arrives() {
		for (err, expected) in planner_cases() {
			// As the planner's own value, which the statements checked before they run hold.
			let pkt = ErrPacket::from(&err);
			assert_eq!((pkt.error_code, pkt.sql_state), number(expected), "{err}");
			assert_eq!(pkt.error_message, err.to_string());

			// As the datastore reports it after lowering a statement.
			for wrapped in [
				format!("MySQL SELECT lowering error: {err}"),
				format!("MySQL INSERT lowering error: lowering error: {err}"),
			] {
				assert_eq!(untyped(&wrapped), number(expected), "{wrapped}");
				let err = TypesError::validation(wrapped.clone(), None);
				assert_eq!(packet(&err), number(expected), "{wrapped}");
				assert_eq!(ErrPacket::from(&err).error_message, wrapped);
			}
		}
	}

	/// A planner message is read back to the kind that wrote it, so the planner's value and the
	/// datastore's text of one error are numbered by the same arm of `plan_number`.
	#[test]
	fn a_planner_message_is_read_back_to_the_kind_that_wrote_it() {
		for (err, expected) in planner_cases() {
			match planner_kind(&err.to_string()) {
				Some(kind) => {
					assert_eq!(std::mem::discriminant(kind), std::mem::discriminant(&err), "{err}");
				}
				// A kind that has no form is one MySQL has no number for.
				None => assert_eq!(expected, ER_UNKNOWN_ERROR, "{err}"),
			}
			assert_eq!(plan_number(&err), expected, "{err}");
		}
	}

	#[test]
	fn the_numbers_are_those_of_mysql() {
		// Each pair was read from MySQL 8.4 for the condition it names.
		assert_eq!(number(ER_NO_SUCH_TABLE), (1146, *b"42S02"));
		assert_eq!(number(ER_BAD_FIELD_ERROR), (1054, *b"42S22"));
		assert_eq!(number(ER_NON_UNIQ_ERROR), (1052, *b"23000"));
		assert_eq!(number(ER_WRONG_FIELD_WITH_GROUP), (1055, *b"42000"));
		assert_eq!(number(ER_INVALID_GROUP_FUNC_USE), (1111, *b"HY000"));
		assert_eq!(number(ER_WINDOW_INVALID_WINDOW_FUNC_USE), (3593, *b"HY000"));
		assert_eq!(number(ER_SP_DOES_NOT_EXIST), (1305, *b"42000"));
		assert_eq!(number(ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT), (1582, *b"42000"));
		assert_eq!(number(ER_DERIVED_MUST_HAVE_ALIAS), (1248, *b"42000"));
		assert_eq!(number(ER_DIVISION_BY_ZERO), (1365, *b"22012"));
		assert_eq!(number(ER_DATA_OUT_OF_RANGE), (1690, *b"22003"));
		assert_eq!(number(ER_BAD_NULL_ERROR), (1048, *b"23000"));
		assert_eq!(number(ER_NO_DEFAULT_FOR_FIELD), (1364, *b"HY000"));
		assert_eq!(number(ER_TRUNCATED_WRONG_VALUE_FOR_FIELD), (1366, *b"HY000"));
		assert_eq!(number(ER_DUP_ENTRY), (1062, *b"23000"));
		assert_eq!(number(ER_DUP_FIELDNAME), (1060, *b"42S21"));
		assert_eq!(number(ER_TABLE_EXISTS_ERROR), (1050, *b"42S01"));
		assert_eq!(number(ER_DB_CREATE_EXISTS), (1007, *b"HY000"));
		assert_eq!(number(ER_BAD_DB_ERROR), (1049, *b"42000"));
		assert_eq!(number(ER_TABLEACCESS_DENIED_ERROR), (1142, *b"42000"));
		assert_eq!(number(ER_PARSE_ERROR), (1064, *b"42000"));
		assert_eq!(number(ER_NOT_SUPPORTED_YET), (1235, *b"42000"));
		assert_eq!(number(ER_UNKNOWN_SYSTEM_VARIABLE), (1193, *b"HY000"));
		assert_eq!(number(ER_QUERY_TIMEOUT), (3024, *b"HY000"));
		assert_eq!(number(ER_QUERY_INTERRUPTED), (1317, *b"70100"));
		assert_eq!(number(ER_LOCK_DEADLOCK), (1213, *b"40001"));
		assert_eq!(number(ER_SIGNAL_EXCEPTION), (1644, *b"45000"));
		assert_eq!(number(ER_UNKNOWN_ERROR), (1105, *b"HY000"));
	}

	/// The name a statement gives is client text, so a name that carries the end of another
	/// form does not make the planner's message another error.
	#[test]
	fn a_name_in_a_planner_message_does_not_choose_its_number() {
		let cases = [
			(PlanError::UndefinedFunction("f\" must be called with an OVER clause".into()), 1305),
			(PlanError::WindowFunctionWithoutOver("f\" does not exist".into()), 1064),
			(
				PlanError::FunctionArgumentCountMismatch {
					name: "f\" does not exist".into(),
					expected: "1".into(),
					got: 2,
				},
				1582,
			),
			(PlanError::UndefinedColumn("a\" is ambiguous".into()), 1054),
			(PlanError::AmbiguousColumn("a\" does not exist".into()), 1052),
			(PlanError::UnsupportedExpression("column \"x\" does not exist".into()), 1235),
			(PlanError::UnsupportedStatement("function \"f\" does not exist".into()), 1235),
		];
		for (err, expected) in cases {
			assert_eq!(ErrPacket::from(&err).error_code, expected, "{err}");
			let wrapped =
				TypesError::validation(format!("MySQL SELECT lowering error: {err}"), None);
			assert_eq!(ErrPacket::from(&wrapped).error_code, expected, "{err}");
		}
	}
}
