//! The SQLSTATE and, for MySQL, the error number of an engine error.
//!
//! The kinds of error that carry details give their code directly. The engine states most
//! statement failures as text with no kind of their own (a planner refusal, a unique index that
//! already holds a key, a value that does not fit its field), so those are read back from the form
//! of the message: the text the engine starts or ends it with, never a word anywhere inside it,
//! because the rest of a message quotes the client's own values and must not choose the code.

use surrealdb_types::{
	AlreadyExistsError, Error, ErrorDetails, NotFoundError, QueryError, ValidationError,
};

use crate::kvs::Dialect;

/// A PostgreSQL SQLSTATE and the MySQL SQLSTATE and error number of the same failure.
#[derive(Clone, Copy)]
struct Code {
	pg: &'static str,
	my_state: &'static str,
	my_errno: i64,
}

const fn code(pg: &'static str, my_state: &'static str, my_errno: i64) -> Code {
	Code {
		pg,
		my_state,
		my_errno,
	}
}

const UNDEFINED_TABLE: Code = code("42P01", "42S02", 1146);
const UNDEFINED_COLUMN: Code = code("42703", "42S22", 1054);
const DUPLICATE_TABLE: Code = code("42P07", "42S01", 1050);
const DUPLICATE_COLUMN: Code = code("42701", "42S21", 1060);
const UNIQUE_VIOLATION: Code = code("23505", "23000", 1062);
const NOT_NULL_VIOLATION: Code = code("23502", "23000", 1048);
const MISSING_VALUE: Code = code("23502", "HY000", 1364);
const WRONG_VALUE: Code = code("42804", "HY000", 1366);
const CHECK_VIOLATION: Code = code("23514", "HY000", 3819);
const FOREIGN_KEY_VIOLATION: Code = code("23503", "23000", 1451);
const SYNTAX: Code = code("42601", "42000", 1064);
const UNSUPPORTED: Code = code("0A000", "42000", 1235);
const OTHER: Code = code("XX000", "HY000", 1105);

/// How the planner words the errors that name something from the client's statement: the text
/// before the name, the text after it, and the code of that kind of error.
const PLANNER_FORMS: &[(&str, &str, Code)] = &[
	("schema or table \"", "\" does not exist", UNDEFINED_TABLE),
	("missing FROM-clause entry for table \"", "\"", code("42P01", "42S22", 1054)),
	("column \"", "\" does not exist", UNDEFINED_COLUMN),
	("column reference \"", "\" is ambiguous", code("42702", "23000", 1052)),
	("GROUP BY position ", " is not in select list", code("42P10", "42S22", 1054)),
	("ORDER BY position ", " is not in select list", code("42P10", "42S22", 1054)),
	(
		"column \"",
		"\" must appear in the GROUP BY clause or be used in an aggregate function",
		code("42803", "42000", 1055),
	),
	("aggregate functions are not allowed in ", "", code("42803", "HY000", 1111)),
	("window functions are not allowed in ", "", code("42P20", "HY000", 3593)),
	("function \"", "\" does not exist", code("42883", "42000", 1305)),
	("function \"", " were provided", code("42883", "42000", 1582)),
	("function \"", "\" must be called with an OVER clause", code("42809", "42000", 1064)),
	("unrecognized configuration parameter \"", "\"", code("42704", "HY000", 1193)),
	("operator does not exist: ", "", code("42883", "HY000", 1105)),
	("invalid input syntax for type: ", "", code("22P02", "HY000", 1366)),
	("unsupported statement: ", "", UNSUPPORTED),
	("unsupported expression: ", "", UNSUPPORTED),
	("constraint \"", "\" does not exist", code("42704", "42000", 1091)),
	("invalid parameter value: ", "", code("22023", "HY000", 1105)),
	("data exception: ", "", code("22000", "HY000", 1105)),
	("program limit exceeded: ", "", code("54000", "HY000", 1105)),
];

/// The planner's own message in the message of a refused statement.
///
/// The datastore reports a statement the planner refuses as `<dialect> <statement> lowering error:
/// <the planner's message>`, where the dialect is `MySQL` or `PostgreSQL` and the statement is one
/// upper-case word; some statements repeat `lowering error: ` before the planner's message.
fn planner_message(message: &str) -> &str {
	let Some(head) = message.strip_prefix("MySQL ").or_else(|| message.strip_prefix("PostgreSQL "))
	else {
		return message;
	};
	match head.split_once(" lowering error: ") {
		Some((statement, reason))
			if !statement.is_empty() && statement.bytes().all(|byte| byte.is_ascii_uppercase()) =>
		{
			reason.strip_prefix("lowering error: ").unwrap_or(reason)
		}
		_ => message,
	}
}

fn planner_code(message: &str) -> Option<Code> {
	let reason = planner_message(message);
	match reason {
		"bigint out of range" => return Some(code("22003", "22003", 1690)),
		"division by zero" => return Some(code("22012", "22012", 1365)),
		"subquery in FROM must have an alias" => return Some(code("42601", "42000", 1248)),
		_ => {}
	}
	PLANNER_FORMS.iter().find_map(|(start, end, code)| {
		(reason.len() >= start.len() + end.len()
			&& reason.starts_with(start)
			&& reason.ends_with(end))
		.then_some(*code)
	})
}

/// Whether `message` refuses a write that a UNIQUE index would hold twice: ``Database index
/// `<name>` already contains <value>, with record `<record>` ``, or the same with the value and
/// record withheld from a caller who may not read them.
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

fn engine_code(message: &str) -> Option<Code> {
	if ["MySQL parse error: ", "PostgreSQL parse error: ", "Parse error: "]
		.iter()
		.any(|start| message.starts_with(start))
	{
		Some(SYNTAX)
	} else if let Some(code) = planner_code(message) {
		Some(code)
	} else if is_unique_violation(message) {
		Some(UNIQUE_VIOLATION)
	} else if message.starts_with("The field '") && message.ends_with("' already exists") {
		Some(DUPLICATE_COLUMN)
	} else if message.starts_with("Found field '") {
		Some(UNDEFINED_COLUMN)
	} else if message.starts_with("Couldn't coerce value for field `") {
		// NULL or NONE ends the message only when it is the whole value written to the field. An
		// element of an array or an object is followed by where in the field it was found, and is
		// a value of the wrong type for that element.
		if message.ends_with(" but found `NULL`") {
			Some(NOT_NULL_VIOLATION)
		} else if message.ends_with(" but found `NONE`") {
			Some(MISSING_VALUE)
		} else {
			Some(WRONG_VALUE)
		}
	} else if message.starts_with("Found ")
		&& message.contains(" for field `")
		&& message.contains(", but field must conform to: ")
	{
		Some(CHECK_VIOLATION)
	} else if message.starts_with("Could not cast into `") {
		Some(WRONG_VALUE)
	} else if message.starts_with("Cannot delete ")
		&& (message.contains(" is referenced by ") || message.contains("it is referenced by "))
		&& message.ends_with("with an ON DELETE REJECT clause")
	{
		Some(FOREIGN_KEY_VIOLATION)
	} else {
		None
	}
}

/// The SQLSTATE of `err`, and for MySQL its error number.
pub(super) fn classify(err: &Error, dialect: Dialect) -> (&'static str, Option<i64>) {
	let mysql = dialect == Dialect::MySql;
	let found = match err.details() {
		ErrorDetails::Query(Some(QueryError::Cancelled)) => code("57014", "70100", 1317),
		ErrorDetails::Query(Some(QueryError::TimedOut {
			..
		})) => code("57014", "HY000", 3024),
		ErrorDetails::Query(Some(QueryError::TransactionConflict)) => code("40001", "40001", 1213),
		ErrorDetails::NotFound(Some(NotFoundError::Table {
			..
		})) => UNDEFINED_TABLE,
		ErrorDetails::NotFound(Some(
			NotFoundError::Database {
				..
			}
			| NotFoundError::Namespace {
				..
			},
		)) => code("3D000", "42000", 1049),
		ErrorDetails::AlreadyExists(Some(AlreadyExistsError::Table {
			..
		})) => DUPLICATE_TABLE,
		ErrorDetails::AlreadyExists(Some(
			AlreadyExistsError::Database {
				..
			}
			| AlreadyExistsError::Namespace {
				..
			},
		)) => code("42P04", "HY000", 1007),
		ErrorDetails::AlreadyExists(_) => UNIQUE_VIOLATION,
		ErrorDetails::NotAllowed(_) => code("42501", "42000", 1142),
		ErrorDetails::Configuration(_) => UNSUPPORTED,
		ErrorDetails::Thrown => code("P0001", "45000", 1644),
		ErrorDetails::Validation(Some(ValidationError::Parse)) => SYNTAX,
		ErrorDetails::Validation(_)
		| ErrorDetails::Query(_)
		| ErrorDetails::Serialization(_)
		| ErrorDetails::NotFound(_)
		| ErrorDetails::Connection(_)
		| ErrorDetails::Internal
		| ErrorDetails::Context => engine_code(err.message()).unwrap_or(OTHER),
		// The kinds are not exhaustive: one added later is read from its message too.
		_ => engine_code(err.message()).unwrap_or(OTHER),
	};
	if mysql {
		(found.my_state, Some(found.my_errno))
	} else {
		(found.pg, None)
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;

	fn pg(err: &Error) -> &'static str {
		classify(err, Dialect::PgSql).0
	}

	fn my(err: &Error) -> (&'static str, Option<i64>) {
		classify(err, Dialect::MySql)
	}

	fn untyped(message: &str) -> Error {
		Error::internal(message.to_string())
	}

	#[test]
	fn the_kind_of_an_error_gives_its_code() {
		let table = || "t".to_string();
		let missing = Error::not_found(
			"m".to_string(),
			Some(NotFoundError::Table {
				name: table(),
			}),
		);
		assert_eq!(pg(&missing), "42P01");
		assert_eq!(my(&missing), ("42S02", Some(1146)));
		let exists = Error::already_exists(
			"m".to_string(),
			Some(AlreadyExistsError::Table {
				name: table(),
			}),
		);
		assert_eq!(pg(&exists), "42P07");
		assert_eq!(my(&exists), ("42S01", Some(1050)));
		let record = Error::already_exists(
			"m".to_string(),
			Some(AlreadyExistsError::Record {
				id: "t:1".to_string(),
			}),
		);
		assert_eq!(pg(&record), "23505");
		assert_eq!(my(&record), ("23000", Some(1062)));
		let conflict = Error::query("m".to_string(), Some(QueryError::TransactionConflict));
		assert_eq!(pg(&conflict), "40001");
		assert_eq!(my(&conflict), ("40001", Some(1213)));
		let timeout = Error::query(
			"m".to_string(),
			Some(QueryError::TimedOut {
				duration: Duration::from_secs(1),
			}),
		);
		assert_eq!(pg(&timeout), "57014");
		assert_eq!(pg(&Error::not_allowed("m".to_string(), None)), "42501");
		assert_eq!(pg(&Error::internal("m".to_string())), "XX000");
		assert_eq!(my(&Error::internal("m".to_string())), ("HY000", Some(1105)));
	}

	#[test]
	fn the_form_of_a_message_gives_the_code_of_an_untyped_error() {
		assert_eq!(
			pg(&untyped("PostgreSQL SELECT lowering error: schema or table \"t\" does not exist")),
			"42P01"
		);
		assert_eq!(
			my(&untyped("MySQL SELECT lowering error: column \"c\" does not exist")),
			("42S22", Some(1054))
		);
		assert_eq!(
			pg(&untyped("Database index `idx` already contains 5, with record `t:1`")),
			"23505"
		);
		assert_eq!(
			my(&untyped(
				"Database index `idx` already contains a conflicting value in another record"
			)),
			("23000", Some(1062))
		);
		assert_eq!(
			pg(&untyped(
				"Couldn't coerce value for field `a` of `t:1`: Expected `int` but found `NONE`"
			)),
			"23502"
		);
		assert_eq!(
			pg(&untyped(
				"Couldn't coerce value for field `a` of `t:1`: Expected `int` but found `'x'`"
			)),
			"42804"
		);
		assert_eq!(
			pg(&untyped(
				"Found 5 for field `a`, with record `t:1`, but field must conform to: $value > 10"
			)),
			"23514"
		);
		assert_eq!(pg(&untyped("PostgreSQL parse error: at 1..2: unexpected")), "42601");
		assert_eq!(
			pg(&untyped(
				"The table is `Database index ` already contains somewhere in a quoted value"
			)),
			"XX000"
		);
	}
}
