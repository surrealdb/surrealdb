//! The `id` column of a base table: stored as the engine's full record id, read as its key.
//!
//! In both SQL dialects the `id` column of a base table is the table's primary key, so reading it
//! yields the key of the stored record id (`record::id`), and a comparison of it with a key value
//! compares keys. A top-level conjunct of a `WHERE` predicate that compares the `id` of the table
//! read with an integer or string literal or a bound parameter instead compares the stored record
//! id with the record the key names on the same table (`type::record(<table>, <key>)`), the form
//! the engine reads a record by directly rather than by scanning the table, for a literal key.
//!
//! A column or expression compared with an `id` may hold the key (a foreign key written through
//! SQL) or a record link to the row (a foreign key written as a record id). It is compared through
//! `type::record(<table>, <value>)`, which names the record of the table by the key of a link and
//! by the value itself otherwise. A value that names no record (NULL, absent, empty or of a kind
//! that is no key) gives NULL, which equals no stored id.

use surrealdb_expr::expr::logical::{
	LogicalCall, LogicalExpr, LogicalIf, LogicalLiteral, LogicalRecordIdKey, LogicalRecordIdLit,
};
use surrealdb_expr::expr::{BinaryOperator, Function, Param, PrefixOperator};
use surrealdb_expr::val::Number;
use surrealdb_strand::{Strand, TableName};

use crate::ir::{BoundColumnRef, BoundExpr, BoundLiteral, KeyRead};
use crate::lower::expr::lower_column_raw;

/// A `BoundExpr` that names the key of a record: an integer or string literal, or a parameter.
pub(crate) enum KeyOperand<'a> {
	Int(i64),
	Str(&'a str),
	Param(&'a str),
}

/// The key `expr` names, when `expr` is an integer or string literal or a parameter.
///
/// Every other expression, including a float, a boolean, `NULL` and the empty string (which is no
/// record key), is not a key operand: it is compared through the key-extraction path, where SQL's
/// own comparison rules apply.
pub(crate) fn key_operand(expr: &BoundExpr) -> Option<KeyOperand<'_>> {
	match expr {
		BoundExpr::Literal(BoundLiteral::Integer(i)) => Some(KeyOperand::Int(*i)),
		BoundExpr::Literal(BoundLiteral::String(s)) if !s.is_empty() => {
			Some(KeyOperand::Str(s.as_str()))
		}
		BoundExpr::Param {
			name,
			..
		} => Some(KeyOperand::Param(name.as_str())),
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Access {
			..
		}
		| BoundExpr::Binary {
			..
		}
		| BoundExpr::Unary {
			..
		}
		| BoundExpr::FunctionCall {
			..
		}
		| BoundExpr::Case {
			..
		}
		| BoundExpr::Between {
			..
		}
		| BoundExpr::InList {
			..
		}
		| BoundExpr::IsNull {
			..
		}
		| BoundExpr::Cast {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::InSubquery {
			..
		}
		| BoundExpr::ArraySubquery(_)
		| BoundExpr::WindowFunction {
			..
		}
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => None,
	}
}

/// The column of `expr` when it reads the key of a base table's record id.
pub(crate) fn id_column(expr: &BoundExpr) -> Option<(&BoundColumnRef, &str)> {
	match expr {
		BoundExpr::Column(col) => col.id_table.as_deref().map(|table| (col, table)),
		BoundExpr::Literal(_)
		| BoundExpr::Access {
			..
		}
		| BoundExpr::Param {
			..
		}
		| BoundExpr::Binary {
			..
		}
		| BoundExpr::Unary {
			..
		}
		| BoundExpr::FunctionCall {
			..
		}
		| BoundExpr::Case {
			..
		}
		| BoundExpr::Between {
			..
		}
		| BoundExpr::InList {
			..
		}
		| BoundExpr::IsNull {
			..
		}
		| BoundExpr::Cast {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::InSubquery {
			..
		}
		| BoundExpr::ArraySubquery(_)
		| BoundExpr::WindowFunction {
			..
		}
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => None,
	}
}

/// `record::id(record)`: the key of the record id `record`.
///
/// A value that is not a record id (the absent id of the null-extended side of an outer join, or a
/// key already) is its own key, so `guarded` makes the key of an absent id absent and the key of a
/// key the key.
pub(crate) fn key_of(record: LogicalExpr, guarded: bool) -> LogicalExpr {
	let call = LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal("record::id".to_string()),
		args: vec![record.clone()],
	}));
	if guarded {
		let is_record = LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal(RECORD_KIND.to_string()),
			args: vec![record.clone()],
		}));
		LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(is_record, call)],
			otherwise: Some(record),
		}))
	} else {
		call
	}
}

/// The key of the base-table column `col`: `record::id` of the stored `id`.
///
/// The stored `id` is read through [`key_of`]'s guard when it may be absent: it belongs to a joined
/// table (an outer join leaves the null-extended side without one), to an outer row, or to the
/// output of an aggregation, whose group of an outer join's null-extended side has none.
pub(crate) fn lower_id_key(col: &BoundColumnRef) -> LogicalExpr {
	key_of(
		lower_column_raw(col),
		col.relation_index > 0 || col.correlation_depth > 0 || col.aggregate_output,
	)
}

/// The functions that test for the kinds of value that are the key of a record: an integer of any
/// width up to 32 bits (a `serial` or `integer` column holds an `i32`) or the 64-bit `int`, a
/// string, a uuid, an array or an object. Every other value (a float, a decimal, a boolean, a
/// datetime, NULL, absent) is no key. A `u64` is no key because only part of its domain names a
/// record.
///
/// This is the one definition of the rule, for the lowering of reads and of writes alike.
pub(crate) const KEY_KINDS: [&str; 11] = [
	"type::is_int",
	"type::is_i32",
	"type::is_i16",
	"type::is_i8",
	"type::is_u32",
	"type::is_u16",
	"type::is_u8",
	"type::is_string",
	"type::is_uuid",
	"type::is_array",
	"type::is_object",
];

/// The function that tests for a record link, which names the record by its key.
pub(crate) const RECORD_KIND: &str = "type::is_record";

/// The functions that test for the kinds of value that name a record: a key, or a record link.
pub(crate) const NAMING_KINDS: [&str; KEY_KINDS.len() + 1] = {
	let mut kinds = [RECORD_KIND; KEY_KINDS.len() + 1];
	let mut i = 0;
	while i < KEY_KINDS.len() {
		kinds[i] = KEY_KINDS[i];
		i += 1;
	}
	kinds
};

/// Whether the value `key` can name a record: it is one of the [`NAMING_KINDS`].
fn is_key_kind(key: &LogicalExpr) -> LogicalExpr {
	let mut tests = NAMING_KINDS.iter().map(|name| {
		LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal((*name).to_string()),
			args: vec![key.clone()],
		}))
	});
	let first = tests.next().unwrap_or(LogicalExpr::Literal(LogicalLiteral::Bool(false)));
	tests.fold(first, |tests, test| LogicalExpr::Binary {
		left: Box::new(tests),
		op: BinaryOperator::Or,
		right: Box::new(test),
	})
}

/// The key a literal names, as the literal `type::record` is called with, or `None` when the
/// literal is no key: an integer, a non-empty string and a uuid are keys, and a float or decimal
/// with an integral value that fits 64 bits is the integer key it equals, as a comparison of it
/// with an integer key by value finds. Every other literal names no record.
fn literal_key(literal: &LogicalLiteral) -> Option<LogicalLiteral> {
	match literal {
		LogicalLiteral::Integer(_) | LogicalLiteral::Uuid(_) => Some(literal.clone()),
		LogicalLiteral::String(text) if !text.as_str().is_empty() => Some(literal.clone()),
		LogicalLiteral::I8(v) => Some(LogicalLiteral::Integer(i64::from(*v))),
		LogicalLiteral::U8(v) => Some(LogicalLiteral::Integer(i64::from(*v))),
		LogicalLiteral::I16(v) => Some(LogicalLiteral::Integer(i64::from(*v))),
		LogicalLiteral::U16(v) => Some(LogicalLiteral::Integer(i64::from(*v))),
		LogicalLiteral::I32(v) => Some(LogicalLiteral::Integer(i64::from(*v))),
		LogicalLiteral::U32(v) => Some(LogicalLiteral::Integer(i64::from(*v))),
		LogicalLiteral::U64(v) => i64::try_from(*v).ok().map(LogicalLiteral::Integer),
		LogicalLiteral::Float(float) => {
			Number::Float(*float).as_int_lossless().map(LogicalLiteral::Integer)
		}
		LogicalLiteral::F16(v) => {
			surrealdb_expr::val::Number::F16(*v).as_int_lossless().map(LogicalLiteral::Integer)
		}
		LogicalLiteral::F32(v) => {
			surrealdb_expr::val::Number::F32(*v).as_int_lossless().map(LogicalLiteral::Integer)
		}
		LogicalLiteral::Decimal(decimal) => {
			Number::Decimal(*decimal).as_int_lossless().map(LogicalLiteral::Integer)
		}
		LogicalLiteral::BigDecimal(decimal) => {
			Number::BigDecimal(decimal.clone()).as_int_lossless().map(LogicalLiteral::Integer)
		}
		LogicalLiteral::String(_)
		| LogicalLiteral::None
		| LogicalLiteral::Null
		| LogicalLiteral::UnboundedRange
		| LogicalLiteral::Bool(_)
		| LogicalLiteral::Bytes(_)
		| LogicalLiteral::Regex(_)
		| LogicalLiteral::Duration(_)
		| LogicalLiteral::Datetime(_)
		| LogicalLiteral::Date(_)
		| LogicalLiteral::Time(_)
		| LogicalLiteral::Timestamp(_)
		| LogicalLiteral::Interval(_)
		| LogicalLiteral::Geometry(_)
		| LogicalLiteral::File(_) => None,
	}
}

/// The record of `table` that `key` names, `type::record(<table>, <key>)`.
///
/// A key that is no record key names no record: `type::record` would turn a float, a boolean, a
/// datetime or an absent or `NULL` key into a string key (`"1.5"`, `"NONE"`), which would match a
/// record with that key, and raises on an empty key. Those keys give `NULL`, which equals no
/// record id and does not equal the absent id an outer join leaves on its null-extended side
/// either.
///
/// A literal is judged when the statement is lowered, by [`literal_key`], and the call is built
/// from the key it gives. A key that is only known when the statement runs is guarded by the same
/// rule, except that a float or a decimal is never a key: only a literal number compares with an
/// integer key by value.
pub(crate) fn record_of(table: &str, key: LogicalExpr) -> LogicalExpr {
	record_of_with(table, key, false)
}

/// Convert keys to records for a comparison that observes NULL or inequality. A non-null value
/// that names no record stays non-null, so an empty string cannot compare as NULL. Lookup and
/// equijoin keys use [`record_of`] because only matching records matter there.
pub(crate) fn record_or_value(table: &str, key: LogicalExpr) -> LogicalExpr {
	record_of_with(table, key, true)
}

fn record_of_with(table: &str, key: LogicalExpr, preserve_value: bool) -> LogicalExpr {
	let call = |key: LogicalExpr| {
		LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal("type::record".to_string()),
			args: vec![LogicalExpr::Literal(LogicalLiteral::String(Strand::from(table))), key],
		}))
	};
	let no_record = || LogicalExpr::Literal(LogicalLiteral::Null);
	match key {
		LogicalExpr::Literal(literal) => match literal_key(&literal) {
			Some(key) => call(LogicalExpr::Literal(key)),
			None if preserve_value
				&& !matches!(literal, LogicalLiteral::None | LogicalLiteral::Null) =>
			{
				LogicalExpr::Literal(literal)
			}
			None => no_record(),
		},
		other @ LogicalExpr::Array(_)
		| other @ LogicalExpr::Set(_)
		| other @ LogicalExpr::Object(_)
		| other @ LogicalExpr::RecordId(_)
		| other @ LogicalExpr::Param(_)
		| other @ LogicalExpr::Table(_)
		| other @ LogicalExpr::Mock(_)
		| other @ LogicalExpr::Constant(_)
		| other @ LogicalExpr::Field(_)
		| other @ LogicalExpr::Doc
		| other @ LogicalExpr::Current
		| other @ LogicalExpr::Access {
			..
		}
		| other @ LogicalExpr::Index {
			..
		}
		| other @ LogicalExpr::First(_)
		| other @ LogicalExpr::Last(_)
		| other @ LogicalExpr::Each(_)
		| other @ LogicalExpr::Flatten {
			..
		}
		| other @ LogicalExpr::Filter {
			..
		}
		| other @ LogicalExpr::Method {
			..
		}
		| other @ LogicalExpr::Destructure {
			..
		}
		| other @ LogicalExpr::Optional {
			..
		}
		| other @ LogicalExpr::Lookup {
			..
		}
		| other @ LogicalExpr::Recurse(_)
		| other @ LogicalExpr::RepeatRecurse(_)
		| other @ LogicalExpr::Broadcast {
			..
		}
		| other @ LogicalExpr::SetField {
			..
		}
		| other @ LogicalExpr::Prefix {
			..
		}
		| other @ LogicalExpr::Postfix {
			..
		}
		| other @ LogicalExpr::Invoke {
			..
		}
		| other @ LogicalExpr::Binary {
			..
		}
		| other @ LogicalExpr::Call(_)
		| other @ LogicalExpr::Closure(_)
		| other @ LogicalExpr::If(_)
		| other @ LogicalExpr::Throw(_)
		| other @ LogicalExpr::Break
		| other @ LogicalExpr::Continue
		| other @ LogicalExpr::Subquery(_) => {
			let empty = equal(
				other.clone(),
				LogicalExpr::Literal(LogicalLiteral::String(Strand::from(""))),
			);
			let not_a_key = LogicalExpr::Prefix {
				op: PrefixOperator::Not,
				expr: Box::new(is_key_kind(&other)),
			};
			LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(
					LogicalExpr::Binary {
						left: Box::new(empty),
						op: BinaryOperator::Or,
						right: Box::new(not_a_key),
					},
					if preserve_value {
						LogicalExpr::Binary {
							left: Box::new(other.clone()),
							op: BinaryOperator::NullCoalescing,
							right: Box::new(no_record()),
						}
					} else {
						no_record()
					},
				)],
				otherwise: Some(call(other)),
			}))
		}
	}
}

/// The variable the `pgsql` and `mysql` RPC methods set to `true` for the statements they run:
/// their parameters carry no declared type, so a parameter that holds text is read as the type of
/// the column or key it meets. A wire connection declares its parameters' types and leaves it
/// unset, so a parameter declared as text stays text.
pub const UNTYPED_PARAMS_VAR: &str = "sql_untyped_params";

/// `is_string(value)`, and, for a parameter, that the statement's parameters are untyped.
pub(crate) fn text_param_guard(value: LogicalExpr) -> LogicalExpr {
	let is_text = LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal("type::is_string".to_string()),
		args: vec![value],
	}));
	LogicalExpr::Binary {
		left: Box::new(is_text),
		op: BinaryOperator::And,
		right: Box::new(LogicalExpr::Param(Param::from(UNTYPED_PARAMS_VAR.to_string()))),
	}
}

/// `key`, read as the key type `read` when it is text. An integer key column is named by `'7'` as
/// it is by `7`; any other value is the key as it is, as is any key of a column whose text is not
/// read.
pub(crate) fn key_read_as(read: KeyRead, key: LogicalExpr) -> LogicalExpr {
	let cast = if read == KeyRead::Int {
		surrealdb_expr::expr::Kind::Int
	} else if read == KeyRead::Uuid {
		surrealdb_expr::expr::Kind::Uuid
	} else {
		return key;
	};
	if let LogicalExpr::Literal(LogicalLiteral::String(text)) = &key
		&& read == KeyRead::Int
		&& let Ok(number) = text.as_str().parse::<i64>()
	{
		return LogicalExpr::Literal(LogicalLiteral::Integer(number));
	}
	if matches!(key, LogicalExpr::Literal(LogicalLiteral::String(_))) && read == KeyRead::Uuid {
		return LogicalExpr::Prefix {
			op: PrefixOperator::Cast(cast),
			expr: Box::new(key),
		};
	}
	if matches!(key, LogicalExpr::Param(_)) {
		return LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(
				text_param_guard(key.clone()),
				LogicalExpr::Prefix {
					op: PrefixOperator::Cast(cast),
					expr: Box::new(key.clone()),
				},
			)],
			otherwise: Some(key),
		}));
	}
	key
}

/// The record of `table` named by the key operand `key`, read as the key type `read`.
pub(crate) fn record_of_operand_typed(
	table: &str,
	key: &KeyOperand<'_>,
	read: KeyRead,
) -> LogicalExpr {
	match key {
		KeyOperand::Str(text) => {
			let literal = LogicalExpr::Literal(LogicalLiteral::String(Strand::from(*text)));
			let read_key = key_read_as(read, literal);
			if let LogicalExpr::Literal(LogicalLiteral::Integer(number)) = read_key {
				record_of_operand(table, &KeyOperand::Int(number))
			} else if read == KeyRead::Uuid {
				record_of(table, read_key)
			} else {
				record_of_operand(table, key)
			}
		}
		KeyOperand::Param(name) => record_of(
			table,
			key_read_as(read, LogicalExpr::Param(Param::from((*name).to_string()))),
		),
		KeyOperand::Int(_) => record_of_operand(table, key),
	}
}

/// The record of `table` named by the key operand `key`.
pub(crate) fn record_of_operand(table: &str, key: &KeyOperand<'_>) -> LogicalExpr {
	match key {
		KeyOperand::Int(i) => LogicalExpr::RecordId(Box::new(LogicalRecordIdLit {
			table: TableName::from(table),
			key: LogicalRecordIdKey::Number(*i),
		})),
		KeyOperand::Str(s) => LogicalExpr::RecordId(Box::new(LogicalRecordIdLit {
			table: TableName::from(table),
			key: LogicalRecordIdKey::String(Strand::from(*s)),
		})),
		KeyOperand::Param(name) => {
			record_of(table, LogicalExpr::Param(Param::from((*name).to_string())))
		}
	}
}

/// Whether `expr` holds a subquery.
pub(crate) fn contains_subquery(expr: &BoundExpr) -> bool {
	match expr {
		BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::InSubquery {
			..
		}
		| BoundExpr::ArraySubquery(_) => true,
		BoundExpr::Binary {
			left,
			right,
			..
		} => contains_subquery(left) || contains_subquery(right),
		BoundExpr::Unary {
			expr,
			..
		}
		| BoundExpr::IsNull {
			expr,
			..
		}
		| BoundExpr::Cast {
			expr,
			..
		}
		| BoundExpr::Access {
			base: expr,
			..
		} => contains_subquery(expr),
		BoundExpr::FunctionCall {
			args,
			..
		}
		| BoundExpr::WindowFunction {
			args,
			..
		} => args.iter().any(contains_subquery),
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => {
			operand.as_deref().is_some_and(contains_subquery)
				|| branches.iter().any(|(c, v)| contains_subquery(c) || contains_subquery(v))
				|| else_expr.as_deref().is_some_and(contains_subquery)
		}
		BoundExpr::Between {
			expr,
			low,
			high,
			..
		} => contains_subquery(expr) || contains_subquery(low) || contains_subquery(high),
		BoundExpr::InList {
			expr,
			list,
			..
		} => contains_subquery(expr) || list.iter().any(contains_subquery),
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => false,
	}
}

/// Whether `expr`, compared with an `id`, may hold the key or a record link to the row: a column
/// or an expression, not a literal, a parameter, the `id` of a base table or an expression that
/// holds a subquery (which would be evaluated for each use of the value).
pub(crate) fn is_key_or_link(expr: &BoundExpr) -> bool {
	!matches!(expr, BoundExpr::Literal(_) | BoundExpr::Param { .. })
		&& id_column(expr).is_none()
		&& !contains_subquery(expr)
}

/// `lhs = rhs` as the engine compares it, with no SQL null handling.
pub(crate) fn equal(lhs: LogicalExpr, rhs: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(lhs),
		op: BinaryOperator::Equal,
		right: Box::new(rhs),
	}
}
