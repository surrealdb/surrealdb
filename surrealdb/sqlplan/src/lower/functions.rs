//! Lowering of the registered SQL functions that have no single engine function to call.
//!
//! Each function here answers what the reference server answers for the dialect: PostgreSQL and
//! MySQL differ on the position of a string, on NULL, and on what is an error.

use surrealdb_expr::expr::logical::{LogicalCall, LogicalExpr, LogicalIf, LogicalLiteral};
use surrealdb_expr::expr::statements::define::SqlTypeDialect;
use surrealdb_expr::expr::statements::define::declared_type::{
	DeclaredSqlType, IntervalFinestField,
};
use surrealdb_expr::expr::{BinaryOperator, Function, Kind, PrefixOperator};
use surrealdb_expr::val::{Duration, Regex};
use surrealdb_strand::Strand;

use crate::descriptor::SqlType;
use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::ir::{BoundExpr, BoundLiteral};
use crate::lower::expr::{is_not_null_expr, is_null_expr, lower_bound_expr};
use crate::lower::temporal::static_type;

mod dates;
mod json;
mod strings;

fn int(value: i64) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::Integer(value))
}

fn text(value: &str) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::String(Strand::from(value)))
}

fn null() -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::Null)
}

fn call(name: &str, args: Vec<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal(name.to_owned()),
		args,
	}))
}

fn binary(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(left),
		op,
		right: Box::new(right),
	}
}

fn when(condition: LogicalExpr, then: LogicalExpr, otherwise: LogicalExpr) -> LogicalExpr {
	LogicalExpr::If(Box::new(LogicalIf {
		branches: vec![(condition, then)],
		otherwise: Some(otherwise),
	}))
}

fn max2(a: LogicalExpr, b: LogicalExpr) -> LogicalExpr {
	call("math::max", vec![LogicalExpr::Array(vec![a, b])])
}

fn min2(a: LogicalExpr, b: LogicalExpr) -> LogicalExpr {
	call("math::min", vec![LogicalExpr::Array(vec![a, b])])
}

/// The element `index` of the values a [`strict`] body reads its arguments from.
fn item(index: i64) -> LogicalExpr {
	LogicalExpr::Index {
		base: Box::new(LogicalExpr::Current),
		index: Box::new(int(index)),
	}
}

/// `values` evaluated once, each read by the expression `body` is given.
fn bind(values: Vec<LogicalExpr>, body: impl FnOnce(&[LogicalExpr]) -> LogicalExpr) -> LogicalExpr {
	let refs: Vec<LogicalExpr> = (0..values.len() as i64).map(item).collect();
	LogicalExpr::Optional {
		base: Box::new(LogicalExpr::Array(values)),
		then: Box::new(body(&refs)),
	}
}

/// `body` over `values`, each evaluated once, and NULL when any of them is NULL or NONE. `body`
/// reads the values by the expressions it is given.
fn strict(
	values: Vec<LogicalExpr>,
	body: impl FnOnce(&[LogicalExpr]) -> LogicalExpr,
) -> LogicalExpr {
	let refs: Vec<LogicalExpr> = (0..values.len() as i64).map(item).collect();
	let any_null = refs
		.iter()
		.cloned()
		.map(is_null_expr)
		.reduce(|a, b| binary(a, BinaryOperator::Or, b))
		.unwrap_or(LogicalExpr::Literal(LogicalLiteral::Bool(false)));
	LogicalExpr::Optional {
		base: Box::new(LogicalExpr::Array(values)),
		then: Box::new(when(any_null, null(), body(&refs))),
	}
}

fn lower_args(args: &[BoundExpr], policy: &DialectPolicy) -> Result<Vec<LogicalExpr>, PlanError> {
	args.iter().map(|arg| lower_bound_expr(arg, policy)).collect()
}

/// The text of a value in `dialect`, with NULL left as NULL. A float or a decimal prints without
/// the suffix the engine writes after it, and a boolean prints as PostgreSQL (`t`, `f`) or MySQL
/// (`1`, `0`) prints it.
fn as_text(dialect: SqlDialect, value: LogicalExpr) -> Result<LogicalExpr, PlanError> {
	let (truth, falsity) = match dialect {
		SqlDialect::Postgres => ("t", "f"),
		SqlDialect::MySql => ("1", "0"),
	};
	let rendered = call("type::string", vec![LogicalExpr::Current]);
	let is_inexact = binary(
		call("type::is_float", vec![LogicalExpr::Current]),
		BinaryOperator::Or,
		call("type::is_decimal", vec![LogicalExpr::Current]),
	);
	Ok(LogicalExpr::Optional {
		base: Box::new(value),
		then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![
				(
					call("type::is_bool", vec![LogicalExpr::Current]),
					when(LogicalExpr::Current, text(truth), text(falsity)),
				),
				(
					is_inexact,
					call("string::replace", vec![rendered.clone(), regex("(f|dec)$")?, text("")]),
				),
			],
			otherwise: Some(rendered),
		}))),
	})
}

/// The values that are not NULL or NONE, in order.
fn non_null(values: Vec<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Filter {
		base: Box::new(LogicalExpr::Array(values)),
		predicate: Box::new(is_not_null_expr(LogicalExpr::Current)),
	}
}

fn regex(pattern: &str) -> Result<LogicalExpr, PlanError> {
	pattern
		.parse::<Regex>()
		.map(|regex| LogicalExpr::Literal(LogicalLiteral::Regex(regex)))
		.map_err(|error| {
			PlanError::UnsupportedExpression(format!("invalid regular expression: {error}"))
		})
}

/// Which end of a string a trim removes.
#[derive(Clone, Copy)]
enum Trim {
	Leading,
	Trailing,
	Both,
}

/// `trim`, `ltrim` and `rtrim`, which remove spaces (or the given characters) and no other white
/// space. The characters are a PostgreSQL argument only, and may be any text expression.
fn lower_trim(
	name: &str,
	side: Trim,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let value = lower_bound_expr(&args[0], policy)?;
	let Some(characters) = args.get(1) else {
		return trim_literal(side, " ", value);
	};
	if policy.dialect == SqlDialect::MySql {
		return Err(PlanError::UnsupportedExpression(format!("{name} with trim characters")));
	}
	if let BoundExpr::Literal(BoundLiteral::String(characters)) = characters {
		return trim_literal(side, characters, value);
	}
	let characters = lower_bound_expr(characters, policy)?;
	Ok(strict(vec![value, characters], |values| trim_runtime(side, &values[0], &values[1])))
}

/// A trim of the characters of a set known when the query is planned, as one regular expression.
/// An empty set removes nothing.
fn trim_literal(
	side: Trim,
	characters: &str,
	value: LogicalExpr,
) -> Result<LogicalExpr, PlanError> {
	if characters.is_empty() {
		return Ok(value);
	}
	let set: String =
		characters.chars().map(|c| regex::escape(c.encode_utf8(&mut [0; 4]))).collect();
	let pattern = match side {
		Trim::Leading => format!("^[{set}]+"),
		Trim::Trailing => format!("[{set}]+$"),
		Trim::Both => format!("^[{set}]+|[{set}]+$"),
	};
	let pattern = regex(&pattern)?;
	Ok(strict(vec![value], |values| {
		call("string::replace", vec![values[0].clone(), pattern, text("")])
	}))
}

/// A trim of the characters of `set`, a string known only when the query runs. Each character of
/// `string` is judged against the set, and the string keeps the run from the first to the last
/// character outside it; a string with none outside it trims to the empty string.
fn trim_runtime(side: Trim, string: &LogicalExpr, set: &LogicalExpr) -> LogicalExpr {
	// `string::split(s, "")` is the characters of `s` between an empty string at either end. Every
	// set contains the empty string, so each end is judged inside the set and moves the first
	// outside position one later and the last one earlier, which the offsets below undo.
	let characters = call("string::split", vec![string.clone(), text("")]);
	let inside = LogicalExpr::Broadcast {
		base: Box::new(call(
			"array::combine",
			vec![characters, LogicalExpr::Array(vec![set.clone()])],
		)),
		body: Box::new(call("string::contains", vec![item(1), item(0)])),
		flatten: false,
	};
	let outside_from = |judged: LogicalExpr| {
		call("array::find_index", vec![judged, LogicalExpr::Literal(LogicalLiteral::Bool(false))])
	};
	// Over `[string, first outside position, first outside position from the end]`.
	let positions = LogicalExpr::Optional {
		base: Box::new(LogicalExpr::Array(vec![string.clone(), inside])),
		then: Box::new(LogicalExpr::Array(vec![
			item(0),
			outside_from(item(1)),
			outside_from(call("array::reverse", vec![item(1)])),
		])),
	};
	let start = binary(item(1), BinaryOperator::Subtract, int(1));
	let end = binary(
		binary(call("string::len", vec![item(0)]), BinaryOperator::Subtract, item(2)),
		BinaryOperator::Add,
		int(1),
	);
	let kept = match side {
		Trim::Leading => call("string::slice", vec![item(0), start]),
		Trim::Trailing => call("string::slice", vec![item(0), int(0), end]),
		Trim::Both => call("string::slice", vec![item(0), start, end]),
	};
	LogicalExpr::Optional {
		base: Box::new(positions),
		then: Box::new(when(is_null_expr(item(1)), text(""), kept)),
	}
}

/// PostgreSQL `substring(s, start[, count])`. Positions count from 1, and a start before 1 uses up
/// the count; a negative count is an error.
fn pgsql_substring(values: &[LogicalExpr], counted: bool) -> LogicalExpr {
	let (string, start) = (values[0].clone(), values[1].clone());
	let first = max2(start.clone(), int(1));
	let from = binary(first.clone(), BinaryOperator::Subtract, int(1));
	if !counted {
		return call("string::slice", vec![string, from]);
	}
	let count = values[2].clone();
	let kept = max2(
		binary(binary(count.clone(), BinaryOperator::Add, start), BinaryOperator::Subtract, first),
		int(0),
	);
	when(
		binary(count, BinaryOperator::LessThan, int(0)),
		LogicalExpr::Throw(Box::new(text("negative substring length not allowed"))),
		call("string::slice", vec![string, from.clone(), binary(from, BinaryOperator::Add, kept)]),
	)
}

/// MySQL's integer for a numeric argument: an integer as it is, and a string, float or decimal
/// rounded to the nearest one.
fn mysql_integer(value: LogicalExpr) -> LogicalExpr {
	when(
		call("type::is_int", vec![value.clone()]),
		value.clone(),
		call("type::int", vec![call("math::round", vec![call("type::float", vec![value])])]),
	)
}

/// MySQL `substring(s, pos[, len])`. Positions count from 1, a negative position counts from the
/// end, position 0 and a count below 1 give the empty string. The position and the count are
/// taken as integers, as [`mysql_integer`] takes them, and any BIGINT is accepted: the slice is
/// bounded by the length of the string before its end is computed, so it cannot overflow.
fn mysql_substring(values: &[LogicalExpr], counted: bool) -> LogicalExpr {
	let (string, position) = (values[0].clone(), mysql_integer(values[1].clone()));
	let chars = call("string::len", vec![string.clone()]);
	let from = when(
		binary(position.clone(), BinaryOperator::MoreThan, int(0)),
		binary(position.clone(), BinaryOperator::Subtract, int(1)),
		binary(chars.clone(), BinaryOperator::Add, position.clone()),
	);
	let mut empty = binary(
		binary(position, BinaryOperator::Equal, int(0)),
		BinaryOperator::Or,
		binary(from.clone(), BinaryOperator::LessThan, int(0)),
	);
	let slice = if counted {
		let count = mysql_integer(values[2].clone());
		empty = binary(
			empty,
			BinaryOperator::Or,
			binary(count.clone(), BinaryOperator::LessThanEqual, int(0)),
		);
		let start = min2(from, chars.clone());
		let end = binary(start.clone(), BinaryOperator::Add, min2(count, chars));
		call("string::slice", vec![string, start, end])
	} else {
		call("string::slice", vec![string, from])
	};
	when(empty, text(""), slice)
}

fn lower_substring(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let counted = args.len() == 3;
	if policy.dialect == SqlDialect::Postgres && string_literal(&args[1]).is_some() {
		return Err(PlanError::UnsupportedExpression("substring with a pattern".to_owned()));
	}
	let values = lower_args(args, policy)?;
	Ok(match policy.dialect {
		SqlDialect::Postgres => strict(values, |values| pgsql_substring(values, counted)),
		SqlDialect::MySql => strict(values, |values| mysql_substring(values, counted)),
	})
}

/// `concat(a, b, ...)`: PostgreSQL skips NULL arguments, MySQL answers NULL if there is one.
fn lower_concat(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let texts = lower_args(args, policy)?
		.into_iter()
		.map(|value| as_text(policy.dialect, value))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(match policy.dialect {
		SqlDialect::Postgres => call("array::join", vec![non_null(texts), text("")]),
		SqlDialect::MySql => strict(texts, |texts| call("string::concat", texts.to_vec())),
	})
}

/// `concat_ws(separator, a, b, ...)`: NULL when the separator is, and NULL arguments are skipped.
/// The separator joins as its text, as the arguments do.
fn lower_concat_ws(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let mut values = lower_args(args, policy)?.into_iter();
	let separator = as_text(policy.dialect, values.next().expect("concat_ws takes a separator"))?;
	let rest = non_null(
		values.map(|value| as_text(policy.dialect, value)).collect::<Result<Vec<_>, _>>()?,
	);
	Ok(strict(vec![separator, rest], |values| {
		call("array::join", vec![values[1].clone(), values[0].clone()])
	}))
}

/// MySQL `length(s)`, the bytes of the text of the value.
fn lower_mysql_length(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?
		.into_iter()
		.map(|value| as_text(policy.dialect, value))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(strict(values, |values| {
		call("bytes::len", vec![call("type::bytes", vec![values[0].clone()])])
	}))
}

fn string_literal(expr: &BoundExpr) -> Option<&str> {
	if let BoundExpr::Literal(BoundLiteral::String(value)) = expr {
		Some(value.as_str())
	} else {
		None
	}
}

fn integer_literal(expr: &BoundExpr) -> Option<i64> {
	if let BoundExpr::Literal(BoundLiteral::Integer(value)) = expr {
		Some(*value)
	} else {
		None
	}
}

fn float(value: f64) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::Float(value))
}

fn failure(message: &str) -> LogicalExpr {
	LogicalExpr::Throw(Box::new(text(message)))
}

fn negate(value: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Prefix {
		op: PrefixOperator::Negate,
		expr: Box::new(value),
	}
}

fn at_most(value: LogicalExpr, bound: i64) -> LogicalExpr {
	binary(value, BinaryOperator::LessThanEqual, int(bound))
}

fn len(value: &LogicalExpr) -> LogicalExpr {
	call("string::len", vec![value.clone()])
}

/// The error PostgreSQL raises, or the NULL MySQL answers, where the operation is undefined.
fn undefined(dialect: SqlDialect, message: &str) -> LogicalExpr {
	match dialect {
		SqlDialect::Postgres => failure(message),
		SqlDialect::MySql => null(),
	}
}

/// `mod(a, b)`: a remainder with the sign of `a`. A zero divisor is an error in PostgreSQL and NULL
/// in MySQL.
fn lower_mod(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let dialect = policy.dialect;
	Ok(strict(lower_args(args, policy)?, |v| {
		when(
			binary(v[1].clone(), BinaryOperator::Equal, int(0)),
			undefined(dialect, "division by zero"),
			binary(v[0].clone(), BinaryOperator::Remainder, v[1].clone()),
		)
	}))
}

/// `value` is not positive: PostgreSQL names the reason, MySQL answers NULL.
fn log_domain(dialect: SqlDialect, value: &LogicalExpr, defined: LogicalExpr) -> LogicalExpr {
	when(
		at_most(value.clone(), 0),
		when(
			binary(value.clone(), BinaryOperator::Equal, int(0)),
			undefined(dialect, "cannot take logarithm of zero"),
			undefined(dialect, "cannot take logarithm of a negative number"),
		),
		defined,
	)
}

/// `ln(x)`, and PostgreSQL `log(x)` (base 10) and MySQL `log(x)` (natural), and `log(base, x)`.
fn lower_log(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let dialect = policy.dialect;
	let values = lower_args(args, policy)?;
	Ok(match (name, values.len()) {
		("ln", _) | ("log", 1) => {
			let engine = if name == "log" && dialect == SqlDialect::Postgres {
				"math::log10"
			} else {
				"math::ln"
			};
			strict(values, |v| log_domain(dialect, &v[0], call(engine, vec![v[0].clone()])))
		}
		_ => strict(values, |v| {
			let (base, value) = (&v[0], &v[1]);
			let defined = match dialect {
				SqlDialect::Postgres => when(
					binary(base.clone(), BinaryOperator::Equal, int(1)),
					failure("division by zero"),
					call("math::log", vec![value.clone(), base.clone()]),
				),
				SqlDialect::MySql => when(
					binary(base.clone(), BinaryOperator::Equal, int(1)),
					null(),
					call("math::log", vec![value.clone(), base.clone()]),
				),
			};
			let value_checked = log_domain(dialect, value, defined);
			when(
				at_most(base.clone(), 0),
				match dialect {
					SqlDialect::Postgres => when(
						binary(base.clone(), BinaryOperator::Equal, int(0)),
						failure("cannot take logarithm of zero"),
						failure("cannot take logarithm of a negative number"),
					),
					SqlDialect::MySql => null(),
				},
				value_checked,
			)
		}),
	})
}

/// `exp(x)`.
fn lower_exp(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| {
		call("math::pow", vec![float(std::f64::consts::E), v[0].clone()])
	}))
}

/// MySQL `truncate(x, d)`: `x` toward zero at `d` decimal places, and at `-d` places to the left of
/// the point when `d` is negative.
fn lower_truncate(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(truncate_values(lower_args(args, policy)?))
}

/// `x` toward zero at `d` decimal places, for the values `[x, d]`.
fn truncate_values(values: Vec<LogicalExpr>) -> LogicalExpr {
	strict(values, |v| {
		let (x, d) = (&v[0], &v[1]);
		let magnitude = call("math::abs", vec![x.clone()]);
		let sign = call("math::sign", vec![x.clone()]);
		let kept = |scale: LogicalExpr, towards_point: bool| {
			let (outer, inner) = if towards_point {
				(BinaryOperator::Divide, BinaryOperator::Multiply)
			} else {
				(BinaryOperator::Multiply, BinaryOperator::Divide)
			};
			binary(
				sign.clone(),
				BinaryOperator::Multiply,
				binary(
					call("math::floor", vec![binary(magnitude.clone(), inner, scale.clone())]),
					outer,
					scale,
				),
			)
		};
		// A value kept to `d` places has scale `d`, as the truncated value of a numeric has.
		let towards_point = kept(call("math::pow", vec![float(10.0), d.clone()]), true);
		when(
			binary(d.clone(), BinaryOperator::MoreThan, int(0)),
			call("math::fixed", vec![towards_point.clone(), d.clone()]),
			when(
				binary(d.clone(), BinaryOperator::Equal, int(0)),
				towards_point,
				kept(call("math::pow", vec![float(10.0), negate(d.clone())]), false),
			),
		)
	})
}

/// `round(x, d)`, half away from zero, computed on decimals so that a halfway case is not decided
/// by binary rounding.
fn lower_round(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| round_half_away(&v[0], &v[1])))
}

/// `round(x)`: an exact numeric (a decimal or an arbitrary-precision decimal) rounds half away
/// from zero, as `round(x, 0)` does; a float or an integer keeps the engine's `math::round`.
fn lower_round_whole(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| {
		when(
			binary(
				call("type::is_float", vec![v[0].clone()]),
				BinaryOperator::Or,
				call("type::is_int", vec![v[0].clone()]),
			),
			call("math::round", vec![v[0].clone()]),
			round_half_away(&v[0], &int(0)),
		)
	}))
}

/// `x` rounded half away from zero to `d` places, computed exactly. A float is converted to a
/// decimal first; an exact numeric keeps its own representation, so an arbitrary-precision value
/// beyond a decimal's range is rounded as it is.
fn round_half_away(x: &LogicalExpr, d: &LogicalExpr) -> LogicalExpr {
	let decimal = |value: LogicalExpr| call("type::decimal", vec![value]);
	let scale = decimal(call("math::pow", vec![float(10.0), d.clone()]));
	let exact = when(call("type::is_float", vec![x.clone()]), decimal(x.clone()), x.clone());
	let scaled = binary(exact, BinaryOperator::Multiply, scale.clone());
	let rounded = call(
		"math::floor",
		vec![binary(
			call("math::abs", vec![scaled.clone()]),
			BinaryOperator::Add,
			decimal(float(0.5)),
		)],
	);
	binary(
		binary(call("math::sign", vec![scaled]), BinaryOperator::Multiply, rounded),
		BinaryOperator::Divide,
		scale,
	)
}

/// `left(s, n)` and `right(s, n)`. A negative `n` is all but the last (first) `-n` characters in
/// PostgreSQL, and the empty string in MySQL, where zero is also the empty string.
fn lower_left_right(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let dialect = policy.dialect;
	let leftmost = name == "left";
	Ok(strict(lower_args(args, policy)?, |v| {
		let (s, n) = (&v[0], &v[1]);
		let slice = |args: Vec<LogicalExpr>| call("string::slice", args);
		let excess = |sign: BinaryOperator| max2(binary(len(s), sign, n.clone()), int(0));
		let counted = if leftmost {
			slice(vec![s.clone(), int(0), n.clone()])
		} else {
			slice(vec![s.clone(), excess(BinaryOperator::Subtract)])
		};
		let negative = match (dialect, leftmost) {
			(SqlDialect::MySql, _) => text(""),
			(SqlDialect::Postgres, true) => {
				slice(vec![s.clone(), int(0), excess(BinaryOperator::Add)])
			}
			(SqlDialect::Postgres, false) => slice(vec![s.clone(), negate(n.clone())]),
		};
		when(
			binary(n.clone(), BinaryOperator::Equal, int(0)),
			text(""),
			when(at_most(n.clone(), 0), negative, counted),
		)
	}))
}

/// PostgreSQL `overlay(s PLACING r FROM n [FOR m])`: `s` with the `m` characters (the length of
/// `r` when there is no count) from position `n` replaced by `r`.
fn lower_overlay(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| {
		let (s, replacement, start) = (&v[0], &v[1], &v[2]);
		let count = v.get(3).cloned().unwrap_or_else(|| len(replacement));
		let before = {
			let n = binary(start.clone(), BinaryOperator::Subtract, int(1));
			when(
				binary(n.clone(), BinaryOperator::LessThan, int(0)),
				failure("negative substring length not allowed"),
				call("string::slice", vec![s.clone(), int(0), n]),
			)
		};
		let after =
			pgsql_substring(&[s.clone(), binary(start.clone(), BinaryOperator::Add, count)], false);
		call("string::concat", vec![before, replacement.clone(), after])
	}))
}

/// `split_part(s, delimiter, n)`: the `n`th field from the start, or from the end when negative;
/// the empty string when there is none. Position zero is an error.
fn lower_split_part(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| {
		let (s, delimiter, n) = (&v[0], &v[1], &v[2]);
		let parts = when(
			binary(delimiter.clone(), BinaryOperator::Equal, text("")),
			LogicalExpr::Array(vec![s.clone()]),
			call("string::split", vec![s.clone(), delimiter.clone()]),
		);
		let (fields, position) = (item(0), item(1));
		let picked = call(
			"array::at",
			vec![
				fields,
				when(
					binary(position.clone(), BinaryOperator::MoreThan, int(0)),
					binary(position.clone(), BinaryOperator::Subtract, int(1)),
					position,
				),
			],
		);
		when(
			binary(n.clone(), BinaryOperator::Equal, int(0)),
			failure("field position must not be zero"),
			LogicalExpr::Optional {
				base: Box::new(LogicalExpr::Array(vec![parts, n.clone()])),
				then: Box::new(binary(picked, BinaryOperator::NullCoalescing, text(""))),
			},
		)
	}))
}

/// `instr(s, sub)`, `locate(sub, s[, pos])` and `position(sub IN s)`: the 1-based position of the
/// first occurrence, or 0. MySQL's comparison ignores case, as its default collation does;
/// PostgreSQL's does not.
fn lower_locate(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let mut values = lower_args(args, policy)?;
	if name == "instr" || name == "strpos" {
		values.swap(0, 1);
	}
	let ignore_case = policy.dialect == SqlDialect::MySql;
	let lower = |value: &LogicalExpr| {
		if ignore_case {
			call("string::lowercase", vec![value.clone()])
		} else {
			value.clone()
		}
	};
	Ok(strict(values, |v| {
		let (needle, haystack) = (&v[0], &v[1]);
		let start = v.get(2).cloned().unwrap_or_else(|| int(1));
		let rest = call(
			"string::slice",
			vec![lower(haystack), binary(start.clone(), BinaryOperator::Subtract, int(1))],
		);
		let fields = call("string::split", vec![rest, lower(needle)]);
		let found = LogicalExpr::Optional {
			base: Box::new(LogicalExpr::Array(vec![fields, start.clone()])),
			then: Box::new(when(
				binary(call("array::len", vec![item(0)]), BinaryOperator::Equal, int(1)),
				int(0),
				binary(
					len(&LogicalExpr::Index {
						base: Box::new(item(0)),
						index: Box::new(int(0)),
					}),
					BinaryOperator::Add,
					item(1),
				),
			)),
		};
		when(at_most(start, 0), int(0), found)
	}))
}

/// The first byte of the string, as MySQL `ascii` answers, or the code point of its first
/// character, as PostgreSQL does.
fn lower_ascii(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let dialect = policy.dialect;
	let bytes = LogicalExpr::Prefix {
		op: PrefixOperator::Cast(Kind::Array(Box::new(Kind::Int), None)),
		expr: Box::new(call("type::bytes", vec![item(0)])),
	};
	let byte = |index: i64| {
		binary(
			LogicalExpr::Index {
				base: Box::new(LogicalExpr::Current),
				index: Box::new(int(index)),
			},
			BinaryOperator::NullCoalescing,
			int(0),
		)
	};
	let continuation = |index: i64| binary(byte(index), BinaryOperator::Subtract, int(128));
	let shift = |accumulated: LogicalExpr, index: i64| {
		binary(
			binary(accumulated, BinaryOperator::Multiply, int(64)),
			BinaryOperator::Add,
			continuation(index),
		)
	};
	let decoded = match dialect {
		SqlDialect::MySql => byte(0),
		SqlDialect::Postgres => {
			let lead = byte(0);
			when(
				binary(lead.clone(), BinaryOperator::LessThan, int(128)),
				lead.clone(),
				when(
					binary(lead.clone(), BinaryOperator::LessThan, int(224)),
					shift(binary(lead.clone(), BinaryOperator::Subtract, int(192)), 1),
					when(
						binary(lead.clone(), BinaryOperator::LessThan, int(240)),
						shift(
							shift(binary(lead.clone(), BinaryOperator::Subtract, int(224)), 1),
							2,
						),
						shift(
							shift(shift(binary(lead, BinaryOperator::Subtract, int(240)), 1), 2),
							3,
						),
					),
				),
			)
		}
	};
	let values = lower_args(args, policy)?;
	Ok(strict(values, |_| LogicalExpr::Optional {
		base: Box::new(bytes),
		then: Box::new(decoded),
	}))
}

/// The text of the character with the code point, for a code point the call gives as a literal.
fn literal_char(name: &str, code: &BoundExpr) -> Result<char, PlanError> {
	let code = integer_literal(code).ok_or_else(|| {
		PlanError::UnsupportedExpression(format!(
			"{name} with an argument that is not an integer literal"
		))
	})?;
	u32::try_from(code)
		.ok()
		.and_then(char::from_u32)
		.filter(|c| *c != '\0' || name == "char")
		.ok_or_else(|| PlanError::Custom(format!("{name}: code point out of range")))
}

fn lower_chr(name: &str, args: &[BoundExpr]) -> Result<LogicalExpr, PlanError> {
	let mut out = String::new();
	for arg in args {
		let c = literal_char(name, arg)?;
		if name == "char" && !c.is_ascii() {
			return Err(PlanError::UnsupportedExpression("char with a byte above 127".to_owned()));
		}
		out.push(c);
	}
	Ok(text(&out))
}

/// `date_part(field, ts)` and `extract(field FROM ts)`, for a field given as a literal.
fn lower_date_part(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let field = string_literal(&args[0])
		.ok_or_else(|| {
			PlanError::UnsupportedExpression("a date part that is not a string literal".to_owned())
		})?
		.to_ascii_lowercase();
	const FIELDS: [&str; 13] = [
		"year",
		"month",
		"day",
		"hour",
		"minute",
		"second",
		"dow",
		"isodow",
		"doy",
		"week",
		"epoch",
		"quarter",
		"microsecond",
	];
	if policy.dialect == SqlDialect::MySql && !FIELDS.contains(&field.as_str()) {
		return Err(PlanError::UnsupportedExpression(format!("the extract unit {field}")));
	}
	let mut value = lower_args(&args[1..], policy)?;
	if policy.dialect == SqlDialect::Postgres {
		return lower_pg_date_part(&field, value.remove(0));
	}
	value = value.into_iter().map(mysql_datetime).collect();
	let part = |function: &str, v: &[LogicalExpr]| call(function, vec![v[0].clone()]);
	Ok(strict(value, |v| match field.as_str() {
		"year" => part("time::year", v),
		"month" => part("time::month", v),
		"day" => part("time::day", v),
		"hour" => part("time::hour", v),
		"minute" => part("time::minute", v),
		"second" => binary(
			binary(
				binary(
					binary(part("time::micros", v), BinaryOperator::Remainder, int(60_000_000)),
					BinaryOperator::Add,
					int(60_000_000),
				),
				BinaryOperator::Remainder,
				int(60_000_000),
			),
			BinaryOperator::Divide,
			float(1_000_000.0),
		),
		"microsecond" => binary(
			binary(
				binary(part("time::micros", v), BinaryOperator::Remainder, int(1_000_000)),
				BinaryOperator::Add,
				int(1_000_000),
			),
			BinaryOperator::Remainder,
			int(1_000_000),
		),
		"dow" => binary(part("time::wday", v), BinaryOperator::Remainder, int(7)),
		"isodow" => part("time::wday", v),
		"doy" => part("time::yday", v),
		"week" if policy.dialect == SqlDialect::MySql => mysql_week(v[0].clone()),
		"week" => part("time::week", v),
		"epoch" => binary(part("time::micros", v), BinaryOperator::Divide, float(1_000_000.0)),
		"quarter" => call(
			"math::ceil",
			vec![binary(part("time::month", v), BinaryOperator::Divide, float(3.0))],
		),
		_ => failure(&format!("unit \"{field}\" not recognized for type timestamp")),
	}))
}

/// The fields PostgreSQL's `extract` and `date_part` read from a date, time, timestamp or
/// interval, by their canonical names.
const PG_EXTRACT_FIELDS: [&str; 19] = [
	"year",
	"month",
	"day",
	"quarter",
	"decade",
	"century",
	"millennium",
	"dow",
	"isodow",
	"doy",
	"week",
	"isoyear",
	"julian",
	"hour",
	"minute",
	"second",
	"milliseconds",
	"microseconds",
	"epoch",
];

/// The canonical name of a PostgreSQL date field, which also accepts the plural and the
/// singular of the time units.
fn pg_extract_field(field: &str) -> &str {
	match field {
		"years" => "year",
		"months" => "month",
		"days" => "day",
		"hours" => "hour",
		"minutes" => "minute",
		"seconds" => "second",
		"millisecond" => "milliseconds",
		"microsecond" => "microseconds",
		"centuries" => "century",
		"decades" => "decade",
		"millennia" => "millennium",
		other => other,
	}
}

/// PostgreSQL `date_part(field, v)` and `extract(field FROM v)` over a date, time, timestamp,
/// timestamptz or interval.
fn lower_pg_date_part(field: &str, value: LogicalExpr) -> Result<LogicalExpr, PlanError> {
	let field = pg_extract_field(field);
	if !PG_EXTRACT_FIELDS.contains(&field) {
		return Ok(strict(vec![value], |_| {
			failure(&format!("unit \"{field}\" not recognized for type timestamp"))
		}));
	}
	// `time::extract` reads the interval and duration kinds as well as the temporal ones, so every
	// operand kind takes the same call and the same field set.
	Ok(strict(vec![value], |v| call("time::extract", vec![v[0].clone(), text(field)])))
}

/// MySQL `WEEK(d)` in its default mode 0: weeks start on Sunday, week 1 holds the first Sunday of
/// the year, and the days before it are week 0.
fn mysql_week(date: LogicalExpr) -> LogicalExpr {
	let day_of_year = call("time::yday", vec![date.clone()]);
	let sunday_based = binary(call("time::wday", vec![date]), BinaryOperator::Remainder, int(7));
	bind(vec![day_of_year, sunday_based], |v| {
		let (doy, dow) = (&v[0], &v[1]);
		// Weekday of 1 January, and the day of the year of the first Sunday.
		let january_first = binary(
			binary(
				binary(
					dow.clone(),
					BinaryOperator::Subtract,
					binary(doy.clone(), BinaryOperator::Subtract, int(1)),
				),
				BinaryOperator::Remainder,
				int(7),
			),
			BinaryOperator::Add,
			int(7),
		);
		let first_sunday = binary(
			binary(
				binary(
					int(7),
					BinaryOperator::Subtract,
					binary(january_first, BinaryOperator::Remainder, int(7)),
				),
				BinaryOperator::Remainder,
				int(7),
			),
			BinaryOperator::Add,
			int(1),
		);
		bind(vec![doy.clone(), first_sunday], |w| {
			when(
				binary(w[0].clone(), BinaryOperator::LessThan, w[1].clone()),
				int(0),
				binary(
					call(
						"math::floor",
						vec![binary(
							binary(w[0].clone(), BinaryOperator::Subtract, w[1].clone()),
							BinaryOperator::Divide,
							float(7.0),
						)],
					),
					BinaryOperator::Add,
					int(1),
				),
			)
		})
	})
}

/// `isfinite(x)` for a `date`, `timestamp` or `timestamptz`: `x` is neither `infinity` nor
/// `-infinity`. The infinite values are compared with the typed constructor of the operand's kind,
/// so a comparison never crosses kinds, where the greatest date would not equal the greatest
/// timestamp. An operand whose type is not known when the statement is lowered (a value an
/// `INSERT` writes) is compared with the infinite values of all three kinds, of which only those
/// of its own kind can equal it. A bare text literal is refused, as PostgreSQL finds the call
/// ambiguous, and so is an operand of any other known type. A NULL operand gives NULL, as in
/// PostgreSQL.
fn lower_isfinite(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let [arg] = args else {
		return Err(PlanError::UnsupportedExpression("isfinite takes one argument".to_owned()));
	};
	let kind = static_type(arg, SqlDialect::Postgres);
	let constructors: &[&str] = if kind == SqlType::Date {
		&["type::date"]
	} else if kind == SqlType::Timestamp {
		&["type::timestamp"]
	} else if kind == SqlType::TimestampTz {
		&["type::datetime"]
	} else if kind == SqlType::Unknown && string_literal(arg).is_none() {
		&["type::date", "type::timestamp", "type::datetime"]
	} else {
		return Err(PlanError::UnsupportedExpression(format!(
			"isfinite of a value of type {kind:?}; it takes a date, timestamp or timestamptz"
		)));
	};
	let value = lower_bound_expr(arg, policy)?;
	let mut comparisons = constructors.iter().flat_map(|constructor| {
		["infinity", "-infinity"].map(|bound| {
			binary(value.clone(), BinaryOperator::Equal, call(constructor, vec![text(bound)]))
		})
	});
	let first = comparisons.next().ok_or_else(|| {
		PlanError::UnsupportedExpression("isfinite with no kind to compare".to_owned())
	})?;
	let infinite =
		comparisons.fold(first, |either, equal| binary(either, BinaryOperator::Or, equal));
	Ok(LogicalExpr::Prefix {
		op: PrefixOperator::Not,
		expr: Box::new(infinite),
	})
}

/// `date_trunc(unit, v)`: a timestamp and a timestamptz keep their type, and a date is a
/// timestamptz as PostgreSQL's promotion makes it.
fn lower_date_trunc(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let unit = string_literal(&args[0])
		.ok_or_else(|| {
			PlanError::UnsupportedExpression(
				"a date_trunc unit that is not a string literal".to_owned(),
			)
		})?
		.to_ascii_lowercase();
	let unit = match unit.as_str() {
		"years" => "year".to_owned(),
		"months" => "month".to_owned(),
		"days" => "day".to_owned(),
		"hours" => "hour".to_owned(),
		"minutes" => "minute".to_owned(),
		"seconds" => "second".to_owned(),
		"millisecond" => "milliseconds".to_owned(),
		"microsecond" => "microseconds".to_owned(),
		"centuries" => "century".to_owned(),
		"decades" => "decade".to_owned(),
		"millennia" => "millennium".to_owned(),
		_ => unit,
	};
	const UNITS: [&str; 13] = [
		"microseconds",
		"milliseconds",
		"second",
		"minute",
		"hour",
		"day",
		"week",
		"month",
		"quarter",
		"year",
		"decade",
		"century",
		"millennium",
	];
	let value = lower_args(&args[1..], policy)?;
	Ok(strict(value, |v| {
		if !UNITS.contains(&unit.as_str()) {
			return failure(&format!(
				"unit \"{unit}\" not recognized for type timestamp with time zone"
			));
		}
		let operand = when(
			call("type::is_date", vec![v[0].clone()]),
			call("type::datetime", vec![v[0].clone()]),
			v[0].clone(),
		);
		call("time::trunc", vec![operand, text(&unit)])
	}))
}

/// A MySQL date or datetime as a datetime: a datetime stays one, a `date` or `timestamp` value is
/// read as UTC midnight or wall time, and text as MySQL writes it (`2024-03-15` or
/// `2024-03-15 10:20:30`) is read as UTC.
fn mysql_datetime(value: LogicalExpr) -> LogicalExpr {
	let current = LogicalExpr::Current;
	LogicalExpr::Optional {
		base: Box::new(value),
		then: Box::new(when(
			call("type::is_datetime", vec![current.clone()]),
			current.clone(),
			call(
				"type::datetime",
				vec![when(
					call("type::is_string", vec![current.clone()]),
					when(
						binary(len(&current), BinaryOperator::Equal, int(10)),
						binary(current.clone(), BinaryOperator::Add, text("T00:00:00Z")),
						binary(
							call("string::replace", vec![current.clone(), text(" "), text("T")]),
							BinaryOperator::Add,
							text("Z"),
						),
					),
					current,
				)],
			),
		)),
	}
}

/// The `strftime` format for a MySQL `DATE_FORMAT` format, or `None` for a specifier with no
/// equivalent.
fn mysql_format(format: &str) -> Option<String> {
	let mut out = String::new();
	let mut chars = format.chars();
	while let Some(c) = chars.next() {
		if c != '%' {
			out.push(c);
			continue;
		}
		out.push_str(match chars.next()? {
			'Y' => "%Y",
			'y' => "%y",
			'm' => "%m",
			'c' => "%-m",
			'd' => "%d",
			'e' => "%-d",
			'H' => "%H",
			'k' => "%-H",
			'h' | 'I' => "%I",
			'l' => "%-I",
			'i' => "%M",
			's' | 'S' => "%S",
			'f' => "%6f",
			'p' => "%p",
			'T' => "%T",
			'M' => "%B",
			'b' => "%b",
			'W' => "%A",
			'a' => "%a",
			'j' => "%j",
			'%' => "%%",
			_ => return None,
		});
	}
	Some(out)
}

fn literal_format(args: &[BoundExpr], name: &str) -> Result<String, PlanError> {
	let format = args.get(1).and_then(string_literal).ok_or_else(|| {
		PlanError::UnsupportedExpression(format!(
			"{name} with a format that is not a string literal"
		))
	})?;
	mysql_format(format)
		.ok_or_else(|| PlanError::UnsupportedExpression(format!("a format specifier of {name}")))
}

fn lower_date_format(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let format = literal_format(args, "date_format")?;
	let value = mysql_datetime(lower_bound_expr(&args[0], policy)?);
	Ok(strict(vec![value], |v| call("time::format", vec![v[0].clone(), text(&format)])))
}

fn lower_from_unixtime(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let value = lower_bound_expr(&args[0], policy)?;
	let datetime = |v: &[LogicalExpr]| call("time::from_unix", vec![v[0].clone()]);
	if args.len() == 2 {
		let format = literal_format(args, "from_unixtime")?;
		return Ok(strict(vec![value], |v| call("time::format", vec![datetime(v), text(&format)])));
	}
	Ok(strict(vec![value], |v| datetime(v)))
}

fn lower_unix_timestamp(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let Some(arg) = args.first() else {
		return Ok(call("time::unix", vec![call("time::now", Vec::new())]));
	};
	let value = mysql_datetime(lower_bound_expr(arg, policy)?);
	Ok(strict(vec![value], |v| call("time::unix", vec![v[0].clone()])))
}

fn lower_datediff(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let day = |value: &LogicalExpr| call("time::group", vec![value.clone(), text("day")]);
	let values = vec![
		mysql_datetime(lower_bound_expr(&args[0], policy)?),
		mysql_datetime(lower_bound_expr(&args[1], policy)?),
	];
	Ok(strict(values, |v| {
		call("duration::days", vec![binary(day(&v[0]), BinaryOperator::Subtract, day(&v[1]))])
	}))
}

fn lower_sha2(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let Some(bits) = integer_literal(&args[1]) else {
		return Err(PlanError::UnsupportedExpression(
			"sha2 with a length that is not an integer literal".to_owned(),
		));
	};
	let engine = match bits {
		0 | 256 => "crypto::sha256",
		512 => "crypto::sha512",
		224 | 384 => {
			return Err(PlanError::UnsupportedExpression("sha2 with 224 or 384 bits".to_owned()));
		}
		_ => return Ok(null()),
	};
	let value = lower_bound_expr(&args[0], policy)?;
	Ok(strict(vec![value], |v| call(engine, vec![v[0].clone()])))
}

/// `pg_typeof(x)`, from the engine's name for the type of the value.
fn lower_pg_typeof(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let names = [
		("bool", "boolean"),
		("i8", "smallint"),
		("i16", "smallint"),
		("i32", "integer"),
		("int", "bigint"),
		("f32", "real"),
		("float", "double precision"),
		("decimal", "numeric"),
		("bigdecimal", "numeric"),
		("string", "text"),
		("datetime", "timestamp with time zone"),
		("date", "date"),
		("time", "time without time zone"),
		("timestamp", "timestamp without time zone"),
		("interval", "interval"),
		("duration", "interval"),
		("uuid", "uuid"),
		("bytes", "bytea"),
		("object", "jsonb"),
		("array", "jsonb"),
	];
	let engine = call("type::of", vec![LogicalExpr::Current]);
	let mut mapped = text("unknown");
	for (from, to) in names.into_iter().rev() {
		mapped = when(binary(engine.clone(), BinaryOperator::Equal, text(from)), text(to), mapped);
	}
	Ok(binary(
		LogicalExpr::Optional {
			base: Box::new(lower_bound_expr(&args[0], policy)?),
			then: Box::new(mapped),
		},
		BinaryOperator::NullCoalescing,
		text("unknown"),
	))
}

/// `json_build_object`, `json_object`: the keys are the text of the arguments in key position.
fn lower_json_object(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	if !args.len().is_multiple_of(2) {
		return Err(PlanError::UnsupportedExpression(
			"a JSON object function with an odd number of arguments".to_owned(),
		));
	}
	let all_literal =
		args.chunks(2).all(|pair| matches!(pair[0], BoundExpr::Literal(BoundLiteral::String(_))));
	if all_literal {
		let mut object = Vec::new();
		for pair in args.chunks(2) {
			let BoundExpr::Literal(BoundLiteral::String(key)) = &pair[0] else {
				unreachable!("every key is a string literal");
			};
			let value = match pair.get(1) {
				Some(value) => lower_bound_expr(value, policy)?,
				None => null(),
			};
			object.push((key.clone(), value));
		}
		return Ok(LogicalExpr::Object(object));
	}
	let mut entries = Vec::new();
	for pair in args.chunks(2) {
		let key = bind(vec![lower_bound_expr(&pair[0], policy)?], |k| {
			when(
				is_null_expr(k[0].clone()),
				failure("null value not allowed for object key"),
				k[0].clone(),
			)
		});
		let key = as_text(policy.dialect, key)?;
		let value = match pair.get(1) {
			Some(value) => lower_bound_expr(value, policy)?,
			None => null(),
		};
		entries.push(LogicalExpr::Array(vec![key, value]));
	}
	Ok(call("object::from_entries", vec![LogicalExpr::Array(entries)]))
}

/// A JSON document given as text is parsed, and one that is not text is used as it is.
fn json_document(value: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Optional {
		base: Box::new(value),
		then: Box::new(when(
			call("type::is_string", vec![LogicalExpr::Current]),
			call("encoding::json::decode", vec![LogicalExpr::Current]),
			LogicalExpr::Current,
		)),
	}
}

/// MySQL `json_extract(doc, path[, path ...])` for paths of keys and indexes (`$.a[1].b`): the
/// value at the path, NULL where there is none, and for several paths the array of the values
/// found.
fn lower_json_extract(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let mut extractions = Vec::new();
	for path in &args[1..] {
		let path = string_literal(path).ok_or_else(|| {
			PlanError::UnsupportedExpression(
				"json_extract with a path that is not a string literal".to_owned(),
			)
		})?;
		let mut extracted = LogicalExpr::Current;
		for step in json_path(path)? {
			extracted = match step {
				JsonStep::Key(key) => LogicalExpr::Access {
					base: Box::new(extracted),
					field: Strand::from(key.as_str()),
				},
				JsonStep::Index(index) => LogicalExpr::Index {
					base: Box::new(extracted),
					index: Box::new(int(index)),
				},
			};
		}
		extractions.push(extracted);
	}
	let document = json_document(lower_bound_expr(&args[0], policy)?);
	let then = match extractions.len() {
		0 => null(),
		1 => extractions.remove(0),
		_ => non_null(extractions),
	};
	Ok(LogicalExpr::Binary {
		left: Box::new(LogicalExpr::Optional {
			base: Box::new(document),
			then: Box::new(then),
		}),
		op: BinaryOperator::NullCoalescing,
		right: Box::new(null()),
	})
}

enum JsonStep {
	Key(String),
	Index(i64),
}

fn json_path(path: &str) -> Result<Vec<JsonStep>, PlanError> {
	let invalid = || PlanError::UnsupportedExpression(format!("json path {path}"));
	let mut rest = path.strip_prefix('$').ok_or_else(invalid)?;
	let mut steps = Vec::new();
	while !rest.is_empty() {
		if let Some(after) = rest.strip_prefix('.') {
			let (key, after) = if let Some(quoted) = after.strip_prefix('"') {
				let end = quoted.find('"').ok_or_else(invalid)?;
				(&quoted[..end], &quoted[end + 1..])
			} else {
				let end = after.find(['.', '[']).unwrap_or(after.len());
				(&after[..end], &after[end..])
			};
			if key.is_empty() || key == "*" {
				return Err(invalid());
			}
			steps.push(JsonStep::Key(key.to_owned()));
			rest = after;
		} else if let Some(after) = rest.strip_prefix('[') {
			let end = after.find(']').ok_or_else(invalid)?;
			let index = after[..end].trim().parse::<u32>().map_err(|_| invalid())?;
			let index = i64::from(index);
			steps.push(JsonStep::Index(index));
			rest = &after[end + 1..];
		} else {
			return Err(invalid());
		}
	}
	Ok(steps)
}

/// `lpad(s, n[, fill])` and `rpad(s, n[, fill])`: `s` cut to `n` characters, or padded to `n` with
/// the repeated `fill` (a space by default) on the left or the right. A negative length is the
/// empty string in PostgreSQL and NULL in MySQL, and so is an empty fill that would have to pad
/// in MySQL, where PostgreSQL gives `s` unpadded.
fn lower_pad(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let dialect = policy.dialect;
	let left = name == "lpad";
	if dialect == SqlDialect::MySql && args.len() == 2 {
		return Err(PlanError::FunctionArgumentCountMismatch {
			name: name.to_owned(),
			expected: "3".to_owned(),
			got: 2,
		});
	}
	let mut values = lower_args(args, policy)?;
	if values.len() == 2 {
		values.push(text(" "));
	}
	Ok(strict(values, |v| {
		let (s, n, fill) = (&v[0], &v[1], &v[2]);
		let length = len(s);
		let needed = binary(n.clone(), BinaryOperator::Subtract, length.clone());
		let fill_length = len(fill);
		let repeats = LogicalExpr::Prefix {
			op: PrefixOperator::Cast(Kind::Int),
			expr: Box::new(call(
				"math::floor",
				vec![binary(
					binary(
						binary(needed.clone(), BinaryOperator::Add, fill_length.clone()),
						BinaryOperator::Subtract,
						int(1),
					),
					BinaryOperator::Divide,
					max2(fill_length, int(1)),
				)],
			)),
		};
		let pad = call(
			"string::slice",
			vec![call("string::repeat", vec![fill.clone(), repeats]), int(0), needed],
		);
		let padded = if left {
			call("string::concat", vec![pad, s.clone()])
		} else {
			call("string::concat", vec![s.clone(), pad])
		};
		let nothing = match dialect {
			SqlDialect::Postgres => text(""),
			SqlDialect::MySql => null(),
		};
		let unpadded = match dialect {
			SqlDialect::Postgres => s.clone(),
			SqlDialect::MySql => null(),
		};
		when(
			binary(n.clone(), BinaryOperator::LessThan, int(0)),
			nothing,
			when(
				binary(n.clone(), BinaryOperator::LessThanEqual, length),
				call("string::slice", vec![s.clone(), int(0), n.clone()]),
				when(binary(len(fill), BinaryOperator::Equal, int(0)), unpadded, padded),
			),
		)
	}))
}

/// PostgreSQL `translate(s, from, to)` for literal sets: each character of `from` becomes the
/// character of `to` at its position, or goes if `to` is shorter. The characters are replaced one
/// after another, so a set where one replacement is another's source is refused.
fn lower_translate(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let (Some(from), Some(to)) = (string_literal(&args[1]), string_literal(&args[2])) else {
		return Err(PlanError::UnsupportedExpression(
			"translate with character sets that are not string literals".to_owned(),
		));
	};
	let to: Vec<char> = to.chars().collect();
	let mut seen = Vec::new();
	let mut steps = Vec::new();
	for (index, source) in from.chars().enumerate() {
		if seen.contains(&source) {
			continue;
		}
		seen.push(source);
		let target = to.get(index).copied();
		if target != Some(source) {
			steps.push((source, target));
		}
	}
	if steps.iter().any(|(_, target)| target.is_some_and(|target| seen.contains(&target))) {
		return Err(PlanError::UnsupportedExpression(
			"translate where a replacement is the source of another".to_owned(),
		));
	}
	let value = lower_bound_expr(&args[0], policy)?;
	Ok(strict(vec![value], |v| {
		steps.iter().fold(v[0].clone(), |acc, (source, target)| {
			call(
				"string::replace",
				vec![
					acc,
					text(&source.to_string()),
					text(&target.map(|c| c.to_string()).unwrap_or_default()),
				],
			)
		})
	}))
}

/// `array_length(array, dimension)`: the number of elements of a one-dimensional array, NULL for
/// an empty array and for any other dimension.
fn lower_array_length(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| {
		when(
			binary(
				binary(v[1].clone(), BinaryOperator::Equal, int(1)),
				BinaryOperator::And,
				binary(call("array::len", vec![v[0].clone()]), BinaryOperator::MoreThan, int(0)),
			),
			call("array::len", vec![v[0].clone()]),
			null(),
		)
	}))
}

/// `string_to_array(s, delimiter)`: the fields of `s`. An empty `s` has none, an empty delimiter
/// leaves `s` whole, and a NULL delimiter gives the characters.
fn lower_string_to_array(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?;
	let string = values[0].clone();
	let delimiter = values[1].clone();
	Ok(bind(vec![string, delimiter], |v| {
		let (s, d) = (&v[0], &v[1]);
		let characters = call(
			"array::slice",
			vec![
				call("string::split", vec![s.clone(), text("")]),
				int(1),
				binary(call("string::len", vec![s.clone()]), BinaryOperator::Add, int(1)),
			],
		);
		when(
			is_null_expr(s.clone()),
			null(),
			when(
				binary(s.clone(), BinaryOperator::Equal, text("")),
				LogicalExpr::Array(Vec::new()),
				when(
					is_null_expr(d.clone()),
					characters,
					when(
						binary(d.clone(), BinaryOperator::Equal, text("")),
						LogicalExpr::Array(vec![s.clone()]),
						call("string::split", vec![s.clone(), d.clone()]),
					),
				),
			),
		)
	}))
}

/// `quote_literal(s)`, and `quote_nullable(s)` where NULL is the text `NULL`.
fn lower_quote_literal(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let nullable = name == "quote_nullable";
	let value = as_text(policy.dialect, lower_bound_expr(&args[0], policy)?)?;
	Ok(bind(vec![value], |v| {
		let s = &v[0];
		let doubled = |value: LogicalExpr, from: &str, to: &str| {
			call("string::replace", vec![value, text(from), text(to)])
		};
		let quoted = doubled(doubled(s.clone(), "\\", "\\\\"), "'", "''");
		let literal = when(
			call("string::contains", vec![s.clone(), text("\\")]),
			call("string::concat", vec![text("E'"), quoted.clone(), text("'")]),
			call("string::concat", vec![text("'"), quoted, text("'")]),
		);
		when(
			is_null_expr(s.clone()),
			if nullable {
				text("NULL")
			} else {
				null()
			},
			literal,
		)
	}))
}

/// `to_timestamp(seconds)`: the instant that many seconds after the epoch.
fn lower_to_timestamp(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| {
		call(
			"time::from_micros",
			vec![LogicalExpr::Prefix {
				op: PrefixOperator::Cast(Kind::Int),
				expr: Box::new(call(
					"math::round",
					vec![binary(v[0].clone(), BinaryOperator::Multiply, int(1_000_000))],
				)),
			}],
		)
	}))
}

/// MySQL `year(d)`, `month(d)`, `day(d)`, `hour(d)`, `minute(d)`, `second(d)`, `dayofyear(d)`,
/// `dayofweek(d)` (Sunday is 1), `date(d)` and `last_day(d)`, over a date or a datetime.
fn lower_mysql_date_part(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let value = mysql_datetime(lower_bound_expr(&args[0], policy)?);
	Ok(strict(vec![value], |v| {
		let date = v[0].clone();
		let part = |function: &str| call(function, vec![date.clone()]);
		match name {
			"year" => part("time::year"),
			"month" => part("time::month"),
			"day" | "dayofmonth" => part("time::day"),
			"hour" => part("time::hour"),
			"minute" => part("time::minute"),
			"second" => part("time::second"),
			"dayofyear" => part("time::yday"),
			"dayofweek" => binary(
				binary(part("time::wday"), BinaryOperator::Remainder, int(7)),
				BinaryOperator::Add,
				int(1),
			),
			"date" => call("type::date", vec![date]),
			_ => {
				let first = call("time::group", vec![date, text("month")]);
				let some_day_of_next_month = binary(
					first,
					BinaryOperator::Add,
					LogicalExpr::Literal(LogicalLiteral::Duration(
						Duration::from_days(32).expect("32 days"),
					)),
				);
				call(
					"type::date",
					vec![binary(
						call("time::group", vec![some_day_of_next_month, text("month")]),
						BinaryOperator::Subtract,
						one_day(),
					)],
				)
			}
		}
	}))
}

/// MySQL `strcmp(a, b)`, which compares without regard to case as the default collation does.
fn lower_strcmp(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let texts = lower_args(args, policy)?
		.into_iter()
		.map(|value| as_text(policy.dialect, value))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(strict(texts, |v| {
		let lower = |value: &LogicalExpr| call("string::lowercase", vec![value.clone()]);
		let (a, b) = (lower(&v[0]), lower(&v[1]));
		when(
			binary(a.clone(), BinaryOperator::LessThan, b.clone()),
			int(-1),
			when(binary(a, BinaryOperator::MoreThan, b), int(1), int(0)),
		)
	}))
}

/// MySQL `isnull(x)`: 1 for NULL and 0 otherwise.
fn lower_isnull(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let value = lower_bound_expr(&args[0], policy)?;
	Ok(bind(vec![value], |v| when(is_null_expr(v[0].clone()), int(1), int(0))))
}

/// MySQL `find_in_set(s, list)`: the 1-based position of `s` among the comma separated entries of
/// `list`, or 0. The comparison ignores case.
fn lower_find_in_set(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| {
		let lower = |value: &LogicalExpr| call("string::lowercase", vec![value.clone()]);
		let entries = call("string::split", vec![lower(&v[1]), text(",")]);
		let index = call("array::find_index", vec![entries, lower(&v[0])]);
		when(
			binary(
				call("string::contains", vec![v[0].clone(), text(",")]),
				BinaryOperator::Or,
				binary(v[1].clone(), BinaryOperator::Equal, text("")),
			),
			int(0),
			binary(
				binary(index, BinaryOperator::NullCoalescing, int(-1)),
				BinaryOperator::Add,
				int(1),
			),
		)
	}))
}

/// MySQL `timestampdiff(unit, a, b)`: the whole units from `a` to `b`, counted toward zero, for the
/// units of a fixed length.
fn lower_timestampdiff(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let unit = string_literal(&args[0]).unwrap_or_default().to_ascii_uppercase();
	let seconds: i64 = match unit.as_str() {
		"SECOND" => 1,
		"MINUTE" => 60,
		"HOUR" => 3_600,
		"DAY" => 86_400,
		"WEEK" => 604_800,
		_ => {
			return Err(PlanError::UnsupportedExpression(format!(
				"timestampdiff with the unit {unit}"
			)));
		}
	};
	let values = vec![
		mysql_datetime(lower_bound_expr(&args[1], policy)?),
		mysql_datetime(lower_bound_expr(&args[2], policy)?),
	];
	Ok(strict(values, |v| {
		let elapsed = binary(
			call("time::micros", vec![v[1].clone()]),
			BinaryOperator::Subtract,
			call("time::micros", vec![v[0].clone()]),
		);
		let towards_zero = binary(
			call("math::sign", vec![elapsed.clone()]),
			BinaryOperator::Multiply,
			call(
				"math::floor",
				vec![binary(
					call("math::abs", vec![elapsed]),
					BinaryOperator::Divide,
					int(seconds * 1_000_000),
				)],
			),
		);
		LogicalExpr::Prefix {
			op: PrefixOperator::Cast(Kind::Int),
			expr: Box::new(towards_zero),
		}
	}))
}

/// MySQL `json_length(doc[, path])`: the elements of an array, the members of an object, and 1 for
/// any other value.
fn lower_json_length(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let steps = match args.get(1) {
		None => Vec::new(),
		Some(path) => json_path(string_literal(path).ok_or_else(|| {
			PlanError::UnsupportedExpression(
				"json_length with a path that is not a string literal".to_owned(),
			)
		})?)?,
	};
	let mut target = LogicalExpr::Current;
	for step in steps {
		target = match step {
			JsonStep::Key(key) => LogicalExpr::Access {
				base: Box::new(target),
				field: Strand::from(key.as_str()),
			},
			JsonStep::Index(index) => LogicalExpr::Index {
				base: Box::new(target),
				index: Box::new(int(index)),
			},
		};
	}
	let document = json_document(lower_bound_expr(&args[0], policy)?);
	let counted = bind(
		vec![LogicalExpr::Optional {
			base: Box::new(document),
			then: Box::new(target),
		}],
		|v| {
			let value = &v[0];
			when(
				is_null_expr(value.clone()),
				null(),
				when(
					call("type::is_array", vec![value.clone()]),
					call("array::len", vec![value.clone()]),
					when(
						call("type::is_object", vec![value.clone()]),
						call("object::len", vec![value.clone()]),
						int(1),
					),
				),
			)
		},
	);
	if args.len() == 1 {
		// The JSON text `null` is a value of length 1, where SQL NULL has none.
		let raw = lower_bound_expr(&args[0], policy)?;
		return Ok(when(binary(raw, BinaryOperator::Equal, text("null")), int(1), counted));
	}
	Ok(counted)
}

/// A sample `stddev` or `variance`: NULL for a group with fewer than two values, where the engine
/// answers NaN or 0.
fn lower_sample_aggregate(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let engine = if name.starts_with("stddev") {
		"math::stddev"
	} else {
		"math::variance"
	};
	let value = lower_bound_expr(&args[0], policy)?;
	let present = call(
		"count",
		vec![when(
			is_not_null_expr(value.clone()),
			LogicalExpr::Literal(LogicalLiteral::Bool(true)),
			null(),
		)],
	);
	Ok(when(binary(present, BinaryOperator::LessThan, int(2)), null(), call(engine, vec![value])))
}

/// A population `stddev_pop`/`std` or `var_pop` (MySQL's `stddev` and `variance` are these too):
/// NULL for a group with no value, and 0 for a group with one, where the engine's sample
/// statistics answer NaN.
fn lower_population_aggregate(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let value = lower_bound_expr(&args[0], policy)?;
	let count = || {
		call(
			"count",
			vec![when(
				is_not_null_expr(value.clone()),
				LogicalExpr::Literal(LogicalLiteral::Bool(true)),
				null(),
			)],
		)
	};
	let variance = binary(
		binary(
			call("math::variance", vec![value.clone()]),
			BinaryOperator::Multiply,
			binary(count(), BinaryOperator::Subtract, int(1)),
		),
		BinaryOperator::Divide,
		count(),
	);
	let statistic = if name.starts_with("var") || name == "variance" {
		variance
	} else {
		call("math::sqrt", vec![variance])
	};
	Ok(when(
		binary(count(), BinaryOperator::LessThan, int(1)),
		null(),
		when(binary(count(), BinaryOperator::LessThan, int(2)), int(0), statistic),
	))
}

/// MySQL `json_unquote(s)`: text that is a JSON string is the string it holds.
fn lower_json_unquote(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let value = lower_bound_expr(&args[0], policy)?;
	Ok(strict(vec![value], |v| {
		when(
			binary(
				call("type::is_string", vec![v[0].clone()]),
				BinaryOperator::And,
				binary(
					call("string::starts_with", vec![v[0].clone(), text("\"")]),
					BinaryOperator::And,
					call("string::ends_with", vec![v[0].clone(), text("\"")]),
				),
			),
			call("encoding::json::decode", vec![v[0].clone()]),
			v[0].clone(),
		)
	}))
}

/// `value` as text of exactly `width` digits, zero-padded on the left.
fn padded(value: LogicalExpr, width: usize) -> LogicalExpr {
	call(
		"string::slice",
		vec![
			call(
				"string::concat",
				vec![text(&"0".repeat(width)), call("type::string", vec![value])],
			),
			int(-(width as i64)),
		],
	)
}

fn one_day() -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::Duration(Duration::from_days(1).expect("a day")))
}

/// MySQL `date_add(date, amount, 'UNIT')` and `date_sub(date, amount, 'UNIT')`, which the binder
/// makes of `DATE_ADD(date, INTERVAL amount UNIT)` and `date + INTERVAL amount UNIT`. Units from a
/// microsecond to a week add a duration; a month, a quarter and a year move the calendar and keep
/// the day of the month, or the last day of a shorter month.
fn lower_date_add(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let unit = string_literal(&args[2]).unwrap_or_default().to_ascii_uppercase();
	let seconds: Option<(u64, u32)> = match unit.as_str() {
		"MICROSECOND" => Some((0, 1_000)),
		"SECOND" => Some((1, 0)),
		"MINUTE" => Some((60, 0)),
		"HOUR" => Some((3_600, 0)),
		"DAY" => Some((86_400, 0)),
		"WEEK" => Some((604_800, 0)),
		_ => None,
	};
	let months_per_unit = match unit.as_str() {
		"MONTH" => 1,
		"QUARTER" => 3,
		"YEAR" => 12,
		_ => 0,
	};
	if seconds.is_none() && months_per_unit == 0 {
		return Err(PlanError::UnsupportedExpression(format!("the interval unit {unit}")));
	}
	let sign = if name == "date_sub" {
		-1
	} else {
		1
	};
	let calendar_unit = !matches!(unit.as_str(), "MICROSECOND" | "SECOND" | "MINUTE" | "HOUR");
	let values = vec![lower_bound_expr(&args[0], policy)?, lower_bound_expr(&args[1], policy)?];
	Ok(strict(values, |outer| {
		let (given, amount) = (outer[0].clone(), outer[1].clone());
		// The sum is a datetime; MySQL answers a `DATE` with a `DATE` for a unit of a day or more
		// and with a `DATETIME` for a smaller one, and a `DATETIME` with a `DATETIME`.
		let restore_kind = |sum: LogicalExpr| {
			let from_date = if calendar_unit {
				call("type::date", vec![sum.clone()])
			} else {
				call("type::timestamp", vec![sum.clone()])
			};
			when(
				call("type::is_date", vec![given.clone()]),
				from_date,
				when(
					call("type::is_timestamp", vec![given.clone()]),
					call("type::timestamp", vec![sum.clone()]),
					sum,
				),
			)
		};
		restore_kind(bind(vec![mysql_datetime(given.clone()), amount], |v| {
			// MySQL reads a quoted amount, as in INTERVAL '1' DAY, as a number.
			let number = when(
				call("type::is_string", vec![v[1].clone()]),
				call("type::number", vec![v[1].clone()]),
				v[1].clone(),
			);
			let amount = binary(number, BinaryOperator::Multiply, int(sign));
			if let Some((secs, nanos)) = seconds {
				let unit = Duration::from_secs(secs) + Duration::from_nanos(u64::from(nanos));
				let span = |count: LogicalExpr| {
					binary(
						LogicalExpr::Literal(LogicalLiteral::Duration(unit)),
						BinaryOperator::Multiply,
						count,
					)
				};
				return binary(
					binary(v[0].clone(), BinaryOperator::Add, span(max2(amount.clone(), int(0)))),
					BinaryOperator::Subtract,
					span(max2(negate(amount), int(0))),
				);
			}
			let months = binary(amount, BinaryOperator::Multiply, int(months_per_unit));
			let date = v[0].clone();
			let total = binary(
				binary(
					binary(
						call("time::year", vec![date.clone()]),
						BinaryOperator::Multiply,
						int(12),
					),
					BinaryOperator::Add,
					binary(
						call("time::month", vec![date.clone()]),
						BinaryOperator::Subtract,
						int(1),
					),
				),
				BinaryOperator::Add,
				months,
			);
			let year =
				call("math::floor", vec![binary(total.clone(), BinaryOperator::Divide, int(12))]);
			let month = binary(
				binary(
					total,
					BinaryOperator::Subtract,
					binary(year.clone(), BinaryOperator::Multiply, int(12)),
				),
				BinaryOperator::Add,
				int(1),
			);
			let day = call("time::day", vec![date.clone()]);
			bind(vec![date, year, month, day], |a| {
				let (year, month) = (&a[1], &a[2]);
				let december = binary(month.clone(), BinaryOperator::Equal, int(12));
				let next_year = when(
					december.clone(),
					binary(year.clone(), BinaryOperator::Add, int(1)),
					year.clone(),
				);
				let next_month =
					when(december, int(1), binary(month.clone(), BinaryOperator::Add, int(1)));
				let first_of_next = call(
					"type::datetime",
					vec![call(
						"string::concat",
						vec![
							padded(next_year, 4),
							text("-"),
							padded(next_month, 2),
							text("-01T00:00:00Z"),
						],
					)],
				);
				let days_in_month = call(
					"time::day",
					vec![binary(first_of_next, BinaryOperator::Subtract, one_day())],
				);
				bind(
					vec![a[0].clone(), a[1].clone(), a[2].clone(), days_in_month, a[3].clone()],
					|b| {
						let day = call(
							"math::min",
							vec![LogicalExpr::Array(vec![b[4].clone(), b[3].clone()])],
						);
						call(
							"type::datetime",
							vec![call(
								"string::concat",
								vec![
									padded(b[1].clone(), 4),
									text("-"),
									padded(b[2].clone(), 2),
									text("-"),
									padded(day, 2),
									text("T"),
									call("time::format", vec![b[0].clone(), text("%H:%M:%S%.f")]),
									text("Z"),
								],
							)],
						)
					},
				)
			})
		}))
	}))
}

/// The patterns that tell what a datetime written as text still needs to be read by the engine.
const DATE_ONLY: &str = r"^\d{4}-\d{2}-\d{2}$";
const TIME_TO_MINUTES: &str = r"T\d{2}:\d{2}$";
const TIME_WITHOUT_ZONE: &str = r"T\d{2}:\d{2}:\d{2}(\.\d+)?$";
const HOURS_ONLY_ZONE: &str = r"T[0-9:.]+[+-]\d{2}$";

/// The declared PostgreSQL interval type a cast target names when it bounds the value: one with
/// a field qualifier (`interval hour`) or a precision (`interval(2)`). `None` for a plain
/// `interval` and every other target.
pub(crate) fn interval_typmod(target_type: &str) -> Option<DeclaredSqlType> {
	let lower = target_type.to_ascii_lowercase();
	if !lower.starts_with("interval ") && !lower.starts_with("interval(") {
		return None;
	}
	let declared = DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, &lower).ok()?;
	(declared.interval_fields().is_some() || declared.fractional_seconds() != Some(6))
		.then_some(declared)
}

/// `value`, an interval, truncated to the finest field of the declared type and its time rounded
/// to the declared fractional digits, half away from zero, as PostgreSQL's
/// `AdjustIntervalForTypmod` does. Each field is read toward zero, so a negative interval is
/// truncated toward zero too. NULL stays NULL.
pub(crate) fn adjust_interval(value: LogicalExpr, declared: &DeclaredSqlType) -> LogicalExpr {
	let finest = declared.interval_fields().map(|fields| fields.finest());
	let digits = declared.fractional_seconds().unwrap_or(6).min(6);
	strict(vec![value], |v| {
		let v = &v[0];
		let field = |name: &str| call(&format!("interval::{name}"), vec![v.clone()]);
		let make = |args: Vec<LogicalExpr>| call("interval::make", args);
		match finest {
			Some(IntervalFinestField::Year) => make(vec![field("years")]),
			Some(IntervalFinestField::Month) => make(vec![field("years"), field("months")]),
			Some(IntervalFinestField::Day) => {
				make(vec![field("years"), field("months"), int(0), field("days")])
			}
			Some(IntervalFinestField::Hour) => {
				make(vec![field("years"), field("months"), int(0), field("days"), field("hours")])
			}
			Some(IntervalFinestField::Minute) => make(vec![
				field("years"),
				field("months"),
				int(0),
				field("days"),
				field("hours"),
				field("mins"),
			]),
			Some(IntervalFinestField::Second) | None if digits < 6 => {
				let decimal = |value: LogicalExpr| call("type::decimal", vec![value]);
				let scale = int(10i64.pow(digits));
				let scaled = binary(field("secs"), BinaryOperator::Multiply, scale.clone());
				let rounded = call(
					"math::floor",
					vec![binary(
						call("math::abs", vec![scaled.clone()]),
						BinaryOperator::Add,
						decimal(float(0.5)),
					)],
				);
				let secs = binary(
					binary(call("math::sign", vec![scaled]), BinaryOperator::Multiply, rounded),
					BinaryOperator::Divide,
					decimal(scale),
				);
				make(vec![
					field("years"),
					field("months"),
					int(0),
					field("days"),
					field("hours"),
					field("mins"),
					secs,
				])
			}
			Some(IntervalFinestField::Second) | None => v.clone(),
		}
	})
}

/// A datetime as the servers write it (`2024-03-15`, `2024-03-15 10:20:30`, `2024-03-15
/// 10:20:30+02`) in the form the engine reads: `T` between the date and the time, a zone, and a
/// zone of hours and minutes. Text that is none of these is left for the engine to refuse.
pub(crate) fn normalize_datetime_text(text: &str) -> String {
	let text = text.trim().replacen(' ', "T", 1);
	let matches = |pattern: &str| regex::Regex::new(pattern).is_ok_and(|re| re.is_match(&text));
	if matches(DATE_ONLY) {
		format!("{text}T00:00:00Z")
	} else if matches(TIME_TO_MINUTES) {
		format!("{text}:00Z")
	} else if matches(TIME_WITHOUT_ZONE) {
		format!("{text}Z")
	} else if matches(HOURS_ONLY_ZONE) {
		format!("{text}:00")
	} else {
		text
	}
}

/// `value` as the operand of a cast to a datetime: text is normalised as
/// [`normalize_datetime_text`] does, and any other value is left as it is.
pub(crate) fn datetime_cast_operand(value: LogicalExpr) -> LogicalExpr {
	if let LogicalExpr::Literal(LogicalLiteral::String(literal)) = &value {
		return text(&normalize_datetime_text(literal.as_str()));
	}
	let matches = |subject: &LogicalExpr, pattern: &str| {
		call("string::matches", vec![subject.clone(), text(pattern)])
	};
	let normalised = bind(
		vec![call("string::replace", vec![LogicalExpr::Current, text(" "), text("T")])],
		|t| {
			let t = &t[0];
			let append = |suffix: &str| call("string::concat", vec![t.clone(), text(suffix)]);
			when(
				matches(t, DATE_ONLY),
				append("T00:00:00Z"),
				when(
					matches(t, TIME_TO_MINUTES),
					append(":00Z"),
					when(
						matches(t, TIME_WITHOUT_ZONE),
						append("Z"),
						when(matches(t, HOURS_ONLY_ZONE), append(":00"), t.clone()),
					),
				),
			)
		},
	);
	LogicalExpr::Optional {
		base: Box::new(value),
		then: Box::new(when(
			call("type::is_string", vec![LogicalExpr::Current]),
			normalised,
			LogicalExpr::Current,
		)),
	}
}

/// A `timetz` text (`10:20:30+02`) as the UTC time of day it names, wrapped to the day: the offset
/// is applied on input and the value is stored as a `time`. Text with no offset is `None`.
pub(crate) fn timetz_text_to_utc(text: &str) -> Option<String> {
	let re =
		regex::Regex::new(r"^(\d{2}):(\d{2})(?::(\d{2})(\.\d+)?)?\s*([+-])(\d{2})(?::?(\d{2}))?$")
			.ok()?;
	let caps = re.captures(text.trim())?;
	let field = |i: usize| caps.get(i).map_or(Some(0i64), |m| m.as_str().parse().ok());
	let (hour, minute, second) = (field(1)?, field(2)?, field(3)?);
	if hour > 24 || minute > 59 || second > 59 {
		return None;
	}
	let offset = (field(6)? * 60 + field(7)?) * 60;
	let offset = if &caps[5] == "-" {
		offset
	} else {
		-offset
	};
	let seconds = (hour * 3600 + minute * 60 + second + offset).rem_euclid(86_400);
	let fraction = caps.get(4).map_or("", |m| m.as_str());
	Some(format!("{:02}:{:02}:{:02}{fraction}", seconds / 3600, seconds / 60 % 60, seconds % 60))
}

/// The operand of a cast to `time`: a text literal that names a date as well (`2024-03-15
/// 10:20:30`) is read as the timestamp it names, whose time of day is the value. Any other
/// operand is left as it is.
pub(crate) fn time_of_timestamp_text(value: LogicalExpr) -> LogicalExpr {
	if let LogicalExpr::Literal(LogicalLiteral::String(literal)) = &value
		&& regex::Regex::new(r"^\s*\d{4}-\d{2}-\d{2}").is_ok_and(|re| re.is_match(literal.as_str()))
	{
		return LogicalExpr::Prefix {
			op: PrefixOperator::Cast(Kind::Timestamp),
			expr: Box::new(text(&normalize_datetime_text(literal.as_str()))),
		};
	}
	value
}

/// The operand of a cast to `timetz`: a text literal with a zone offset is read as the UTC time of
/// day it names, and any other operand is left as it is.
pub(crate) fn timetz_cast_operand(target: &str, value: LogicalExpr) -> LogicalExpr {
	let base = target.split('(').next().unwrap_or(target).trim();
	if !matches!(base, "timetz" | "time with time zone") {
		return value;
	}
	if let LogicalExpr::Literal(LogicalLiteral::String(literal)) = &value
		&& let Some(utc) = timetz_text_to_utc(literal.as_str())
	{
		return text(&utc);
	}
	value
}

/// The lowering of `name` called with `args`, or `None` when the engine function the registry
/// names does the whole job.
pub(super) fn lower_function(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<Option<LogicalExpr>, PlanError> {
	let lowered = match (name, policy.dialect) {
		_ if dates::handles(name, policy.dialect, args.len()) => dates::lower(name, args, policy)?,
		_ if strings::handles(name) => strings::lower(name, args, policy)?,
		_ if json::handles(name, policy.dialect) => json::lower(name, args, policy)?,
		("substring" | "substr" | "mid", _) => lower_substring(args, policy)?,
		("trim", _) => lower_trim(name, Trim::Both, args, policy)?,
		("ltrim", _) => lower_trim(name, Trim::Leading, args, policy)?,
		("rtrim", _) => lower_trim(name, Trim::Trailing, args, policy)?,
		("concat", _) => lower_concat(args, policy)?,
		("concat_ws", _) => lower_concat_ws(args, policy)?,
		("length", SqlDialect::MySql) => lower_mysql_length(args, policy)?,
		("current_date" | "curdate", _) => call("type::date", vec![call("time::now", Vec::new())]),
		("current_time" | "curtime", SqlDialect::Postgres) => {
			call("type::time", vec![call("time::now", Vec::new())])
		}
		("current_time" | "curtime", _) => {
			call("type::interval", vec![call("type::time", vec![call("time::now", Vec::new())])])
		}
		("mod", _) => lower_mod(args, policy)?,
		("pi", _) => float(std::f64::consts::PI),
		("exp", _) => lower_exp(args, policy)?,
		("ln" | "log", _) => lower_log(name, args, policy)?,
		("truncate", _) => lower_truncate(args, policy)?,
		("round", _) if args.len() == 2 => lower_round(args, policy)?,
		("round", _) if args.len() == 1 => lower_round_whole(args, policy)?,
		("left" | "right", _) => lower_left_right(name, args, policy)?,
		("split_part", _) => lower_split_part(args, policy)?,
		("instr" | "locate" | "position" | "strpos", _) => lower_locate(name, args, policy)?,
		("overlay", _) => lower_overlay(args, policy)?,
		("ascii", _) => lower_ascii(args, policy)?,
		("chr" | "char", _) => lower_chr(name, args)?,
		("date_part" | "extract", _) => lower_date_part(args, policy)?,
		("date_trunc", _) => lower_date_trunc(args, policy)?,
		("isfinite", SqlDialect::Postgres) => lower_isfinite(args, policy)?,
		("date_format", _) => lower_date_format(args, policy)?,
		("date_add" | "date_sub", _) => lower_date_add(name, args, policy)?,
		("from_unixtime", _) => lower_from_unixtime(args, policy)?,
		("unix_timestamp", _) => lower_unix_timestamp(args, policy)?,
		("datediff", _) => lower_datediff(args, policy)?,
		("sha2", _) => lower_sha2(args, policy)?,
		("lpad" | "rpad", _) => lower_pad(name, args, policy)?,
		("btrim", _) => lower_trim(name, Trim::Both, args, policy)?,
		("translate", _) => lower_translate(args, policy)?,
		// Arrays are one-based, so the upper bound of the first dimension is its length.
		("array_length" | "array_upper", _) => lower_array_length(args, policy)?,
		("string_to_array", _) => lower_string_to_array(args, policy)?,
		("quote_literal" | "quote_nullable", _) => lower_quote_literal(name, args, policy)?,
		("to_timestamp", _) => lower_to_timestamp(args, policy)?,
		(
			"year" | "month" | "day" | "dayofmonth" | "hour" | "minute" | "second" | "dayofweek"
			| "dayofyear" | "date" | "last_day",
			_,
		) => lower_mysql_date_part(name, args, policy)?,
		("strcmp", _) => lower_strcmp(args, policy)?,
		("isnull", _) => lower_isnull(args, policy)?,
		("find_in_set", _) => lower_find_in_set(args, policy)?,
		("timestampdiff", _) => lower_timestampdiff(args, policy)?,
		("json_length", _) => lower_json_length(args, policy)?,
		("stddev" | "variance", SqlDialect::MySql) | ("std" | "stddev_pop" | "var_pop", _) => {
			lower_population_aggregate(name, args, policy)?
		}
		("stddev" | "stddev_samp" | "variance" | "var_samp", _) => {
			lower_sample_aggregate(name, args, policy)?
		}
		("pg_typeof", _) => lower_pg_typeof(args, policy)?,
		("to_json" | "to_jsonb", _) => lower_bound_expr(&args[0], policy)?,
		("jsonb_build_object" | "json_build_object" | "json_object", _) => {
			lower_json_object(args, policy)?
		}
		("json_extract", _) => lower_json_extract(args, policy)?,
		("json_unquote", _) => lower_json_unquote(args, policy)?,
		("uuid", _) => call("type::string", vec![call("rand::uuid::v4", Vec::new())]),
		_ => return Ok(None),
	};
	Ok(Some(lowered))
}
