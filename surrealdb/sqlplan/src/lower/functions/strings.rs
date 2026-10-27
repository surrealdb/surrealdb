//! Lowering of the byte-length, trigonometric and logarithmic domain, regular-expression and
//! array-text functions, and the boolean aggregates, that have no single engine function to call.

use surrealdb_expr::expr::BinaryOperator;
use surrealdb_expr::expr::logical::{LogicalExpr, LogicalLiteral};

use super::{
	as_text, binary, bind, call, int, is_not_null_expr, is_null_expr, log_domain, lower_args,
	lower_bound_expr, negate, null, regex, strict, string_literal, text, undefined, when,
};
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::BoundExpr;

/// The names this module lowers.
pub(super) fn handles(name: &str) -> bool {
	matches!(
		name,
		"octet_length"
			| "asin" | "acos"
			| "log10" | "log2"
			| "bit_length"
			| "trunc" | "starts_with"
			| "regexp_replace"
			| "array_to_string"
			| "bool_and"
			| "bool_or"
			| "every" | "space"
			| "elt"
	)
}

pub(super) fn lower(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	match name {
		"asin" | "acos" => lower_inverse_trig(name, args, policy),
		"log10" | "log2" => lower_base_log(name, args, policy),
		"octet_length" => byte_length(1, args, policy),
		"bit_length" => byte_length(8, args, policy),
		"trunc" => lower_trunc(args, policy),
		"starts_with" => lower_starts_with(args, policy),
		"regexp_replace" => lower_regexp_replace(args, policy),
		"array_to_string" => lower_array_to_string(args, policy),
		"bool_and" | "every" => lower_boolean_aggregate(true, args, policy),
		"bool_or" => lower_boolean_aggregate(false, args, policy),
		"space" => lower_space(args, policy),
		"elt" => lower_elt(args, policy),
		_ => Err(PlanError::UnsupportedExpression(name.to_owned())),
	}
}

/// `octet_length(s)` and, scaled by `bits_per_byte`, `bit_length(s)`: the bytes of the text.
fn byte_length(
	bits_per_byte: i64,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?
		.into_iter()
		.map(|value| as_text(policy.dialect, value))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(strict(values, |v| {
		let bytes = call("bytes::len", vec![call("type::bytes", vec![v[0].clone()])]);
		if bits_per_byte == 1 {
			bytes
		} else {
			binary(bytes, BinaryOperator::Multiply, int(bits_per_byte))
		}
	}))
}

/// `trunc(x)` and `trunc(x, d)`: `x` toward zero at `d` decimal places.
fn lower_trunc(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let mut values = lower_args(args, policy)?;
	if values.len() == 1 {
		values.push(int(0));
	}
	Ok(super::truncate_values(values))
}

/// `starts_with(s, prefix)`.
fn lower_starts_with(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| {
		call("string::starts_with", vec![v[0].clone(), v[1].clone()])
	}))
}

/// `regexp_replace(s, pattern, replacement, flags)` with the `g` flag, which replaces every match
/// by the replacement text. Without `g` PostgreSQL replaces the first match only, and a
/// replacement may refer to the groups of the match (`\1`); the engine's regular-expression
/// replacement does neither, so those forms are refused rather than answered differently.
fn lower_regexp_replace(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let unsupported =
		|what: &str| PlanError::UnsupportedExpression(format!("regexp_replace {what}"));
	let (Some(pattern), Some(replacement)) = (string_literal(&args[1]), string_literal(&args[2]))
	else {
		return Err(unsupported("with a pattern or replacement that is not a text literal"));
	};
	let flags = match args.get(3) {
		None => "",
		Some(flags) => {
			string_literal(flags).ok_or_else(|| unsupported("with non-literal flags"))?
		}
	};
	if !flags.contains('g') {
		return Err(unsupported("without the g flag, which replaces the first match only"));
	}
	if replacement.contains('\\') {
		return Err(unsupported(
			"with a backslash in the replacement, which escapes or refers to a group",
		));
	}
	// PostgreSQL's `.` matches a newline unless the `n` flag asks for newline-sensitive matching,
	// which also makes `^` and `$` match at lines.
	let mut options = String::from("s");
	for flag in flags.chars() {
		match flag {
			'g' => {}
			'i' => options.push('i'),
			'x' => options.push('x'),
			'n' | 'm' => {
				options.retain(|c| c != 's');
				options.push('m');
			}
			_ => return Err(unsupported("with an unsupported flag")),
		}
	}
	let pattern = regex(&format!("(?{options}){pattern}"))?;
	let subject = as_text(policy.dialect, lower_bound_expr(&args[0], policy)?)?;
	Ok(strict(vec![subject], |v| {
		call("string::replace", vec![v[0].clone(), pattern, text(replacement)])
	}))
}

/// `array_to_string(array, delimiter [, null_text])`: the elements as text joined by the delimiter,
/// leaving NULL elements out unless `null_text` stands for them.
fn lower_array_to_string(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?;
	let dialect = policy.dialect;
	let null_text = values.get(2).cloned();
	let element = as_text(dialect, LogicalExpr::Current)?;
	Ok(bind(vec![values[0].clone(), values[1].clone()], |v| {
		let rendered = match &null_text {
			Some(replacement) => LogicalExpr::Broadcast {
				base: Box::new(v[0].clone()),
				body: Box::new(when(
					is_null_expr(LogicalExpr::Current),
					replacement.clone(),
					element.clone(),
				)),
				flatten: false,
			},
			None => LogicalExpr::Broadcast {
				base: Box::new(LogicalExpr::Filter {
					base: Box::new(v[0].clone()),
					predicate: Box::new(is_not_null_expr(LogicalExpr::Current)),
				}),
				body: Box::new(element.clone()),
				flatten: false,
			},
		};
		when(
			binary(is_null_expr(v[0].clone()), BinaryOperator::Or, is_null_expr(v[1].clone())),
			null(),
			call("array::join", vec![rendered, v[1].clone()]),
		)
	}))
}

/// `bool_and(x)` (`every`) and `bool_or(x)` over a group: NULL for a group with no value, and
/// otherwise whether no value (every value, for `bool_or`) is false (true).
fn lower_boolean_aggregate(
	all: bool,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let value = lower_bound_expr(&args[0], policy)?;
	let flag = |wanted: bool| LogicalExpr::Literal(LogicalLiteral::Bool(wanted));
	let counted = |condition: LogicalExpr| call("count", vec![when(condition, flag(true), null())]);
	let present = counted(is_not_null_expr(value.clone()));
	let decided = if all {
		binary(
			counted(binary(value, BinaryOperator::Equal, flag(false))),
			BinaryOperator::Equal,
			int(0),
		)
	} else {
		binary(
			counted(binary(value, BinaryOperator::Equal, flag(true))),
			BinaryOperator::MoreThan,
			int(0),
		)
	};
	Ok(when(binary(present, BinaryOperator::Equal, int(0)), null(), decided))
}

/// MySQL `space(n)`: `n` spaces, none for a count below one.
fn lower_space(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| {
		when(
			binary(v[0].clone(), BinaryOperator::LessThan, int(1)),
			text(""),
			call("string::repeat", vec![text(" "), v[0].clone()]),
		)
	}))
}

/// MySQL `elt(n, s1, s2, ...)`: the `n`th string, NULL when `n` is NULL or outside them.
fn lower_elt(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?;
	let count = values.len() as i64 - 1;
	let index = values[0].clone();
	let choices = LogicalExpr::Array(values[1..].to_vec());
	Ok(bind(vec![index, choices], move |v| {
		when(
			is_null_expr(v[0].clone()),
			null(),
			when(
				binary(
					binary(v[0].clone(), BinaryOperator::LessThan, int(1)),
					BinaryOperator::Or,
					binary(v[0].clone(), BinaryOperator::MoreThan, int(count)),
				),
				null(),
				call(
					"array::at",
					vec![v[1].clone(), binary(v[0].clone(), BinaryOperator::Subtract, int(1))],
				),
			),
		)
	}))
}

/// `asin(x)` and `acos(x)`, which are an error in PostgreSQL and NULL in MySQL outside `[-1, 1]`.
fn lower_inverse_trig(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let engine = format!("math::{name}");
	let dialect = policy.dialect;
	Ok(strict(lower_args(args, policy)?, |v| {
		let outside = binary(
			binary(v[0].clone(), BinaryOperator::MoreThan, int(1)),
			BinaryOperator::Or,
			binary(v[0].clone(), BinaryOperator::LessThan, negate(int(1))),
		);
		when(
			outside,
			undefined(dialect, "input is out of range"),
			call(&engine, vec![v[0].clone()]),
		)
	}))
}

/// `log10(x)` and MySQL `log2(x)`: undefined for a value that is not positive.
fn lower_base_log(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let engine = format!("math::{name}");
	let dialect = policy.dialect;
	Ok(strict(lower_args(args, policy)?, |v| {
		log_domain(dialect, &v[0], call(&engine, vec![v[0].clone()]))
	}))
}
