//! Lowering of the JSON and array functions that have no single engine function to call.

use surrealdb_expr::expr::BinaryOperator;
use surrealdb_expr::expr::logical::LogicalExpr;

use super::{
	binary, bind, call, failure, int, is_null_expr, json_document, lower_args, lower_bound_expr,
	null, string_literal, text, when,
};
use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::ir::BoundExpr;
use crate::lower::expr::{absent_as_null, json_path_step, json_text};

/// Whether this module lowers `name` in `dialect`.
pub(super) fn handles(name: &str, dialect: SqlDialect) -> bool {
	match name {
		"jsonb_array_length"
		| "json_array_length"
		| "jsonb_typeof"
		| "json_typeof"
		| "jsonb_build_array"
		| "json_build_array"
		| "jsonb_extract_path"
		| "json_extract_path"
		| "jsonb_extract_path_text"
		| "json_extract_path_text"
		| "array_append"
		| "array_prepend"
		| "array_cat"
		| "array_remove"
		| "array_position"
		| "cardinality" => dialect == SqlDialect::Postgres,
		"json_keys" | "json_type" | "json_quote" => dialect == SqlDialect::MySql,
		_ => false,
	}
}

pub(super) fn lower(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	match name {
		"jsonb_array_length" | "json_array_length" => lower_array_length(args, policy),
		"jsonb_typeof" | "json_typeof" => lower_typeof(args, policy),
		"jsonb_build_array" | "json_build_array" => {
			Ok(LogicalExpr::Array(lower_args(args, policy)?))
		}
		"jsonb_extract_path" | "json_extract_path" => lower_extract_path(false, args, policy),
		"jsonb_extract_path_text" | "json_extract_path_text" => {
			lower_extract_path(true, args, policy)
		}
		"array_append" => lower_array_append(args, policy),
		"array_prepend" => lower_array_prepend(args, policy),
		"array_cat" => lower_array_cat(args, policy),
		"array_remove" => lower_array_remove(args, policy),
		"array_position" => lower_array_position(args, policy),
		"cardinality" => lower_cardinality(args, policy),
		"json_keys" => lower_json_keys(args, policy),
		"json_type" => lower_json_type(args, policy),
		"json_quote" => lower_json_quote(args, policy),
		_ => Err(PlanError::UnsupportedExpression(name.to_owned())),
	}
}

/// A JSON document argument: text written in the statement is parsed, and any other value, which
/// is a document already, is used as it is.
fn document(arg: &BoundExpr, policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let value = lower_bound_expr(arg, policy)?;
	Ok(if string_literal(arg).is_some() {
		json_document(value)
	} else {
		value
	})
}

/// Whether the argument is the JSON text `null` written in the statement.
fn is_json_null_literal(arg: &BoundExpr) -> bool {
	string_literal(arg).is_some_and(|text| text.trim() == "null")
}

/// `jsonb_array_length(doc)`: the elements of an array, and an error for a value that is not one.
fn lower_array_length(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let doc = document(&args[0], policy)?;
	Ok(bind(vec![doc], |v| {
		let doc = &v[0];
		when(
			is_null_expr(doc.clone()),
			null(),
			when(
				call("type::is_array", vec![doc.clone()]),
				call("array::len", vec![doc.clone()]),
				when(
					call("type::is_object", vec![doc.clone()]),
					failure("cannot get array length of a non-array"),
					failure("cannot get array length of a scalar"),
				),
			),
		)
	}))
}

/// The JSON type name of a value: `object`, `array`, `string`, `number`, `boolean` or `null`.
fn json_type_name(value: &LogicalExpr, names: [&str; 6]) -> LogicalExpr {
	let [object, array, string, number, boolean, null_name] = names;
	let is = |function: &str| call(function, vec![value.clone()]);
	when(
		is("type::is_object"),
		text(object),
		when(
			is("type::is_array"),
			text(array),
			when(
				is("type::is_string"),
				text(string),
				when(
					is("type::is_number"),
					text(number),
					when(is("type::is_bool"), text(boolean), text(null_name)),
				),
			),
		),
	)
}

/// `jsonb_typeof(doc)`, NULL for an SQL NULL and `null` for the JSON text `null`.
fn lower_typeof(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	if is_json_null_literal(&args[0]) {
		return Ok(text("null"));
	}
	let doc = document(&args[0], policy)?;
	Ok(bind(vec![doc], |v| {
		when(
			is_null_expr(v[0].clone()),
			null(),
			json_type_name(&v[0], ["object", "array", "string", "number", "boolean", "null"]),
		)
	}))
}

/// `jsonb_extract_path(doc, key, ...)` and, for the text form, `jsonb_extract_path_text`: the
/// value at the path of keys and indexes, NULL where there is none.
fn lower_extract_path(
	as_text: bool,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let steps = args[1..]
		.iter()
		.map(|arg| {
			string_literal(arg).ok_or_else(|| {
				PlanError::UnsupportedExpression(
					"a JSON extract path with a step that is not a string literal".to_owned(),
				)
			})
		})
		.collect::<Result<Vec<_>, _>>()?;
	let base = document(&args[0], policy)?;
	let mut two_sided = 0;
	let place =
		steps.into_iter().fold(base, |base, step| json_path_step(base, step, &mut two_sided));
	Ok(if as_text {
		json_text(place)
	} else {
		absent_as_null(place)
	})
}

/// `array_append(array, element)`: the array with the element last; a NULL array is empty.
fn lower_array_append(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?;
	Ok(bind(values, |v| call("array::push", vec![nullable_array(&v[0]), v[1].clone()])))
}

/// `array_prepend(element, array)`.
fn lower_array_prepend(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?;
	Ok(bind(values, |v| call("array::prepend", vec![nullable_array(&v[1]), v[0].clone()])))
}

/// `array_cat(a, b)`: the elements of `a` and then of `b`; a NULL array is empty, and two NULL
/// arrays are NULL.
fn lower_array_cat(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?;
	Ok(bind(values, |v| {
		when(
			binary(is_null_expr(v[0].clone()), BinaryOperator::And, is_null_expr(v[1].clone())),
			null(),
			call("array::concat", vec![nullable_array(&v[0]), nullable_array(&v[1])]),
		)
	}))
}

/// The array, or the empty array for NULL.
fn nullable_array(value: &LogicalExpr) -> LogicalExpr {
	when(is_null_expr(value.clone()), LogicalExpr::Array(Vec::new()), value.clone())
}

/// `array_remove(array, element)`: the array without the elements equal to `element`.
fn lower_array_remove(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?;
	Ok(bind(values, |v| {
		when(
			is_null_expr(v[0].clone()),
			null(),
			call("array::complement", vec![v[0].clone(), LogicalExpr::Array(vec![v[1].clone()])]),
		)
	}))
}

/// `array_position(array, element)`: the 1-based position of the first equal element, else NULL.
fn lower_array_position(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?;
	Ok(bind(values, |v| {
		when(
			is_null_expr(v[0].clone()),
			null(),
			absent_plus_one(call("array::find_index", vec![v[0].clone(), v[1].clone()])),
		)
	}))
}

/// `index + 1`, NULL when there is no index.
fn absent_plus_one(index: LogicalExpr) -> LogicalExpr {
	bind(vec![index], |v| {
		when(is_null_expr(v[0].clone()), null(), binary(v[0].clone(), BinaryOperator::Add, int(1)))
	})
}

/// `cardinality(array)`: the elements of the array, NULL for NULL.
fn lower_cardinality(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?;
	Ok(bind(values, |v| {
		when(is_null_expr(v[0].clone()), null(), call("array::len", vec![v[0].clone()]))
	}))
}

/// MySQL `json_keys(doc)`: the keys of an object, NULL for any other value.
fn lower_json_keys(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	if args.len() > 1 {
		return Err(PlanError::UnsupportedExpression("json_keys with a path".to_owned()));
	}
	let doc = document(&args[0], policy)?;
	Ok(bind(vec![doc], |v| {
		when(
			call("type::is_object", vec![v[0].clone()]),
			call("object::keys", vec![v[0].clone()]),
			null(),
		)
	}))
}

/// MySQL `json_type(doc)`.
fn lower_json_type(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	if is_json_null_literal(&args[0]) {
		return Ok(text("NULL"));
	}
	let doc = document(&args[0], policy)?;
	Ok(bind(vec![doc], |v| {
		let number =
			when(call("type::is_int", vec![v[0].clone()]), text("INTEGER"), text("DOUBLE"));
		when(
			is_null_expr(v[0].clone()),
			null(),
			when(
				call("type::is_number", vec![v[0].clone()]),
				number,
				json_type_name(&v[0], ["OBJECT", "ARRAY", "STRING", "NUMBER", "BOOLEAN", "NULL"]),
			),
		)
	}))
}

/// MySQL `json_quote(s)`: the text as a JSON string literal.
fn lower_json_quote(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let values = lower_args(args, policy)?;
	Ok(bind(values, |v| {
		when(is_null_expr(v[0].clone()), null(), call("encoding::json::encode", vec![v[0].clone()]))
	}))
}
