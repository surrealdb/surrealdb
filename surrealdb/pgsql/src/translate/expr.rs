//! Expression translation from PostgreSQL to SurrealQL.

use std::fmt::Write;

use crate::ast::*;
use crate::translate::context::{ParamStyle, TranslationContext};
use crate::translate::error::TranslationError;

/// Translates a PostgreSQL expression into its SurrealQL string representation.
pub fn translate_expr(
	expr: &Expr,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	match expr {
		Expr::Literal {
			lit,
			..
		} => translate_literal(lit, out),
		Expr::ColumnRef(col) => translate_column_ref(col, ctx, out),
		Expr::Param {
			id,
			..
		} => {
			match &ctx.param_style {
				ParamStyle::UnderscoreIndexed => {
					write!(out, "${}", crate::translate::context::positional_param_name(*id))?
				}
				ParamStyle::Verbatim => write!(out, "${id}")?,
				ParamStyle::NamedPrefix(p) => write!(out, "${p}{id}")?,
			}
			Ok(())
		}
		Expr::UnaryOp {
			op,
			expr,
			..
		} => {
			match op {
				UnaryOperator::Plus => out.push('+'),
				UnaryOperator::Minus => out.push('-'),
				UnaryOperator::Not => out.push('!'),
				UnaryOperator::BitNot => out.push('~'),
				UnaryOperator::Custom(s) => out.push_str(s),
			}
			out.push('(');
			translate_expr(expr, ctx, out)?;
			out.push(')');
			Ok(())
		}
		Expr::BinaryOp {
			left,
			op,
			right,
			..
		} => {
			out.push('(');
			translate_expr(left, ctx, out)?;
			out.push(' ');
			let op_str = match op {
				BinaryOperator::Add => "+",
				BinaryOperator::Sub => "-",
				BinaryOperator::Mul => "*",
				BinaryOperator::Div => "/",
				BinaryOperator::Mod => "%",
				BinaryOperator::Exp => "**",
				BinaryOperator::And => "AND",
				BinaryOperator::Or => "OR",
				BinaryOperator::Eq => "=",
				BinaryOperator::NotEq => "!=",
				BinaryOperator::Lt => "<",
				BinaryOperator::LtEq => "<=",
				BinaryOperator::Gt => ">",
				BinaryOperator::GtEq => ">=",
				BinaryOperator::Concat => "+",
				BinaryOperator::Like => "~",
				BinaryOperator::ILike => "~*",
				BinaryOperator::NotLike => "!~",
				BinaryOperator::NotILike => "!~*",
				BinaryOperator::Arrow | BinaryOperator::LongArrow => ".",
				BinaryOperator::Contains => "CONTAINS",
				BinaryOperator::ContainedBy => "IN",
				BinaryOperator::BitAnd => "&",
				BinaryOperator::BitOr => "|",
				BinaryOperator::BitXor => "^",
				BinaryOperator::ShiftLeft => "<<",
				BinaryOperator::ShiftRight => ">>",
				BinaryOperator::SimilarTo => "~",
				BinaryOperator::NotSimilarTo => "!~",
				BinaryOperator::RegexMatch => "~",
				BinaryOperator::RegexMatchCi => "~*",
				BinaryOperator::RegexNotMatch => "!~",
				BinaryOperator::RegexNotMatchCi => "!~*",
				BinaryOperator::HashArrow | BinaryOperator::HashLongArrow => ".",
				BinaryOperator::Custom(s) if is_vector_operator(s) => {
					return Err(TranslationError::Unsupported(format!(
						"the {s} vector operator must be lowered via surrealdb_sqlplan"
					)));
				}
				BinaryOperator::Custom(s) => s.as_str(),
			};
			out.push_str(op_str);
			out.push(' ');
			translate_expr(right, ctx, out)?;
			out.push(')');
			Ok(())
		}
		Expr::FunctionCall {
			name,
			args,
			..
		} => translate_func_call(name, args, ctx, out),
		Expr::TypeCast {
			expr,
			data_type,
			..
		} => {
			let cast_type = map_type_to_surreal(&data_type.name);
			write!(out, "<{cast_type}> ")?;
			out.push('(');
			translate_expr(expr, ctx, out)?;
			out.push(')');
			Ok(())
		}
		Expr::IsNull {
			expr,
			negated,
			..
		} => {
			out.push('(');
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" != NONE)");
			} else {
				out.push_str(" = NONE)");
			}
			Ok(())
		}
		Expr::IsTrue {
			expr,
			negated,
			..
		} => {
			out.push('(');
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" != true)");
			} else {
				out.push_str(" = true)");
			}
			Ok(())
		}
		Expr::IsFalse {
			expr,
			negated,
			..
		} => {
			out.push('(');
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" != false)");
			} else {
				out.push_str(" = false)");
			}
			Ok(())
		}
		Expr::IsUnknown {
			expr,
			negated,
			..
		} => {
			out.push('(');
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" != NONE)");
			} else {
				out.push_str(" = NONE)");
			}
			Ok(())
		}
		Expr::IsDistinctFrom {
			left,
			right,
			negated,
			..
		} => {
			out.push('(');
			translate_expr(left, ctx, out)?;
			if *negated {
				out.push_str(" = ");
			} else {
				out.push_str(" != ");
			}
			translate_expr(right, ctx, out)?;
			out.push(')');
			Ok(())
		}
		Expr::Between {
			expr,
			low,
			high,
			negated,
			..
		} => {
			out.push('(');
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" NOT BETWEEN ");
			} else {
				out.push_str(" BETWEEN ");
			}
			translate_expr(low, ctx, out)?;
			out.push_str(" AND ");
			translate_expr(high, ctx, out)?;
			out.push(')');
			Ok(())
		}
		Expr::InList {
			expr,
			list,
			negated,
			..
		} => {
			out.push('(');
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" NOT IN [");
			} else {
				out.push_str(" IN [");
			}
			let mut first = true;
			for item in list {
				if !first {
					out.push_str(", ");
				}
				first = false;
				translate_expr(item, ctx, out)?;
			}
			out.push_str("])");
			Ok(())
		}
		Expr::Case {
			operand,
			branches,
			else_expr,
			..
		} => {
			out.push_str("(IF ");
			let mut first = true;
			for branch in branches {
				if !first {
					out.push_str(" ELSE IF ");
				}
				first = false;
				if let Some(op) = operand {
					translate_expr(op, ctx, out)?;
					out.push_str(" = ");
				}
				translate_expr(&branch.when, ctx, out)?;
				out.push_str(" THEN ");
				translate_expr(&branch.then, ctx, out)?;
			}
			if let Some(def) = else_expr {
				out.push_str(" ELSE ");
				translate_expr(def, ctx, out)?;
			}
			out.push_str(" END)");
			Ok(())
		}
		Expr::ArrayConstructor {
			elements,
			..
		} => {
			out.push('[');
			let mut first = true;
			for elem in elements {
				if !first {
					out.push_str(", ");
				}
				first = false;
				translate_expr(elem, ctx, out)?;
			}
			out.push(']');
			Ok(())
		}
		Expr::Row {
			fields,
			..
		} => {
			out.push('[');
			let mut first = true;
			for field in fields {
				if !first {
					out.push_str(", ");
				}
				first = false;
				translate_expr(field, ctx, out)?;
			}
			out.push(']');
			Ok(())
		}
		Expr::Subscript {
			expr,
			index,
			..
		} => {
			translate_expr(expr, ctx, out)?;
			out.push('[');
			translate_expr(index, ctx, out)?;
			out.push(']');
			Ok(())
		}
		Expr::Slice {
			expr,
			lower,
			upper,
			..
		} => {
			translate_expr(expr, ctx, out)?;
			out.push('[');
			if let Some(l) = lower {
				translate_expr(l, ctx, out)?;
			}
			out.push_str("..");
			if let Some(u) = upper {
				translate_expr(u, ctx, out)?;
			}
			out.push(']');
			Ok(())
		}
		Expr::Collate {
			expr,
			..
		} => translate_expr(expr, ctx, out),
		Expr::Grouping {
			expr,
			..
		} => {
			out.push('(');
			translate_expr(expr, ctx, out)?;
			out.push(')');
			Ok(())
		}
		Expr::FieldSelect {
			expr,
			field,
			..
		} => {
			translate_expr(expr, ctx, out)?;
			out.push('.');
			if is_identifier_safe(field) {
				out.push_str(field);
			} else {
				out.push('`');
				out.push_str(field);
				out.push('`');
			}
			Ok(())
		}
		Expr::IsBound {
			expr,
			negated,
			..
		} => {
			out.push('(');
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" IS NONE)");
			} else {
				out.push_str(" IS NOT NONE)");
			}
			Ok(())
		}
		Expr::JsonObject {
			fields,
			..
		} => {
			out.push('{');
			for (i, (k, v)) in fields.iter().enumerate() {
				if i > 0 {
					out.push_str(", ");
				}
				translate_expr(k, ctx, out)?;
				out.push_str(": ");
				translate_expr(v, ctx, out)?;
			}
			out.push('}');
			Ok(())
		}
		Expr::JsonArray {
			elements,
			..
		} => {
			out.push('[');
			for (i, el) in elements.iter().enumerate() {
				if i > 0 {
					out.push_str(", ");
				}
				translate_expr(el, ctx, out)?;
			}
			out.push(']');
			Ok(())
		}
		Expr::JsonExists {
			context,
			path,
			..
		} => {
			out.push('(');
			translate_expr(context, ctx, out)?;
			let clean_path = path.trim_start_matches("$.");
			if !clean_path.is_empty() {
				out.push('.');
				out.push_str(clean_path);
			}
			out.push_str(" != NONE)");
			Ok(())
		}
		Expr::JsonValue {
			context,
			path,
			..
		} => {
			translate_expr(context, ctx, out)?;
			let clean_path = path.trim_start_matches("$.");
			if !clean_path.is_empty() {
				out.push('.');
				out.push_str(clean_path);
			}
			Ok(())
		}
		Expr::JsonQuery {
			context,
			path,
			..
		} => {
			translate_expr(context, ctx, out)?;
			let clean_path = path.trim_start_matches("$.");
			if !clean_path.is_empty() {
				out.push('.');
				out.push_str(clean_path);
			}
			Ok(())
		}
		Expr::Subquery {
			..
		}
		| Expr::Exists {
			..
		}
		| Expr::InSubquery {
			..
		}
		| Expr::ArraySubquery {
			..
		}
		| Expr::Quantified {
			..
		}
		| Expr::QuantifiedArray {
			..
		} => Err(TranslationError::Unsupported("Subquery expressions".to_string())),
	}
}

/// The digits of PostgreSQL numeric constant `text` in positional form, with an exponent such as
/// `1.5e3` applied to the decimal point (`1500`). A constant whose significant digits would run
/// past the numeric type's limits, 131072 before the point and 16383 after it, is refused.
fn positional_numeric(text: &str) -> Result<String, TranslationError> {
	let Some((mantissa, exponent)) = text.split_once(['e', 'E']) else {
		return Ok(text.to_owned());
	};
	let out_of_range =
		|| TranslationError::Unsupported(format!("numeric constant out of range: {text}"));
	if !crate::parser::expr::numeric_in_range(text) {
		return Err(out_of_range());
	}
	let exponent: i64 = exponent.parse().map_err(|_| out_of_range())?;
	let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
	let digits = format!("{integer}{fraction}");
	let point = i64::try_from(integer.len()).map_err(|_| out_of_range())?.saturating_add(exponent);
	let digit_count = i64::try_from(digits.len()).map_err(|_| out_of_range())?;
	let positional = if point <= 0 {
		let zeros = usize::try_from(-point).map_err(|_| out_of_range())?;
		format!("0.{}{digits}", "0".repeat(zeros))
	} else if point >= digit_count {
		let zeros = usize::try_from(point - digit_count).map_err(|_| out_of_range())?;
		format!("{digits}{}", "0".repeat(zeros))
	} else {
		let point = usize::try_from(point).map_err(|_| out_of_range())?;
		let (integer, fraction) = (
			digits.get(..point).ok_or_else(out_of_range)?,
			digits.get(point..).ok_or_else(out_of_range)?,
		);
		format!("{integer}.{fraction}")
	};
	Ok(positional)
}

/// The suffix of a SurrealQL literal for the exact positional number `text` (digits with at most
/// one point, no sign): `dec` when the fixed decimal type holds it exactly (at most 28 fraction
/// digits once trailing zeros are dropped, and a coefficient within 96 bits), otherwise `bigdec`.
fn exact_literal_suffix(text: &str) -> &'static str {
	const MAX_SCALE: usize = 28;
	const MAX_COEFFICIENT: &str = "79228162514264337593543950335";
	let (integer, fraction) = text.split_once('.').unwrap_or((text, ""));
	let fraction = fraction.trim_end_matches('0');
	let coefficient = format!("{integer}{fraction}");
	let coefficient = coefficient.trim_start_matches('0');
	let fits = fraction.len() <= MAX_SCALE
		&& (coefficient.len() < MAX_COEFFICIENT.len()
			|| (coefficient.len() == MAX_COEFFICIENT.len() && coefficient <= MAX_COEFFICIENT));
	if fits {
		"dec"
	} else {
		"bigdec"
	}
}

fn translate_literal(lit: &Literal, out: &mut String) -> Result<(), TranslationError> {
	match lit {
		Literal::Integer(i) => write!(out, "{i}")?,
		Literal::Float(f) => write!(out, "{f}")?,
		// A numeric constant is exact, as a SurrealQL decimal is. Exponent form is written out in
		// positional digits, which the decimal suffixes take.
		Literal::Numeric(text) => {
			let positional = positional_numeric(text)?;
			write!(out, "{positional}{}", exact_literal_suffix(&positional))?
		}
		Literal::String(s) => {
			out.push('\'');
			for c in s.chars() {
				if c == '\'' {
					out.push_str("\\'");
				} else {
					out.push(c);
				}
			}
			out.push('\'');
		}
		Literal::Boolean(b) => {
			out.push_str(if *b {
				"true"
			} else {
				"false"
			});
		}
		Literal::Null => out.push_str("NONE"),
		Literal::BitString {
			value,
			..
		} => {
			write!(out, "'{value}'")?;
		}
	}
	Ok(())
}

fn translate_column_ref(
	col: &ColumnRef,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	if ctx.map_pg_catalog
		&& let Some(name) = col.fields.last()
	{
		if name.eq_ignore_ascii_case("current_database") {
			out.push_str("db()");
			return Ok(());
		}
		if name.eq_ignore_ascii_case("current_schema") {
			out.push_str("'public'");
			return Ok(());
		}
	}

	// SurrealQL reads `t.*` as the elements of a field `t`, not as the columns of a relation.
	if let [qualifier @ .., star] = col.fields.as_slice()
		&& !qualifier.is_empty()
		&& star == "*"
	{
		return Err(TranslationError::Unsupported(format!(
			"{}.* selects the columns of a relation",
			qualifier.join(".")
		)));
	}

	let mut first = true;
	for field in &col.fields {
		if !first {
			out.push('.');
		}
		first = false;
		if field == "*" {
			out.push('*');
		} else if is_identifier_safe(field) {
			out.push_str(field);
		} else {
			out.push('`');
			out.push_str(field);
			out.push('`');
		}
	}
	Ok(())
}

fn translate_func_call(
	name: &QualifiedName,
	args: &[Expr],
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	let fn_name = match name.parts.last() {
		Some(p) => p.to_ascii_lowercase(),
		None => {
			return Err(TranslationError::Invalid("Empty function name".to_string()));
		}
	};

	if fn_name.contains("::") {
		return Err(TranslationError::Unsupported(format!(
			"Function '{fn_name}' contains illegal namespace separator"
		)));
	}

	if ctx.map_pg_catalog {
		match fn_name.as_str() {
			"version" => {
				out.push_str("'PostgreSQL 16.0 (SurrealDB)'");
				return Ok(());
			}
			"current_database" => {
				out.push_str("db()");
				return Ok(());
			}
			"current_schema" => {
				out.push_str("'public'");
				return Ok(());
			}
			"current_schemas" => {
				out.push_str("['pg_catalog', 'public']");
				return Ok(());
			}
			"current_user" | "session_user" | "user" => {
				out.push_str("'root'");
				return Ok(());
			}
			"pg_backend_pid" => {
				write!(out, "{}", std::process::id())?;
				return Ok(());
			}
			"pg_is_in_recovery" => {
				out.push_str("false");
				return Ok(());
			}
			"pg_get_userbyid" => {
				out.push_str("'root'");
				return Ok(());
			}
			"pg_table_is_visible"
			| "pg_type_is_visible"
			| "pg_function_is_visible"
			| "has_schema_privilege"
			| "has_table_privilege"
			| "has_database_privilege"
			| "has_column_privilege" => {
				out.push_str("true");
				return Ok(());
			}
			"current_setting" => {
				if args.is_empty() || args.len() > 2 {
					return Err(TranslationError::Invalid(
						"current_setting requires 1 or 2 arguments".to_string(),
					));
				}
				if let Some(Expr::Literal {
					lit: crate::ast::Literal::String(s),
					..
				}) = args.first()
				{
					let val = match s.to_ascii_lowercase().as_str() {
						"timezone" | "time zone" => "UTC",
						"server_version" => "16.0-surrealdb",
						"server_encoding" => "UTF8",
						"client_encoding" => "UTF8",
						"datestyle" => "ISO, MDY",
						"integer_datetimes" => "on",
						"standard_conforming_strings" => "on",
						"application_name" => "",
						"search_path" => "\"$user\", public",
						"extra_float_digits" => "1",
						"bytea_output" => "hex",
						"intervalstyle" => "postgres",
						"is_superuser" => "on",
						"session_authorization" => "root",
						other => {
							if args.len() == 2 {
								out.push_str("NONE");
								return Ok(());
							} else {
								return Err(TranslationError::Invalid(format!(
									"unrecognized configuration parameter \"{other}\""
								)));
							}
						}
					};
					write!(out, "'{val}'")?;
					return Ok(());
				}
			}
			_ => {}
		}
	}

	let mapped_fn = match fn_name.as_str() {
		"now" | "current_timestamp" => "time::now",
		"length" | "char_length" | "character_length" => "string::len",
		"lower" => "string::lowercase",
		"upper" => "string::uppercase",
		"trim" => "string::trim",
		"concat" => "string::concat",
		"coalesce" => "type::coalesce",
		"sum" => "math::sum",
		"avg" => "math::mean",
		"min" => "math::min",
		"max" => "math::max",
		"abs" => "math::abs",
		"round" => "math::round",
		"floor" => "math::floor",
		"ceil" | "ceiling" => "math::ceil",
		"count" => "count",
		_ => fn_name.as_str(),
	};

	out.push_str(mapped_fn);
	out.push('(');
	let mut first = true;
	for arg in args {
		if !first {
			out.push_str(", ");
		}
		first = false;
		translate_expr(arg, ctx, out)?;
	}
	out.push(')');
	Ok(())
}

fn map_type_to_surreal(name: &QualifiedName) -> &'static str {
	let last = match name.parts.last() {
		Some(s) => s.to_ascii_lowercase(),
		None => return "string",
	};

	match last.as_str() {
		"int" | "integer" | "int2" | "int4" | "int8" | "smallint" | "bigint" => "int",
		"float" | "float4" | "float8" | "real" | "double precision" | "numeric" | "decimal" => {
			"float"
		}
		"text" | "varchar" | "char" | "bpchar" | "string" => "string",
		"bool" | "boolean" => "bool",
		"date" | "time" | "timestamp" | "timestamptz" => "datetime",
		"json" | "jsonb" => "object",
		"bytea" => "bytes",
		"uuid" => "uuid",
		_ => "string",
	}
}

fn is_identifier_safe(s: &str) -> bool {
	if s.is_empty() {
		return false;
	}
	let mut chars = s.chars();
	if let Some(first) = chars.next()
		&& !first.is_ascii_alphabetic()
		&& first != '_'
	{
		return false;
	}
	for c in chars {
		if !c.is_ascii_alphanumeric() && c != '_' {
			return false;
		}
	}
	true
}

/// Whether `op` is one of the pgvector distance operators, which SurrealQL spells differently (the
/// same text is a graph traversal there).
fn is_vector_operator(op: &str) -> bool {
	matches!(op, "<->" | "<=>" | "<#>" | "<+>")
}
