//! Expression translation from MySQL AST to SurrealQL.

use std::fmt::Write;

use crate::ast::expr::{Expr, Quantifier, TruthValue};
use crate::ast::literal::Literal;
use crate::ast::operator::{BinaryOperator, UnaryOperator};
use crate::translate::context::TranslationContext;
use crate::translate::error::TranslationError;
use crate::translate::stmt::translate_select;

/// Translates a MySQL expression into SurrealQL.
pub fn translate_expr(
	expr: &Expr,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	match expr {
		Expr::Literal(lit, _) => translate_literal(lit, out),
		Expr::ColumnRef(col) => {
			for (i, field) in col.fields.iter().enumerate() {
				if i > 0 {
					out.push('.');
				}
				if field == "*" {
					out.push('*');
				} else {
					out.push_str(field);
				}
			}
			Ok(())
		}
		Expr::UserVar(v, _) => {
			write!(out, "${v}")?;
			Ok(())
		}
		Expr::SystemVar {
			name,
			..
		} => {
			// System variables mapped to pseudo parameters or internal variables
			write!(out, "$__{name}")?;
			Ok(())
		}
		Expr::Param(idx, _) => {
			write!(out, "$_{idx}")?;
			Ok(())
		}
		Expr::UnaryOp {
			op,
			expr,
			..
		} => {
			match op {
				UnaryOperator::Plus => {
					out.push('+');
					translate_expr(expr, ctx, out)?;
				}
				UnaryOperator::Minus => {
					out.push('-');
					translate_expr(expr, ctx, out)?;
				}
				UnaryOperator::Not => {
					out.push('!');
					translate_expr(expr, ctx, out)?;
				}
				UnaryOperator::BitNot => {
					out.push('~');
					translate_expr(expr, ctx, out)?;
				}
				UnaryOperator::Binary => {
					translate_expr(expr, ctx, out)?;
				}
			}
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
			match op {
				BinaryOperator::Add => out.push('+'),
				BinaryOperator::Sub => out.push('-'),
				BinaryOperator::Mul => out.push('*'),
				BinaryOperator::Div => out.push('/'),
				BinaryOperator::IntDiv => out.push('/'),
				BinaryOperator::Mod => out.push('%'),
				BinaryOperator::BitAnd => out.push('&'),
				BinaryOperator::BitOr => out.push('|'),
				BinaryOperator::BitXor => out.push('^'),
				BinaryOperator::ShiftLeft => out.push_str("<<"),
				BinaryOperator::ShiftRight => out.push_str(">>"),
				BinaryOperator::Equal => out.push_str("=="),
				BinaryOperator::NullSafeEqual => out.push_str("=="),
				BinaryOperator::NotEqual => out.push_str("!="),
				BinaryOperator::Less => out.push('<'),
				BinaryOperator::LessEqual => out.push_str("<="),
				BinaryOperator::Greater => out.push('>'),
				BinaryOperator::GreaterEqual => out.push_str(">="),
				BinaryOperator::Like => out.push('~'),
				BinaryOperator::Regexp | BinaryOperator::Rlike => out.push_str("=~"),
				BinaryOperator::And => out.push_str("AND"),
				BinaryOperator::Or => out.push_str("OR"),
				BinaryOperator::Xor => out.push_str("&&"),
				BinaryOperator::Concat => out.push('+'),
				BinaryOperator::Assign => out.push('='),
			}
			out.push(' ');
			translate_expr(right, ctx, out)?;
			out.push(')');
			Ok(())
		}
		Expr::FunctionCall {
			name,
			distinct,
			args,
			over,
			..
		} => {
			if over.is_some() {
				return Err(TranslationError::Unsupported(
					"OVER clause must be lowered via surrealdb_sqlplan typed plan lowering".into(),
				));
			}
			translate_function_call(name, *distinct, args, ctx, out)
		}
		Expr::Cast {
			expr,
			target_type,
			..
		} => {
			write!(out, "<{target_type}> ")?;
			translate_expr(expr, ctx, out)
		}
		Expr::Case {
			operand,
			branches,
			else_expr,
			..
		} => {
			// In SurrealQL, IF cond THEN val ELSE ... END
			if let Some(op) = operand {
				// Simple CASE op WHEN val THEN ...
				let mut first = true;
				for b in branches {
					if !first {
						out.push_str(" ELSE ");
					}
					first = false;
					out.push_str("IF (");
					translate_expr(op, ctx, out)?;
					out.push_str(" == ");
					translate_expr(&b.when, ctx, out)?;
					out.push_str(") THEN ");
					translate_expr(&b.then, ctx, out)?;
				}
				out.push_str(" ELSE ");
				if let Some(el) = else_expr {
					translate_expr(el, ctx, out)?;
				} else {
					out.push_str("NONE");
				}
				out.push_str(" END");
			} else {
				// Searched CASE WHEN cond THEN ...
				let mut first = true;
				for b in branches {
					if !first {
						out.push_str(" ELSE ");
					}
					first = false;
					out.push_str("IF ");
					translate_expr(&b.when, ctx, out)?;
					out.push_str(" THEN ");
					translate_expr(&b.then, ctx, out)?;
				}
				out.push_str(" ELSE ");
				if let Some(el) = else_expr {
					translate_expr(el, ctx, out)?;
				} else {
					out.push_str("NONE");
				}
				out.push_str(" END");
			}
			Ok(())
		}
		Expr::InList {
			expr,
			negated,
			list,
			..
		} => {
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" NOT IN [");
			} else {
				out.push_str(" IN [");
			}
			for (i, item) in list.iter().enumerate() {
				if i > 0 {
					out.push_str(", ");
				}
				translate_expr(item, ctx, out)?;
			}
			out.push(']');
			Ok(())
		}
		Expr::InSubquery {
			expr,
			negated,
			subquery,
			..
		} => {
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" NOT IN (");
			} else {
				out.push_str(" IN (");
			}
			translate_select(subquery, ctx, out)?;
			out.push(')');
			Ok(())
		}
		Expr::Between {
			expr,
			negated,
			low,
			high,
			..
		} => {
			if *negated {
				out.push_str("!(");
			}
			out.push('(');
			translate_expr(expr, ctx, out)?;
			out.push_str(" >= ");
			translate_expr(low, ctx, out)?;
			out.push_str(" AND ");
			translate_expr(expr, ctx, out)?;
			out.push_str(" <= ");
			translate_expr(high, ctx, out)?;
			out.push(')');
			if *negated {
				out.push(')');
			}
			Ok(())
		}
		Expr::IsNull {
			expr,
			negated,
			..
		} => {
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" != NONE");
			} else {
				out.push_str(" == NONE");
			}
			Ok(())
		}
		Expr::IsTruthValue {
			expr,
			negated,
			value,
			..
		} => {
			translate_expr(expr, ctx, out)?;
			if *negated {
				out.push_str(" != ");
			} else {
				out.push_str(" == ");
			}
			match value {
				TruthValue::True => out.push_str("true"),
				TruthValue::False => out.push_str("false"),
				TruthValue::Unknown => out.push_str("NONE"),
			}
			Ok(())
		}
		Expr::Exists {
			subquery,
			..
		} => {
			out.push_str("EXISTS (");
			translate_select(subquery, ctx, out)?;
			out.push(')');
			Ok(())
		}
		Expr::Subquery {
			subquery,
			..
		} => {
			out.push('(');
			translate_select(subquery, ctx, out)?;
			out.push(')');
			Ok(())
		}
		Expr::Collate {
			expr,
			..
		} => translate_expr(expr, ctx, out),
		Expr::Interval {
			expr,
			unit,
			..
		} => {
			translate_expr(expr, ctx, out)?;
			write!(out, "{unit}")?;
			Ok(())
		}
		Expr::Quantified {
			expr,
			op,
			quantifier,
			subquery,
			..
		} => {
			if *op == BinaryOperator::Equal
				&& matches!(quantifier, Quantifier::Any | Quantifier::Some)
			{
				translate_expr(expr, ctx, out)?;
				out.push_str(" IN (");
				translate_select(subquery, ctx, out)?;
				out.push(')');
				Ok(())
			} else if *op == BinaryOperator::NotEqual && matches!(quantifier, Quantifier::All) {
				translate_expr(expr, ctx, out)?;
				out.push_str(" NOT IN (");
				translate_select(subquery, ctx, out)?;
				out.push(')');
				Ok(())
			} else {
				Err(TranslationError::Unsupported(format!(
					"Quantified comparison with operator {} and quantifier {}",
					op.as_str(),
					quantifier.as_str()
				)))
			}
		}
	}
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
		Literal::Integer(n) => write!(out, "{n}")?,
		Literal::Unsigned(n) => write!(out, "{n}u64")?,
		Literal::Float(fl) => write!(out, "{fl}")?,
		Literal::Decimal(text) => write!(out, "{text}{}", exact_literal_suffix(text))?,
		Literal::String(s) => {
			out.push('\'');
			for ch in s.chars() {
				match ch {
					'\'' => out.push_str("\\'"),
					'\\' => out.push_str("\\\\"),
					_ => out.push(ch),
				}
			}
			out.push('\'');
		}
		Literal::IntroducedString {
			value,
			..
		} => {
			out.push('\'');
			for ch in value.chars() {
				match ch {
					'\'' => out.push_str("\\'"),
					'\\' => out.push_str("\\\\"),
					_ => out.push(ch),
				}
			}
			out.push('\'');
		}
		Literal::HexString(bytes) => {
			out.push_str("0x");
			for b in bytes {
				write!(out, "{:02X}", b)?;
			}
		}
		Literal::IntroducedHexString {
			value,
			..
		} => {
			out.push_str("0x");
			for b in value {
				write!(out, "{:02X}", b)?;
			}
		}
		Literal::BitString(bits) => {
			out.push_str("b'");
			for b in bits {
				write!(out, "{b}")?;
			}
			out.push('\'');
		}
		Literal::Boolean(b) => {
			if *b {
				out.push_str("true");
			} else {
				out.push_str("false");
			}
		}
		Literal::Null => out.push_str("NONE"),
	}
	Ok(())
}

fn translate_function_call(
	name: &str,
	distinct: bool,
	args: &[Expr],
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	let lower = name.to_ascii_lowercase();
	match lower.as_str() {
		"now" | "current_timestamp" => {
			out.push_str("time::now()");
			return Ok(());
		}
		"uuid" => {
			out.push_str("rand::uuid::v4()");
			return Ok(());
		}
		"md5" if args.len() == 1 => {
			out.push_str("crypto::md5(");
			translate_expr(&args[0], ctx, out)?;
			out.push(')');
			return Ok(());
		}
		"sha1" if args.len() == 1 => {
			out.push_str("crypto::sha1(");
			translate_expr(&args[0], ctx, out)?;
			out.push(')');
			return Ok(());
		}
		"sha2" if args.len() == 2 => {
			out.push_str("crypto::sha256(");
			translate_expr(&args[0], ctx, out)?;
			out.push(')');
			return Ok(());
		}
		"lower" if args.len() == 1 => {
			out.push_str("string::lowercase(");
			translate_expr(&args[0], ctx, out)?;
			out.push(')');
			return Ok(());
		}
		"upper" if args.len() == 1 => {
			out.push_str("string::uppercase(");
			translate_expr(&args[0], ctx, out)?;
			out.push(')');
			return Ok(());
		}
		"length" if args.len() == 1 => {
			out.push_str("string::len(");
			translate_expr(&args[0], ctx, out)?;
			out.push(')');
			return Ok(());
		}
		"concat" => {
			out.push_str("string::concat(");
			for (i, a) in args.iter().enumerate() {
				if i > 0 {
					out.push_str(", ");
				}
				translate_expr(a, ctx, out)?;
			}
			out.push(')');
			return Ok(());
		}
		"ifnull" if args.len() == 2 => {
			out.push('(');
			translate_expr(&args[0], ctx, out)?;
			out.push_str(" ?? ");
			translate_expr(&args[1], ctx, out)?;
			out.push(')');
			return Ok(());
		}
		_ => {}
	}

	// Standard function call
	out.push_str(name);
	out.push('(');
	if distinct {
		out.push_str("DISTINCT ");
	}
	for (i, a) in args.iter().enumerate() {
		if i > 0 {
			out.push_str(", ");
		}
		translate_expr(a, ctx, out)?;
	}
	out.push(')');
	Ok(())
}
