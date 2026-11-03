//! Casts between text and the PostgreSQL types whose values have a form of their own: `money`,
//! `pg_lsn`, `inet`, `cidr`, `macaddr`, `macaddr8`, `bit`, `varbit`, the geometric types, and the
//! ranges and multiranges.
//!
//! A cast of a text literal to one of these types is read when the statement is lowered, by the
//! same text reader and type validator the write path applies to a value written to a column of
//! the type, so `'08-00-2B-01-02-03'::macaddr` and a write of that text to a `macaddr` column
//! produce the same canonical value. A cast of such a cast back to `text` is written when the
//! statement is lowered too, as the PostgreSQL text of the value.
//!
//! A cast of a column or any other expression evaluated per row is not read here.

use std::ops::Bound;

use surrealdb_expr::expr::logical::{LogicalExpr, LogicalLiteral, LogicalPostfixOperator};
use surrealdb_expr::expr::statements::define::SqlTypeDialect;
use surrealdb_expr::expr::statements::define::declared_type::{DeclaredSqlType, TypeViolation};
use surrealdb_expr::expr::statements::define::sql_types::text_shaped::network;
use surrealdb_expr::expr::statements::define::sql_types::{self, text_form};
use surrealdb_expr::expr::{BinaryOperator, PrefixOperator};
use surrealdb_expr::val::{Number, Value};
use surrealdb_strand::Strand;

use crate::SqlType;
use crate::error::PlanError;
use crate::ir::{BoundExpr, BoundLiteral};

/// The declared type a cast target names when it is one of the types this module reads: a scalar
/// PostgreSQL type with a family of its own. `None` for every other target.
fn family_target(target_type: &str) -> Option<DeclaredSqlType> {
	let declared = DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, target_type).ok()?;
	if !declared.array_dimensions().is_empty() {
		return None;
	}
	SqlType::of_declared(&declared).map(|_| declared)
}

/// Whether a cast target is one of the types this module reads.
pub(crate) fn is_family_target(target_type: &str) -> bool {
	family_target(target_type).is_some()
}

/// The value a text reads as for the declared type: its text form read, then validated and
/// canonicalised by the type's family.
fn read_value(declared: &DeclaredSqlType, text: &str) -> Result<Value, PlanError> {
	let value = match text_form::read(declared, text) {
		Some(read) => read.map_err(violation)?,
		None => Value::String(Strand::from(text)),
	};
	sql_types::validate(declared, value, "").map_err(violation)
}

fn violation(violation: TypeViolation) -> PlanError {
	if violation.code == "22P02" {
		PlanError::InvalidTextRepresentation(violation.message)
	} else {
		PlanError::DataException(violation.message)
	}
}

/// The lowering of `text::target_type`. `None` when the target is not one of this module's types.
pub(crate) fn lower_text_cast(
	text: &str,
	target_type: &str,
) -> Option<Result<LogicalExpr, PlanError>> {
	let declared = family_target(target_type)?;
	Some(read_value(&declared, text).and_then(|value| {
		value_expr(&value).ok_or_else(|| {
			PlanError::Custom(format!("a {} value has no literal form", declared.base()))
		})
	}))
}

/// The lowering of `operand::text` where `operand` is a text literal cast to one of this module's
/// types: the PostgreSQL text of the value the literal reads as. `None` for any other operand.
pub(crate) fn lower_cast_to_text(
	operand: &BoundExpr,
	target_type: &str,
) -> Option<Result<LogicalExpr, PlanError>> {
	let base = target_type.to_ascii_lowercase();
	let base = base.split('(').next().unwrap_or_default().trim();
	if !matches!(base, "text" | "varchar" | "character varying" | "bpchar" | "char") {
		return None;
	}
	let BoundExpr::Cast {
		expr,
		target_type: inner_type,
	} = operand
	else {
		return None;
	};
	let BoundExpr::Literal(BoundLiteral::String(text)) = expr.as_ref() else {
		return None;
	};
	let declared = family_target(inner_type)?;
	Some(read_value(&declared, text).and_then(|value| {
		let text = match text_form::write(&declared, &value) {
			Some(text) => text,
			// The cast to text writes an `inet` with its prefix, whole or not.
			None if matches!(declared.base(), "inet" | "cidr")
				&& let Value::String(text) = &value
				&& let Ok(parsed) = network::parse(text.as_str()) =>
			{
				parsed.cidr_text()
			}
			None => {
				let Value::String(text) = value else {
					return Err(PlanError::Custom(format!(
						"a {} value has no text form: {value:?}",
						declared.base()
					)));
				};
				text.as_str().to_owned()
			}
		};
		Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(text.as_str()))))
	}))
}

/// The expression that evaluates to `value`, for the values the families store. `None` for a
/// value no family produces.
fn value_expr(value: &Value) -> Option<LogicalExpr> {
	let literal = |literal| Some(LogicalExpr::Literal(literal));
	match value {
		Value::Null => literal(LogicalLiteral::Null),
		Value::None => literal(LogicalLiteral::None),
		Value::Bool(b) => literal(LogicalLiteral::Bool(*b)),
		Value::String(s) => literal(LogicalLiteral::String(s.clone())),
		Value::Number(number) => literal(match number {
			Number::Int(n) => LogicalLiteral::Integer(*n),
			Number::Float(n) => LogicalLiteral::Float(*n),
			Number::Decimal(n) => LogicalLiteral::Decimal(*n),
			Number::BigDecimal(n) => LogicalLiteral::BigDecimal(n.clone()),
			Number::I8(n) => LogicalLiteral::I8(*n),
			Number::U8(n) => LogicalLiteral::U8(*n),
			Number::I16(n) => LogicalLiteral::I16(*n),
			Number::U16(n) => LogicalLiteral::U16(*n),
			Number::I32(n) => LogicalLiteral::I32(*n),
			Number::U32(n) => LogicalLiteral::U32(*n),
			Number::U64(n) => LogicalLiteral::U64(*n),
			Number::F16(n) => LogicalLiteral::F16(*n),
			Number::F32(n) => LogicalLiteral::F32(*n),
		}),
		Value::Datetime(v) => literal(LogicalLiteral::Datetime(*v)),
		Value::Date(v) => literal(LogicalLiteral::Date(*v)),
		Value::Timestamp(v) => literal(LogicalLiteral::Timestamp(*v)),
		Value::Geometry(v) => literal(LogicalLiteral::Geometry(v.clone())),
		Value::Array(items) => {
			Some(LogicalExpr::Array(items.iter().map(value_expr).collect::<Option<Vec<_>>>()?))
		}
		Value::Object(object) => Some(LogicalExpr::Object(
			object
				.iter()
				.map(|(key, value)| Some((key.as_str().to_owned(), value_expr(value)?)))
				.collect::<Option<Vec<_>>>()?,
		)),
		Value::Range(range) => range_expr(&range.start, &range.end),
		Value::Duration(_)
		| Value::Time(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Closure(_) => None,
	}
}

/// The range expression with the two bounds.
fn range_expr(start: &Bound<Value>, end: &Bound<Value>) -> Option<LogicalExpr> {
	let operand = |value: &Value| value_expr(value).map(Box::new);
	Some(match (start, end) {
		(Bound::Unbounded, Bound::Unbounded) => {
			LogicalExpr::Literal(LogicalLiteral::UnboundedRange)
		}
		(Bound::Unbounded, Bound::Excluded(hi)) => LogicalExpr::Prefix {
			op: PrefixOperator::Range,
			expr: operand(hi)?,
		},
		(Bound::Unbounded, Bound::Included(hi)) => LogicalExpr::Prefix {
			op: PrefixOperator::RangeInclusive,
			expr: operand(hi)?,
		},
		(Bound::Included(lo), Bound::Unbounded) => LogicalExpr::Postfix {
			expr: operand(lo)?,
			op: LogicalPostfixOperator::Range,
		},
		(Bound::Excluded(lo), Bound::Unbounded) => LogicalExpr::Postfix {
			expr: operand(lo)?,
			op: LogicalPostfixOperator::RangeSkip,
		},
		(Bound::Included(lo), Bound::Excluded(hi)) => {
			binary(operand(lo)?, BinaryOperator::Range, operand(hi)?)
		}
		(Bound::Included(lo), Bound::Included(hi)) => {
			binary(operand(lo)?, BinaryOperator::RangeInclusive, operand(hi)?)
		}
		(Bound::Excluded(lo), Bound::Excluded(hi)) => {
			binary(operand(lo)?, BinaryOperator::RangeSkip, operand(hi)?)
		}
		(Bound::Excluded(lo), Bound::Included(hi)) => {
			binary(operand(lo)?, BinaryOperator::RangeSkipInclusive, operand(hi)?)
		}
	})
}

fn binary(left: Box<LogicalExpr>, op: BinaryOperator, right: Box<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Binary {
		left,
		op,
		right,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn lowered(text: &str, target: &str) -> LogicalExpr {
		lower_text_cast(text, target).expect("a family target").expect("reads")
	}

	fn as_text(text: &str, target: &str) -> String {
		let operand = BoundExpr::Cast {
			expr: Box::new(BoundExpr::Literal(BoundLiteral::String(text.to_owned()))),
			target_type: target.to_owned(),
		};
		let LogicalExpr::Literal(LogicalLiteral::String(text)) =
			lower_cast_to_text(&operand, "text").expect("a family cast").expect("writes")
		else {
			panic!("a text literal");
		};
		text.as_str().to_owned()
	}

	#[test]
	fn a_text_cast_reads_and_canonicalises_the_value() {
		assert_eq!(
			lowered("08-00-2B-01-02-03", "macaddr"),
			LogicalExpr::Literal(LogicalLiteral::String(Strand::from("08:00:2b:01:02:03")))
		);
		assert_eq!(
			lowered("16/B374D848", "pg_lsn"),
			LogicalExpr::Literal(LogicalLiteral::U64(0x16_B374_D848))
		);
		assert!(matches!(
			lowered("(1,2)", "point"),
			LogicalExpr::Literal(LogicalLiteral::Geometry(_))
		));
		assert!(matches!(
			lowered("[1,5]", "int4range"),
			LogicalExpr::Binary {
				op: BinaryOperator::Range,
				..
			}
		));
	}

	#[test]
	fn other_targets_are_not_read_here() {
		assert!(lower_text_cast("1", "int4").is_none());
		assert!(lower_text_cast("{1}", "int4range[]").is_none());
	}

	#[test]
	fn a_text_that_does_not_read_is_refused() {
		assert!(matches!(
			lower_text_cast("not a mac", "macaddr"),
			Some(Err(PlanError::InvalidTextRepresentation(_)))
		));
		assert!(lower_text_cast("(1,", "point").expect("a family target").is_err());
	}

	#[test]
	fn a_cast_back_to_text_prints_the_postgresql_form() {
		assert_eq!(as_text("1234.5", "money"), "$1,234.50");
		assert_eq!(as_text("[1,5]", "int4range"), "[1,6)");
		assert_eq!(as_text("08-00-2B-01-02-03", "macaddr"), "08:00:2b:01:02:03");
		assert_eq!(as_text("( 1 , 2 )", "point"), "(1,2)");
		assert_eq!(as_text("192.168.1.5/32", "inet"), "192.168.1.5/32");
		assert_eq!(as_text("192.168.1.5", "inet"), "192.168.1.5/32");
	}
}
