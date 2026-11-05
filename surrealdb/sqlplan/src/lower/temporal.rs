//! The implicit conversions SQL applies between date and time operands: PostgreSQL promotes a
//! `date` to a `timestamp` to a `timestamptz` before comparing or subtracting them, and reads a
//! quoted literal beside a date, time, timestamp or interval as that type. SurrealQL operators do
//! not promote, so the conversions are written into the lowered expression as explicit casts.

use crate::descriptor::SqlType;
use crate::dialect::SqlDialect;
use crate::functions::FunctionRegistry;
use crate::ir::{BoundBinaryOp, BoundExpr, BoundLiteral, BoundUnaryOp};

/// The type an expression has where it can be told without running it: a declared column, a cast,
/// a registered function with arguments of known types, and arithmetic over those.
pub(crate) fn static_type(expr: &BoundExpr, dialect: SqlDialect) -> SqlType {
	match expr {
		BoundExpr::Column(col) => col.declared_type,
		BoundExpr::Cast {
			target_type,
			..
		} => cast_type(target_type, dialect),
		BoundExpr::FunctionCall {
			name,
			args,
			..
		} => {
			let types: Vec<Option<SqlType>> = args
				.iter()
				.map(|arg| {
					if matches!(arg, BoundExpr::Literal(BoundLiteral::Null)) {
						None
					} else {
						Some(static_type(arg, dialect))
					}
				})
				.collect();
			FunctionRegistry::infer_return_type_of_call_in(name, dialect, &types)
		}
		BoundExpr::Binary {
			left,
			op,
			right,
		} => SqlType::temporal_arithmetic(
			static_type(left, dialect),
			*op,
			static_type(right, dialect),
		)
		.unwrap_or(SqlType::Unknown),
		BoundExpr::Unary {
			op: BoundUnaryOp::Negate | BoundUnaryOp::Positive,
			expr,
		} => static_type(expr, dialect),
		BoundExpr::Literal(BoundLiteral::Numeric(_)) => SqlType::Numeric,
		BoundExpr::Literal(_)
		| BoundExpr::Access {
			..
		}
		| BoundExpr::Param {
			..
		}
		| BoundExpr::Unary {
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
		} => SqlType::Unknown,
	}
}

/// The type a cast to the SQL type `target` produces. MySQL `TIME` is an interval and
/// `DATETIME` a timestamp.
pub(crate) fn cast_type(target: &str, dialect: SqlDialect) -> SqlType {
	let lower = target.to_ascii_lowercase();
	let base = lower.split('(').next().unwrap_or(&lower).trim();
	if dialect == SqlDialect::MySql {
		match base {
			"time" => return SqlType::Interval,
			"datetime" => return SqlType::Timestamp,
			"timestamp" => return SqlType::TimestampTz,
			_ => {}
		}
	}
	SqlType::from_name(base)
}

/// The SQL type name a cast to `ty` is written with in `dialect`.
pub(crate) fn cast_name(ty: SqlType, dialect: SqlDialect) -> &'static str {
	match (ty, dialect) {
		(SqlType::Date, _) => "date",
		(SqlType::Time | SqlType::Timetz, SqlDialect::Postgres) => "time",
		(SqlType::Timestamp, SqlDialect::Postgres) => "timestamp",
		(SqlType::TimestampTz, SqlDialect::Postgres) => "timestamptz",
		(SqlType::Interval, SqlDialect::Postgres) => "interval",
		(SqlType::Time | SqlType::Timetz | SqlType::Interval, SqlDialect::MySql) => "time",
		(SqlType::Timestamp, SqlDialect::MySql) => "datetime",
		(SqlType::TimestampTz, SqlDialect::MySql) => "timestamp",
		_ => "",
	}
}

/// Where the type of an operand is the instant types' rank: a date is the narrowest.
fn instant_rank(ty: SqlType) -> Option<u8> {
	if ty == SqlType::Date {
		Some(0)
	} else if ty == SqlType::Timestamp {
		Some(1)
	} else if ty == SqlType::TimestampTz {
		Some(2)
	} else {
		None
	}
}

fn cast(expr: &BoundExpr, ty: SqlType, dialect: SqlDialect) -> BoundExpr {
	BoundExpr::Cast {
		expr: Box::new(expr.clone()),
		target_type: cast_name(ty, dialect).to_owned(),
	}
}

fn is_text_literal(expr: &BoundExpr) -> bool {
	matches!(expr, BoundExpr::Literal(BoundLiteral::String(_)))
}

/// The operands of `left op right` with the conversions the dialect applies written as casts, or
/// `None` when it applies none.
///
/// - Comparing dates and timestamps of different widths casts the narrower to the wider.
/// - Comparing a text literal with a date, time, timestamp or interval casts the literal to that
///   type.
/// - Adding or subtracting a text literal to or from a date, time or timestamp casts the literal to
///   an interval, and subtracting dates and timestamps of different widths promotes the narrower.
pub(crate) fn promoted_operands(
	left: &BoundExpr,
	op: BoundBinaryOp,
	right: &BoundExpr,
	dialect: SqlDialect,
) -> Option<(BoundExpr, BoundExpr)> {
	let comparison = matches!(
		op,
		BoundBinaryOp::Equal
			| BoundBinaryOp::NotEqual
			| BoundBinaryOp::LessThan
			| BoundBinaryOp::LessThanEqual
			| BoundBinaryOp::GreaterThan
			| BoundBinaryOp::GreaterThanEqual
			| BoundBinaryOp::NullSafeEqual
			| BoundBinaryOp::DistinctFrom
	);
	let arithmetic = matches!(op, BoundBinaryOp::Add | BoundBinaryOp::Subtract);
	if !comparison && !arithmetic {
		return None;
	}
	let (lt, rt) = (static_type(left, dialect), static_type(right, dialect));
	if !lt.is_temporal() && !rt.is_temporal() {
		return None;
	}
	if let (Some(a), Some(b)) = (instant_rank(lt), instant_rank(rt))
		&& a != b
		&& (comparison || op == BoundBinaryOp::Subtract)
	{
		return Some(if a < b {
			(cast(left, rt, dialect), right.clone())
		} else {
			(left.clone(), cast(right, lt, dialect))
		});
	}
	if comparison {
		if lt.is_temporal() && is_text_literal(right) {
			return Some((left.clone(), cast(right, lt, dialect)));
		}
		if rt.is_temporal() && is_text_literal(left) {
			return Some((cast(left, rt, dialect), right.clone()));
		}
	} else if dialect == SqlDialect::Postgres {
		if lt.is_temporal() && is_text_literal(right) {
			return Some((left.clone(), cast(right, SqlType::Interval, dialect)));
		}
		if rt.is_temporal() && is_text_literal(left) {
			return Some((cast(left, SqlType::Interval, dialect), right.clone()));
		}
	}
	None
}

/// The operands of a test that compares a value with several others at once (`BETWEEN`, `IN`)
/// with the conversions the dialect applies written as casts, or `None` when it applies none.
///
/// - Operands that are dates and timestamps of different widths are cast to the widest.
/// - A text literal among them is cast to the temporal type of the other operands: the widest
///   instant type if there is one, else the first date, time or interval type.
pub(crate) fn promoted_group(
	operands: &[&BoundExpr],
	dialect: SqlDialect,
) -> Option<Vec<BoundExpr>> {
	let types: Vec<SqlType> = operands.iter().map(|expr| static_type(expr, dialect)).collect();
	let widest = types
		.iter()
		.copied()
		.filter(|ty| instant_rank(*ty).is_some())
		.max_by_key(|ty| instant_rank(*ty));
	let literal_type = widest.or_else(|| types.iter().copied().find(|ty| ty.is_temporal()))?;
	let mut changed = false;
	let promoted = operands
		.iter()
		.zip(&types)
		.map(|(expr, ty)| {
			let target = if is_text_literal(expr) {
				Some(literal_type)
			} else {
				match (widest, instant_rank(*ty)) {
					(Some(wide), Some(rank)) if instant_rank(wide) > Some(rank) => Some(wide),
					_ => None,
				}
			};
			match target.filter(|target| !cast_name(*target, dialect).is_empty()) {
				Some(target) => {
					changed = true;
					cast(expr, target, dialect)
				}
				None => (*expr).clone(),
			}
		})
		.collect();
	changed.then_some(promoted)
}

/// The operand at `index` of the promoted operands [`promoted_group`] answered, or `original`
/// when it answered none.
pub(crate) fn operand_at<'a>(
	promoted: &'a Option<Vec<BoundExpr>>,
	index: usize,
	original: &'a BoundExpr,
) -> &'a BoundExpr {
	promoted.as_ref().map_or(original, |operands| &operands[index])
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::lower::expr::cast_kind;
	use crate::lower::functions::timetz_text_to_utc;

	fn cast_to(target: &str) -> BoundExpr {
		BoundExpr::Cast {
			expr: Box::new(BoundExpr::Literal(BoundLiteral::String("x".to_owned()))),
			target_type: target.to_owned(),
		}
	}

	fn text(value: &str) -> BoundExpr {
		BoundExpr::Literal(BoundLiteral::String(value.to_owned()))
	}

	#[test]
	fn arithmetic_over_the_temporal_types_has_the_type_postgresql_gives() {
		use SqlType::*;
		let add = BoundBinaryOp::Add;
		let sub = BoundBinaryOp::Subtract;
		let cases = [
			(Date, add, Int4, Some(Date)),
			(Date, sub, Date, Some(Int4)),
			(Date, add, Interval, Some(Timestamp)),
			(Date, add, Time, Some(Timestamp)),
			(Time, sub, Time, Some(Interval)),
			(Time, add, Interval, Some(Time)),
			(Timestamp, sub, Timestamp, Some(Interval)),
			(Timestamp, add, Interval, Some(Timestamp)),
			(TimestampTz, add, Interval, Some(TimestampTz)),
			(TimestampTz, sub, Timestamp, Some(Interval)),
			(Interval, add, Interval, Some(Interval)),
			(Interval, BoundBinaryOp::Multiply, Float8, Some(Interval)),
			(Interval, BoundBinaryOp::Divide, Int4, Some(Interval)),
			(Timestamp, add, Text, Some(Timestamp)),
			(Int4, add, Int4, None),
			(Date, BoundBinaryOp::Multiply, Int4, None),
		];
		for (left, op, right, want) in cases {
			assert_eq!(
				SqlType::temporal_arithmetic(left, op, right),
				want,
				"{left:?} {op:?} {right:?}"
			);
		}
	}

	#[test]
	fn a_date_beside_a_timestamp_is_promoted_to_it() {
		let pg = SqlDialect::Postgres;
		let (l, r) =
			promoted_operands(&cast_to("date"), BoundBinaryOp::Equal, &cast_to("timestamp"), pg)
				.expect("a promotion");
		assert_eq!(static_type(&l, pg), SqlType::Timestamp);
		assert_eq!(static_type(&r, pg), SqlType::Timestamp);
		let (l, _) = promoted_operands(
			&cast_to("timestamp"),
			BoundBinaryOp::LessThan,
			&cast_to("timestamptz"),
			pg,
		)
		.expect("a promotion");
		assert_eq!(static_type(&l, pg), SqlType::TimestampTz);
		assert!(
			promoted_operands(&cast_to("date"), BoundBinaryOp::Equal, &cast_to("date"), pg)
				.is_none()
		);
	}

	#[test]
	fn a_text_literal_beside_a_temporal_operand_is_read_as_its_type() {
		let pg = SqlDialect::Postgres;
		let (_, r) =
			promoted_operands(&cast_to("date"), BoundBinaryOp::Equal, &text("2024-01-01"), pg)
				.expect("a cast");
		assert_eq!(static_type(&r, pg), SqlType::Date);
		let (_, r) =
			promoted_operands(&cast_to("timestamp"), BoundBinaryOp::Add, &text("1 day"), pg)
				.expect("a cast");
		assert_eq!(static_type(&r, pg), SqlType::Interval);
		let my = SqlDialect::MySql;
		let (_, r) =
			promoted_operands(&cast_to("datetime"), BoundBinaryOp::Equal, &text("2024-01-01"), my)
				.expect("a cast");
		assert_eq!(static_type(&r, my), SqlType::Timestamp);
	}

	#[test]
	fn operands_tested_together_are_promoted_to_the_widest_instant() {
		let pg = SqlDialect::Postgres;
		let date = cast_to("date");
		let wide = cast_to("timestamptz");
		let operands =
			promoted_group(&[&date, &wide, &text("2024-01-01")], pg).expect("a promotion");
		assert_eq!(operands.len(), 3);
		for operand in &operands {
			assert_eq!(static_type(operand, pg), SqlType::TimestampTz);
		}
		assert!(promoted_group(&[&date, &date], pg).is_none());
		assert!(promoted_group(&[&text("a"), &text("b")], pg).is_none());
	}

	#[test]
	fn a_cast_target_names_the_kind_of_the_dialect() {
		use surrealdb_expr::expr::Kind;
		let pg = SqlDialect::Postgres;
		let my = SqlDialect::MySql;
		assert_eq!(cast_kind("date", pg), Kind::Date);
		assert_eq!(cast_kind("time", pg), Kind::Time);
		assert_eq!(cast_kind("time", my), Kind::Interval);
		assert_eq!(cast_kind("timestamp", pg), Kind::Timestamp);
		assert_eq!(cast_kind("timestamp", my), Kind::Datetime);
		assert_eq!(cast_kind("datetime", my), Kind::Timestamp);
		assert_eq!(cast_kind("timestamptz", pg), Kind::Datetime);
		assert_eq!(cast_kind("interval", pg), Kind::Interval);
		assert_eq!(cast_kind("interval day to second(3)", pg), Kind::Interval);
		assert_eq!(cast_kind("interval(2)", pg), Kind::Interval);
		assert_eq!(cast_type("interval hour", pg), SqlType::Interval);
		assert_eq!(SqlType::from_name("interval day to second(3)"), SqlType::Interval);
		assert_eq!(SqlType::from_name("interval(2)"), SqlType::Interval);
		assert_eq!(SqlType::from_name("intervals"), SqlType::Unknown);
	}

	#[test]
	fn a_timetz_offset_is_applied_on_input() {
		assert_eq!(timetz_text_to_utc("10:20:30+02").as_deref(), Some("08:20:30"));
		assert_eq!(timetz_text_to_utc("01:00:00.5-05:30").as_deref(), Some("06:30:00.5"));
		assert_eq!(timetz_text_to_utc("23:30+01").as_deref(), Some("22:30:00"));
		assert_eq!(timetz_text_to_utc("00:30:00+02").as_deref(), Some("22:30:00"));
		assert_eq!(timetz_text_to_utc("10:20:30"), None);
	}
}
