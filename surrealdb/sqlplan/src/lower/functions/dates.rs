//! Lowering of the calendar functions that have no single engine function to call: MySQL's week,
//! day-name and day-count functions, `timestampadd` and `adddate`, `time_format`, `now(fsp)`, and
//! PostgreSQL's `to_char` over a timestamp and `make_date`.

use surrealdb_expr::expr::BinaryOperator;
use surrealdb_expr::expr::logical::{LogicalExpr, LogicalLiteral};
use surrealdb_expr::val::Duration;

use super::{
	binary, bind, call, float, int, lower_args, lower_bound_expr, lower_date_add, mysql_datetime,
	mysql_format, mysql_week, null, one_day, padded, strict, string_literal, text, when,
};
use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::ir::{BoundExpr, BoundLiteral};

/// Whether this module lowers a call of `name` with `arguments` arguments in `dialect`.
pub(super) fn handles(name: &str, dialect: SqlDialect, arguments: usize) -> bool {
	match name {
		"week" | "weekofyear" | "quarter" | "dayname" | "monthname" | "weekday" | "to_days"
		| "makedate" | "time_format" | "timestampadd" | "adddate" | "subdate" => {
			dialect == SqlDialect::MySql
		}
		"now" => arguments == 1,
		"to_char" | "make_date" | "make_time" | "make_timestamp" | "make_interval" | "age"
		| "to_date" => dialect == SqlDialect::Postgres,
		_ => false,
	}
}

pub(super) fn lower(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	match name {
		"week" => lower_week(args, policy),
		"weekofyear" => date_function(args, policy, |d| call("time::week", vec![d])),
		"quarter" => date_function(args, policy, |d| {
			call(
				"math::ceil",
				vec![binary(call("time::month", vec![d]), BinaryOperator::Divide, float(3.0))],
			)
		}),
		"dayname" => date_function(args, policy, |d| call("time::format", vec![d, text("%A")])),
		"monthname" => date_function(args, policy, |d| call("time::format", vec![d, text("%B")])),
		"weekday" => date_function(args, policy, |d| {
			binary(call("time::wday", vec![d]), BinaryOperator::Subtract, int(1))
		}),
		"to_days" => date_function(args, policy, |d| {
			binary(
				call(
					"math::floor",
					vec![binary(
						call("time::micros", vec![d]),
						BinaryOperator::Divide,
						int(86_400_000_000),
					)],
				),
				BinaryOperator::Add,
				int(719_528),
			)
		}),
		"makedate" => lower_makedate(args, policy),
		"time_format" => lower_time_format(args, policy),
		"timestampadd" => lower_timestampadd(args, policy),
		"adddate" | "subdate" => lower_adddate(name, args, policy),
		"now" if policy.dialect == SqlDialect::MySql => lower_now(args),
		"to_char" => lower_to_char(args, policy),
		"make_date" => lower_make_date(args, policy),
		"make_time" => lower_strict_call("time::make_time", args, policy),
		"make_timestamp" => lower_strict_call("time::make_timestamp", args, policy),
		"make_interval" => lower_make_interval(args, policy),
		"age" => lower_age(args, policy),
		"to_date" => lower_to_date(args, policy),
		_ => Err(PlanError::UnsupportedExpression(name.to_owned())),
	}
}

/// `body` over the one date argument, as a datetime, and NULL when it is NULL.
fn date_function(
	args: &[BoundExpr],
	policy: &DialectPolicy,
	body: impl FnOnce(LogicalExpr) -> LogicalExpr,
) -> Result<LogicalExpr, PlanError> {
	let value = mysql_datetime(lower_bound_expr(&args[0], policy)?);
	Ok(strict(vec![value], |v| body(v[0].clone())))
}

/// MySQL `week(d)` and `week(d, mode)`, for mode 0 (weeks from Sunday) and mode 3 (ISO weeks).
fn lower_week(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let mode = match args.get(1) {
		None => 0,
		Some(mode) => super::integer_literal(mode).ok_or_else(|| {
			PlanError::UnsupportedExpression("week with a mode that is not an integer".to_owned())
		})?,
	};
	let value = mysql_datetime(lower_bound_expr(&args[0], policy)?);
	match mode {
		0 => Ok(strict(vec![value], |v| mysql_week(v[0].clone()))),
		3 => Ok(strict(vec![value], |v| call("time::week", vec![v[0].clone()]))),
		_ => Err(PlanError::UnsupportedExpression(format!("week with the mode {mode}"))),
	}
}

/// MySQL `makedate(year, day_of_year)`: that day of the year, NULL for a day below one.
fn lower_makedate(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| {
		let january_first = call(
			"type::datetime",
			vec![call("string::concat", vec![padded(v[0].clone(), 4), text("-01-01T00:00:00Z")])],
		);
		when(
			binary(v[1].clone(), BinaryOperator::LessThan, int(1)),
			null(),
			call(
				"type::date",
				vec![binary(
					january_first,
					BinaryOperator::Add,
					binary(
						one_day(),
						BinaryOperator::Multiply,
						binary(v[1].clone(), BinaryOperator::Subtract, int(1)),
					),
				)],
			),
		)
	}))
}

/// MySQL `time_format(t, format)` for the time specifiers, over a time (`10:20:30`) or a datetime.
fn lower_time_format(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let format = string_literal(&args[1]).ok_or_else(|| {
		PlanError::UnsupportedExpression(
			"time_format with a format that is not a string literal".to_owned(),
		)
	})?;
	let mut chars = format.chars();
	while let Some(c) = chars.next() {
		if c == '%'
			&& !matches!(
				chars.next(),
				Some('H' | 'h' | 'I' | 'i' | 'k' | 'l' | 'p' | 's' | 'S' | 'T' | 'f' | '%')
			) {
			return Err(PlanError::UnsupportedExpression(
				"a format specifier of time_format".to_owned(),
			));
		}
	}
	let format = mysql_format(format).ok_or_else(|| {
		PlanError::UnsupportedExpression("a format specifier of time_format".to_owned())
	})?;
	let value = lower_bound_expr(&args[0], policy)?;
	Ok(strict(vec![value], |v| {
		let current = &v[0];
		let datetime = when(
			call("type::is_datetime", vec![current.clone()]),
			current.clone(),
			when(
				call("string::contains", vec![current.clone(), text("-")]),
				mysql_datetime(current.clone()),
				call(
					"type::datetime",
					vec![call(
						"string::concat",
						vec![text("1970-01-01T"), current.clone(), text("Z")],
					)],
				),
			),
		);
		call("time::format", vec![datetime, text(&format)])
	}))
}

/// MySQL `timestampadd(unit, n, datetime)`, which is `date_add(datetime, INTERVAL n unit)`.
fn lower_timestampadd(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let reordered = [args[2].clone(), args[1].clone(), args[0].clone()];
	lower_date_add("date_add", &reordered, policy)
}

/// MySQL `adddate(d, days)` and `subdate(d, days)`.
fn lower_adddate(
	name: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let unit = BoundExpr::Literal(BoundLiteral::String("DAY".to_owned()));
	let reordered = [args[0].clone(), args[1].clone(), unit];
	let direction = if name == "adddate" {
		"date_add"
	} else {
		"date_sub"
	};
	lower_date_add(direction, &reordered, policy)
}

/// MySQL `now(fsp)`: the current time truncated to `fsp` fractional digits.
fn lower_now(args: &[BoundExpr]) -> Result<LogicalExpr, PlanError> {
	let digits = super::integer_literal(&args[0])
		.filter(|digits| (0..=6).contains(digits))
		.ok_or_else(|| {
			PlanError::UnsupportedExpression("now with a precision that is not 0 to 6".to_owned())
		})?;
	let step = Duration::from_nanos(10u64.pow(9 - digits as u32));
	Ok(call(
		"time::floor",
		vec![call("time::now", Vec::new()), LogicalExpr::Literal(LogicalLiteral::Duration(step))],
	))
}

/// PostgreSQL `make_date(year, month, day)`.
fn lower_make_date(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	lower_strict_call("time::make_date", args, policy)
}

/// The engine function `function` over the arguments, NULL when any is NULL.
fn lower_strict_call(
	function: &str,
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	Ok(strict(lower_args(args, policy)?, |v| call(function, v.to_vec())))
}

/// PostgreSQL `make_interval(years, months, weeks, days, hours, mins, secs)`, each argument
/// defaulting to zero.
fn lower_make_interval(
	args: &[BoundExpr],
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	lower_strict_call("interval::make", args, policy)
}

/// PostgreSQL `age(a, b)`, and `age(a)` which is `age(current_date, a)`.
fn lower_age(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let mut values = lower_args(args, policy)?;
	if values.len() == 1 {
		let today = call("type::date", vec![call("time::now", Vec::new())]);
		values.insert(0, today);
	}
	Ok(strict(values, |v| call("time::age", v.to_vec())))
}

/// PostgreSQL `to_date(text, template)` for a template of `YYYY`, `YY`, `MM` and `DD` fields with
/// literal separators, which read the digits at fixed positions. Any other pattern has no
/// equivalent.
fn lower_to_date(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let template = string_literal(&args[1]).ok_or_else(|| {
		PlanError::UnsupportedExpression(
			"to_date with a template that is not a string literal".to_owned(),
		)
	})?;
	let unsupported = || {
		PlanError::UnsupportedExpression(
			"to_date with a template pattern that has no equivalent".to_owned(),
		)
	};
	let (mut year, mut month, mut day): (Option<(usize, usize)>, Option<usize>, Option<usize>) =
		(None, None, None);
	let mut offset = 0usize;
	let mut rest = template;
	while !rest.is_empty() {
		if let Some(after) = rest.strip_prefix("YYYY") {
			year = Some((offset, 4));
			offset += 4;
			rest = after;
		} else if let Some(after) = rest.strip_prefix("YY") {
			year = Some((offset, 2));
			offset += 2;
			rest = after;
		} else if let Some(after) = rest.strip_prefix("MM") {
			month = Some(offset);
			offset += 2;
			rest = after;
		} else if let Some(after) = rest.strip_prefix("DD") {
			day = Some(offset);
			offset += 2;
			rest = after;
		} else {
			let c = rest.chars().next().ok_or_else(unsupported)?;
			if c.is_ascii_alphanumeric() || c == '"' {
				return Err(unsupported());
			}
			offset += 1;
			rest = &rest[c.len_utf8()..];
		}
	}
	let ((year_at, year_width), month_at, day_at) = match (year, month, day) {
		(Some(y), Some(m), Some(d)) => (y, m, d),
		_ => return Err(unsupported()),
	};
	let value = lower_bound_expr(&args[0], policy)?;
	Ok(strict(vec![value], |v| {
		let field = |at: usize, width: usize| {
			call(
				"type::int",
				vec![call(
					"string::slice",
					vec![v[0].clone(), int(at as i64), int((at + width) as i64)],
				)],
			)
		};
		let year = if year_width == 4 {
			field(year_at, 4)
		} else {
			// PostgreSQL reads a two-digit year of 70 to 99 as 19xx and 00 to 69 as 20xx.
			bind(vec![field(year_at, 2)], |y| {
				binary(
					y[0].clone(),
					BinaryOperator::Add,
					when(
						binary(y[0].clone(), BinaryOperator::LessThan, int(70)),
						int(2000),
						int(1900),
					),
				)
			})
		};
		call("time::make_date", vec![year, field(month_at, 2), field(day_at, 2)])
	}))
}

/// The `strftime` format for a PostgreSQL `to_char` template, or `None` for a pattern with no
/// equivalent. `FM` turns off the padding of the pattern that directly follows it.
fn pg_format(template: &str) -> Option<String> {
	const PATTERNS: [(&str, &str, &str); 17] = [
		("YYYY", "%Y", "%Y"),
		("HH24", "%H", "%-H"),
		("HH12", "%I", "%-I"),
		("DDD", "%j", "%-j"),
		("YY", "%y", "%y"),
		("MM", "%m", "%-m"),
		("DD", "%d", "%-d"),
		("HH", "%I", "%-I"),
		("MI", "%M", "%-M"),
		("SS", "%S", "%-S"),
		("MS", "%3f", "%3f"),
		("US", "%6f", "%6f"),
		("IW", "%V", "%-V"),
		("Mon", "%b", "%b"),
		("Dy", "%a", "%a"),
		("AM", "%p", "%p"),
		("PM", "%p", "%p"),
	];
	let mut out = String::new();
	let mut rest = template;
	let mut fill_mode = false;
	'next: while !rest.is_empty() {
		if let Some(after) = rest.strip_prefix("FM") {
			fill_mode = true;
			rest = after;
			continue;
		}
		if let Some(quoted) = rest.strip_prefix('"') {
			let end = quoted.find('"')?;
			out.push_str(&quoted[..end].replace('%', "%%"));
			rest = &quoted[end + 1..];
			continue;
		}
		for (pattern, name) in [("Month", "%B"), ("Day", "%A")] {
			if let Some(after) = rest.strip_prefix(pattern) {
				// Without FM PostgreSQL blank-pads the name to the longest one.
				if !fill_mode {
					return None;
				}
				out.push_str(name);
				fill_mode = false;
				rest = after;
				continue 'next;
			}
		}
		for (pattern, padded, unpadded) in PATTERNS {
			if let Some(after) = rest.strip_prefix(pattern) {
				out.push_str(if fill_mode {
					unpadded
				} else {
					padded
				});
				fill_mode = false;
				rest = after;
				continue 'next;
			}
		}
		let c = rest.chars().next()?;
		if c.is_ascii_alphabetic() {
			return None;
		}
		// A literal between `FM` and a pattern cancels it, as PostgreSQL does.
		fill_mode = false;
		if c == '%' {
			out.push_str("%%");
		} else {
			out.push(c);
		}
		rest = &rest[c.len_utf8()..];
	}
	Some(out)
}

/// `to_char(timestamp, template)` for a template given as a literal.
fn lower_to_char(args: &[BoundExpr], policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let template = string_literal(&args[1]).ok_or_else(|| {
		PlanError::UnsupportedExpression(
			"to_char with a template that is not a string literal".to_owned(),
		)
	})?;
	let format = pg_format(template).ok_or_else(|| {
		PlanError::UnsupportedExpression(
			"to_char with a template pattern that has no equivalent".to_owned(),
		)
	})?;
	let value = lower_bound_expr(&args[0], policy)?;
	Ok(strict(vec![value], |v| {
		let format_of = |at: LogicalExpr| call("time::format", vec![at, text(&format)]);
		when(
			binary(
				binary(
					call("type::is_datetime", vec![v[0].clone()]),
					BinaryOperator::Or,
					call("type::is_timestamp", vec![v[0].clone()]),
				),
				BinaryOperator::Or,
				call("type::is_date", vec![v[0].clone()]),
			),
			format_of(v[0].clone()),
			when(
				call("type::is_time", vec![v[0].clone()]),
				format_of(call(
					"type::datetime",
					vec![call(
						"string::concat",
						vec![
							text("1970-01-01T"),
							call("type::string", vec![v[0].clone()]),
							text("Z"),
						],
					)],
				)),
				LogicalExpr::Throw(Box::new(text(
					"to_char is supported for dates, times and timestamps only",
				))),
			),
		)
	}))
}
