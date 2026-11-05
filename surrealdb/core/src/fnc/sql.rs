//! The functions the SQL front ends lower PostgreSQL and MySQL semantics into: the order of
//! network addresses, the range operators, the PostgreSQL text of a value, and a cast to a
//! declared SQL type.
//!
//! They are reached only from plans the SQL planner builds. The SurrealQL parser has no path to
//! them, so their names are not part of SurrealQL.

use anyhow::{Result, bail};

use crate::expr::statements::define::SqlTypeDialect;
use crate::expr::statements::define::declared_type::DeclaredSqlType;
use crate::expr::statements::define::sql_types::text_shaped::network;
use crate::expr::statements::define::sql_types::{self, range, text_form};
use crate::val::{Bytes, Number, Range, Value};

/// `sql::network_key(value)`: a key whose order is PostgreSQL's order of `inet` and `cidr`
/// values. `NULL` and `NONE` pass through. A value that does not read as a network is refused, as
/// PostgreSQL refuses it as input to the comparison.
pub fn network_key((value,): (Value,)) -> Result<Value> {
	if value.is_nullish() {
		return Ok(value);
	}
	let Value::String(text) = &value else {
		bail!("invalid input syntax for type inet: {}", value.kind_of());
	};
	match network::parse(text.as_str()) {
		Ok(parsed) => Ok(Value::Bytes(Bytes::from(network::sort_key(&parsed)))),
		Err(_) => bail!("invalid input syntax for type inet: \"{}\"", text.as_str()),
	}
}

/// `sql::network_text(value, type)`: the canonical text of the network `value` reads as, as a
/// value of `type` (`inet` or `cidr`) is stored: an `inet` keeps its host bits and drops a
/// full-length prefix, and a `cidr` always writes its prefix. `NULL` and `NONE` give `NULL`; a
/// value that does not read as a network, or a `cidr` with a host bit set to the right of its
/// prefix, is refused, as PostgreSQL refuses its input.
pub fn network_text((value, ty): (Value, Value)) -> Result<Value> {
	if value.is_nullish() {
		return Ok(Value::Null);
	}
	let cidr = matches!(&ty, Value::String(ty) if ty.as_str() == "cidr");
	let ty = if cidr {
		"cidr"
	} else {
		"inet"
	};
	let Value::String(text) = &value else {
		bail!("invalid input syntax for type {ty}: {}", value.kind_of());
	};
	if cidr {
		return match network::parse_cidr(text.as_str()) {
			Ok(parsed) => Ok(Value::from(parsed.cidr_text())),
			Err(error) if network::parse(text.as_str()).is_ok() => bail!("{}", error.reason),
			Err(_) => bail!("invalid input syntax for type cidr: \"{}\"", text.as_str()),
		};
	}
	match network::parse(text.as_str()) {
		Ok(parsed) => Ok(Value::from(parsed.inet_text())),
		Err(_) => bail!("invalid input syntax for type inet: \"{}\"", text.as_str()),
	}
}

/// The range a range operator reads, `None` for `NULL` or `NONE`.
fn range_operand(function: &str, value: Value) -> Result<Option<Range>> {
	if let Value::Range(range) = value {
		return Ok(Some(*range));
	}
	if value.is_nullish() {
		return Ok(None);
	}
	bail!(
		"Incorrect arguments for function {function}(). Expected a range, found {}",
		value.kind_of()
	)
}

/// `sql::range_lower(range)`: PostgreSQL's `lower()`.
pub fn range_lower((value,): (Value,)) -> Result<Value> {
	Ok(range_operand("lower", value)?.map_or(Value::Null, |range| range::lower(&range)))
}

/// `sql::range_upper(range)`: PostgreSQL's `upper()`.
pub fn range_upper((value,): (Value,)) -> Result<Value> {
	Ok(range_operand("upper", value)?.map_or(Value::Null, |range| range::upper(&range)))
}

/// `sql::range_overlaps(a, b)`: PostgreSQL's range `&&`, `NULL` over a `NULL`.
pub fn range_overlaps((a, b): (Value, Value)) -> Result<Value> {
	let (a, b) = (range_operand("&&", a)?, range_operand("&&", b)?);
	Ok(match (a, b) {
		(Some(a), Some(b)) => Value::Bool(range::overlaps(&a, &b)),
		_ => Value::Null,
	})
}

/// `sql::range_contains(outer, inner)`: PostgreSQL's range `@>` range, `NULL` over a `NULL`.
pub fn range_contains((outer, inner): (Value, Value)) -> Result<Value> {
	let (outer, inner) = (range_operand("@>", outer)?, range_operand("@>", inner)?);
	Ok(match (outer, inner) {
		(Some(outer), Some(inner)) => Value::Bool(range::contains(&outer, &inner)),
		_ => Value::Null,
	})
}

/// The PostgreSQL type a type argument names, `None` for the empty text, which names none.
fn declared_type(text: &Value) -> Result<Option<DeclaredSqlType>> {
	let Value::String(text) = text else {
		bail!("Incorrect arguments for a declared SQL type. Expected a string");
	};
	if text.is_empty() {
		return Ok(None);
	}
	match DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, text.as_str()) {
		Ok(declared) => Ok(Some(declared)),
		Err(error) => bail!("invalid declared SQL type '{}': {error}", text.as_str()),
	}
}

/// `sql::text(value, type)`: the text PostgreSQL's cast to `text` gives `value`, read as a value
/// of the PostgreSQL type `type` names, or by its own kind when `type` is empty. `NULL` and `NONE`
/// give `NULL`. A value with no PostgreSQL text of its own is cast to a string as SurrealQL casts
/// it.
pub fn text((value, ty): (Value, Value)) -> Result<Value> {
	if value.is_nullish() {
		return Ok(Value::Null);
	}
	if let Some(declared) = declared_type(&ty)? {
		// A cast of a `char(n)` to text drops the spaces that pad it to its width.
		if matches!(declared.base(), "bpchar" | "char" | "character")
			&& let Value::String(text) = &value
		{
			return Ok(Value::from(text.as_str().trim_end_matches(' ')));
		}
		// The cast to text writes an `inet` with its prefix, whole or not, as `cidr` writes it.
		if matches!(declared.base(), "inet" | "cidr")
			&& let Value::String(text) = &value
			&& let Ok(parsed) = network::parse(text.as_str())
		{
			return Ok(Value::from(parsed.cidr_text()));
		}
		if let Some(text) = text_form::write(&declared, &value) {
			return Ok(Value::from(text));
		}
	}
	match text_form::value::postgres_text(&value) {
		Some(text) => Ok(Value::from(text)),
		None => Ok(Value::from(value.into_raw_string())),
	}
}

/// `sql::fit(value, type, dialect)`: `value` as a value of the PostgreSQL type `type` names, as a
/// write to a column of the type holds it. Text is read in the type's text form, and the value is
/// canonicalised: a numeric is rounded half away from zero to its scale, a range normalised, a
/// network written canonically. A value the type cannot hold is refused, except that a numeric
/// with more integer digits than the precision leaves is clamped to the greatest value of the
/// type of its sign in the `mysql` dialect, as MySQL's `CAST` clamps it. `NULL` and `NONE` give
/// `NULL`.
pub fn fit((value, ty, dialect): (Value, Value, Value)) -> Result<Value> {
	if value.is_nullish() {
		return Ok(Value::Null);
	}
	let Some(declared) = declared_type(&ty)? else {
		return Ok(value);
	};
	let value = if let Value::String(text) = &value {
		match text_form::read(&declared, text.as_str()) {
			Some(Ok(read)) => read,
			Some(Err(violation)) => bail!("{}", violation.message),
			None => value,
		}
	} else {
		value
	};
	let negative = matches!(&value, Value::Number(number) if *number < Number::Int(0));
	match sql_types::validate(&declared, value, "") {
		Ok(value) => Ok(value),
		Err(violation) if matches!(&dialect, Value::String(d) if d.as_str() == "mysql") => {
			match (declared.precision(), declared.scale()) {
				(Some(precision), Some(scale)) => Ok(greatest(precision, scale, negative)),
				_ => bail!("{} (cast to {})", violation.message, declared.canonical_text()),
			}
		}
		Err(violation) => {
			bail!("{} (cast to {})", violation.message, declared.canonical_text())
		}
	}
}

/// The value of greatest magnitude a `numeric(precision, scale)` holds, of the sign asked for:
/// `precision` nines with `scale` of them after the point.
fn greatest(precision: u64, scale: i64, negative: bool) -> Value {
	let digits = "9".repeat(usize::try_from(precision).unwrap_or(0).max(1));
	let scale = usize::try_from(scale).unwrap_or(0).min(digits.len());
	let (integer, fraction) = digits.split_at(digits.len() - scale);
	let integer = if integer.is_empty() {
		"0"
	} else {
		integer
	};
	let sign = if negative {
		"-"
	} else {
		""
	};
	let text = if fraction.is_empty() {
		format!("{sign}{integer}")
	} else {
		format!("{sign}{integer}.{fraction}")
	};
	match text.parse::<rust_decimal::Decimal>() {
		Ok(decimal) => Value::Number(Number::Decimal(decimal)),
		Err(_) => match text.parse::<surrealdb_types::BigDecimal>() {
			Ok(big) => Value::Number(Number::BigDecimal(Box::new(big))),
			Err(_) => Value::Null,
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn text_of(value: Value, ty: &str) -> Value {
		text((value, Value::from(ty))).expect("text succeeds")
	}

	#[test]
	fn an_inet_is_written_with_its_prefix() {
		assert_eq!(text_of(Value::from("192.168.1.5"), "inet"), Value::from("192.168.1.5/32"));
		assert_eq!(text_of(Value::from("10.0.0.0/8"), "cidr"), Value::from("10.0.0.0/8"));
	}

	#[test]
	fn a_char_drops_its_padding() {
		assert_eq!(text_of(Value::from("ab  "), "char(4)"), Value::from("ab"));
		assert_eq!(text_of(Value::from("ab  "), "bpchar"), Value::from("ab"));
		assert_eq!(text_of(Value::from("ab  "), "varchar(4)"), Value::from("ab  "));
	}

	#[test]
	fn a_null_is_null_and_an_undeclared_value_is_written_by_its_kind() {
		assert_eq!(text_of(Value::Null, "inet"), Value::Null);
		assert_eq!(text_of(Value::Bool(true), ""), Value::from("true"));
	}

	#[test]
	fn a_network_parameter_reads_as_the_stored_text_of_its_network() {
		let text = |value: &str, ty: &str| network_text((Value::from(value), Value::from(ty)));
		assert_eq!(text("10.0.0.2/32", "inet").expect("reads"), Value::from("10.0.0.2"));
		assert_eq!(text("10.0.0.0/16", "cidr").expect("reads"), Value::from("10.0.0.0/16"));
		assert!(text("not an address", "inet").is_err());
		assert!(text("10.0.0.1/8", "cidr").is_err());
		assert_eq!(text("10.0.0.1/8", "inet").expect("reads"), Value::from("10.0.0.1/8"));
		assert_eq!(network_text((Value::Null, Value::from("inet"))).expect("null"), Value::Null);
	}

	#[test]
	fn a_network_key_orders_by_address_and_refuses_other_values() {
		let key = |text: &str| network_key((Value::from(text),)).expect("key succeeds");
		assert!(key("9.255.255.255") < key("10.0.0.0/16"));
		assert!(key("10.0.0.2") < key("10.0.0.10"));
		assert!(key("100.0.0.1") < key("::1"));
		assert!(network_key((Value::from("not an address"),)).is_err());
		assert_eq!(network_key((Value::Null,)).expect("null"), Value::Null);
	}

	#[test]
	fn fit_rounds_to_the_scale_and_mysql_clamps_an_overflow() {
		let fit_to = |value: &str, dialect: &str| {
			fit((
				Value::Number(Number::Decimal(value.parse().expect("decimal"))),
				Value::from("numeric(3,1)"),
				Value::from(dialect),
			))
		};
		let decimal = |text: &str| Value::Number(Number::Decimal(text.parse().expect("decimal")));
		assert_eq!(fit_to("1.25", "postgresql").expect("fits"), decimal("1.3"));
		assert!(fit_to("1000", "postgresql").is_err());
		assert_eq!(fit_to("-1000", "mysql").expect("clamps"), decimal("-99.9"));
	}
}
