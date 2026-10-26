//! The wire forms of the pgvector `vector` type.
//!
//! The text form is `[1,2,3]`. The binary form is the number of dimensions as a big-endian
//! `i16`, an unused `i16` that is zero, and each element as a big-endian `f32`. A vector is held
//! as an array of floats.

use std::fmt::Write as _;
use std::str::FromStr;

use surrealdb_types::{Number, Value};

use super::error::PgError;

/// The most dimensions a vector has.
const MAX_DIMENSIONS: usize = 16_000;

fn element(value: &Value) -> Result<f64, PgError> {
	let number = if let Value::Number(number) = value {
		number.to_f64()
	} else if let Value::String(text) = value {
		f64::from_str(text.trim()).ok()
	} else {
		None
	};
	match number {
		Some(number) if number.is_finite() => Ok(number),
		Some(_) => Err(PgError::invalid_text("NaN and infinity are not allowed in a vector")),
		None => Err(PgError::internal(format!(
			"cannot encode a {} element into a vector",
			value.kind()
		))),
	}
}

fn elements(value: &Value) -> Result<Vec<f64>, PgError> {
	let Value::Array(items) = value else {
		return Err(PgError::internal(format!(
			"cannot encode a {} cell into a vector column",
			value.kind()
		)));
	};
	if items.len() > MAX_DIMENSIONS {
		return Err(PgError::new(
			"54000",
			format!("vector cannot have more than {MAX_DIMENSIONS} dimensions"),
		));
	}
	items.iter().map(element).collect()
}

/// The text form of a vector, which is its own text when it already is one.
pub(super) fn text(value: Value) -> Result<Vec<u8>, PgError> {
	if let Value::String(text) = value {
		return Ok(text.into_bytes());
	}
	let mut out = String::from("[");
	for (index, element) in elements(&value)?.into_iter().enumerate() {
		if index > 0 {
			out.push(',');
		}
		let _ = write!(out, "{element}");
	}
	out.push(']');
	Ok(out.into_bytes())
}

/// The binary form of a vector.
pub(super) fn binary(value: Value) -> Result<Vec<u8>, PgError> {
	if let Value::String(text) = value {
		return binary(decode_text(&text)?);
	}
	let elements = elements(&value)?;
	let mut out = Vec::with_capacity(4 + 4 * elements.len());
	// At most 16000, so within an `i16`.
	out.extend_from_slice(&(elements.len() as i16).to_be_bytes());
	out.extend_from_slice(&0i16.to_be_bytes());
	for element in elements {
		let narrowed = element as f32;
		if !narrowed.is_finite() {
			return Err(PgError::invalid_text(format!(
				"{element} is out of range for a vector element"
			)));
		}
		out.extend_from_slice(&narrowed.to_be_bytes());
	}
	Ok(out)
}

fn malformed(text: &str) -> PgError {
	PgError::invalid_text(format!("invalid input syntax for type vector: \"{text}\""))
}

/// The vector the text `text` spells.
pub(super) fn decode_text(text: &str) -> Result<Value, PgError> {
	let inner = text
		.trim()
		.strip_prefix('[')
		.and_then(|rest| rest.strip_suffix(']'))
		.ok_or_else(|| malformed(text))?;
	let mut items = Vec::new();
	for item in inner.split(',') {
		let item = item.trim();
		let value = f64::from_str(item).ok().filter(|value| !item.is_empty() && value.is_finite());
		let Some(value) = value else {
			return Err(malformed(text));
		};
		items.push(Value::Number(Number::Float(value)));
		if items.len() > MAX_DIMENSIONS {
			return Err(PgError::new(
				"54000",
				format!("vector cannot have more than {MAX_DIMENSIONS} dimensions"),
			));
		}
	}
	Ok(Value::Array(items.into()))
}

/// The vector the binary form `bytes` holds.
///
/// Each element is a `f32`, and is read as the shortest decimal that is that `f32`, so a client
/// that sends `0.1` is stored `0.1`.
pub(super) fn decode_binary(bytes: &[u8]) -> Result<Value, PgError> {
	let invalid = |detail: &str| PgError::invalid_text(format!("invalid vector: {detail}"));
	let Some((header, body)) = bytes.split_at_checked(4) else {
		return Err(invalid("too short"));
	};
	let dimensions = usize::from(u16::from_be_bytes([header[0], header[1]]));
	if header[2] != 0 || header[3] != 0 {
		return Err(invalid("unexpected header"));
	}
	if dimensions == 0 || dimensions > MAX_DIMENSIONS || body.len() != 4 * dimensions {
		return Err(invalid("wrong length"));
	}
	let mut items = Vec::with_capacity(dimensions);
	let mut shown = String::new();
	for chunk in body.chunks_exact(4) {
		let element = f32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
		if !element.is_finite() {
			return Err(PgError::invalid_text("NaN and infinity are not allowed in a vector"));
		}
		shown.clear();
		let _ = write!(shown, "{element}");
		let shortest = f64::from_str(&shown).unwrap_or_else(|_| f64::from(element));
		items.push(Value::Number(Number::Float(shortest)));
	}
	Ok(Value::Array(items.into()))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn floats(values: &[f64]) -> Value {
		Value::Array(
			values.iter().map(|v| Value::Number(Number::Float(*v))).collect::<Vec<_>>().into(),
		)
	}

	fn ints(values: &[i64]) -> Value {
		Value::Array(
			values.iter().map(|v| Value::Number(Number::Int(*v))).collect::<Vec<_>>().into(),
		)
	}

	#[test]
	fn text_form_is_a_bracketed_list() {
		assert_eq!(text(floats(&[1.0, 2.5, -3.0])).unwrap(), b"[1,2.5,-3]");
		assert_eq!(text(ints(&[1, 2, 3])).unwrap(), b"[1,2,3]");
		assert_eq!(text(floats(&[])).unwrap(), b"[]");
		assert_eq!(text(Value::String("[4,5]".into())).unwrap(), b"[4,5]");
	}

	#[test]
	fn text_form_round_trips() {
		for source in ["[1,2,3]", "[0.1,-0.25,1e-7]", " [ 1 , 2 ] "] {
			let decoded = decode_text(source).unwrap();
			let again = decode_text(std::str::from_utf8(&text(decoded.clone()).unwrap()).unwrap());
			assert_eq!(again.unwrap(), decoded, "{source}");
		}
	}

	#[test]
	fn text_form_refuses_what_is_not_a_vector() {
		for source in ["", "[]x", "1,2", "[1,2", "[a]", "[1,]", "[NaN]", "[Infinity]"] {
			assert!(decode_text(source).is_err(), "{source}");
		}
		assert_eq!(decode_text("[1,x]").unwrap_err().code, "22P02");
	}

	#[test]
	fn a_vector_has_at_most_sixteen_thousand_dimensions() {
		let at_limit = format!("[{}]", vec!["1"; MAX_DIMENSIONS].join(","));
		assert!(decode_text(&at_limit).is_ok());
		let over = format!("[{}]", vec!["1"; MAX_DIMENSIONS + 1].join(","));
		assert_eq!(decode_text(&over).unwrap_err().code, "54000");
	}

	#[test]
	fn a_vector_that_is_not_finite_is_refused() {
		assert!(text(floats(&[f64::NAN])).is_err());
		assert!(binary(floats(&[f64::INFINITY])).is_err());
	}

	#[test]
	fn a_cell_that_is_not_an_array_is_refused() {
		assert!(text(Value::Bool(true)).is_err());
		assert!(binary(Value::Bool(true)).is_err());
		assert!(text(Value::Array(vec![Value::Bool(true)].into())).is_err());
	}

	#[test]
	fn binary_form_is_the_dimensions_a_zero_and_f32s() {
		let out = binary(floats(&[1.0, 2.0])).unwrap();
		let mut expected = vec![0, 2, 0, 0];
		expected.extend_from_slice(&1.0f32.to_be_bytes());
		expected.extend_from_slice(&2.0f32.to_be_bytes());
		assert_eq!(out, expected);
	}

	#[test]
	fn binary_form_round_trips_and_reads_an_f32_as_its_shortest_decimal() {
		let out = binary(floats(&[0.1, 1.5])).unwrap();
		assert_eq!(decode_binary(&out).unwrap(), floats(&[0.1, 1.5]));
	}

	#[test]
	fn binary_form_refuses_what_is_not_a_vector() {
		assert!(decode_binary(&[]).is_err());
		assert!(decode_binary(&[0, 1, 0, 0]).is_err());
		assert!(decode_binary(&[0, 0, 0, 0]).is_err());
		assert!(decode_binary(&[0, 1, 0, 1, 0, 0, 0, 0]).is_err());
		let mut nan = vec![0, 1, 0, 0];
		nan.extend_from_slice(&f32::NAN.to_be_bytes());
		assert!(decode_binary(&nan).is_err());
	}

	#[test]
	fn binary_form_reads_a_text_cell() {
		assert_eq!(
			binary(Value::String("[1,2]".into())).unwrap(),
			binary(floats(&[1.0, 2.0])).unwrap()
		);
	}
}
