//! Literal constant representations in PostgreSQL AST expressions.

use std::fmt;

/// Literal values in PostgreSQL expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
	/// Integer literal (e.g. `42`, `0x2A`).
	Integer(i64),
	/// Floating-point or numeric literal (e.g. `3.14159`, `1e-4`).
	Float(f64),
	/// String constant (e.g. `'hello'`, `E'escaped\n'`).
	String(String),
	/// Boolean literal (`TRUE` or `FALSE`).
	Boolean(bool),
	/// NULL literal.
	Null,
	/// Bit-string literal (e.g. `B'1010'`, `X'FF'`).
	BitString {
		/// True if hexadecimal bit string.
		is_hex: bool,
		/// Bit string payload.
		value: String,
	},
}

impl fmt::Display for Literal {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Integer(n) => write!(f, "{}", n),
			Self::Float(n) => write!(f, "{}", n),
			Self::String(s) => write!(f, "'{}'", s.replace('\'', "''")),
			Self::Boolean(b) => f.write_str(if *b {
				"TRUE"
			} else {
				"FALSE"
			}),
			Self::Null => f.write_str("NULL"),
			Self::BitString {
				is_hex,
				value,
			} => {
				if *is_hex {
					write!(f, "X'{}'", value)
				} else {
					write!(f, "B'{}'", value)
				}
			}
		}
	}
}
