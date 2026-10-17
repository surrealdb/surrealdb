//! Literal values in MySQL AST.

/// A literal value constant.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
	Integer(i64),
	Float(f64),
	String(String),
	IntroducedString {
		charset: String,
		value: String,
	},
	HexString(Vec<u8>),
	IntroducedHexString {
		charset: String,
		value: Vec<u8>,
	},
	BitString(Vec<u8>),
	Boolean(bool),
	Null,
}

impl Literal {
	/// Render literal in MySQL SQL syntax.
	pub fn to_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Integer(n) => write!(f, "{n}"),
			Self::Float(fl) => write!(f, "{fl}"),
			Self::String(s) => {
				write!(f, "'")?;
				for ch in s.chars() {
					match ch {
						'\'' => write!(f, "''")?,
						'\\' => write!(f, "\\\\")?,
						'\n' => write!(f, "\\n")?,
						'\r' => write!(f, "\\r")?,
						'\t' => write!(f, "\\t")?,
						'\0' => write!(f, "\\0")?,
						_ => write!(f, "{ch}")?,
					}
				}
				write!(f, "'")
			}
			Self::IntroducedString {
				charset,
				value,
			} => {
				write!(f, "_{}'", charset)?;
				for ch in value.chars() {
					match ch {
						'\'' => write!(f, "''")?,
						'\\' => write!(f, "\\\\")?,
						'\n' => write!(f, "\\n")?,
						'\r' => write!(f, "\\r")?,
						'\t' => write!(f, "\\t")?,
						'\0' => write!(f, "\\0")?,
						_ => write!(f, "{ch}")?,
					}
				}
				write!(f, "'")
			}
			Self::HexString(bytes) => {
				write!(f, "0x")?;
				for b in bytes {
					write!(f, "{:02X}", b)?;
				}
				Ok(())
			}
			Self::IntroducedHexString {
				charset,
				value,
			} => {
				write!(f, "_{} 0x", charset)?;
				for b in value {
					write!(f, "{:02X}", b)?;
				}
				Ok(())
			}
			Self::BitString(bits) => {
				write!(f, "b'")?;
				for bit in bits {
					write!(f, "{bit}")?;
				}
				write!(f, "'")
			}
			Self::Boolean(b) => {
				if *b {
					write!(f, "TRUE")
				} else {
					write!(f, "FALSE")
				}
			}
			Self::Null => write!(f, "NULL"),
		}
	}
}
