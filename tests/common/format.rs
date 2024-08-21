use std::{fmt, string::ToString};

#[derive(Debug, Copy, Clone)]
pub enum Format {
	Json,
	Cbor,
	Pack,
}

impl fmt::Display for Format {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Json => "json".fmt(f),
			Self::Cbor => "cbor".fmt(f),
			Self::Pack => "msgpack".fmt(f),
		}
	}
}
