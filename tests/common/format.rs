use std::string::ToString;

#[derive(Debug, Copy, Clone)]
pub enum Format {
	Json,
	Cbor,
	Pack,
}

impl ToString for Format {
	fn to_string(&self) -> String {
		match self {
			Self::Json => "json".to_owned(),
			Self::Cbor => "cbor".to_owned(),
			Self::Pack => "msgpack".to_owned(),
		}
	}
}
