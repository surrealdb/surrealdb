pub mod cbor;
pub mod flatbuffers;
pub mod json;

pub const PROTOCOLS: [&str; 3] = [
	"json",        // For basic JSON serialisation
	"cbor",        // For basic CBOR serialisation
	"flatbuffers", // For flatbuffers serialisation
];

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Format {
	Json,        // For basic JSON serialisation
	Cbor,        // For basic CBOR serialisation
	Flatbuffers, // For flatbuffers serialisation
	Unsupported, // Unsupported format
}

impl From<&str> for Format {
	fn from(v: &str) -> Self {
		match v {
			"json" => Format::Json,
			"cbor" => Format::Cbor,
			"flatbuffers" => Format::Flatbuffers,
			_ => Format::Unsupported,
		}
	}
}
