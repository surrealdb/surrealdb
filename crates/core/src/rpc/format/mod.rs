pub mod bincode;
pub mod cbor;
pub mod json;
pub mod revision;

pub const PROTOCOLS: [&str; 4] = [
	"json",     // For basic JSON serialisation
	"cbor",     // For basic CBOR serialisation
	"bincode",  // For full internal serialisation
	"revision", // For full versioned serialisation
];

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Format {
	Json,        // For basic JSON serialisation
	Cbor,        // For basic CBOR serialisation
	Bincode,     // For full internal serialisation
	Revision,    // For full versioned serialisation
	Unsupported, // Unsupported format
}

impl From<&str> for Format {
	fn from(v: &str) -> Self {
		match v {
			"json" => Format::Json,
			"cbor" => Format::Cbor,
			"bincode" => Format::Bincode,
			"revision" => Format::Revision,
			_ => Format::Unsupported,
		}
	}
}
