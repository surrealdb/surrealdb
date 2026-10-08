pub mod cbor;
pub mod encoded;
pub mod flatbuffers;
pub mod json;

pub use encoded::decode_rows;

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

impl Format {
	/// The encoding the executor can write this format's rows in, for a
	/// format whose responses splice encoded rows.
	pub fn row_format(&self) -> Option<surrealdb_rpc::RowFormat> {
		match self {
			Format::Json => Some(surrealdb_rpc::RowFormat::Json),
			Format::Cbor => Some(surrealdb_rpc::RowFormat::Cbor),
			Format::Flatbuffers | Format::Unsupported => None,
		}
	}
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
