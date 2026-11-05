pub mod cbor;
pub mod encoded;
pub mod flatbuffers;
pub mod json;
#[cfg(all(test, feature = "shaped"))]
mod request_blocks_tests;
#[cfg(all(test, feature = "shaped"))]
pub(crate) mod shaped_tests;

pub use encoded::decode_rows;

/// The WebSocket subprotocols this build serves.
#[cfg(feature = "shaped")]
pub const PROTOCOLS: [&str; 5] = [
	"json",        // For basic JSON serialisation
	"cbor.2",      // For CBOR v2 serialisation
	"cbor",        // For basic CBOR serialisation
	"flatbuffers", // For flatbuffers serialisation
	"shaped",      // For CBOR with row results as shaped frames
];

/// The WebSocket subprotocols this build serves.
#[cfg(not(feature = "shaped"))]
pub const PROTOCOLS: [&str; 4] = [
	"json",        // For basic JSON serialisation
	"cbor.2",      // For CBOR v2 serialisation
	"cbor",        // For basic CBOR serialisation
	"flatbuffers", // For flatbuffers serialisation
];

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Format {
	Json,        // For basic JSON serialisation
	Cbor,        // For basic CBOR serialisation
	Cbor2,       // For CBOR v2 serialisation
	Flatbuffers, // For flatbuffers serialisation
	/// CBOR, with every list of result rows written as a shaped block
	/// (`surrealdb_types::shaped`).
	Shaped,
	Unsupported, // Unsupported format
}

impl Format {
	/// The encoding the executor can write this format's rows in, for a
	/// format whose responses splice encoded rows.
	pub fn row_format(&self) -> Option<surrealdb_rpc::RowFormat> {
		match self {
			Format::Json => Some(surrealdb_rpc::RowFormat::Json),
			Format::Cbor => Some(surrealdb_rpc::RowFormat::Cbor),
			Format::Cbor2 => Some(surrealdb_rpc::RowFormat::Cbor2),
			#[cfg(feature = "shaped")]
			Format::Shaped => Some(surrealdb_rpc::RowFormat::Shaped),
			#[cfg(not(feature = "shaped"))]
			Format::Shaped => None,
			Format::Flatbuffers | Format::Unsupported => None,
		}
	}
}

impl Format {
	/// The shaped format where this build serves it, and [`Format::Unsupported`] where it does
	/// not: a request for shaped frames from a build without them is answered as one for any
	/// format it does not serve.
	pub const fn shaped() -> Format {
		if cfg!(feature = "shaped") {
			Format::Shaped
		} else {
			Format::Unsupported
		}
	}
}

impl From<&str> for Format {
	fn from(v: &str) -> Self {
		match v {
			"json" => Format::Json,
			"cbor" => Format::Cbor,
			"cbor.2" => Format::Cbor2,
			"flatbuffers" => Format::Flatbuffers,
			"shaped" => Format::shaped(),
			_ => Format::Unsupported,
		}
	}
}
