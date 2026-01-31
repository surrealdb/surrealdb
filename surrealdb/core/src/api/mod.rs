pub mod err;
pub mod invocation;
pub mod middleware;
pub mod path;
pub mod request;
pub mod response;

pub mod format {
	//! MIME type string constants for use in HTTP headers

	pub const JSON: &str = "application/json";
	pub const CBOR: &str = "application/cbor";
	pub const FLATBUFFERS: &str = "application/vnd.surrealdb.flatbuffers";
	pub const NATIVE: &str = "application/vnd.surrealdb.native";

	pub const PLAIN: &str = "text/plain";
	pub const OCTET_STREAM: &str = "application/octet-stream";
}
