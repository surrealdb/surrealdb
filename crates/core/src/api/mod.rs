pub mod body;
pub mod context;
pub mod err;
pub mod invocation;
pub mod middleware;
pub mod path;
pub mod request;
pub mod response;

pub mod format {
	//! MIME types

	pub const JSON: &str = "application/json";
	pub const CBOR: &str = "application/cbor";
	pub const FLATBUFFERS: &str = "application/vnd.surrealdb.flatbuffers";

	pub const PLAIN: &str = "text/plain";
	pub const OCTET_STREAM: &str = "application/octet-stream";
}
