//! MIME type string constants for use in HTTP headers

pub const ANY: &str = "*/*";

pub const JSON: &str = "application/json";
pub const CBOR: &str = "application/cbor";
pub const CBOR2: &str = "application/cbor; v=2";
pub const FLATBUFFERS: &str = "application/vnd.surrealdb.flatbuffers";
pub const SHAPED: &str = "application/vnd.surrealdb.shaped";
pub const NATIVE: &str = "application/vnd.surrealdb.native";

pub const PLAIN: &str = "text/plain";
pub const OCTET_STREAM: &str = "application/octet-stream";
