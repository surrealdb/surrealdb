// The name and version of this build
pub const PKG_NAME: &'static str = env!("CARGO_PKG_NAME");
pub const PKG_VERS: &'static str = env!("CARGO_PKG_VERSION");

// The publicly visible name of the server
pub const SERVER_NAME: &'static str = "SurrealDB";

// The public endpoint for the database administration interface
pub const APP_ENDPOINT: &'static str = "https://app.surrealdb.com";

// Specifies how many subqueries will be processed recursively before the query fails.
pub const MAX_RECURSIVE_QUERIES: usize = 16;

// The characters which are supported in server record IDs.
pub const ID_CHARS: [char; 36] = [
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];
