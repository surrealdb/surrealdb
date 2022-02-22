// The name and version of this build
pub const PKG_NAME: &'static str = env!("CARGO_PKG_NAME");
pub const PKG_VERS: &'static str = env!("CARGO_PKG_VERSION");

// The publicly visible name of the server
pub const SERVER_NAME: &'static str = "SurrealDB";

// The public endpoint for the database administration interface
pub const APP_ENDPOINT: &'static str = "https://app.surrealdb.com";
