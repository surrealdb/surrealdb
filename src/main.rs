// This binary delegates to the library entrypoint so both binary and
// embedded use-cases share the same runtime bootstrap and initialization.
#![allow(deprecated)]
#![deny(clippy::mem_forget)]

use std::process::ExitCode;

use surrealdb_core::CommunityComposer;
use surrealdb_server::init;

fn main() -> ExitCode {
	// Use the default storage flavor and default HTTP router shipped with the binary
	init(CommunityComposer())
}
