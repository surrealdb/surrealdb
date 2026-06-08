#![no_main]

use std::time::Duration;

use libfuzzer_sys::fuzz_target;

/// Per-command wall-clock bound, so a pathological input that bypasses the
/// engine's own deadline still can't stall the fuzzer.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

fuzz_target!(|commands: &str| {
	let commands: Vec<&str> = commands.split_inclusive(";").collect();
	let blacklisted_command_strings = ["sleep", "SLEEP"];

	use surrealdb_core::{dbs::Session, kvs::Datastore};
	let max_commands = 500;
	if commands.len() > max_commands {
		return;
	}

	tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
		let dbs = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().with_ns("test").with_db("test");
		for command in commands.iter() {
			for blacklisted_string in blacklisted_command_strings.iter() {
				if command.contains(blacklisted_string) {
					return;
				}
			}
			let _ = tokio::time::timeout(COMMAND_TIMEOUT, dbs.execute(command, &ses, None)).await;
		}
	})
});
