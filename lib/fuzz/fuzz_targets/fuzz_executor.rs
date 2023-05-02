#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|commands: Vec<&str>| {
	let blacklisted_command_strings = ["sleep"];

	use surrealdb::{dbs::Session, kvs::Datastore};
	let max_commands = 500;
	if commands.len() > max_commands {
		return;
	}

	futures::executor::block_on(async {
		let dbs = Datastore::new("memory").await.unwrap();
		let ses = Session::for_kv().with_ns("test").with_db("test");
		for command in commands.iter() {
			for blacklisted_string in blacklisted_command_strings.iter() {
				if command.contains(blacklisted_string) {
					return;
				}
			}
			let _ignore_the_result = dbs.execute(command, &ses, None, false).await;

			// TODO: Add some async timeout and `tokio::select!` between it and the query
			// Alternatively, wrap future in `tokio::time::Timeout`.
		}
	});
});
