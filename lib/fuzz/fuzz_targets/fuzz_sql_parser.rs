#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
	// Don't crash.
	_ = surrealdb::sql::parse(data);
});
