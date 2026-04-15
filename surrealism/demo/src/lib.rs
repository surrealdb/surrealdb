use std::sync::OnceLock;

use anyhow::Result;
use surrealdb_types::SurrealValue;
use surrealism::surrealism;

/// Check whether a person is old enough to drive.
#[surrealism]
fn can_drive(age: i64) -> bool {
	age >= 18
}

#[derive(Debug, SurrealValue)]
struct User {
	name: String,
	age: i64,
	enabled: bool,
}

/// Create a new user record if one does not already exist.
#[surrealism(writeable)]
fn create_user(user: User) -> Result<String> {
	let exists: bool =
		surrealism::run("fn::user_exists".to_string(), None, (user.name.clone(), user.age))?;
	if exists {
		return Ok(format!("User {} already exists", user.name));
	}
	Ok(format!("Created user {} of age {}. Enabled? {}", user.name, user.age, user.enabled))
}

#[surrealism(name = "other")]
fn can_drive_bla(age: i64) -> bool {
	age >= 18
}

/// Check whether a person is old enough to drink.
#[surrealism]
fn can_drink(#[name = "person_age"] age: i64) -> bool {
	age >= 21
}

#[surrealism(default)]
fn def(age: i64) -> bool {
	age >= 18
}

/// This doc comment is overridden by the explicit comment below.
#[surrealism(comment = "Divide two integers, returning an error on division by zero.")]
fn safe_divide(a: i64, b: i64) -> Result<i64, String> {
	if b == 0 {
		Err("Division by zero".to_string())
	} else {
		Ok(a / b)
	}
}

#[surrealism]
fn parse_number(input: String) -> Result<i64, std::num::ParseIntError> {
	input.parse::<i64>()
}

#[surrealism]
fn result(should_fail: bool) -> Result<String> {
	if should_fail {
		anyhow::bail!("Failed")
	} else {
		Ok("Success".to_string())
	}
}

#[surrealism]
fn test_kv() -> Result<()> {
	surrealism::kv::set("test", 0).expect("set test");
	let tmp: Option<i64> = surrealism::kv::get("test").expect("get test");
	assert_eq!(tmp, Some(0), "get test value");
	surrealism::kv::del("test").expect("del test");
	let exists = surrealism::kv::exists("test").expect("exists test");
	assert!(!exists, "test should not exist after delete");

	surrealism::kv::set("test1", 1).expect("set test1");
	surrealism::kv::set("test2", 2).expect("set test2");
	surrealism::kv::set("test3", 3).expect("set test3");
	surrealism::kv::set("test4", 4).expect("set test4");
	surrealism::kv::set("test5", 5).expect("set test5");
	surrealism::kv::set("test6", 6).expect("set test6");

	let keys = surrealism::kv::keys(..).expect("keys");
	assert_eq!(keys, vec!["test1", "test2", "test3", "test4", "test5", "test6"], "keys");
	let values: Vec<i64> = surrealism::kv::values(..).expect("values");
	assert_eq!(values, vec![1, 2, 3, 4, 5, 6], "values");
	let entries: Vec<(String, i64)> = surrealism::kv::entries(..).expect("entries");
	assert_eq!(
		entries,
		vec![
			("test1".to_string(), 1),
			("test2".to_string(), 2),
			("test3".to_string(), 3),
			("test4".to_string(), 4),
			("test5".to_string(), 5),
			("test6".to_string(), 6),
		],
		"entries"
	);
	let count = surrealism::kv::count(..).expect("count");
	assert_eq!(count, 6, "count");

	let keys_to_4 = surrealism::kv::keys(.."test4".to_string()).expect("keys_to_4");
	assert_eq!(keys_to_4, vec!["test1", "test2", "test3"], "keys_to_4");
	let values_to_4: Vec<i64> = surrealism::kv::values(.."test4".to_string()).expect("values_to_4");
	assert_eq!(values_to_4, vec![1, 2, 3], "values_to_4");
	let entries_to_4: Vec<(String, i64)> =
		surrealism::kv::entries(.."test4".to_string()).expect("entries_to_4");
	assert_eq!(
		entries_to_4,
		vec![("test1".to_string(), 1), ("test2".to_string(), 2), ("test3".to_string(), 3),],
		"entries_to_4"
	);
	let count_to_4 = surrealism::kv::count(.."test4".to_string()).expect("count_to_4");
	assert_eq!(count_to_4, 3, "count_to_4");

	let batch = surrealism::kv::get_batch(vec!["test1", "test3", "test5"]).expect("get_batch");
	assert_eq!(batch, vec![Some(1), Some(3), Some(5)], "get_batch values");
	surrealism::kv::set_batch(vec![("test1", 10), ("test3", 30), ("test5", 50)])
		.expect("set_batch");
	let values: Vec<i64> = surrealism::kv::values(..).expect("values after set_batch");
	assert_eq!(values, vec![10, 2, 30, 4, 50, 6], "values after set_batch");
	surrealism::kv::del_batch(vec!["test2", "test4", "test6"]).expect("del_batch");
	let values: Vec<i64> = surrealism::kv::values(..).expect("values after del_batch");
	assert_eq!(values, vec![10, 30, 50], "values after del_batch");

	surrealism::kv::del_rng(.."test4".to_string()).expect("del_rng_to_4");
	let values: Vec<i64> = surrealism::kv::values(..).expect("values after del_rng_to_4");
	assert_eq!(values, vec![50], "values after del_rng_to_4");
	surrealism::kv::del_rng(..).expect("del_rng");
	let count = surrealism::kv::count(..).expect("count after del_rng");
	assert_eq!(count, 0, "count after del_rng");

	surrealism::kv::set("a", 1).expect("set a");
	surrealism::kv::set("b", 2).expect("set b");
	surrealism::kv::set("c", 3).expect("set c");
	surrealism::kv::set("test", 10).expect("set test");
	surrealism::kv::set("z", 26).expect("set z");

	let values_a_z: Vec<i64> =
		surrealism::kv::values("a".to_string().."z".to_string()).expect("values a..z");
	assert_eq!(values_a_z, vec![1, 2, 3, 10], "values a..z");
	let values_a_ze: Vec<i64> =
		surrealism::kv::values("a".to_string()..="z".to_string()).expect("values a..=z");
	assert_eq!(values_a_ze, vec![1, 2, 3, 10, 26], "values a..=z");
	let values_test_on: Vec<i64> =
		surrealism::kv::values("test".to_string()..).expect("values test..");
	assert_eq!(values_test_on, vec![10, 26], "values test..");
	let values_to_test: Vec<i64> =
		surrealism::kv::values(.."test".to_string()).expect("values ..test");
	assert_eq!(values_to_test, vec![1, 2, 3], "values ..test");
	let values_to_teste: Vec<i64> =
		surrealism::kv::values(..="test".to_string()).expect("values ..=test");
	assert_eq!(values_to_teste, vec![1, 2, 3, 10], "values ..=test");

	println!("kv test passed");

	Ok(())
}

#[surrealism]
fn test_io() -> Result<String> {
	println!("This is a test message to stdout");
	eprintln!("This is a test message to stderr");
	Ok("I/O test completed".to_string())
}

#[surrealism]
fn test_none_value() -> Result<Vec<surrealdb_types::Value>> {
	Ok(vec![surrealdb_types::Value::None])
}

static GREETING_CACHE: OnceLock<String> = OnceLock::new();

#[surrealism(init)]
fn init_greeting() -> Result<()> {
	GREETING_CACHE.get_or_init(|| match std::fs::read_to_string("/greeting.txt") {
		Ok(content) => content,
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
			eprintln!("greeting.txt not found, using default");
			"Hello".to_string()
		}
		Err(e) => {
			eprintln!("Failed to read greeting.txt: {e}");
			"Hello".to_string()
		}
	});
	Ok(())
}

#[surrealism]
fn cached_greeting() -> Result<String> {
	let greeting = GREETING_CACHE.get().ok_or(anyhow::anyhow!("greeting not loaded"))?;
	Ok(greeting.clone())
}

#[surrealism]
fn read_greeting() -> Result<String> {
	std::fs::read_to_string("/greeting.txt")
		.map_err(|e| anyhow::anyhow!("Failed to read /greeting.txt: {e}"))
}

#[surrealism]
fn read_config_version() -> Result<i64> {
	let raw = std::fs::read_to_string("/data/config.json")
		.map_err(|e| anyhow::anyhow!("Failed to read /data/config.json: {e}"))?;
	let parsed: serde_json::Value = serde_json::from_str(&raw)
		.map_err(|e| anyhow::anyhow!("Failed to parse config.json: {e}"))?;
	parsed["version"]
		.as_i64()
		.ok_or_else(|| anyhow::anyhow!("version field missing or not an integer"))
}

#[surrealism]
fn list_fs_root() -> Result<Vec<String>> {
	let mut entries: Vec<String> = std::fs::read_dir("/")
		.map_err(|e| anyhow::anyhow!("Failed to read /: {e}"))?
		.filter_map(|entry| entry.ok().map(|e| e.file_name().to_string_lossy().to_string()))
		.collect();
	entries.sort();
	Ok(entries)
}

#[surrealism]
fn kv_set_value(key: String, value: i64) -> Result<()> {
	surrealism::kv::set(&key, value)?;
	Ok(())
}

#[surrealism]
fn kv_get_value(key: String) -> Result<Option<i64>> {
	surrealism::kv::get(&key)
}

// ---------------------------------------------------------------------------
// Module namespace demo: exercises #[surrealism] on mod blocks
// ---------------------------------------------------------------------------

#[surrealism]
mod math {
	#[surrealism(default)]
	fn double(x: i64) -> i64 {
		x * 2
	}

	/// Add two integers.
	#[surrealism]
	fn add(a: i64, b: i64) -> i64 {
		a + b
	}

	#[surrealism(name = "multiply")]
	fn mul(a: i64, b: i64) -> i64 {
		a * b
	}
}

#[surrealism(name = "util")]
mod utility_helpers {
	#[surrealism(default)]
	fn identity(x: i64) -> i64 {
		x
	}

	#[surrealism(name = "negate")]
	fn neg(x: i64) -> i64 {
		-x
	}

	#[surrealism]
	mod nested {
		#[surrealism]
		fn deep(x: i64) -> i64 {
			x + 100
		}
	}
}
