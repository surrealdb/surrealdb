#![allow(clippy::needless_pass_by_value)]
// Makes `attribute_preserved` a compile-time guard rather than a convention:
// see its doc comment.
#![deny(deprecated)]
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::Result;
use surrealdb_types::SurrealValue;
use surrealism::{ReadWrite, Transaction, surrealism};

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
///
/// Writes are opted into through the parameter type: `Transaction<ReadWrite>` is
/// what makes this function report `writeable` in the exports manifest, so the
/// host lends it a writeable transaction.
#[surrealism]
async fn create_user(tx: Transaction<ReadWrite>, user: User) -> Result<String> {
	let exists: bool = tx.run("fn::user_exists", (user.name.clone(), user.age)).await?;
	if exists {
		return Ok(format!("User {} already exists", user.name));
	}
	Ok(format!("Created user {} of age {}. Enabled? {}", user.name, user.age, user.enabled))
}

/// Two host calls awaited together, to show that they overlap rather than
/// running back to back. Each `tx` call is a component-model subtask, so the
/// host resolves both while the guest waits once.
#[surrealism]
async fn concurrent_queries(tx: Transaction) -> Result<Vec<surrealdb_types::Value>> {
	let (a, b) = futures::future::try_join(
		tx.sql::<surrealdb_types::Value>("SELECT * FROM ONLY { value: 1 }"),
		tx.sql::<surrealdb_types::Value>("SELECT * FROM ONLY { value: 2 }"),
	)
	.await?;
	Ok(vec![a, b])
}

/// Two reads awaited together, each of which takes `millis` to answer.
///
/// The host runs the calls of one invocation side by side when neither can
/// write, so this takes about `millis` rather than twice that. Paired with
/// [`concurrent_runs`] in the test suite, which measures the same shape for
/// calls the host has to serialise.
#[surrealism]
async fn concurrent_sleeps(tx: Transaction, millis: i64) -> Result<i64> {
	futures::future::try_join(
		tx.sql::<surrealdb_types::Value>(format!("SLEEP {millis}ms")),
		tx.sql::<surrealdb_types::Value>(format!("SLEEP {millis}ms")),
	)
	.await?;
	Ok(2)
}

/// Two calls to `fn::slow` awaited together, each of which takes `millis`.
///
/// A function body is opaque to the host, so it cannot know the call does not
/// write, and these run one at a time: about twice `millis`.
#[surrealism]
async fn concurrent_runs(tx: Transaction, millis: i64) -> Result<i64> {
	futures::future::try_join(
		tx.run::<surrealdb_types::Value>("fn::slow", (millis,)),
		tx.run::<surrealdb_types::Value>("fn::slow", (millis,)),
	)
	.await?;
	Ok(2)
}

/// Advance a sequence through a read-only transaction.
///
/// `sequence::nextval` modifies durable state without any statement appearing
/// in the query: it is a function call, and the counter it moves is not
/// snapshotted by the transaction. A refusal that looked only for statement
/// forms would let this through, which is why it has its own fixture.
#[surrealism]
async fn sneaky_sequence(tx: Transaction) -> Result<surrealdb_types::Value> {
	tx.sql("RETURN sequence::nextval('order_no')").await
}

/// Attempt a bucket write through a read-only transaction, in *method* form.
///
/// `f'bucket:/k'.put(..)` is a `Part::Method`, not a function call: it
/// dispatches straight into the builtin with only its arguments going back
/// through the expression walk, so it reaches `file::put` by a route the
/// call-form fixtures above cannot exercise. Exists to hold that refusal.
#[surrealism]
async fn sneaky_file_method(tx: Transaction, bucket: String) -> Result<surrealdb_types::Value> {
	tx.sql(format!("RETURN f'{bucket}:/evil.txt'.put('data')")).await
}

/// Attempt a write through a read-only transaction.
///
/// The mode marker cannot prevent this on its own: `sql` takes a string. The
/// host refuses it, because this function declared `Transaction` rather than
/// `Transaction<ReadWrite>`. Exists to hold that behaviour in the test suite.
#[surrealism]
async fn sneaky_write(tx: Transaction) -> Result<surrealdb_types::Value> {
	tx.sql("CREATE customer SET name = 'mallory'").await
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
fn test_io() -> Result<String> {
	println!("This is a test message to stdout");
	eprintln!("This is a test message to stderr");
	Ok("I/O test completed".to_string())
}

#[surrealism]
fn test_none_value() -> Result<Vec<surrealdb_types::Value>> {
	Ok(vec![surrealdb_types::Value::None])
}

/// Mutable state in linear memory, shared by every invocation that reaches the
/// same instance pool. A module is cached per namespace/database, so two
/// tenants loading the same package must not observe each other's writes.
static TENANT_STATE: AtomicI64 = AtomicI64::new(0);

/// Store a value in linear memory and return the value it replaced.
#[surrealism]
fn set_state(value: i64) -> i64 {
	TENANT_STATE.swap(value, Ordering::SeqCst)
}

/// Read back whatever `set_state` last stored for this instance pool.
#[surrealism]
fn get_state() -> i64 {
	TENANT_STATE.load(Ordering::SeqCst)
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

/// Echo the input asynchronously, demonstrating async fn support.
#[surrealism]
async fn async_echo(input: String) -> String {
	input
}

/// Async function that can return an error.
#[surrealism]
async fn async_result(should_fail: bool) -> Result<String> {
	if should_fail {
		anyhow::bail!("async failure")
	} else {
		Ok("async success".to_string())
	}
}

/// Fetch a Pokémon's name from a small JSON API by Pokémon name or Pokédex ID.
///
/// `api_base` may name a host rather than an address: the guest resolves it
/// through `wasi:sockets/ip-name-lookup`, which the host filters against the
/// extension's `allow_net`. Add the host to `allow_net` in `package.toml`.
#[surrealism]
async fn fetch_pokemon(api_base: String, name: String) -> Result<String> {
	let url = url::Url::parse(&format!(
		"{}/pokemon/{}",
		api_base.trim_end_matches('/'),
		name.trim_start_matches('/')
	))
	.map_err(|e| anyhow::anyhow!("Invalid API URL: {e}"))?;
	let host = url.host_str().ok_or_else(|| anyhow::anyhow!("API URL missing host"))?;
	let port =
		url.port_or_known_default().ok_or_else(|| anyhow::anyhow!("API URL missing port"))?;

	// `connect` takes `(host, port)` and resolves the name itself, so this works
	// against a real hostname and not just a literal address.
	let mut stream = TcpStream::connect((host, port))
		.map_err(|e| anyhow::anyhow!("HTTP connect to {host}:{port} failed: {e}"))?;
	let path = match url.query() {
		Some(query) => format!("{}?{query}", url.path()),
		None => url.path().to_string(),
	};
	// Build the whole request up front and send it in one `write_all` so it
	// leaves as a single segment where possible, rather than the several small
	// writes `write!` would emit.
	let request = format!(
		"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
	);
	stream
		.write_all(request.as_bytes())
		.map_err(|e| anyhow::anyhow!("HTTP request write failed: {e}"))?;
	stream.flush().map_err(|e| anyhow::anyhow!("HTTP request flush failed: {e}"))?;
	let mut response = String::new();
	stream
		.read_to_string(&mut response)
		.map_err(|e| anyhow::anyhow!("HTTP response read failed: {e}"))?;
	let (headers, body) = response
		.split_once("\r\n\r\n")
		.ok_or_else(|| anyhow::anyhow!("HTTP response missing headers"))?;
	let status = headers
		.lines()
		.next()
		.and_then(|line| line.split_whitespace().nth(1))
		.and_then(|code| code.parse::<u16>().ok())
		.ok_or_else(|| anyhow::anyhow!("HTTP response missing status"))?;
	if !(200..300).contains(&status) {
		anyhow::bail!("API returned HTTP {status}");
	}
	let parsed: serde_json::Value =
		serde_json::from_str(body).map_err(|e| anyhow::anyhow!("Invalid JSON from API: {e}"))?;
	parsed["name"]
		.as_str()
		.map(str::to_string)
		.ok_or_else(|| anyhow::anyhow!("API response missing 'name'"))
}

/// Deprecated on purpose, so that [`attribute_preserved`] has something its
/// `#[allow]` must suppress.
#[deprecated(note = "exists only to give the attribute-preservation guard a target")]
fn deprecated_helper() -> i64 {
	1
}

/// Standing guard that the macro emits the author's own attributes.
///
/// The crate denies `deprecated`, so calling [`deprecated_helper`] compiles
/// only while the `#[allow]` below reaches the emitted function. A macro that
/// rebuilt the item from its visibility, signature and block would drop it, and
/// this stops compiling — which is the failure mode that once ate
/// `#[tokio::main]` and left a tokio-shaped export with no runtime entered,
/// silently, because nothing else in the tree fails when an attribute is lost.
#[surrealism]
#[allow(deprecated)]
fn attribute_preserved() -> i64 {
	deprecated_helper()
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

	/// An export gated by a Cargo feature, inside a `#[surrealism]` mod.
	///
	/// The `extra` feature is off in every ordinary build, so this function is
	/// absent from the built component — and so must be everything the macro
	/// emits for it. Inside a mod is where that can go wrong: the mod's own
	/// attribute macro receives these items with their `#[cfg]` still on them
	/// and walks them itself, so it emits a registration for a function that
	/// the later cfg-strip then removes. (A `#[cfg]` on a *free* function is
	/// evaluated before the attribute macro runs, so that case never reaches
	/// codegen at all.)
	#[surrealism]
	#[cfg(feature = "extra")]
	fn gated(x: i64) -> i64 {
		x
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
