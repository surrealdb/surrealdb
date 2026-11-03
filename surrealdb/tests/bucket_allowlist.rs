//! The embedded engines take the directories a local `file:` bucket may live
//! in from `Config::bucket_folder_allowlist`, since they read no environment.

#![cfg(feature = "kv-mem")]
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;

fn capabilities() -> Capabilities {
	Capabilities::new()
}

fn file_url(dir: &std::path::Path) -> String {
	let raw = dir.to_str().expect("a UTF-8 scratch path");
	let raw = raw.strip_prefix(r"\\?\").unwrap_or(raw).replace('\\', "/");
	if raw.starts_with('/') {
		format!("file://{raw}")
	} else {
		format!("file:///{raw}")
	}
}

async fn write_a_file(config: Config, dir: &std::path::Path) -> surrealdb::Result<()> {
	let db = Surreal::new::<Mem>(config).await?;
	db.use_ns("test").use_db("test").await?;
	db.query(format!(r#"DEFINE BUCKET b BACKEND "{}" PERMISSIONS FULL;"#, file_url(dir)))
		.await?
		.check()?;
	db.query("RETURN type::file('b', '/hello.txt').put(<bytes>'hello');").await?.check()?;
	Ok(())
}

#[tokio::test]
async fn an_allowlisted_directory_hosts_a_file_bucket() {
	let dir = std::env::temp_dir().join(format!("surreal-bucket-allowlist-{}", std::process::id()));
	std::fs::create_dir_all(&dir).expect("a scratch directory");
	let dir = std::fs::canonicalize(&dir).expect("a canonical scratch directory");

	let allowed = Config::new().capabilities(capabilities()).bucket_folder_allowlist([&dir]);
	write_a_file(allowed, &dir).await.expect("an allowlisted directory accepts the bucket");
	assert_eq!(std::fs::read(dir.join("hello.txt")).expect("the file is on disk"), b"hello");

	let unlisted = Config::new().capabilities(capabilities());
	let refused = write_a_file(unlisted, &dir).await.expect_err("no allowlist refuses every path");
	assert!(refused.to_string().contains("File access denied"), "{refused}");

	let trailing_space = dir.join("buckets ");
	let without_space = dir.join("buckets");
	let spaced =
		Config::new().capabilities(capabilities()).bucket_folder_allowlist([&trailing_space]);
	let refused = write_a_file(spaced, &without_space)
		.await
		.expect_err("trailing whitespace on an allowlist entry is preserved, not trimmed");
	assert!(refused.to_string().contains("File access denied"), "{refused}");

	std::fs::remove_dir_all(&dir).ok();
}
