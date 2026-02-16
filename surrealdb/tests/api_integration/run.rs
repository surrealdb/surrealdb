#![allow(clippy::unwrap_used)]

// API integration tests for the `run` method (running SurrealQL functions)

use surrealdb::opt::Config;
use ulid::Ulid;

use super::CreateDb;

define_include_tests!(run => {
	#[test_log::test(tokio::test)]
	run_no_args,
	#[test_log::test(tokio::test)]
	run_with_single_arg,
	#[test_log::test(tokio::test)]
	run_with_multiple_args,
	#[test_log::test(tokio::test)]
	run_builtin_math,
	#[test_log::test(tokio::test)]
	run_custom_function_string,
});

pub async fn run_no_args(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;
	let ns = Ulid::new().to_string();
	let dbn = Ulid::new().to_string();
	db.use_ns(&ns).use_db(&dbn).await.unwrap();

	db.query("DEFINE FUNCTION fn::answer() { RETURN 42; }")
		.await
		.unwrap()
		.check()
		.unwrap();

	let result: i64 = db.run("fn::answer").await.unwrap();
	assert_eq!(result, 42);

	drop(permit);
}

pub async fn run_with_single_arg(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;
	let ns = Ulid::new().to_string();
	let dbn = Ulid::new().to_string();
	db.use_ns(&ns).use_db(&dbn).await.unwrap();

	db.query("DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2; }")
		.await
		.unwrap()
		.check()
		.unwrap();

	let result: i64 = db.run("fn::double").args(21).await.unwrap();
	assert_eq!(result, 42);

	drop(permit);
}

pub async fn run_with_multiple_args(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;
	let ns = Ulid::new().to_string();
	let dbn = Ulid::new().to_string();
	db.use_ns(&ns).use_db(&dbn).await.unwrap();

	// math::log(base, exponent) returns log_base(exponent), e.g. math::log(10, 100) = 2
	let result: f64 = db.run("math::log").args((100, 10)).await.unwrap();
	assert!((result - 2.0).abs() < 1e-10);

	drop(permit);
}

pub async fn run_builtin_math(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;
	let ns = Ulid::new().to_string();
	let dbn = Ulid::new().to_string();
	db.use_ns(&ns).use_db(&dbn).await.unwrap();

	let abs_result: i64 = db.run("math::abs").args(-42).await.unwrap();
	assert_eq!(abs_result, 42);

	// math::max() expects a single array argument, not multiple arguments
	let max_result: i64 = db
		.run("math::max")
		.args(vec![vec![1_i64, 2, 3, 4, 5, 6]])
		.await
		.unwrap();
	assert_eq!(max_result, 6);

	drop(permit);
}

pub async fn run_custom_function_string(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;
	let ns = Ulid::new().to_string();
	let dbn = Ulid::new().to_string();
	db.use_ns(&ns).use_db(&dbn).await.unwrap();

	db.query("DEFINE FUNCTION fn::greet($name: string) { RETURN 'Hello, ' + $name + '!'; }")
		.await
		.unwrap()
		.check()
		.unwrap();

	let result: String = db.run("fn::greet").args("world").await.unwrap();
	assert_eq!(result, "Hello, world!");

	drop(permit);
}
