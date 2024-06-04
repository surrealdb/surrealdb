use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::thread::Builder;

use surrealdb::dbs::capabilities::Capabilities;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::{Auth, Level, Role};
use surrealdb::kvs::Datastore;
use surrealdb_core::dbs::Response;
use surrealdb_core::sql::{value, Value};

pub async fn new_ds() -> Result<Datastore, Error> {
	Ok(Datastore::new("memory").await?.with_capabilities(Capabilities::all()).with_notifications())
}

#[allow(dead_code)]
pub async fn iam_run_case(
	prepare: &str,
	test: &str,
	check: &str,
	check_expected_result: &[&str],
	ds: &Datastore,
	sess: &Session,
	should_succeed: bool,
) -> Result<(), Box<dyn std::error::Error>> {
	// Use the session as the test statement, but change the Auth to run the check with full permissions
	let mut owner_sess = sess.clone();
	owner_sess.au = Arc::new(Auth::for_root(Role::Owner));

	// Prepare statement
	{
		if !prepare.is_empty() {
			let resp = ds.execute(prepare, &owner_sess, None).await.unwrap();
			for r in resp.into_iter() {
				let tmp = r.output();
				if tmp.is_err() {
					return Err(format!("Prepare statement failed: {}", tmp.unwrap_err()).into());
				}
			}
		}
	}

	// Execute statement
	let mut resp = ds.execute(test, sess, None).await.unwrap();

	// Check datastore state first
	{
		let resp = ds.execute(check, &owner_sess, None).await.unwrap();
		if resp.len() != check_expected_result.len() {
			return Err(format!(
				"Check statement failed for test: expected {} results, got {}",
				check_expected_result.len(),
				resp.len()
			)
			.into());
		}

		for (i, r) in resp.into_iter().enumerate() {
			let tmp = r.output();
			if tmp.is_err() {
				return Err(
					format!("Check statement errored for test: {}", tmp.unwrap_err()).into()
				);
			}

			let tmp = tmp.unwrap().to_string();
			if tmp != check_expected_result[i] {
				return Err(format!(
					"Check statement failed for test: expected value '{}' doesn't match '{}'",
					check_expected_result[i], tmp
				)
				.into());
			}
		}
	}

	// Check statement result. If the statement should succeed, check that the result is Ok, otherwise check that the result is a 'Not Allowed' error
	let res = resp.pop().unwrap().output();
	if should_succeed {
		if res.is_err() {
			return Err(format!("Test statement failed: {}", res.unwrap_err()).into());
		}
	} else {
		if res.is_ok() {
			return Err(
				format!("Test statement succeeded when it should have failed: {:?}", res).into()
			);
		}

		let err = res.unwrap_err().to_string();
		if !err.contains("Not enough permissions to perform this action") {
			return Err(format!("Test statement failed with unexpected error: {}", err).into());
		}
	}
	Ok(())
}

type CaseIter<'a> = std::slice::Iter<'a, ((Level, Role), (&'a str, &'a str), bool)>;

#[allow(dead_code)]
pub async fn iam_check_cases(
	cases: CaseIter<'_>,
	scenario: &HashMap<&str, &str>,
	check_results: [Vec<&str>; 2],
) -> Result<(), Box<dyn std::error::Error>> {
	let prepare = scenario.get("prepare").unwrap();
	let test = scenario.get("test").unwrap();
	let check = scenario.get("check").unwrap();

	for ((level, role), (ns, db), should_succeed) in cases {
		println!("* Testing '{test}' for '{level}Actor({role})' on '({ns}, {db})'");
		let sess = Session::for_level(level.to_owned(), role.to_owned()).with_ns(ns).with_db(db);
		let expected_result = if *should_succeed {
			check_results.first().unwrap()
		} else {
			check_results.get(1).unwrap()
		};
		// Auth enabled
		{
			let ds = new_ds().await.unwrap().with_auth_enabled(true);
			iam_run_case(prepare, test, check, expected_result, &ds, &sess, *should_succeed)
				.await?;
		}

		// Auth disabled
		{
			let ds = new_ds().await.unwrap().with_auth_enabled(false);
			iam_run_case(prepare, test, check, expected_result, &ds, &sess, *should_succeed)
				.await?;
		}
	}

	// Anonymous user
	let ns = "NS";
	let db = "DB";
	for auth_enabled in [true, false].into_iter() {
		{
			println!(
				"* Testing '{test}' for 'Anonymous' on '({ns}, {db})' with {auth_enabled}",
				auth_enabled = if auth_enabled {
					"auth enabled"
				} else {
					"auth disabled"
				}
			);
			let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);
			let expected_result = if auth_enabled {
				check_results.get(1).unwrap()
			} else {
				check_results.first().unwrap()
			};
			iam_run_case(
				prepare,
				test,
				check,
				expected_result,
				&ds,
				&Session::default().with_ns(ns).with_db(db),
				!auth_enabled,
			)
			.await?;
		}
	}

	Ok(())
}

#[allow(dead_code)]
pub fn with_enough_stack(
	fut: impl Future<Output = Result<(), Error>> + Send + 'static,
) -> Result<(), Error> {
	#[allow(unused_mut)]
	let mut builder = Builder::new();

	// Roughly how much stack is allocated for surreal server workers in release mode
	#[cfg(not(debug_assertions))]
	{
		builder = builder.stack_size(10_000_000);
	}

	// Same for debug mode
	#[cfg(debug_assertions)]
	{
		builder = builder.stack_size(24_000_000);
	}

	builder
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
			runtime.block_on(fut)
		})
		.unwrap()
		.join()
		.unwrap()
}

#[allow(dead_code)]
pub fn skip_ok(res: &mut Vec<Response>, skip: usize) {
	for i in 0..skip {
		if res.is_empty() {
			panic!("No more result #{i}");
		}
		let r = res.remove(0).result;
		let _ = r.is_err_and(|e| {
			panic!("Statement #{i} fails with: {e}");
		});
	}
}

#[allow(dead_code)]
pub struct Test {
	pub ds: Datastore,
	pub session: Session,
	pub responses: Vec<Response>,
	pos: usize,
}

impl Debug for Test {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "Responses left: {:?}.", self.responses)
	}
}

impl Test {
	#[allow(dead_code)]
	pub async fn new(sql: &str) -> Self {
		Self::try_new(sql).await.unwrap_or_else(|e| panic!("{e}"))
	}

	#[allow(dead_code)]
	pub async fn try_new(sql: &str) -> Result<Self, Error> {
		let ds = new_ds().await?;
		let session = Session::owner().with_ns("test").with_db("test");
		let responses = ds.execute(sql, &session, None).await?;
		Ok(Self {
			ds,
			session,
			responses,
			pos: 0,
		})
	}

	#[allow(dead_code)]
	pub fn size(&mut self, expected: usize) -> &mut Self {
		assert_eq!(
			self.responses.len(),
			expected,
			"Unexpected number of results: {} - Expected: {expected}",
			self.responses.len()
		);
		self
	}

	#[allow(dead_code)]
	pub fn next(&mut self) -> Response {
		if self.responses.is_empty() {
			panic!("No response left - last position: {}", self.pos);
		}
		self.pos += 1;
		self.responses.remove(0)
	}

	#[allow(dead_code)]
	pub fn skip_ok(&mut self, skip: usize) -> &mut Self {
		skip_ok(&mut self.responses, skip);
		self.pos += skip;
		self
	}

	#[allow(dead_code)]
	pub fn expect_value(&mut self, val: Value) -> &mut Self {
		let tmp = self
			.next()
			.result
			.unwrap_or_else(|e| panic!("Unexpected error: {e} - Index: {}", self.pos));
		// Then check they are indeed the same values
		//
		// If it is a constant we need to transform it as a number
		let val = if let Value::Constant(c) = val {
			c.compute().unwrap_or_else(|e| panic!("Can't convert constant {c} - {e}"))
		} else {
			val
		};
		if val.is_nan() {
			assert!(tmp.is_nan(), "Expected NaN but got: {tmp}");
		} else {
			assert_eq!(tmp, val, "{tmp:#}");
		}
		//
		self
	}

	#[allow(dead_code)]
	pub fn expect_values(&mut self, values: &[Value]) -> &mut Self {
		for value in values {
			self.expect_value(value.clone());
		}
		self
	}

	#[allow(dead_code)]
	pub fn expect_val(&mut self, val: &str) -> &mut Self {
		self.expect_value(value(val).unwrap())
	}

	#[allow(dead_code)]
	pub fn expect_vals(&mut self, vals: &[&str]) -> &mut Self {
		for val in vals {
			self.expect_val(val);
		}
		self
	}

	#[allow(dead_code)]
	pub fn expect_error(&mut self, error: &str) -> &mut Self {
		let tmp = self.next().result;
		assert!(
			matches!(
				&tmp,
				Err(e) if e.to_string() == error
			),
			"{tmp:?} didn't match {error}"
		);
		self
	}

	#[allow(dead_code)]
	pub fn expect_errors(&mut self, errors: &[&str]) -> &mut Self {
		for error in errors {
			self.expect_error(error);
		}
		self
	}
}

impl Drop for Test {
	fn drop(&mut self) {
		if !self.responses.is_empty() {
			panic!("Not every response has been checked");
		}
	}
}
