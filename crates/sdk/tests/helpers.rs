#![cfg(test)]

use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::thread::Builder;

use anyhow::ensure;
use regex::Regex;
use surrealdb::Result;
use surrealdb_core::dbs::capabilities::Capabilities;
use surrealdb_core::dbs::{Response, Session};
use surrealdb_core::iam::{Auth, Level, Role};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::syn;
use surrealdb_core::val::{Number, Value};

pub async fn new_ds() -> Result<Datastore> {
	Ok(Datastore::new("memory").await?.with_capabilities(Capabilities::all()).with_notifications())
}

#[allow(dead_code)]
#[expect(clippy::too_many_arguments)]
pub async fn iam_run_case(
	test_index: i32,
	prepare: &str,
	test: &str,
	check: &str,
	check_expected_result: &str,
	ds: &Datastore,
	sess: &Session,
	use_ns: bool,
	use_db: bool,
	should_succeed: bool,
) -> Result<()> {
	// Use the session as the test statement, but change the Auth to run the check
	// with full permissions
	let mut owner_sess = sess.clone();
	owner_sess.au = Arc::new(Auth::for_root(Role::Owner));

	if use_ns {
		if let Some(ns) = &sess.ns {
			ds.execute(&format!("USE NS {ns}"), &owner_sess, None).await.unwrap();
		}
	}
	if use_db {
		if let Some(db) = &sess.db {
			ds.execute(&format!("USE DB {db}"), &owner_sess, None).await.unwrap();
		}
	}

	// Prepare statement
	{
		if !prepare.is_empty() {
			let resp = ds.execute(prepare, &owner_sess, None).await.unwrap();
			for r in resp.into_iter() {
				let tmp = r.output();
				ensure!(tmp.is_ok(), "Prepare statement failed: {}", tmp.unwrap_err());
			}
		}
	}

	// Execute statement
	let mut resp = ds.execute(test, sess, None).await.unwrap();

	// Check datastore state first
	{
		let mut resp = ds.execute(check, &owner_sess, None).await.unwrap();
		assert_eq!(
			resp.len(),
			1,
			"Check statement failed for test {test_index} ({test}): expected 1 result, got {}",
			resp.len()
		);

		let tmp = resp.pop().unwrap().output();
		ensure!(
			tmp.is_ok(),
			"Check statement errored for test {test_index} ({test}): {}",
			tmp.unwrap_err()
		);

		let tmp = tmp.unwrap();
		let expected = syn::value(check_expected_result)?;
		ensure!(
			tmp == expected,
			"Check statement failed for test {test_index} ({test}): expected value \n'{expected:#}' \ndoesn't match \n'{tmp:#}'",
		)
	}

	// Check statement result. If the statement should succeed, check that the
	// result is Ok, otherwise check that the result is a 'Not Allowed' error
	let res = resp.pop().unwrap().output();
	if should_succeed {
		ensure!(
			res.is_ok(),
			"Test statement failed for test {test_index} ({test}): {}",
			res.unwrap_err()
		);
	} else {
		ensure!(
			res.is_err(),
			"Test statement succeeded for test {test_index} ({test}) when it should have failed: {:?}",
			res
		);

		let err = res.unwrap_err().to_string();
		ensure!(
			err.contains("Not enough permissions to perform this action"),
			"Test statement failed with unexpected error: {}",
			err
		);
	}
	Ok(())
}

type CaseIter<'a> = std::slice::Iter<'a, ((Level, Role), (&'a str, &'a str), bool, String)>;

#[allow(dead_code)]
pub async fn iam_check_cases(
	cases: CaseIter<'_>,
	scenario: &HashMap<&str, &str>,
	expected_anonymous_success_result: &str,
	expected_anonymous_failure_result: &str,
) -> Result<()> {
	iam_check_cases_impl(
		cases,
		scenario,
		expected_anonymous_success_result,
		expected_anonymous_failure_result,
		true,
		true,
	)
	.await
}

#[allow(dead_code)]
pub async fn iam_check_cases_impl(
	cases: CaseIter<'_>,
	scenario: &HashMap<&str, &str>,
	expected_anonymous_success_result: &str,
	expected_anonymous_failure_result: &str,
	use_ns: bool,
	use_db: bool,
) -> Result<()> {
	let prepare = scenario.get("prepare").unwrap();
	let test = scenario.get("test").unwrap();
	let check = scenario.get("check").unwrap();

	for (test_index, ((level, role), (ns, db), should_succeed, expected_result)) in
		cases.enumerate()
	{
		println!("* Testing '{test}' for '{level}Actor({role})' on '({ns}, {db})' - {test_index}");
		let sess = Session::for_level(level.to_owned(), role.to_owned()).with_ns(ns).with_db(db);

		// Auth enabled
		{
			let ds = new_ds().await.unwrap().with_auth_enabled(true);
			iam_run_case(
				test_index as i32,
				prepare,
				test,
				check,
				expected_result,
				&ds,
				&sess,
				use_ns,
				use_db,
				*should_succeed,
			)
			.await?;
		}

		// Auth disabled
		{
			let ds = new_ds().await.unwrap().with_auth_enabled(false);
			iam_run_case(
				test_index as i32,
				prepare,
				test,
				check,
				expected_result,
				&ds,
				&sess,
				use_ns,
				use_db,
				*should_succeed,
			)
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
				expected_anonymous_failure_result
			} else {
				expected_anonymous_success_result
			};
			iam_run_case(
				-1,
				prepare,
				test,
				check,
				expected_result,
				&ds,
				&Session::default().with_ns(ns).with_db(db),
				use_ns,
				use_db,
				!auth_enabled,
			)
			.await?;
		}
	}

	Ok(())
}

#[allow(dead_code)]
pub fn with_enough_stack(fut: impl Future<Output = Result<()>> + Send + 'static) -> Result<()> {
	let mut builder = Builder::new();

	// Roughly how much stack is allocated for surreal server workers in release
	// mode
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

#[track_caller]
#[allow(dead_code)]
fn skip_ok_pos(res: &mut Vec<Response>, pos: usize) -> Result<()> {
	assert!(!res.is_empty(), "At position {pos} - No more result!");
	let r = res.remove(0).result;
	let _ = r.is_err_and(|e| {
		panic!("At position {pos} - Statement fails with: {e}");
	});
	Ok(())
}

/// Skip the specified number of successful results from a vector of responses.
/// This function will panic if there are not enough results in the vector or if
/// an error occurs.
#[track_caller]
#[allow(dead_code)]
pub fn skip_ok(res: &mut Vec<Response>, skip: usize) -> Result<()> {
	for i in 0..skip {
		skip_ok_pos(res, i)?;
	}
	Ok(())
}

/// Struct representing a test scenario.
///
/// # Fields
/// - `ds`: The datastore for the test.
/// - `session`: The session for the test.
/// - `responses`: The list of responses for the test.
/// - `pos`: The current position in the responses list.
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
	pub async fn new_ds_session(ds: Datastore, session: Session, sql: &str) -> Result<Self> {
		let responses = ds.execute(sql, &session, None).await?;
		Ok(Self {
			ds,
			session,
			responses,
			pos: 0,
		})
	}

	#[allow(dead_code)]
	pub async fn new_ds(ds: Datastore, sql: &str) -> Result<Self> {
		Self::new_ds_session(ds, Session::owner().with_ns("test").with_db("test"), sql).await
	}

	/// Creates a new instance of the `Self` struct with the given SQL query.
	/// Arguments `sql` - A string slice representing the SQL query.
	/// Panics if an error occurs.#[expect(dead_code)]
	#[allow(dead_code)]
	pub async fn new(sql: &str) -> Result<Self> {
		Self::new_ds(new_ds().await?, sql).await
	}

	/// Simulates restarting the Datastore
	/// - Data are persistent (including memory store)
	/// - Flushing caches (jwks, IndexStore, ...)
	#[allow(dead_code)]
	pub async fn restart(self, sql: &str) -> Result<Self> {
		Self::new_ds(self.ds.restart(), sql).await
	}

	/// Checks if the number of responses matches the expected size.
	/// Panics if the number of responses does not match the expected size
	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_size(&mut self, expected: usize) -> Result<&mut Self> {
		assert_eq!(
			self.responses.len(),
			expected,
			"Unexpected number of results: {} - Expected: {expected}",
			self.responses.len()
		);
		Ok(self)
	}

	/// Retrieves the next response from the responses list.
	/// This method will panic if the responses list is empty, indicating that
	/// there are no more responses to retrieve. The panic message will include
	/// the last position in the responses list before it was emptied.
	#[track_caller]
	#[allow(dead_code)]
	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Result<Response> {
		assert!(!self.responses.is_empty(), "No response left - last position: {}", self.pos);
		self.pos += 1;
		Ok(self.responses.remove(0))
	}

	/// Retrieves the next value from the responses list.
	/// This method will panic if the responses list is empty, indicating that
	/// there are no more responses to retrieve. The panic message will include
	/// the last position in the responses list before it was emptied.
	#[track_caller]
	pub fn next_value(&mut self) -> Result<Value> {
		self.next()?.result
	}

	/// Skips a specified number of elements from the beginning of the
	/// `responses` vector and updates the position.
	#[track_caller]
	#[allow(dead_code)]
	pub fn skip_ok(&mut self, skip: usize) -> Result<&mut Self> {
		for _ in 0..skip {
			skip_ok_pos(&mut self.responses, self.pos)?;
			self.pos += 1;
		}
		Ok(self)
	}

	/// Expects the next value to be equal to the provided value.
	/// Panics if the expected value is not equal to the actual value.
	/// Compliant with NaN and Constants.
	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_value_info<I: Display>(&mut self, val: Value, info: I) -> Result<&mut Self> {
		let tmp = self.next_value()?;
		// Then check they are indeed the same values
		//
		// If it is a constant we need to transform it as a number
		if val.as_number().map(|x| x.is_nan()).unwrap_or(false) {
			assert!(
				tmp.as_number().map(|x| x.is_nan()).unwrap_or(false),
				"Expected NaN but got {info}: {tmp}"
			);
		} else {
			assert_eq!(tmp, val, "{info} {tmp:#}");
		}
		//
		Ok(self)
	}

	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_regex(&mut self, regex: &str) -> Result<&mut Self> {
		let tmp = self.next_value()?.to_string();
		let regex = Regex::new(regex)?;
		assert!(regex.is_match(&tmp), "Output '{tmp}' doesn't match regex '{regex}'",);
		Ok(self)
	}

	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_value(&mut self, val: Value) -> Result<&mut Self> {
		self.expect_value_info(val, "")
	}

	/// Expect values in the given slice to be present in the responses,
	/// following the same order.
	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_values(&mut self, values: &[Value]) -> Result<&mut Self> {
		for value in values {
			self.expect_value(value.clone())?;
		}
		Ok(self)
	}

	/// Expect the given value to be equals to the next response.
	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_val(&mut self, val: &str) -> Result<&mut Self> {
		self.expect_val_info(val, "")
	}

	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_val_info<I: Display>(&mut self, val: &str, info: I) -> Result<&mut Self> {
		self.expect_value_info(
			syn::value(val).unwrap_or_else(|_| panic!("INVALID VALUE {info}:\n{val}")),
			info,
		)
	}

	#[track_caller]
	#[allow(dead_code)]
	/// Expect values in the given slice to be present in the responses,
	/// following the same order.
	pub fn expect_vals(&mut self, vals: &[&str]) -> Result<&mut Self> {
		for (i, val) in vals.iter().enumerate() {
			self.expect_val_info(val, i)?;
		}
		Ok(self)
	}

	/// Expects the next result to be an error with the given check function
	/// returning true. This function will panic if the next result is not an
	/// error or if the error message does not pass the check.
	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_error_func<F: Fn(&anyhow::Error) -> bool>(
		&mut self,
		check: F,
	) -> Result<&mut Self> {
		let tmp = self.next()?.result;
		match &tmp {
			Ok(val) => {
				panic!("At position {} - Expect error, but got OK: {val}", self.pos);
			}
			Err(e) => {
				assert!(check(e), "At position {} - Err didn't match: {e}", self.pos)
			}
		}
		Ok(self)
	}

	#[track_caller]
	#[allow(dead_code)]
	/// Expects the next result to be an error with the specified error message.
	pub fn expect_error(&mut self, error: &str) -> Result<&mut Self> {
		self.expect_error_func(|e| e.to_string() == error)
	}

	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_errors(&mut self, errors: &[&str]) -> Result<&mut Self> {
		for error in errors {
			self.expect_error(error)?;
		}
		Ok(self)
	}

	/// Expects the next value to be a floating-point number and compares it
	/// with the given value.
	///
	/// # Arguments
	///
	/// * `val` - The expected floating-point value
	/// * `precision` - The allowed difference between the expected and actual value
	///
	/// # Panics
	///
	/// Panics if the next value is not a number or if the difference
	/// between the expected and actual value exceeds the precision.
	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_float(&mut self, val: f64, precision: f64) -> Result<&mut Self> {
		let tmp = self.next_value()?;
		if let Value::Number(Number::Float(n)) = tmp {
			let diff = (n - val).abs();
			assert!(
				diff <= precision,
				"{tmp} does not match expected: {val} - diff: {diff} - precision: {precision}"
			);
		} else {
			panic!("At position {}: Value {tmp} is not a number", self.pos);
		}
		Ok(self)
	}

	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_floats(&mut self, vals: &[f64], precision: f64) -> Result<&mut Self> {
		for val in vals {
			self.expect_float(*val, precision)?;
		}
		Ok(self)
	}

	/// Expects the next value to be bytes
	#[track_caller]
	#[allow(dead_code)]
	pub fn expect_bytes(&mut self, val: impl Into<Vec<u8>>) -> Result<&mut Self> {
		self.expect_bytes_info(val, "")
	}

	pub fn expect_bytes_info<I: Display>(
		&mut self,
		val: impl Into<Vec<u8>>,
		info: I,
	) -> Result<&mut Self> {
		let val: Vec<u8> = val.into();
		let val = Value::Bytes(val.into());
		self.expect_value_info(val, info)
	}
}

/// Creates a new b-tree map of key-value pairs
#[macro_export]
macro_rules! map {
    ($($k:expr_2021 $(, if let $grant:pat = $check:expr_2021)? $(, if $guard:expr_2021)? => $v:expr_2021),* $(,)? $( => $x:expr_2021 )?) => {{
        let mut m = ::std::collections::BTreeMap::new();
    	$(m.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)?
		$( $(if let $grant = $check)? $(if $guard)? { m.insert($k, $v); };)+
        m
    }};
}
