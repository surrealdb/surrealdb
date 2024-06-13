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
use surrealdb_core::sql::{value, Number, Value};

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
fn skip_ok_pos(res: &mut Vec<Response>, pos: usize) -> Result<(), Error> {
	assert!(!res.is_empty(), "At position {pos} - No more result!");
	let r = res.remove(0).result;
	let _ = r.is_err_and(|e| {
		panic!("At position {pos} - Statement fails with: {e}");
	});
	Ok(())
}

/// Skip the specified number of successful results from a vector of responses.
/// This function will panic if there are not enough results in the vector or if an error occurs.
#[allow(dead_code)]
pub fn skip_ok(res: &mut Vec<Response>, skip: usize) -> Result<(), Error> {
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
	/// Creates a new instance of the `Self` struct with the given SQL query.
	/// Arguments `sql` - A string slice representing the SQL query.
	/// Panics if an error occurs.
	#[allow(dead_code)]
	pub async fn new(sql: &str) -> Result<Self, Error> {
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

	/// Checks if the number of responses matches the expected size.
	/// Panics if the number of responses does not match the expected size
	#[allow(dead_code)]
	pub fn expect_size(&mut self, expected: usize) -> Result<&mut Self, Error> {
		assert_eq!(
			self.responses.len(),
			expected,
			"Unexpected number of results: {} - Expected: {expected}",
			self.responses.len()
		);
		Ok(self)
	}

	/// Retrieves the next response from the responses list.
	/// This method will panic if the responses list is empty, indicating that there are no more responses to retrieve.
	/// The panic message will include the last position in the responses list before it was emptied.
	#[allow(dead_code)]
	pub fn next(&mut self) -> Result<Response, Error> {
		assert!(!self.responses.is_empty(), "No response left - last position: {}", self.pos);
		self.pos += 1;
		Ok(self.responses.remove(0))
	}

	/// Retrieves the next value from the responses list.
	/// This method will panic if the responses list is empty, indicating that there are no more responses to retrieve.
	/// The panic message will include the last position in the responses list before it was emptied.
	pub fn next_value(&mut self) -> Result<Value, Error> {
		self.next()?.result
	}

	/// Skips a specified number of elements from the beginning of the `responses` vector
	/// and updates the position.
	#[allow(dead_code)]
	pub fn skip_ok(&mut self, skip: usize) -> Result<&mut Self, Error> {
		for _ in 0..skip {
			skip_ok_pos(&mut self.responses, self.pos)?;
			self.pos += 1;
		}
		Ok(self)
	}

	/// Expects the next value to be equal to the provided value.
	/// Panics if the expected value is not equal to the actual value.
	/// Compliant with NaN and Constants.
	#[allow(dead_code)]
	pub fn expect_value(&mut self, val: Value) -> Result<&mut Self, Error> {
		let tmp = self.next_value()?;
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
		Ok(self)
	}

	/// Expect values in the given slice to be present in the responses, following the same order.
	#[allow(dead_code)]
	pub fn expect_values(&mut self, values: &[Value]) -> Result<&mut Self, Error> {
		for value in values {
			self.expect_value(value.clone())?;
		}
		Ok(self)
	}

	/// Expect the given value to be equals to the next response.
	#[allow(dead_code)]
	pub fn expect_val(&mut self, val: &str) -> Result<&mut Self, Error> {
		self.expect_value(value(val).unwrap())
	}

	#[allow(dead_code)]
	/// Expect values in the given slice to be present in the responses, following the same order.
	pub fn expect_vals(&mut self, vals: &[&str]) -> Result<&mut Self, Error> {
		for val in vals {
			self.expect_val(val)?;
		}
		Ok(self)
	}

	/// Expects the next result to be an error with the given check function returning true.
	/// This function will panic if the next result is not an error or if the error
	/// message does not pass the check.
	#[allow(dead_code)]
	pub fn expect_error_func<F: Fn(&Error) -> bool>(
		&mut self,
		check: F,
	) -> Result<&mut Self, Error> {
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

	#[allow(dead_code)]
	/// Expects the next result to be an error with the specified error message.
	pub fn expect_error(&mut self, error: &str) -> Result<&mut Self, Error> {
		self.expect_error_func(|e| e.to_string() == error)
	}

	#[allow(dead_code)]
	pub fn expect_errors(&mut self, errors: &[&str]) -> Result<&mut Self, Error> {
		for error in errors {
			self.expect_error(error)?;
		}
		Ok(self)
	}

	/// Expects the next value to be a floating-point number and compares it with the given value.
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
	#[allow(dead_code)]
	pub fn expect_float(&mut self, val: f64, precision: f64) -> Result<&mut Self, Error> {
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

	#[allow(dead_code)]
	pub fn expect_floats(&mut self, vals: &[f64], precision: f64) -> Result<&mut Self, Error> {
		for val in vals {
			self.expect_float(*val, precision)?;
		}
		Ok(self)
	}
}

impl Drop for Test {
	/// Drops the instance of the struct
	/// This method will panic if there are remaining responses that have not been checked.
	fn drop(&mut self) {
		// Check for a panic to make sure test doesnt cause a double panic.
		if !std::thread::panicking() && !self.responses.is_empty() {
			panic!("Not every response has been checked");
		}
	}
}
