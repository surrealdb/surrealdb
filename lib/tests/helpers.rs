use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::thread::Builder;

use surrealdb::dbs::capabilities::Capabilities;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::{Auth, Level, Role};
use surrealdb::kvs::Datastore;
use surrealdb_core::dbs::Response;
use surrealdb_core::sql::Value;

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
pub fn skip_ok(res: &mut Vec<Response>, skip: usize) -> Result<(), Error> {
	for i in 0..skip {
		if res.is_empty() {
			panic!("No more result #{i}");
		}
		let r = res.remove(0).result;
		let _ = r.is_err_and(|e| {
			panic!("Statement #{i} fails with: {e}");
		});
	}
	Ok(())
}

#[allow(dead_code)]
pub async fn dbs_test_execute(sql: &str) -> Result<(Datastore, Session, Vec<Response>), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = dbs.execute(sql, &ses, None).await?;
	Ok((dbs, ses, res))
}

#[allow(dead_code)]
pub fn expected_value<T: Into<Value>>(res: &mut Vec<Response>, val: T) -> Result<(), Error> {
	let tmp = res.remove(0).result?;
	let val = val.into();
	// First check using JSON format (diff is easier to compare by humans!)
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	// Then check they are indeed the same values
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[allow(dead_code)]
pub fn expected_values<T: Into<Value>>(
	res: &mut Vec<Response>,
	values: Vec<T>,
) -> Result<(), Error> {
	assert!(
		res.len() >= values.len(),
		"Expected at least {} values, but got: {}",
		values.len(),
		res.len()
	);
	for val in values {
		expected_value(res, val)?;
	}
	Ok(())
}
