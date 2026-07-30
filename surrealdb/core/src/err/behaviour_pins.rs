//! Behavioural pins for the error predicates that steer control flow.
//!
//! The wire snapshot pins what a client *sees*. These pin what the engine
//! *does*. A handful of predicates decide whether a token gets refreshed, a
//! transaction gets retried, a query errors or yields `NONE`, and whether
//! `IF EXISTS` swallows a miss. Every one of them is a `downcast_ref` or a
//! `matches!` over a concrete error variant, so any change to where a variant
//! lives, or to the set a predicate accepts, changes behaviour while the code
//! still compiles and the wire output stays byte-identical.
//!
//! Each test asserts the consequence rather than the match, so it keeps its
//! meaning after a variant moves to another crate.

use anyhow::anyhow;

use super::Error;
use crate::doc::Error as DocError;
use crate::err::EngineError;
use crate::exe::FlowResultExt;
use crate::exec::Error as ExecError;
use crate::expr::{ControlFlow, Error as ExprError};
use crate::kvs::Error as KvsError;
use crate::val::Value;

/// An error type from outside the engine, for the "not one of ours" cases.
#[derive(Debug, thiserror::Error)]
#[error("foreign")]
struct Foreign;

// ---------------------------------------------------------------------------
// Expired tokens are reported as expired
//
// `iam::is_expired_token_error` has a single caller, the SDK's local engine,
// which refreshes the session token when it fires. If it stops firing the SDK
// silently stops refreshing and long-lived sessions die at the token lifetime.
// ---------------------------------------------------------------------------

mod expired_token {
	use chrono::{Duration, Utc};
	use jsonwebtoken::{EncodingKey, encode};

	use crate::dbs::Session;
	use crate::iam::is_expired_token_error;
	use crate::iam::token::{Claims, HEADER};
	use crate::iam::verify::token;
	use crate::kvs::Datastore;

	const SECRET: &str = "jwt_secret";

	/// A datastore with a JWT access method named `token` on `test`/`test`.
	async fn datastore_with_jwt_access() -> Datastore {
		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			&format!(
				"DEFINE ACCESS token ON DATABASE TYPE JWT ALGORITHM HS512 KEY '{SECRET}' \
				 DURATION FOR SESSION 30d, FOR TOKEN 30d"
			),
			&sess,
			None,
		)
		.await
		.unwrap();
		ds
	}

	fn claims_for(nbf_offset: Duration, exp_offset: Duration) -> Claims {
		Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some((Utc::now() - Duration::hours(2)).timestamp()),
			nbf: Some((Utc::now() + nbf_offset).timestamp()),
			exp: Some((Utc::now() + exp_offset).timestamp()),
			ac: Some("token".to_string()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			roles: None,
			..Claims::default()
		}
	}

	async fn verify_failure(claims: Claims) -> anyhow::Error {
		let ds = datastore_with_jwt_access().await;
		let key = EncodingKey::from_secret(SECRET.as_ref());
		let enc = encode(&HEADER, &claims, &key).unwrap();
		let mut sess = Session::default();
		token(&ds, &mut sess, &enc).await.expect_err("token should have been rejected")
	}

	/// A token whose `exp` is in the past is reported as expired, which is what
	/// tells the SDK to refresh rather than to give up.
	#[tokio::test]
	async fn an_expired_token_is_reported_as_expired() {
		let err = verify_failure(claims_for(-Duration::hours(2), -Duration::hours(1))).await;
		assert!(
			is_expired_token_error(&err),
			"an expired token must be recognised as expired, got: {err}"
		);
	}

	/// A token that is merely not valid yet is a neighbouring auth failure and
	/// must not be treated as expired: refreshing would not help.
	#[tokio::test]
	async fn a_not_yet_valid_token_is_not_reported_as_expired() {
		let err = verify_failure(claims_for(Duration::hours(1), Duration::hours(2))).await;
		// Guards against the test passing because verification failed for some
		// unrelated reason: this claim set must fail the `nbf` check, which is
		// the auth failure sitting immediately next to the expiry one.
		assert_eq!(err.to_string(), "There was a problem with authentication");
		assert!(
			!is_expired_token_error(&err),
			"a not-yet-valid token must not be recognised as expired, got: {err}"
		);
	}
}

// ---------------------------------------------------------------------------
// Query lifecycle signals
//
// `is_query_cancelled` / `is_query_timedout` are public and the SDK's
// background task loop branches on them to decide whether a node-membership
// update was cancelled, timed out, or genuinely failed. Both shapes occur:
// raised bare, or wrapped by a function typed on `Error`.
// ---------------------------------------------------------------------------

fn a_timeout() -> EngineError {
	EngineError::QueryTimedout(std::time::Duration::from_secs(1))
}

#[test]
fn a_cancelled_query_is_reported_as_cancelled_bare_or_wrapped() {
	for error in
		[anyhow!(EngineError::QueryCancelled), anyhow!(Error::Engine(EngineError::QueryCancelled))]
	{
		assert!(super::is_query_cancelled(&error), "cancellation not detected: {error}");
		assert!(!super::is_query_timedout(&error), "cancellation read as a timeout: {error}");
	}
}

#[test]
fn a_timed_out_query_is_reported_as_timed_out_bare_or_wrapped() {
	for error in [anyhow!(a_timeout()), anyhow!(Error::Engine(a_timeout()))] {
		assert!(super::is_query_timedout(&error), "timeout not detected: {error}");
		assert!(!super::is_query_cancelled(&error), "timeout read as a cancellation: {error}");
	}
}

#[test]
fn an_unrelated_failure_is_neither_cancelled_nor_timed_out() {
	let error = anyhow!(ExecError::Thrown("boom".to_string()));
	assert!(!super::is_query_cancelled(&error));
	assert!(!super::is_query_timedout(&error));
}

// ---------------------------------------------------------------------------
// A user THROW keeps its identity
//
// The AUTHENTICATE, SIGNIN and SIGNUP flows forward a `THROW` raised inside a
// record-access clause to the client verbatim instead of collapsing it to a
// generic auth failure, and they recognise it through `err::exec_error`.
// Statement execution raises `Thrown` bare from the recursive path and wrapped
// in `Error::Exec` from the planned one, so a check that sees only one shape
// stops forwarding for the other.
// ---------------------------------------------------------------------------

#[test]
fn a_thrown_error_is_recognised_bare_or_wrapped() {
	for error in [
		anyhow!(ExecError::Thrown("boom".to_string())),
		anyhow!(Error::Exec(ExecError::Thrown("boom".to_string()))),
	] {
		assert!(
			matches!(super::exec_error(&error), Some(ExecError::Thrown(_))),
			"a THROW was not recognised: {error}"
		);
	}
	assert!(
		super::exec_error(&anyhow!(Foreign)).is_none(),
		"a foreign error must not be read as an execution failure"
	);
}

// ---------------------------------------------------------------------------
// Ignorable evaluation failures
//
// `ControlFlow::is_ignorable` decides whether a failing expression aborts the
// query or evaluates to `NONE`. The membership is semantic, not tidy: `TryRem`
// and `TryFrom` sit next to the arithmetic variants that *are* ignorable and
// are deliberately excluded. Asserted through `or_none`, the consequence the
// aggregate operator and every `.or_none()` caller actually depend on.
// ---------------------------------------------------------------------------

/// Generic over the error type so the membership can be stated across the
/// layers that raise these failures, not just the one that owns the predicate.
fn or_none_of<E: std::error::Error + Send + Sync + 'static>(
	error: E,
) -> Result<Value, ControlFlow> {
	Err(ControlFlow::Err(anyhow!(error))).or_none()
}

fn assert_yields_none<E: std::error::Error + Send + Sync + 'static>(error: E) {
	let display = error.to_string();
	match or_none_of(error) {
		Ok(Value::None) => {}
		other => panic!("expected `{display}` to evaluate to NONE, got {other:?}"),
	}
}

fn assert_propagates<E: std::error::Error + Send + Sync + 'static>(error: E) {
	let display = error.to_string();
	match or_none_of(error) {
		Err(ControlFlow::Err(_)) => {}
		other => panic!("expected `{display}` to propagate, got {other:?}"),
	}
}

fn a_coerce_error() -> crate::val::CoerceError {
	crate::val::CoerceError::InvalidKind {
		from: Value::None,
		into: "sample".to_string(),
	}
}

fn a_cast_error() -> crate::val::CastError {
	crate::val::CastError::InvalidKind {
		from: Value::None,
		into: "sample".to_string(),
	}
}

#[test]
fn data_shape_failures_evaluate_to_none() {
	assert_yields_none(ExprError::Coerce(a_coerce_error()));
	assert_yields_none(ExprError::Cast(a_cast_error()));
	assert_yields_none(ExprError::InvalidFunctionArguments {
		name: "f".to_string(),
		message: "m".to_string(),
	});
	assert_yields_none(ExprError::TryAdd("a".to_string(), "b".to_string()));
	assert_yields_none(ExprError::TrySub("a".to_string(), "b".to_string()));
	assert_yields_none(ExprError::TryMul("a".to_string(), "b".to_string()));
	assert_yields_none(ExprError::TryDiv("a".to_string(), "b".to_string()));
	assert_yields_none(ExprError::TryPow("a".to_string(), "b".to_string()));
	assert_yields_none(ExprError::TryNeg("a".to_string()));
	assert_yields_none(ExprError::TryExtend("a".to_string()));
}

/// `TryRem` and `TryFrom` are arithmetic-adjacent but deliberately excluded:
/// they must surface as query errors, not be swallowed as `NONE`.
#[test]
fn remainder_and_conversion_failures_still_propagate() {
	assert_propagates(ExprError::TryRem("a".to_string(), "b".to_string()));
	assert_propagates(ExprError::TryFrom("a".to_string(), "b"));
}

#[test]
fn system_and_unknown_failures_still_propagate() {
	assert_propagates(ExecError::Thrown("boom".to_string()));
	assert_propagates(Error::Engine(EngineError::QueryCancelled));

	let foreign: Result<Value, ControlFlow> = Err(ControlFlow::Err(anyhow!(Foreign))).or_none();
	assert!(matches!(foreign, Err(ControlFlow::Err(_))), "a foreign error must propagate");
}

#[test]
fn control_flow_signals_are_never_ignorable() {
	assert!(!ControlFlow::Break.is_ignorable());
	assert!(!ControlFlow::Continue.is_ignorable());
	assert!(!ControlFlow::Return(Value::None).is_ignorable());
}

// ---------------------------------------------------------------------------
// Schema-related failures during UPSERT
//
// `doc::Error::is_schema_related` is read by `doc::upsert`, which retries the
// document as an update instead of rolling back to the save point when a
// create fails for a schema reason. The membership is the whole contract.
//
// Gap: the retry only differs from the neighbouring arm in its transaction
// bookkeeping, which a unit test cannot observe without a datastore and a
// contended write, so this pins the predicate itself rather than the retry.
// ---------------------------------------------------------------------------

#[test]
fn field_failures_are_schema_related() {
	let field = crate::expr::Idiom::field("f".to_string());
	assert!(
		DocError::FieldCoerce {
			record: "t:1".to_string(),
			field_name: "f".to_string(),
			error: Box::new(a_coerce_error()),
		}
		.is_schema_related()
	);
	assert!(
		DocError::FieldValue {
			record: "t:1".to_string(),
			value: "v".to_string(),
			field: field.clone(),
			check: "c".to_string(),
		}
		.is_schema_related()
	);
	assert!(
		DocError::FieldReadonly {
			record: "t:1".to_string(),
			field: field.clone(),
		}
		.is_schema_related()
	);
	assert!(
		DocError::FieldUndefined {
			table: "t".to_string(),
			field,
		}
		.is_schema_related()
	);
}

#[test]
fn conflicts_and_unrelated_failures_are_not_schema_related() {
	assert!(
		!DocError::RecordExists {
			record: crate::val::RecordId::new("t".into(), 1),
		}
		.is_schema_related()
	);
	assert!(
		!DocError::TableIsView {
			table: "t".to_string(),
		}
		.is_schema_related()
	);
	assert!(
		!DocError::IdMismatch {
			value: "v".to_string(),
		}
		.is_schema_related()
	);
}

// ---------------------------------------------------------------------------
// Transaction retry and shutdown
//
// `kvs::is_retryable_transaction_conflict` drives the transactor's retry loop
// and `kvs::is_shutdown_error` keeps the concurrent index builder from
// recording a restart as a permanent failure. Both shapes occur in practice:
// a bare `KvsError` from the store, or one wrapped in `Error::Kvs`.
// ---------------------------------------------------------------------------

#[test]
fn a_write_conflict_is_retryable_bare_or_wrapped() {
	for error in [
		anyhow!(KvsError::TransactionConflict("busy".to_string())),
		anyhow!(Error::Kvs(KvsError::TransactionConflict("busy".to_string()))),
	] {
		assert!(
			crate::kvs::is_retryable_transaction_conflict(&error),
			"conflict not recognised as retryable: {error}"
		);
		assert!(!crate::kvs::is_shutdown_error(&error), "conflict misread as a shutdown: {error}");
	}
}

#[test]
fn a_shutdown_is_recognised_bare_or_wrapped() {
	for error in [anyhow!(KvsError::Shutdown), anyhow!(Error::Kvs(KvsError::Shutdown))] {
		assert!(crate::kvs::is_shutdown_error(&error), "shutdown not recognised: {error}");
		assert!(
			!crate::kvs::is_retryable_transaction_conflict(&error),
			"shutdown misread as a retryable conflict: {error}"
		);
	}
}

#[test]
fn an_unrelated_failure_is_neither_retryable_nor_a_shutdown() {
	let error = anyhow!(ExecError::Thrown("boom".to_string()));
	assert!(!crate::kvs::is_retryable_transaction_conflict(&error));
	assert!(!crate::kvs::is_shutdown_error(&error));
}

// ---------------------------------------------------------------------------
// REMOVE ... IF EXISTS
//
// Each `REMOVE` statement swallows exactly one `NotFound` variant when
// `if_exists` is set, by downcasting the lookup failure. If the downcast stops
// matching, `IF EXISTS` starts raising the very error it exists to suppress.
// Driven end to end so the pin covers the statement, not the predicate.
// ---------------------------------------------------------------------------

mod remove_if_exists {
	use crate::dbs::Session;
	use crate::dbs::capabilities::{Capabilities, ExperimentalTarget, Targets};
	use crate::kvs::Datastore;

	async fn setup() -> (Datastore, Session) {
		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE test", &sess, None).await.unwrap();
		(ds, sess)
	}

	async fn run(ds: &Datastore, sess: &Session, sql: &str) -> Result<(), String> {
		let mut res = ds.execute(sql, sess, None).await.unwrap();
		res.remove(0).result.map(|_| ()).map_err(|e| e.to_string())
	}

	/// `IF EXISTS` succeeds on a missing target; without it the miss is an error.
	async fn assert_if_exists_swallows(with: &str, without: &str) {
		assert_if_exists_swallows_given(&[], with, without).await;
	}

	/// As above, but for statements whose target is scoped to something that
	/// must exist first - a missing table is a different failure from a missing
	/// index, and only the latter is what `IF EXISTS` may swallow.
	async fn assert_if_exists_swallows_given(given: &[&str], with: &str, without: &str) {
		let (ds, sess) = setup().await;
		for sql in given {
			run(&ds, &sess, sql).await.unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
		}
		run(&ds, &sess, with).await.unwrap_or_else(|e| panic!("`{with}` should succeed: {e}"));
		let err = run(&ds, &sess, without)
			.await
			.expect_err(&format!("`{without}` should report the missing target"));
		assert!(err.contains("does not exist"), "unexpected error for `{without}`: {err}");
	}

	#[tokio::test]
	async fn remove_param_if_exists_swallows_a_missing_param() {
		assert_if_exists_swallows("REMOVE PARAM IF EXISTS $nope", "REMOVE PARAM $nope").await;
	}

	#[tokio::test]
	async fn remove_function_if_exists_swallows_a_missing_function() {
		assert_if_exists_swallows("REMOVE FUNCTION IF EXISTS fn::nope", "REMOVE FUNCTION fn::nope")
			.await;
	}

	#[tokio::test]
	async fn remove_analyzer_if_exists_swallows_a_missing_analyzer() {
		assert_if_exists_swallows("REMOVE ANALYZER IF EXISTS nope", "REMOVE ANALYZER nope").await;
	}

	#[tokio::test]
	async fn remove_sequence_if_exists_swallows_a_missing_sequence() {
		assert_if_exists_swallows("REMOVE SEQUENCE IF EXISTS nope", "REMOVE SEQUENCE nope").await;
	}

	#[tokio::test]
	async fn alter_sequence_if_exists_swallows_a_missing_sequence() {
		assert_if_exists_swallows(
			"ALTER SEQUENCE IF EXISTS nope TIMEOUT 1s",
			"ALTER SEQUENCE nope TIMEOUT 1s",
		)
		.await;
	}

	#[tokio::test]
	async fn remove_index_if_exists_swallows_a_missing_index() {
		assert_if_exists_swallows_given(
			&["DEFINE TABLE example"],
			"REMOVE INDEX IF EXISTS nope ON example",
			"REMOVE INDEX nope ON example",
		)
		.await;
	}

	#[tokio::test]
	async fn remove_event_if_exists_swallows_a_missing_event() {
		assert_if_exists_swallows_given(
			&["DEFINE TABLE example"],
			"REMOVE EVENT IF EXISTS nope ON example",
			"REMOVE EVENT nope ON example",
		)
		.await;
	}

	/// `REMOVE MODULE` is gated behind an experimental capability, so it needs a
	/// datastore the shared `setup` does not build.
	#[tokio::test]
	async fn remove_module_if_exists_swallows_a_missing_module() {
		let ds = Datastore::builder()
			.with_capabilities(
				Capabilities::default()
					.with_experimental(Targets::Some([ExperimentalTarget::Surrealism].into())),
			)
			.build_with_path("memory")
			.await
			.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE test", &sess, None).await.unwrap();

		run(&ds, &sess, "REMOVE MODULE IF EXISTS mod::nope")
			.await
			.expect("`REMOVE MODULE IF EXISTS` should succeed");
		let err = run(&ds, &sess, "REMOVE MODULE mod::nope")
			.await
			.expect_err("`REMOVE MODULE` should report the missing module");
		assert!(err.contains("does not exist"), "unexpected error: {err}");
	}
}
