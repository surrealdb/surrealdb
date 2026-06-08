use anyhow::Result;

use crate::tests::{
	run::RunConfig,
	schema::{BoolOr, Capabilities as TestCapabilities, SchemaTarget},
};
use surrealdb_core::{
	dbs::{Capabilities, NewPlannerStrategy, Session, capabilities::Targets},
	kvs::Datastore,
};
use surrealdb_types::Value as SurValue;

use crate::tests::{
	TestRun,
	schema::{AuthLevel, TestAuth, TestConfig},
};

/// Builds a `Session` from a test config and a specific planner strategy.
pub fn session_from_test_config(config: &TestConfig, strategy: NewPlannerStrategy) -> Session {
	let env = &config.env;

	let ns = env.namespace();
	let db = env.database();

	let mut session = if let Some(auth) = env.auth.as_ref() {
		match auth {
			TestAuth::Root {
				level,
			} => match level {
				AuthLevel::Owner => Session::owner(),
				AuthLevel::Editor => Session::editor(),
				AuthLevel::Viewer => Session::viewer(),
			},
			TestAuth::Namespace {
				namespace,
				level,
			} => {
				let session = match level {
					AuthLevel::Owner => Session::owner(),
					AuthLevel::Editor => Session::editor(),
					AuthLevel::Viewer => Session::viewer(),
				};
				session.with_ns(namespace)
			}
			TestAuth::Database {
				namespace,
				database,
				level,
			} => {
				let session = match level {
					AuthLevel::Owner => Session::owner(),
					AuthLevel::Editor => Session::editor(),
					AuthLevel::Viewer => Session::viewer(),
				};
				session.with_ns(namespace).with_db(database)
			}
			TestAuth::Record {
				namespace,
				database,
				access,
				rid,
			} => {
				let v = SurValue::RecordId(rid.0.clone());
				Session::for_record(namespace, database, access, v)
			}
		}
	} else if env.signin.is_none() && env.signin.is_none() {
		Session::owner()
	} else {
		Session::default()
	};

	session.ns = ns.map(|x| x.to_owned());
	session.db = db.map(|x| x.to_owned());

	session.new_planner_strategy = strategy;

	session.redact_volatile_explain_attrs = env.redact_volatile_explain_attrs.unwrap_or(true);

	session
}

pub struct ImportFailure {
	pub path: String,
	pub message: String,
}

pub async fn run_imports<T: RunConfig>(
	run: &TestRun<T>,
	mut session: Session,
	dbs: &Datastore,
) -> Result<Option<ImportFailure>> {
	if let Some(ref x) = session.ns {
		let db = session.db.take();
		dbs.execute(&format!("DEFINE NAMESPACE `{x}`"), &session, None).await?;
		session.db = db;
	}

	if let Some(ref x) = session.db {
		dbs.execute(&format!("DEFINE DATABASE `{x}`"), &session, None).await?;
	}

	let mut import_session = Session::owner();
	dbs.process_use(None, &mut import_session, session.ns.clone(), session.db.clone()).await?;

	for import in run.case.imports.iter() {
		match dbs.execute(&import.source, &import_session, None).await {
			Err(e) => {
				return Ok(Some(ImportFailure {
					path: import.origin.path.clone(),
					message: format!("Failed to run import: `{e}`"),
				}));
			}
			Ok(results) => {
				// Check if any import result contains an error.
				// Without this, errors within transaction blocks (e.g. constraint
				// violations, write conflicts) are silently ignored, causing
				// subsequent test queries to see empty data.
				for result in &results {
					if let Err(ref e) = result.result {
						return Ok(Some(ImportFailure {
							path: import.origin.path.clone(),
							message: format!("Failed to run import: `{e}`"),
						}));
					}
				}
			}
		}
	}
	Ok(None)
}

/// Creates the right core capabilities from a test config.
pub fn core_capabilities_from_test_config(cap: &TestCapabilities) -> Capabilities {
	/// Returns Targets::All if there is no value and none_on_missing is false,
	/// Returns Targets::None if there is no value and none_on_missing is true ensuring the default
	/// behaviour is to allow everything.
	///
	/// If there is a value it will return Targets::All on the value true, Targets::None on the
	/// value false, and otherwise the returns the specified values.
	fn extract_targets<T>(v: &BoolOr<Vec<SchemaTarget<T>>>) -> Targets<T>
	where
		T: Eq + std::hash::Hash + Ord + Clone,
	{
		match v {
			BoolOr::Bool(true) => Targets::All,
			BoolOr::Bool(false) => Targets::None,
			BoolOr::Value(x) => Targets::Some(x.iter().map(|x| x.0.clone()).collect()),
		}
	}

	Capabilities::none()
		.with_scripting(cap.scripting)
		.with_guest_access(cap.quest_access)
		.with_live_query_notifications(cap.live_query_notifications)
		.with_functions(extract_targets(&cap.allow_functions))
		.without_functions(extract_targets(&cap.deny_functions))
		.with_network_targets(extract_targets(&cap.allow_net))
		.without_network_targets(extract_targets(&cap.deny_net))
		.with_rpc_methods(extract_targets(&cap.allow_rpc))
		.without_rpc_methods(extract_targets(&cap.deny_rpc))
		.with_http_routes(extract_targets(&cap.allow_http))
		.without_http_routes(extract_targets(&cap.deny_http))
		.with_experimental(extract_targets(&cap.allow_experimental))
		.without_experimental(extract_targets(&cap.deny_experimental))
}
