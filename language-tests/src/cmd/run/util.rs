use surrealdb_core::dbs::{NewPlannerStrategy, Session};
use surrealdb_types::Value as SurValue;

use crate::tests::schema::{AuthLevel, TestAuth, TestConfig};

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
