use crate::tests::schema::{AuthLevel, BoolOr, SchemaTarget, TestAuth, TestConfig};
use surrealdb_core::dbs::{
	Session,
	capabilities::{Capabilities, Targets},
};
use surrealdb_core::val::Value as SurValue;

/// Creates the right core capabilities from a test config.
pub fn core_capabilities_from_test_config(config: &TestConfig) -> Capabilities {
	/// Returns Targets::All if there is no value and none_on_missing is false,
	/// Returns Targets::None if there is no value and none_on_missing is true ensuring the default behaviour
	/// is to allow everything.
	///
	/// If there is a value it will return Targets::All on the value true, Targets::None on the
	/// value false, and otherwise the returns the specified values.
	fn extract_targets<T>(
		v: &Option<BoolOr<Vec<SchemaTarget<T>>>>,
		none_on_missing: bool,
	) -> Targets<T>
	where
		T: std::cmp::Eq + std::hash::Hash + Clone,
	{
		v.as_ref()
			.map(|x| match x {
				BoolOr::Bool(true) => Targets::All,
				BoolOr::Bool(false) => Targets::None,
				BoolOr::Value(x) => Targets::Some(x.iter().map(|x| x.0.clone()).collect()),
			})
			.unwrap_or(if none_on_missing {
				Targets::None
			} else {
				Targets::All
			})
	}

	config
		.env
		.as_ref()
		.and_then(|x| x.capabilities.as_ref())
		.map(|x| {
			let schema_cap = match x {
				BoolOr::Bool(true) => return Capabilities::all(),
				BoolOr::Bool(false) => return Capabilities::none(),
				BoolOr::Value(x) => x,
			};

			Capabilities::none()
				.with_scripting(schema_cap.scripting.unwrap_or(true))
				.with_guest_access(schema_cap.quest_access.unwrap_or(true))
				.with_live_query_notifications(schema_cap.live_query_notifications.unwrap_or(true))
				.with_functions(extract_targets(&schema_cap.allow_functions, false))
				.without_functions(extract_targets(&schema_cap.deny_functions, true))
				.with_network_targets(extract_targets(&schema_cap.allow_net, false))
				.without_network_targets(extract_targets(&schema_cap.deny_net, true))
				.with_rpc_methods(extract_targets(&schema_cap.allow_rpc, false))
				.without_rpc_methods(extract_targets(&schema_cap.deny_rpc, true))
				.with_http_routes(extract_targets(&schema_cap.allow_http, false))
				.without_http_routes(extract_targets(&schema_cap.deny_http, true))
				.with_experimental(extract_targets(&schema_cap.allow_experimental, true))
				.without_experimental(extract_targets(&schema_cap.deny_experimental, true))
		})
		.unwrap_or_else(Capabilities::all)
}

/// Creates the right core capabilities from a test config.
pub fn session_from_test_config(config: &TestConfig) -> Session {
	let Some(env) = config.env.as_ref() else {
		return Session::owner().with_ns("test").with_db("test");
	};

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
				session.with_ns(&namespace)
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
				session.with_ns(&namespace).with_db(&database)
			}
			TestAuth::Record {
				namespace,
				database,
				access,
				rid,
			} => {
				let v = SurValue::RecordId(rid.0.clone());
				Session::for_record(&namespace, &database, &access, v)
			}
		}
	} else if env.signin.is_none() && env.signin.is_none() {
		Session::owner()
	} else {
		Session::default()
	};

	session.ns = ns.map(|x| x.to_owned());
	session.db = db.map(|x| x.to_owned());

	session
}
