use crate::tests::schema::{BoolOr, SchemaTarget, TestConfig};
use surrealdb_core::dbs::capabilities::{Capabilities, Targets};

/// Creates the right core capabilities from a test config.
pub fn core_capabilities_from_test_config(config: &TestConfig) -> Capabilities {
	/// Returns Targets::All if there is no value and deny is false,
	/// Returns Targets::None if there is no value and deny is true ensuring the default behaviour
	/// is to allow everything.
	///
	/// If there is a value it will return Targets::All on the value true, Targets::None on the
	/// value false, and otherwise the returns the specified values.
	fn extract_targets<T>(v: &Option<BoolOr<Vec<SchemaTarget<T>>>>, deny: bool) -> Targets<T>
	where
		T: std::cmp::Eq + std::hash::Hash + Clone,
	{
		v.as_ref()
			.map(|x| match x {
				BoolOr::Bool(true) => Targets::All,
				BoolOr::Bool(false) => Targets::None,
				BoolOr::Value(x) => Targets::Some(x.iter().map(|x| x.0.clone()).collect()),
			})
			.unwrap_or(if deny {
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

			Capabilities::all()
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
				.with_experimental(extract_targets(&schema_cap.allow_experimental, false))
				.without_experimental(extract_targets(&schema_cap.deny_experimental, true))
		})
		.unwrap_or_else(Capabilities::all)
}
