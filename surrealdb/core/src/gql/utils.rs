use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::{Name, Value as GqlValue};

use super::error::GqlError;
use crate::dbs::Session;
use crate::expr::LogicalPlan;
use crate::kvs::Datastore;
use crate::val::Value as SqlValue;

pub(crate) trait GqlValueUtils {
	fn as_i64(&self) -> Option<i64>;
	fn as_string(&self) -> Option<String>;
	fn as_list(&self) -> Option<&Vec<GqlValue>>;
	fn as_object(&self) -> Option<&IndexMap<Name, GqlValue>>;
}

impl GqlValueUtils for GqlValue {
	fn as_i64(&self) -> Option<i64> {
		if let GqlValue::Number(n) = self {
			n.as_i64()
		} else {
			None
		}
	}

	fn as_string(&self) -> Option<String> {
		if let GqlValue::String(s) = self {
			Some(s.to_owned())
		} else {
			None
		}
	}
	fn as_list(&self) -> Option<&Vec<GqlValue>> {
		if let GqlValue::List(a) = self {
			Some(a)
		} else {
			None
		}
	}
	fn as_object(&self) -> Option<&IndexMap<Name, GqlValue>> {
		if let GqlValue::Object(o) = self {
			Some(o)
		} else {
			None
		}
	}
}

/// Helper function to execute a LogicalPlan and extract the first result value
pub(crate) async fn execute_plan(
	ds: &Datastore,
	sess: &Session,
	plan: LogicalPlan,
) -> Result<SqlValue, GqlError> {
	let results = ds
		.process_plan(plan, sess, None)
		.await
		.map_err(|e| GqlError::InternalError(format!("Failed to execute query plan: {}", e)))?;

	// Take the first result
	let first_result = results
		.into_iter()
		.next()
		.ok_or_else(|| GqlError::InternalError("No results returned from query".to_string()))?;

	// Extract the value from the result and convert from PublicValue to internal Value
	first_result
		.result
		.map(|v| v.into())
		.map_err(|e| GqlError::InternalError(format!("Query execution failed: {}", e)))
}
