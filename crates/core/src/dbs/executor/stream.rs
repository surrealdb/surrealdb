use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use surrealdb_types::{Array, Value};

use crate::dbs::QueryType;
use crate::dbs::response::QueryResult;
use crate::exec::{ExecutionContext, ExecutionPlan, PlannedStatement, ValueBatchStream};
use crate::expr::{ControlFlow, FlowResult};
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::val::convert_value_to_public_value;

pub struct StreamExecutor {
	outputs: Vec<PlannedStatement>,
}

impl StreamExecutor {
	/// Creates a new stream executor.
	pub(crate) fn new(outputs: Vec<PlannedStatement>) -> Self {
		Self {
			outputs,
		}
	}

	/// Executes each output one at a time, in order and collects the results.
	///
	/// NOTE: This is not optimal, we should execute all outputs in parallel (as parallel as
	/// possible) and stream the results back rather than executing them sequentially and
	/// collecting the results.
	pub(crate) async fn execute_collected(
		self,
		ds: &Datastore,
	) -> Result<Vec<QueryResult>, anyhow::Error> {
		let txn = Arc::new(ds.transaction(TransactionType::Read, LockType::Optimistic).await?);
		let mut outputs = Vec::with_capacity(self.outputs.len());

		// Create empty parameters for now
		let params = Arc::new(HashMap::<Cow<'static, str>, Arc<crate::val::Value>>::new());
		let ctx = ExecutionContext::new("ns", "db", txn, params);

		for statement in self.outputs {
			// For now, only handle Query statements
			let output = match statement {
				PlannedStatement::Query(plan) => plan,
				PlannedStatement::SessionCommand(_) => {
					// TODO: Handle session commands
					continue;
				}
			};

			let output_stream = output.execute(&ctx)?;

			let query_result = match collect_query_result(output_stream).await {
				Ok(query_result) => query_result,
				Err(ctrl) => match ctrl {
					ControlFlow::Break => break,
					ControlFlow::Continue => continue,
					ControlFlow::Return(value) => {
						outputs.push(QueryResult {
							time: Duration::ZERO,
							result: Ok(convert_value_to_public_value(value)?),
							query_type: QueryType::Other,
						});
						return Ok(outputs);
					}
					ControlFlow::Err(e) => return Err(e),
				},
			};

			outputs.push(query_result);
		}
		Ok(outputs)
	}
}

async fn collect_query_result(mut stream: ValueBatchStream) -> FlowResult<QueryResult> {
	let mut values = Vec::new();
	while let Some(batch) = stream.try_next().await? {
		for value in batch.values {
			values.push(convert_value_to_public_value(value)?);
		}
	}

	// TODO: Fill in time and query type.
	Ok(QueryResult {
		time: Duration::ZERO,
		result: Ok(Value::Array(Array::from(values))),
		query_type: QueryType::Other,
	})
}
