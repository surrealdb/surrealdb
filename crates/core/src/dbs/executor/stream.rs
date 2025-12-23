use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use surrealdb_types::{Array, Value};

use crate::dbs::QueryType;
use crate::dbs::response::QueryResult;
use crate::exec::{ExecutionPlan, ValueBatchStream};
use crate::expr::{ControlFlow, FlowResult};
use crate::val::convert_value_to_public_value;

pub struct StreamExecutor {
	outputs: Vec<Arc<dyn ExecutionPlan>>,
}

impl StreamExecutor {
    /// Creates a new stream executor.
	pub(crate) fn new(outputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
		Self {
			outputs,
		}
	}

    /// Executes each output one at a time, in order and collects the results.
	pub(crate) async fn execute_collected(self) -> Result<Vec<QueryResult>, anyhow::Error> {
		let mut outputs = Vec::with_capacity(self.outputs.len());
		for output in self.outputs {
			let output_stream = output.execute()?;

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
