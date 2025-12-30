use futures::stream;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::err::Error;
use crate::exec::{ExecutionContext, ExecutionPlan, ValueBatch, ValueBatchStream};
use crate::val::RecordId;

/// Direct lookup of a record by its ID
#[derive(Debug, Clone)]
pub struct RecordIdLookup {
	pub(crate) record_id: RecordId,
	// fields: Vec<Field>,
}

impl ExecutionPlan for RecordIdLookup {
	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// Clone what we need for the async block
		let record_id = self.record_id.clone();
		let ns = ctx.ns.clone();
		let db = ctx.db.clone();
		let txn = ctx.txn.clone();

		// Create an async stream that looks up the record
		let stream = stream::once(async move {
			use crate::expr::ControlFlow;

			// Get namespace and database IDs
			let ns_id = txn
				.expect_ns_by_name(&ns)
				.await
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get namespace: {}", e)))?;
			let db_id = txn
				.expect_db_by_name(&ns, &db)
				.await
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get database: {}", e)))?;

			// Look up the record
			let record = txn
				.get_record(
					ns_id.namespace_id,
					db_id.database_id,
					&record_id.table,
					&record_id.key,
					None,
				)
				.await
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get record: {}", e)))?;

			// Convert Record to Value
			let value = record.data.as_ref().clone();

			// Return as a batch with single value
			Ok(ValueBatch {
				values: vec![value],
			})
		});

		Ok(Box::pin(stream))
	}
}
