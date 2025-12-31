use futures::stream;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::err::Error;
use crate::exec::{ContextLevel, ExecutionContext, ExecutionPlan, ValueBatch, ValueBatchStream};
use crate::val::RecordId;

/// Direct lookup of a record by its ID.
///
/// Requires database-level context since it looks up a record
/// in a specific table within the selected namespace and database.
#[derive(Debug, Clone)]
pub struct RecordIdLookup {
	pub(crate) record_id: RecordId,
	// fields: Vec<Field>,
}

impl ExecutionPlan for RecordIdLookup {
	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// Get database context - we declared Database level, so this should succeed
		let db_ctx = ctx.database()?;

		// Clone what we need for the async block
		let record_id = self.record_id.clone();
		let ns_name = db_ctx.ns_ctx.ns.name.clone();
		let db_name = db_ctx.db.name.clone();
		let txn = db_ctx.ns_ctx.txn.clone();

		// Create an async stream that looks up the record
		let stream = stream::once(async move {
			use crate::expr::ControlFlow;

			// Get namespace and database IDs
			let ns_id = txn
				.expect_ns_by_name(&ns_name)
				.await
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get namespace: {}", e)))?;
			let db_id = txn
				.expect_db_by_name(&ns_name, &db_name)
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
