use futures::stream;

use crate::catalog::providers::TableProvider;
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
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let _ns_name = db_ctx.ns_ctx.ns.name.clone();
		let db_id = db_ctx.db.database_id;
		let _db_name = db_ctx.db.name.clone();
		let txn = db_ctx.ns_ctx.txn.clone();

		// Create an async stream that looks up the record
		let stream = stream::once(async move {
			use crate::expr::ControlFlow;

			// Look up the record
			let record = txn
				.get_record(ns_id, db_id, &record_id.table, &record_id.key, None)
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
