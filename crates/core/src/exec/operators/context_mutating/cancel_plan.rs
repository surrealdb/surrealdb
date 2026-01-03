//! CANCEL operator - cancels/rolls back the current write transaction.
//!
//! CANCEL is a context-mutating operator that rolls back all changes
//! made in the current transaction.

use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{OperatorPlan, ValueBatchStream};

/// CANCEL operator - cancels/rolls back the current transaction.
///
/// Implements `OperatorPlan` with `mutates_context() = true`.
/// The `output_context()` method cancels the current transaction.
#[derive(Debug)]
pub struct CancelPlan;

impl OperatorPlan for CancelPlan {
	fn name(&self) -> &'static str {
		"Cancel"
	}

	fn required_context(&self) -> ContextLevel {
		// CANCEL requires a transaction, which is at root level
		ContextLevel::Root
	}

	fn execute(&self, _ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// CANCEL produces no data output - it only mutates context
		Ok(Box::pin(stream::empty()))
	}

	fn mutates_context(&self) -> bool {
		true
	}

	fn output_context(&self, input: &ExecutionContext) -> Result<ExecutionContext, Error> {
		// Get the current transaction
		let txn = input.txn();

		// Check if the transaction is writable
		if !txn.writeable() {
			return Err(Error::Thrown(
				"CANCEL requires a write transaction (use BEGIN first)".to_string(),
			));
		}

		// Cancel/rollback the transaction
		futures::executor::block_on(txn.cancel())
			.map_err(|e| Error::Thrown(format!("Failed to cancel transaction: {}", e)))?;

		// Return the same context (transaction is now cancelled)
		Ok(input.clone())
	}
}

impl ToSql for CancelPlan {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("CANCEL TRANSACTION");
	}
}
