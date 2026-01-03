//! COMMIT operator - commits the current write transaction.
//!
//! COMMIT is a context-mutating operator that commits all changes
//! made in the current transaction.

use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{OperatorPlan, ValueBatchStream};

/// COMMIT operator - commits the current transaction.
///
/// Implements `OperatorPlan` with `mutates_context() = true`.
/// The `output_context()` method commits the current transaction.
#[derive(Debug)]
pub struct CommitPlan;

impl OperatorPlan for CommitPlan {
	fn name(&self) -> &'static str {
		"Commit"
	}

	fn required_context(&self) -> ContextLevel {
		// COMMIT requires a transaction, which is at root level
		ContextLevel::Root
	}

	fn execute(&self, _ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// COMMIT produces no data output - it only mutates context
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
				"COMMIT requires a write transaction (use BEGIN first)".to_string(),
			));
		}

		// Commit the transaction
		futures::executor::block_on(txn.commit())
			.map_err(|e| Error::Thrown(format!("Failed to commit transaction: {}", e)))?;

		// Return the same context (transaction is now committed)
		Ok(input.clone())
	}
}

impl ToSql for CommitPlan {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("COMMIT TRANSACTION");
	}
}
