//! BEGIN operator - starts a write transaction.
//!
//! BEGIN is a context-mutating operator that creates a new write transaction
//! and adds it to the execution context.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{AccessMode, OperatorPlan, ValueBatchStream};
use crate::kvs::{LockType, TransactionType};

/// BEGIN operator - starts a write transaction.
///
/// Implements `OperatorPlan` with `mutates_context() = true`.
/// The `output_context()` method creates a new write transaction
/// and returns a context with that transaction.
#[derive(Debug)]
pub struct BeginPlan;

#[async_trait]
impl OperatorPlan for BeginPlan {
	fn name(&self) -> &'static str {
		"Begin"
	}

	fn required_context(&self) -> ContextLevel {
		// BEGIN can run at any level, but requires datastore access
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		// BEGIN only mutates context (transaction state), not data
		AccessMode::ReadOnly
	}

	fn execute(&self, _ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// BEGIN returns NONE as its result
		Ok(Box::pin(stream::once(async {
			Ok(crate::exec::ValueBatch {
				values: vec![crate::val::Value::None],
			})
		})))
	}

	fn mutates_context(&self) -> bool {
		true
	}

	async fn output_context(&self, input: &ExecutionContext) -> Result<ExecutionContext, Error> {
		// Get the datastore to create a new write transaction
		let ds = input
			.datastore()
			.ok_or_else(|| Error::Thrown("BEGIN requires datastore access".to_string()))?;

		// Create a new write transaction with optimistic locking
		let write_txn = ds
			.transaction(TransactionType::Write, LockType::Optimistic)
			.await
			.map_err(|e| Error::Thrown(format!("Failed to create write transaction: {}", e)))?;

		// Return context with the new write transaction
		input.with_transaction(Arc::new(write_txn))
	}
}

impl ToSql for BeginPlan {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("BEGIN TRANSACTION");
	}
}
