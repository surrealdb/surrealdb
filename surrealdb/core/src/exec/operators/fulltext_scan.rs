//! Full-text search scan operator.
//!
//! This operator retrieves records using full-text search indexes,
//! supporting the MATCHES operator with BM25 or VS scoring.

use async_trait::async_trait;

use crate::exec::index::access_path::IndexRef;
use crate::exec::permission::{should_check_perms, validate_record_user_access};
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, ValueBatch,
	ValueBatchStream,
};
use crate::expr::operator::MatchesOperator;
use crate::iam::Action;

/// Full-text search scan operator.
///
/// Executes a full-text search query against a FullText index and returns
/// matching records ordered by relevance score.
#[derive(Debug)]
pub struct FullTextScan {
	/// Reference to the index definition
	pub index_ref: IndexRef,
	/// The search query string
	pub query: String,
	/// The MATCHES operator configuration (reference number, scoring)
	pub operator: MatchesOperator,
	/// Table name for record fetching
	pub table_name: crate::val::TableName,
}

#[async_trait]
impl ExecOperator for FullTextScan {
	fn name(&self) -> &'static str {
		"FullTextScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("index".to_string(), self.index_ref.name.clone()),
			("query".to_string(), self.query.clone()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone for the async block
		let index_ref = self.index_ref.clone();
		let query = self.query.clone();
		let _operator = self.operator.clone();
		let table_name = self.table_name.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			// TODO: Implement full-text search
			// This is a placeholder that needs to be filled in with proper
			// full-text search implementation using the FullTextIndex.
			//
			// The implementation would:
			// 1. Open the FullTextIndex
			// 2. Parse and execute the search query
			// 3. Score and rank results
			// 4. Fetch matching records and check permissions
			// 5. Yield batches of results
			//
			// For now, return empty result to allow compilation
			let _table_name = table_name;
			let _index_ref = index_ref;
			let _query = query;
			let _check_perms = check_perms;
			let _ctx = ctx;

			// Yield nothing - placeholder
			if false {
				yield ValueBatch { values: Vec::new() };
			}
		};

		Ok(Box::pin(stream))
	}
}
