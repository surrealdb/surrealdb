//! Table source operator with index selection.
//!
//! This operator represents a table source in a SELECT query. At execution time,
//! it analyzes available indexes and selects the best access path based on:
//! - WITH INDEX/NOINDEX hints
//! - WHERE conditions
//! - ORDER BY clauses

use std::sync::Arc;

use async_trait::async_trait;

use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::index::access_path::{AccessPath, select_access_path};
use crate::exec::index::analysis::IndexAnalyzer;
use crate::exec::permission::{should_check_perms, validate_record_user_access};
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, ValueBatch,
	ValueBatchStream,
};
use crate::expr::order::Ordering;
use crate::expr::with::With;
use crate::expr::{Cond, ControlFlow};
use crate::iam::Action;
use crate::idx::planner::ScanDirection;
use crate::val::TableName;

/// Table source operator with runtime index selection.
///
/// At execution time, this operator:
/// 1. Fetches available indexes for the table
/// 2. Analyzes WHERE/ORDER/WITH to select the best access path
/// 3. Delegates to the appropriate specialized operator (IndexScan, FullTextScan, KnnScan, or table scan)
#[derive(Debug)]
pub struct TableSource {
	/// The table to scan
	pub table: TableName,
	/// Optional WHERE condition for index matching
	pub cond: Option<Cond>,
	/// Optional ORDER BY for index matching
	pub order: Option<Ordering>,
	/// Optional WITH hints for index selection
	pub with: Option<With>,
	/// Optional version timestamp for time-travel queries
	pub version: Option<u64>,
}

#[async_trait]
impl ExecOperator for TableSource {
	fn name(&self) -> &'static str {
		"TableSource"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("table".to_string(), self.table.to_string())];
		if let Some(with) = &self.with {
			use surrealdb_types::ToSql;
			attrs.push(("with".to_string(), with.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		use crate::exec::operators::Scan;
		use crate::exec::planner::expr_to_physical_expr;
		use crate::expr::{Expr, Literal};

		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Clone for the async block
		let table = self.table.clone();
		let cond = self.cond.clone();
		let order = self.order.clone();
		let with = self.with.clone();
		let version = self.version;
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Fetch available indexes for this table
			let indexes = txn
				.all_tb_indexes(ns.namespace_id, db.database_id, &table)
				.await
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to fetch indexes: {e}")))?;

			// Analyze and select access path
			let analyzer = IndexAnalyzer::new(&table, indexes.clone(), with.as_ref());
			let candidates = analyzer.analyze(cond.as_ref(), order.as_ref());
			
			// Default direction is forward
			let direction = ScanDirection::Forward;
			
			let access_path = select_access_path(table.clone(), candidates, with.as_ref(), direction);

			// Based on the selected access path, delegate to the appropriate execution strategy
			match access_path {
				AccessPath::TableScan { .. } => {
					// Fall back to regular Scan operator
					let table_expr = expr_to_physical_expr(
						Expr::Literal(Literal::String(table.as_str().to_string())),
						ctx.ctx(),
					)
					.map_err(|e| ControlFlow::Err(e.into()))?;
					
					let scan = Scan {
						source: table_expr,
						version,
					};
					
					// Execute the scan and forward its stream
					let mut scan_stream = scan.execute(&ctx)?;
					while let Some(batch) = scan_stream.next().await {
						yield batch?;
					}
				}
				
				AccessPath::BTreeScan { index_ref, access, direction } => {
					// Use IndexScan operator
					use crate::exec::operators::IndexScan;
					
					let index_scan = IndexScan {
						index_ref,
						access,
						direction,
						table_name: table.clone(),
					};
					
					let mut index_stream = index_scan.execute(&ctx)?;
					while let Some(batch) = index_stream.next().await {
						yield batch?;
					}
				}
				
				AccessPath::FullTextSearch { index_ref, query, operator } => {
					// Use FullTextScan operator
					use crate::exec::operators::FullTextScan;
					
					let ft_scan = FullTextScan {
						index_ref,
						query,
						operator,
						table_name: table.clone(),
					};
					
					let mut ft_stream = ft_scan.execute(&ctx)?;
					while let Some(batch) = ft_stream.next().await {
						yield batch?;
					}
				}
				
				AccessPath::KnnSearch { index_ref, vector, k, ef } => {
					// Use KnnScan operator
					use crate::exec::operators::KnnScan;
					
					let knn_scan = KnnScan {
						index_ref,
						vector,
						k,
						ef,
						table_name: table.clone(),
					};
					
					let mut knn_stream = knn_scan.execute(&ctx)?;
					while let Some(batch) = knn_stream.next().await {
						yield batch?;
					}
				}
				
				AccessPath::PointLookup { .. } | AccessPath::IndexUnion { .. } | AccessPath::CountIndex { .. } => {
					// TODO: Implement these access paths
					Err(ControlFlow::Err(anyhow::anyhow!(
						"Access path {:?} not yet implemented",
						access_path
					)))?
				}
			}
		};

		Ok(Box::pin(stream))
	}
}
