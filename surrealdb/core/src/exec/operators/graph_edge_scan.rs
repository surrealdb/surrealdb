//! Graph edge scanning operator for the streaming execution engine.
//!
//! This operator scans graph edges based on a source record, direction, and
//! target edge tables. It is used to implement graph traversal idioms like
//! `person:alice->knows->person`.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::catalog::providers::TableProvider;
use crate::exec::physical_part::LookupDirection;
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::expr::{ControlFlow, Dir};
use crate::idx::planner::ScanDirection;
use crate::val::{RecordId, TableName, Value};

/// Batch size for collecting graph edges before yielding.
const BATCH_SIZE: usize = 1000;

/// What kind of output the GraphEdgeScan should produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]

pub enum GraphScanOutput {
	/// Return the edge record IDs (e.g., `knows:1`)
	EdgeId,
	/// Return the target record IDs (e.g., `person:bob`)
	TargetId,
	/// Return the full edge records (fetched from the datastore)
	FullEdge,
}

impl Default for GraphScanOutput {
	fn default() -> Self {
		Self::TargetId
	}
}

/// Scans graph edges for a given source record.
///
/// This operator takes a source expression (which should evaluate to one or more RecordIds),
/// a direction (In, Out, or Both), and target edge tables to scan. It produces a stream
/// of either edge IDs, target IDs, or full edge records depending on the output mode.
#[derive(Debug, Clone)]
pub struct GraphEdgeScan {
	/// Source expression that evaluates to RecordId(s)
	pub(crate) source: Arc<dyn PhysicalExpr>,

	/// Direction of the edge traversal (In = `<-`, Out = `->`, Both = `<->`)
	pub(crate) direction: LookupDirection,

	/// Target edge table(s) to scan (e.g., `knows`, `follows`)
	/// If empty, scans all edge tables in that direction.
	pub(crate) edge_tables: Vec<TableName>,

	/// What to output: EdgeId, TargetId, or FullEdge
	pub(crate) output_mode: GraphScanOutput,
}

#[async_trait]
impl ExecOperator for GraphEdgeScan {
	fn name(&self) -> &'static str {
		"GraphEdgeScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let dir = match self.direction {
			LookupDirection::Out => "->",
			LookupDirection::In => "<-",
			LookupDirection::Both => "<->",
			LookupDirection::Reference => "<~",
		};
		let tables = if self.edge_tables.is_empty() {
			"*".to_string()
		} else {
			self.edge_tables.iter().map(|t| t.as_str()).collect::<Vec<_>>().join(", ")
		};
		vec![
			("source".to_string(), self.source.to_sql()),
			("direction".to_string(), dir.to_string()),
			("tables".to_string(), tables),
			("output".to_string(), format!("{:?}", self.output_mode)),
		]
	}

	fn required_context(&self) -> ContextLevel {
		// Graph edge scanning requires database context
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		// Graph scan is read-only, but propagate source expression's access mode
		self.source.access_mode()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		let source_expr = Arc::clone(&self.source);
		let direction = self.direction;
		let edge_tables = self.edge_tables.clone();
		let output_mode = self.output_mode;
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let txn = Arc::clone(ctx.txn());
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Evaluate the source expression to get RecordId(s)
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let source_value = source_expr.evaluate(eval_ctx).await.map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!("Failed to evaluate source: {}", e))
			})?;

			// Convert source value to a list of RecordIds
			let source_rids = match source_value {
				Value::RecordId(rid) => vec![rid],
				Value::Array(arr) => {
					let mut rids = Vec::with_capacity(arr.len());
					for v in arr.iter() {
						if let Value::RecordId(rid) = v {
							rids.push(rid.clone());
						}
					}
					rids
				}
				_ => vec![],
			};

			if source_rids.is_empty() {
				return;
			}

			// Determine the directions to scan
			let directions: Vec<Dir> = match direction {
				LookupDirection::Out => vec![Dir::Out],
				LookupDirection::In => vec![Dir::In],
				LookupDirection::Both => vec![Dir::Out, Dir::In],
				LookupDirection::Reference => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"Reference lookups should use ReferenceScan, not GraphEdgeScan"
					)))?
				}
			};

			let mut batch = Vec::with_capacity(BATCH_SIZE);

			// Scan edges for each source record
			for rid in &source_rids {
				for dir in &directions {
					// If specific edge tables are provided, scan each one
					// Otherwise, scan all edges in that direction
					if edge_tables.is_empty() {
						// Scan all edges in this direction
						let beg = crate::key::graph::egprefix(
							ns.namespace_id,
							db.database_id,
							&rid.table,
							&rid.key,
							dir,
						).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix: {}", e)))?;

						let end = crate::key::graph::egsuffix(
							ns.namespace_id,
							db.database_id,
							&rid.table,
							&rid.key,
							dir,
						).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix: {}", e)))?;

						let kv_stream = txn.stream_keys(beg..end, None, None, ScanDirection::Forward);
						futures::pin_mut!(kv_stream);

						while let Some(result) = kv_stream.next().await {
							let key = result.map_err(|e| {
								ControlFlow::Err(anyhow::anyhow!("Failed to scan graph edge: {}", e))
							})?;

							let value = decode_graph_key(&key, output_mode, &ctx).await?;
							batch.push(value);

							if batch.len() >= BATCH_SIZE {
								yield ValueBatch { values: std::mem::take(&mut batch) };
								batch.reserve(BATCH_SIZE);
							}
						}
					} else {
						// Scan specific edge tables
						for edge_table in &edge_tables {
							let beg = crate::key::graph::ftprefix(
								ns.namespace_id,
								db.database_id,
								&rid.table,
								&rid.key,
								dir,
								edge_table,
							).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix: {}", e)))?;

							let end = crate::key::graph::ftsuffix(
								ns.namespace_id,
								db.database_id,
								&rid.table,
								&rid.key,
								dir,
								edge_table,
							).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix: {}", e)))?;

							let kv_stream = txn.stream_keys(beg..end, None, None, ScanDirection::Forward);
							futures::pin_mut!(kv_stream);

							while let Some(result) = kv_stream.next().await {
								let key = result.map_err(|e| {
									ControlFlow::Err(anyhow::anyhow!("Failed to scan graph edge: {}", e))
								})?;

								let value = decode_graph_key(&key, output_mode, &ctx).await?;
								batch.push(value);

								if batch.len() >= BATCH_SIZE {
									yield ValueBatch { values: std::mem::take(&mut batch) };
									batch.reserve(BATCH_SIZE);
								}
							}
						}
					}
				}
			}

			// Yield remaining batch
			if !batch.is_empty() {
				yield ValueBatch { values: batch };
			}
		};

		Ok(Box::pin(stream))
	}
}

/// Decode a graph key and return the appropriate value based on output mode.
async fn decode_graph_key(
	key: &[u8],
	output_mode: GraphScanOutput,
	ctx: &ExecutionContext,
) -> Result<Value, ControlFlow> {
	// Decode the graph key to extract the foreign table and key
	let decoded = crate::key::graph::Graph::decode_key(key)
		.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to decode graph key: {}", e)))?;

	// The foreign record ID is what we're looking for (the target of the edge)
	let target_rid = RecordId {
		table: decoded.ft.into_owned(),
		key: decoded.fk.into_owned(),
	};

	match output_mode {
		GraphScanOutput::TargetId => Ok(Value::RecordId(target_rid)),
		GraphScanOutput::EdgeId => {
			// The edge record ID uses the source table (which is stored in the key)
			// and the source key. However, this is the graph key structure, not the edge record.
			// For edges, we need to construct the edge ID from the source/target relationship.
			// The actual edge record ID would be something like `knows:123`.
			// This is more complex - for now, return the target ID.
			// TODO: Properly extract edge record IDs from graph metadata
			Ok(Value::RecordId(target_rid))
		}
		GraphScanOutput::FullEdge => {
			// Fetch the full edge record
			let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
			let txn = ctx.txn();

			let record = txn
				.get_record(
					db_ctx.ns_ctx.ns.namespace_id,
					db_ctx.db.database_id,
					&target_rid.table,
					&target_rid.key,
					None,
				)
				.await
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to fetch record: {}", e)))?;

			if record.data.as_ref().is_none() {
				Ok(Value::None)
			} else {
				let mut value = record.data.as_ref().clone();
				value.def(&target_rid);
				Ok(value)
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::RecordIdKey;

	#[test]
	fn test_graph_edge_scan_attrs() {
		use crate::exec::physical_expr::Literal;

		let scan = GraphEdgeScan {
			source: Arc::new(Literal(Value::RecordId(RecordId {
				table: "person".into(),
				key: RecordIdKey::String("alice".to_string()),
			}))),
			direction: LookupDirection::Out,
			edge_tables: vec!["knows".into(), "follows".into()],
			output_mode: GraphScanOutput::TargetId,
		};

		assert_eq!(scan.name(), "GraphEdgeScan");
		let attrs = scan.attrs();
		assert!(attrs.iter().any(|(k, v)| k == "direction" && v == "->"));
		assert!(attrs.iter().any(|(k, v)| k == "tables" && v.contains("knows")));
	}
}
