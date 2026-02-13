//! Graph edge scanning operator for the streaming execution engine.
//!
//! This operator scans graph edges based on a source record, direction, and
//! target edge tables. It is used to implement graph traversal idioms like
//! `person:alice->knows->person`.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use super::common::{BATCH_SIZE, evaluate_bound_key, extract_record_ids, resolve_record_batch};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::parts::LookupDirection;
use crate::exec::{
	AccessMode, ContextLevel, ControlFlowExt, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::{ControlFlow, Dir};
use crate::idx::planner::ScanDirection;
use crate::kvs::KVKey;
use crate::val::{RecordId, TableName};

/// What kind of output the GraphEdgeScan should produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GraphScanOutput {
	/// Return the target record IDs (e.g., `person:bob`)
	#[default]
	TargetId,
	/// Return the full edge records (fetched from the datastore)
	FullEdge,
}

/// Specification for an edge table to scan, optionally with ID range bounds.
///
/// When range bounds are present, the scan is restricted to edges whose IDs fall
/// within the specified range instead of scanning the entire table.
#[derive(Debug, Clone)]
pub struct EdgeTableSpec {
	/// The edge table name (e.g., `edge`, `knows`)
	pub table: TableName,
	/// Range start bound. When `Unbounded`, starts from the table prefix.
	pub range_start: Bound<Arc<dyn PhysicalExpr>>,
	/// Range end bound. When `Unbounded`, ends at the table suffix.
	pub range_end: Bound<Arc<dyn PhysicalExpr>>,
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

	/// Target edge table(s) to scan, optionally with range bounds.
	/// If empty, scans all edge tables in that direction.
	pub(crate) edge_tables: Vec<EdgeTableSpec>,

	/// What to output: EdgeId, TargetId, or FullEdge
	pub(crate) output_mode: GraphScanOutput,

	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl GraphEdgeScan {
	pub(crate) fn new(
		source: Arc<dyn PhysicalExpr>,
		direction: LookupDirection,
		edge_tables: Vec<EdgeTableSpec>,
		output_mode: GraphScanOutput,
	) -> Self {
		Self {
			source,
			direction,
			edge_tables,
			output_mode,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
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
			self.edge_tables.iter().map(|t| t.table.as_str()).collect::<Vec<_>>().join(", ")
		};
		vec![
			("source".to_string(), self.source.to_sql()),
			("direction".to_string(), dir.to_string()),
			("tables".to_string(), tables),
			("output".to_string(), format!("{:?}", self.output_mode)),
		]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		self.source.access_mode()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		let source_expr = Arc::clone(&self.source);
		let direction = self.direction;
		let edge_tables = self.edge_tables.clone();
		let output_mode = self.output_mode;
		let ctx = ctx.clone();
		let fetch_full = output_mode == GraphScanOutput::FullEdge;

		let stream = async_stream::try_stream! {
			let txn = ctx.txn();
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;

			// Evaluate the source expression to get RecordId(s)
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let source_value = source_expr.evaluate(eval_ctx).await?;
			let source_rids = extract_record_ids(source_value);

			if source_rids.is_empty() {
				return;
			}

			// Determine the directions to scan
			// Note: For Both, we scan In first then Out to match legacy executor behavior
			let directions: Vec<Dir> = match direction {
				LookupDirection::Out => vec![Dir::Out],
				LookupDirection::In => vec![Dir::In],
				LookupDirection::Both => vec![Dir::In, Dir::Out],
				LookupDirection::Reference => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"Reference lookups should use ReferenceScan, not GraphEdgeScan"
					)))?
				}
			};

			let mut rid_batch: Vec<RecordId> = Vec::with_capacity(BATCH_SIZE);

			// Scan edges for each source record
			for rid in &source_rids {
				for dir in &directions {
					// Compute all key ranges to scan for this rid + direction
					let ranges = compute_graph_ranges(
						ns_id, db_id, rid, dir, &edge_tables, &ctx,
					).await?;

					for (beg, end) in ranges {
						let kv_stream = txn.stream_keys(
							beg..end, None, None, 0, ScanDirection::Forward,
						);
						futures::pin_mut!(kv_stream);

						while let Some(result) = kv_stream.next().await {
							let keys = result.context("Failed to scan graph edge")?;

							for key in keys {
								let target_rid = decode_graph_edge(&key)?;
								rid_batch.push(target_rid);

								if rid_batch.len() >= BATCH_SIZE {
									let values = resolve_record_batch(
										&txn, ns_id, db_id, &rid_batch, fetch_full,
									).await?;
									yield ValueBatch { values };
									rid_batch.clear();
								}
							}
						}
					}
				}
			}

			// Yield remaining batch
			if !rid_batch.is_empty() {
				let values = resolve_record_batch(
					&txn, ns_id, db_id, &rid_batch, fetch_full,
				).await?;
				yield ValueBatch { values };
			}
		};

		Ok(monitor_stream(Box::pin(stream), "GraphEdgeScan", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Compute all KV key ranges to scan for a single record + direction.
///
/// When `edge_tables` is empty, returns a single wildcard range covering all
/// edges in the given direction. Otherwise returns one range per edge table,
/// respecting any range bounds on each [`EdgeTableSpec`].
async fn compute_graph_ranges(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	dir: &Dir,
	edge_tables: &[EdgeTableSpec],
	ctx: &ExecutionContext,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ControlFlow> {
	if edge_tables.is_empty() {
		// Scan all edges in this direction
		let beg = crate::key::graph::egprefix(ns_id, db_id, &rid.table, &rid.key, dir)
			.context("Failed to create graph prefix")?;
		let end = crate::key::graph::egsuffix(ns_id, db_id, &rid.table, &rid.key, dir)
			.context("Failed to create graph suffix")?;
		Ok(vec![(beg, end)])
	} else {
		let mut ranges = Vec::with_capacity(edge_tables.len());
		for spec in edge_tables {
			let beg =
				eval_graph_bound(ns_id, db_id, rid, dir, &spec.table, &spec.range_start, true, ctx)
					.await?;
			let end =
				eval_graph_bound(ns_id, db_id, rid, dir, &spec.table, &spec.range_end, false, ctx)
					.await?;
			ranges.push((beg, end));
		}
		Ok(ranges)
	}
}

/// Evaluate a single start or end bound of a graph edge key range.
///
/// `is_start` determines the fallback for `Unbounded` (prefix vs suffix) and
/// the suffix byte semantics for `Included` / `Excluded` bounds:
///
/// | Bound     | start (`is_start=true`)  | end (`is_start=false`)     |
/// |-----------|--------------------------|----------------------------|
/// | Unbounded | `ftprefix`               | `ftsuffix`                 |
/// | Included  | exact key                | key + `0x00` (include key) |
/// | Excluded  | key + `0x00` (skip past) | exact key (stop before)    |
#[allow(clippy::too_many_arguments)]
async fn eval_graph_bound(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	dir: &Dir,
	edge_table: &TableName,
	bound: &Bound<Arc<dyn PhysicalExpr>>,
	is_start: bool,
	ctx: &ExecutionContext,
) -> Result<Vec<u8>, ControlFlow> {
	match bound {
		Bound::Unbounded => {
			if is_start {
				crate::key::graph::ftprefix(ns_id, db_id, &rid.table, &rid.key, dir, edge_table)
					.context("Failed to create graph table prefix")
			} else {
				crate::key::graph::ftsuffix(ns_id, db_id, &rid.table, &rid.key, dir, edge_table)
					.context("Failed to create graph table suffix")
			}
		}
		Bound::Included(expr) => {
			let fk = evaluate_bound_key(expr, ctx).await?;
			let mut key = encode_graph_key(ns_id, db_id, rid, dir, edge_table, fk)?;
			// Included start: exact key; Included end: append suffix to include key
			if !is_start {
				key.push(0x00);
			}
			Ok(key)
		}
		Bound::Excluded(expr) => {
			let fk = evaluate_bound_key(expr, ctx).await?;
			let mut key = encode_graph_key(ns_id, db_id, rid, dir, edge_table, fk)?;
			// Excluded start: append suffix to skip past key; Excluded end: exact key
			if is_start {
				key.push(0x00);
			}
			Ok(key)
		}
	}
}

/// Encode a graph key for a specific edge table and record ID key.
fn encode_graph_key(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	dir: &Dir,
	edge_table: &TableName,
	fk: crate::val::RecordIdKey,
) -> Result<Vec<u8>, ControlFlow> {
	crate::key::graph::new(
		ns_id,
		db_id,
		&rid.table,
		&rid.key,
		dir,
		&RecordId {
			table: edge_table.clone(),
			key: fk,
		},
	)
	.encode_key()
	.context("Failed to encode graph range key")
}

/// Decode a graph key into the target [`RecordId`].
fn decode_graph_edge(key: &[u8]) -> Result<RecordId, ControlFlow> {
	let decoded =
		crate::key::graph::Graph::decode_key(key).context("Failed to decode graph key")?;
	Ok(RecordId {
		table: decoded.ft.into_owned(),
		key: decoded.fk.into_owned(),
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::{RecordIdKey, Value};

	#[test]
	fn test_graph_edge_scan_attrs() {
		use crate::exec::physical_expr::Literal;

		let scan = GraphEdgeScan::new(
			Arc::new(Literal(Value::RecordId(RecordId {
				table: "person".into(),
				key: RecordIdKey::String("alice".to_string()),
			}))),
			LookupDirection::Out,
			vec![
				EdgeTableSpec {
					table: "knows".into(),
					range_start: Bound::Unbounded,
					range_end: Bound::Unbounded,
				},
				EdgeTableSpec {
					table: "follows".into(),
					range_start: Bound::Unbounded,
					range_end: Bound::Unbounded,
				},
			],
			GraphScanOutput::TargetId,
		);

		assert_eq!(scan.name(), "GraphEdgeScan");
		let attrs = scan.attrs();
		assert!(attrs.iter().any(|(k, v)| k == "direction" && v == "->"));
		assert!(attrs.iter().any(|(k, v)| k == "tables" && v.contains("knows")));
	}
}
