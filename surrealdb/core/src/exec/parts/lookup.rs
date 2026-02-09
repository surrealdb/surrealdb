//! Lookup part -- graph/reference traversal.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::ExecOperator;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::FlowResult;
use crate::idx::planner::ScanDirection;
use crate::val::{RecordId, Value};

/// Direction for lookup operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupDirection {
	/// Outgoing edges: `->`
	Out,
	/// Incoming edges: `<-`
	In,
	/// Both directions: `<->`
	Both,
	/// Record references: `<~`
	Reference,
}

impl From<crate::expr::Dir> for LookupDirection {
	fn from(dir: crate::expr::Dir) -> Self {
		match dir {
			crate::expr::Dir::Out => LookupDirection::Out,
			crate::expr::Dir::In => LookupDirection::In,
			crate::expr::Dir::Both => LookupDirection::Both,
		}
	}
}

impl From<&crate::expr::Dir> for LookupDirection {
	fn from(dir: &crate::expr::Dir) -> Self {
		match dir {
			crate::expr::Dir::Out => LookupDirection::Out,
			crate::expr::Dir::In => LookupDirection::In,
			crate::expr::Dir::Both => LookupDirection::Both,
		}
	}
}

/// Graph/reference lookup - `->edge->target`, `<-edge<-source`, `<~table`.
#[derive(Debug, Clone)]
pub struct LookupPart {
	/// The direction of the lookup (In, Out, Both for graph; Reference for <~)
	pub direction: LookupDirection,

	/// The edge/reference tables to scan (e.g., `knows`, `follows`)
	/// Empty means scan all tables in that direction.
	pub edge_tables: Vec<crate::val::TableName>,

	/// The pre-planned operator tree for executing the lookup.
	/// This includes GraphEdgeScan/ReferenceScan + optional Filter, Sort, Limit, Project.
	pub plan: Arc<dyn ExecOperator>,

	/// When true, extract just the RecordId from result objects.
	/// This is set when the scan uses FullEdge mode for WHERE/SPLIT filtering
	/// but no explicit SELECT clause is present, so the final result should be
	/// RecordIds rather than full objects.
	pub extract_id: bool,
}

#[async_trait]
impl PhysicalExpr for LookupPart {
	fn name(&self) -> &'static str {
		"Lookup"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		Ok(evaluate_lookup(&value, self, ctx).await?)
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		self.plan.access_mode()
	}
}

impl ToSql for LookupPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self.direction {
			LookupDirection::Out => f.push_str("->..."),
			LookupDirection::In => f.push_str("<-..."),
			LookupDirection::Both => f.push_str("<->..."),
			LookupDirection::Reference => f.push_str("<~..."),
		}
	}
}

/// Lookup evaluation - graph/reference traversal.
async fn evaluate_lookup(
	value: &Value,
	lookup: &LookupPart,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	match value {
		Value::RecordId(rid) => {
			// Perform graph edge scan for this RecordId
			evaluate_lookup_for_rid(rid, lookup, ctx).await
		}
		Value::Object(obj) => {
			// When lookup is on an Object, extract the `id` field and evaluate on that
			// This matches SurrealDB semantics: `->edge` on an object uses its `id`
			match obj.get("id") {
				Some(Value::RecordId(rid)) => {
					Box::pin(evaluate_lookup(&Value::RecordId(rid.clone()), lookup, ctx)).await
				}
				Some(other) => {
					// If `id` is not a RecordId, try to evaluate on it anyway
					Box::pin(evaluate_lookup(other, lookup, ctx)).await
				}
				None => Ok(Value::None),
			}
		}
		Value::Array(arr) => {
			// Apply lookup to each element and flatten results
			// This matches SurrealDB semantics: `->edge` on an array of records
			// returns a flat array of all targets, not nested arrays
			let mut results = Vec::new();
			for item in arr.iter() {
				let result = Box::pin(evaluate_lookup(item, lookup, ctx.clone())).await?;
				// Flatten: extend results with array elements, or push single values
				match result {
					Value::Array(inner) => results.extend(inner.into_iter()),
					other => results.push(other),
				}
			}
			Ok(Value::Array(results.into()))
		}
		_ => Ok(Value::None),
	}
}

/// Perform graph/reference lookup for a specific RecordId by executing the pre-planned operator
/// tree.
async fn evaluate_lookup_for_rid(
	rid: &RecordId,
	lookup: &LookupPart,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	use crate::exec::planner::LOOKUP_SOURCE_PARAM;

	// Create a new execution context with the source RecordId bound to the special parameter.
	let bound_ctx = ctx.exec_ctx.with_param(LOOKUP_SOURCE_PARAM, Value::RecordId(rid.clone()));

	// Execute the lookup plan
	let stream = lookup.plan.execute(&bound_ctx).map_err(|e| match e {
		crate::expr::ControlFlow::Err(e) => e,
		crate::expr::ControlFlow::Return(v) => {
			anyhow::anyhow!("Unexpected return in lookup: {:?}", v)
		}
		crate::expr::ControlFlow::Break => anyhow::anyhow!("Unexpected break in lookup"),
		crate::expr::ControlFlow::Continue => anyhow::anyhow!("Unexpected continue in lookup"),
	})?;

	// Collect all results into an array
	let mut results = Vec::new();
	futures::pin_mut!(stream);

	while let Some(batch_result) = stream.next().await {
		let batch = batch_result.map_err(|e| match e {
			crate::expr::ControlFlow::Err(e) => e,
			crate::expr::ControlFlow::Return(v) => {
				anyhow::anyhow!("Unexpected return in lookup: {:?}", v)
			}
			crate::expr::ControlFlow::Break => anyhow::anyhow!("Unexpected break in lookup"),
			crate::expr::ControlFlow::Continue => {
				anyhow::anyhow!("Unexpected continue in lookup")
			}
		})?;
		results.extend(batch.values);
	}

	// When extract_id is set, the scan used FullEdge mode for WHERE/SPLIT filtering
	// but no explicit SELECT clause was present. Project results back to RecordIds.
	if lookup.extract_id {
		let results = results
			.into_iter()
			.filter_map(|v| match v {
				Value::Object(ref obj) => {
					obj.get("id").filter(|id| matches!(id, Value::RecordId(_))).cloned()
				}
				Value::RecordId(_) => Some(v),
				_ => None,
			})
			.collect();
		return Ok(Value::Array(results));
	}

	Ok(Value::Array(results.into()))
}

/// Perform reference lookup (<~) for a specific RecordId.
///
/// Reference lookups find all records that reference the given record ID
/// through a specific field. This is the inverse of a record link.
pub(crate) async fn evaluate_reference_lookup(
	rid: &RecordId,
	lookup: &LookupPart,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	// Get database context
	let db_ctx = ctx.exec_ctx.database().map_err(|e| anyhow::anyhow!("{}", e))?;
	let txn = ctx.exec_ctx.txn();
	let ns = &db_ctx.ns_ctx.ns;
	let db = &db_ctx.db;

	let mut results = Vec::new();

	// For reference lookups, edge_tables contains the referencing tables
	// If empty, we need to scan all references
	if lookup.edge_tables.is_empty() {
		// Scan all references to this record
		let beg = crate::key::r#ref::prefix(ns.namespace_id, db.database_id, &rid.table, &rid.key)
			.map_err(|e| anyhow::anyhow!("Failed to create prefix: {}", e))?;

		let end = crate::key::r#ref::suffix(ns.namespace_id, db.database_id, &rid.table, &rid.key)
			.map_err(|e| anyhow::anyhow!("Failed to create suffix: {}", e))?;

		let kv_stream = txn.stream_keys(beg..end, None, None, ScanDirection::Forward);
		futures::pin_mut!(kv_stream);

		while let Some(result) = kv_stream.next().await {
			let key = result.map_err(|e| anyhow::anyhow!("Failed to scan reference: {}", e))?;

			// Decode the reference key to get the referencing record ID
			let decoded = crate::key::r#ref::Ref::decode_key(&key)
				.map_err(|e| anyhow::anyhow!("Failed to decode ref key: {}", e))?;

			let referencing_rid = RecordId {
				table: decoded.ft.into_owned(),
				key: decoded.fk.into_owned(),
			};
			results.push(Value::RecordId(referencing_rid));
		}
	} else {
		// Scan references from specific tables
		for ref_table in &lookup.edge_tables {
			let beg = crate::key::r#ref::ftprefix(
				ns.namespace_id,
				db.database_id,
				&rid.table,
				&rid.key,
				ref_table.as_str(),
			)
			.map_err(|e| anyhow::anyhow!("Failed to create prefix: {}", e))?;

			let end = crate::key::r#ref::ftsuffix(
				ns.namespace_id,
				db.database_id,
				&rid.table,
				&rid.key,
				ref_table.as_str(),
			)
			.map_err(|e| anyhow::anyhow!("Failed to create suffix: {}", e))?;

			let kv_stream = txn.stream_keys(beg..end, None, None, ScanDirection::Forward);
			futures::pin_mut!(kv_stream);

			while let Some(result) = kv_stream.next().await {
				let key =
					result.map_err(|e| anyhow::anyhow!("Failed to scan reference: {}", e))?;

				let decoded = crate::key::r#ref::Ref::decode_key(&key)
					.map_err(|e| anyhow::anyhow!("Failed to decode ref key: {}", e))?;

				let referencing_rid = RecordId {
					table: decoded.ft.into_owned(),
					key: decoded.fk.into_owned(),
				};
				results.push(Value::RecordId(referencing_rid));
			}
		}
	}

	Ok(Value::Array(results.into()))
}
