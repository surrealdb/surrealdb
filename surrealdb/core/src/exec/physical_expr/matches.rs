//! Physical expression for evaluating MATCHES (`@@` / `@N@`) operators.
//!
//! MATCHES is purely index-driven: it checks whether a record is in the
//! full-text index's hit set for the given query, using `get_doc_id()` +
//! `contains_doc()` (a KV lookup + bitmap check). There is no slow
//! tokenization fallback.
//!
//! When no full-text index exists for the field, evaluation returns `false`
//! (matching the old executor's `ExecutorOption::None` path).

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::FlowResult;
use crate::expr::idiom::Idiom;
use crate::expr::operator::MatchesOperator;
use crate::idx::ft::fulltext::{FullTextIndex, QueryTerms};
use crate::val::Value;

/// Evaluates a MATCHES (`@@` / `@N@`) predicate by reading the full-text index.
///
/// Created by the planner when a `BinaryOperator::Matches` is encountered with
/// an idiom on the left and a string literal on the right. The full-text index
/// is lazily opened on first evaluation and cached for subsequent rows.
///
/// Evaluation mirrors the old executor's `fulltext_matches_with_doc_id` path:
/// 1. Resolve `RecordId → DocId` via `fti.get_doc_id()`
/// 2. Check `qt.contains_doc(doc_id)` (bitmap check)
///
/// Returns `false` when no full-text index exists for the field.
pub struct MatchesOp {
	/// Left side expression (kept for `ToSql` display).
	pub(crate) left: Arc<dyn PhysicalExpr>,
	/// Right side expression (kept for `ToSql` display).
	pub(crate) right: Arc<dyn PhysicalExpr>,
	/// The MATCHES operator (for display: `@@`, `@1@`, etc.).
	pub(crate) operator: MatchesOperator,
	/// Field idiom from the left side (used to find the matching FT index).
	pub(crate) idiom: Idiom,
	/// Search query string from the right side (extracted at plan time).
	pub(crate) query: String,
	/// Cached full-text index resources. `None` = no FT index found (always `false`).
	ft_cache: tokio::sync::OnceCell<Option<(FullTextIndex, QueryTerms)>>,
}

impl MatchesOp {
	/// Create a new MatchesOp.
	pub fn new(
		left: Arc<dyn PhysicalExpr>,
		right: Arc<dyn PhysicalExpr>,
		operator: MatchesOperator,
		idiom: Idiom,
		query: String,
	) -> Self {
		Self {
			left,
			right,
			operator,
			idiom,
			query,
			ft_cache: tokio::sync::OnceCell::new(),
		}
	}

	/// Get or lazily initialize the full-text index resources.
	///
	/// Returns `None` if no full-text index exists for this field/table,
	/// in which case MATCHES always evaluates to `false`.
	///
	/// This mirrors `MatchContext::ft_resources()` but without the Scorer
	/// (scoring is handled separately by `search::score()` via `IndexFunction`).
	async fn ft_resources(
		&self,
		ctx: &EvalContext<'_>,
	) -> Result<&Option<(FullTextIndex, QueryTerms)>, anyhow::Error> {
		self.ft_cache
			.get_or_try_init(|| async {
				use crate::catalog::providers::TableProvider;

				let frozen = ctx.exec_ctx.ctx();
				let root = ctx.exec_ctx.root();
				let opt = root
					.options
					.as_ref()
					.ok_or_else(|| anyhow::anyhow!("MatchesOp requires Options context"))?;
				let tx = ctx.txn();

				// Get namespace and database IDs from the execution context
				let db_ctx = ctx
					.exec_ctx
					.database()
					.map_err(|e| anyhow::anyhow!("MatchesOp requires database context: {}", e))?;
				let ns_id = db_ctx.ns_ctx.ns.namespace_id;
				let db_id = db_ctx.db.database_id;

				// Determine the table name. We need it to look up the index definition.
				// Extract from the current value's RecordId if available, otherwise
				// fall back to the matches context on the FrozenContext.
				let table_name = if let Some(value) = ctx.current_value {
					extract_table_from_value(value)
				} else {
					None
				};
				let table_name = match table_name {
					Some(t) => t,
					None => {
						// Try to get it from the matches context (set by the SELECT planner)
						if let Some(mc) = frozen.get_matches_context()
							&& let Some(table) = mc.table()
						{
							table.clone()
						} else {
							// No table name available → cannot find the index
							return Ok(None);
						}
					}
				};

				// Find the full-text index for this table and idiom
				let indexes = tx.all_tb_indexes(ns_id, db_id, &table_name).await?;
				let index_def = indexes.iter().find(|idx| {
					matches!(&idx.index, crate::catalog::Index::FullText(_))
						&& idx.cols.iter().any(|col| col.0 == self.idiom.0)
				});

				let index_def = match index_def {
					Some(def) => def,
					// No full-text index for this field → MATCHES always returns false
					None => return Ok(None),
				};

				let ft_params = match &index_def.index {
					crate::catalog::Index::FullText(params) => params,
					_ => unreachable!("Already checked for FullText above"),
				};

				let ikb =
					crate::idx::IndexKeyBase::new(ns_id, db_id, table_name, index_def.index_id);

				// Open the full-text index
				let fti =
					FullTextIndex::new(frozen.get_index_stores(), tx.as_ref(), ikb, ft_params)
						.await?;

				// Extract query terms
				let query_terms = {
					let mut stack = reblessive::TreeStack::new();
					stack
						.enter(|stk| {
							fti.extract_querying_terms(stk, frozen, opt, self.query.clone())
						})
						.finish()
						.await?
				};

				Ok(Some((fti, query_terms)))
			})
			.await
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for MatchesOp {
	fn name(&self) -> &'static str {
		"MatchesOp"
	}

	fn required_context(&self) -> ContextLevel {
		// Need Root context for transaction and index store access,
		// plus whatever the child expressions need.
		let children = self.left.required_context().max(self.right.required_context());
		children.max(ContextLevel::Root)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let ft = self.ft_resources(&ctx).await?;

		let (fti, qt) = match ft {
			// No full-text index → always false (old executor ExecutorOption::None path)
			None => return Ok(Value::Bool(false)),
			Some(resources) => resources,
		};

		// Empty query terms → no possible matches
		if qt.is_empty() {
			return Ok(Value::Bool(false));
		}

		// Extract RecordId from the current value
		let rid = extract_record_id(ctx.current_value)?;

		let tx = ctx.txn();

		// Resolve RecordId → DocId via the full-text index, then bitmap check.
		// This mirrors the old executor's `fulltext_matches_with_doc_id` path.
		let matches = match fti.get_doc_id(&tx, &rid).await? {
			Some(doc_id) => qt.contains_doc(doc_id),
			// Record not in the index → doesn't match
			None => false,
		};

		Ok(Value::Bool(matches))
	}

	fn references_current_value(&self) -> bool {
		// Always references the current document (needs RecordId)
		true
	}

	fn access_mode(&self) -> AccessMode {
		// Read-only: we only read from the FT index
		self.left.access_mode().combine(self.right.access_mode())
	}
}

impl ToSql for MatchesOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{} {} {}", self.left, self.operator, self.right);
	}
}

impl Clone for MatchesOp {
	fn clone(&self) -> Self {
		Self {
			left: self.left.clone(),
			right: self.right.clone(),
			operator: self.operator.clone(),
			idiom: self.idiom.clone(),
			query: self.query.clone(),
			// OnceCell is not Clone — new instance starts uninitialized.
			// This is fine: the clone will lazily re-init on first evaluate().
			ft_cache: tokio::sync::OnceCell::new(),
		}
	}
}

impl std::fmt::Debug for MatchesOp {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("MatchesOp")
			.field("idiom", &self.idiom)
			.field("query", &self.query)
			.field("operator", &self.operator)
			.field("initialized", &self.ft_cache.initialized())
			.finish()
	}
}

/// Extract the RecordId from the current row value.
fn extract_record_id(value: Option<&Value>) -> Result<crate::val::RecordId, anyhow::Error> {
	let current =
		value.ok_or_else(|| anyhow::anyhow!("MATCHES evaluation requires a current document"))?;

	match current {
		Value::Object(obj) => match obj.get("id") {
			Some(Value::RecordId(rid)) => Ok(rid.clone()),
			Some(_) => Err(anyhow::anyhow!("Current document 'id' field is not a RecordId")),
			None => Err(anyhow::anyhow!("Current document has no 'id' field")),
		},
		Value::RecordId(rid) => Ok(rid.clone()),
		_ => Err(anyhow::anyhow!(
			"Expected current document to be an Object for MATCHES evaluation, got: {}",
			current.kind_of()
		)),
	}
}

/// Try to extract a table name from a Value's RecordId.
fn extract_table_from_value(value: &Value) -> Option<crate::val::TableName> {
	match value {
		Value::Object(obj) => match obj.get("id") {
			Some(Value::RecordId(rid)) => Some(rid.table.clone()),
			_ => None,
		},
		Value::RecordId(rid) => Some(rid.table.clone()),
		_ => None,
	}
}
