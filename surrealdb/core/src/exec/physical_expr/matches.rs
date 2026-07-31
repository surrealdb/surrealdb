//! Physical expression for evaluating MATCHES (`@@` / `@N@`) operators.
//!
//! Evaluation mirrors the legacy executor's per-row decision tree exactly
//! (`QueryExecutor::matches` + `fnc::operate::get_executor_option`):
//!
//! 1. Rows without a record id never match and never error (legacy `ExecutorOption::None`:
//!    array/value sources, documents without `id`).
//! 2. Rows of tables the enclosing SELECT does not iterate as a full table source evaluate to
//!    `false` — record-id sources and graph/link hops never get a legacy `QueryExecutor`. See
//!    [`MatchesScope`].
//! 3. On executor-table rows, an expression that was **not** registered from the SELECT's WHERE
//!    condition (projection position, function arguments, a different expression than the one in
//!    WHERE, or no WHERE at all) raises [`Error::NoIndexFoundForMatch`].
//! 4. A registered expression resolves an index for the row's table:
//!    - a full-text index on the row's own table whose first column is the idiom → `RecordId →
//!      DocId` + bitmap check (`get_doc_id()` + `contains_doc()`);
//!    - otherwise a record-link traversal (`t.name @@ 'x'` where `t` is a `record<..>`-typed field)
//!      to a full-text index on the link target table → the idiom's value is evaluated and analyzed
//!      with the remote index's analyzer, then compared against the query terms
//!      (`FullTextIndex::matches_value`), matching the legacy "matches with value" path;
//!    - if neither resolves, the outcome follows rule 3 ([`Error::NoIndexFoundForMatch`]).
//!
//! SECURITY: when permissions are enforced for the session, indexes whose
//! columns are governed by a field definition with a non-`Full` SELECT
//! permission are never used — mirroring the legacy planner's
//! `resolve_indexes` guard — so a record user cannot learn whether a
//! restricted field matches a search query. Such expressions fall through to
//! rule 2 and error on table sources exactly like the legacy executor.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::catalog::{FieldDefinition, Index, Permission};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel};
use crate::expr::idiom::Idiom;
use crate::expr::operator::{BinaryOperator, MatchesOperator, PrefixOperator};
use crate::expr::{Expr, FlowResult, Kind};
use crate::iam::Action;
use crate::idx::ft::fulltext::{FullTextIndex, QueryTerms};
use crate::idx::{Error, IndexKeyBase};
use crate::kvs::index::filter_online_indexes;
use crate::val::{TableName, Value};

/// Per-SELECT registration scope for MATCHES expressions, mirroring which
/// expressions the legacy planner's `Tree` analysis registers on a table's
/// `QueryExecutor`.
///
/// The legacy tree walks the WHERE condition through `Expr::Binary` nodes
/// only — a MATCHES nested inside a function argument, an idiom part filter,
/// or appearing in a projection is never registered and errors at runtime on
/// executor-table rows. `allowlist` reproduces that reachable set, plus the
/// operand of a logical NOT so the bitmap-fusion planner's negated full-text
/// branch (`!(body @@ 'x')`) stays evaluable in the residual filter (see
/// [`collect_cond_matches`]). It is built from both the original condition and
/// its param-resolved/constant-folded form so plan-time rewrites don't break
/// node identity, and `executor_tables` lists
/// the tables the legacy executor would have created a `QueryExecutor` for:
/// full table sources only — record-id sources never get one (see
/// `Iterator::prepare_record_id`), so their rows evaluate MATCHES to `false`
/// even when the expression sits in the WHERE condition.
pub(crate) struct MatchesScope {
	/// MATCHES binary expressions reachable from the WHERE condition through
	/// `Expr::Binary` nodes.
	pub(crate) allowlist: HashSet<Expr>,
	/// Tables iterated as full table sources by this SELECT.
	pub(crate) executor_tables: Arc<[TableName]>,
}

/// Collect MATCHES expressions reachable from `expr` through `Expr::Binary`
/// nodes and logical-NOT (`Expr::Prefix { Not, .. }`) nodes.
///
/// The legacy `Tree::eval_value` traversal walks `Expr::Binary` only and treats
/// every other node (function calls, arrays, idioms, prefixes) as an opaque
/// leaf. The new executor's bitmap-fusion planner additionally turns a negated
/// full-text branch (`... AND !(body @@ 'x')`) into a `BitmapAndNot` over the
/// term's posting bitmap and keeps the negated MATCHES in the residual `Filter`
/// for per-row re-evaluation — so the inner MATCHES must be registered too, or
/// it would raise [`Error::NoIndexFoundForMatch`]. Descending through the `Not`
/// prefix (a deliberate extension past the legacy walk) registers it.
pub(crate) fn collect_cond_matches(expr: &Expr, out: &mut HashSet<Expr>) {
	match expr {
		Expr::Binary {
			left,
			op,
			right,
		} => {
			if matches!(op, BinaryOperator::Matches(_)) {
				out.insert(expr.clone());
			}
			collect_cond_matches(left, out);
			collect_cond_matches(right, out);
		}
		Expr::Prefix {
			op: PrefixOperator::Not,
			expr: inner,
		} => {
			collect_cond_matches(inner, out);
		}
		_ => {}
	}
}

/// A resolved evaluation strategy for one row table.
enum MatchTarget {
	/// Full-text index on the row's own table: check via `RecordId → DocId`
	/// and the query-term bitmaps.
	Local(FullTextIndex, QueryTerms),
	/// Full-text index on a record-link target table: evaluate the idiom's
	/// value and analyze it with the remote index's analyzer.
	Remote(FullTextIndex, QueryTerms),
	/// No usable full-text index for this table.
	Unresolved,
}

/// Evaluates a MATCHES (`@@` / `@N@`) predicate.
///
/// Created by the planner when a `BinaryOperator::Matches` is encountered
/// with an idiom on the left and a string literal (or resolvable bind
/// parameter) on the right. Index resolution is performed lazily per row
/// table and cached for subsequent rows. See the module docs for the full
/// decision tree and its legacy-executor mapping.
pub struct MatchesOp {
	/// Left side expression (the idiom side — also evaluated per row on the
	/// record-link path).
	pub(crate) left: Arc<dyn PhysicalExpr>,
	/// Right side expression (kept for `ToSql` display).
	pub(crate) right: Arc<dyn PhysicalExpr>,
	/// The MATCHES operator (`@@`, `@1@`, `@AND@`, …; carries the boolean
	/// operator used on the record-link value path).
	pub(crate) operator: MatchesOperator,
	/// Field idiom from the left side (used to find the matching FT index).
	pub(crate) idiom: Idiom,
	/// Search query string from the right side (extracted at plan time).
	pub(crate) query: String,
	/// Whether this expression was registered from the enclosing SELECT's
	/// WHERE condition (see [`MatchesScope`]).
	registered: bool,
	/// Tables whose rows a legacy `QueryExecutor` would evaluate MATCHES
	/// against. Rows of any other table evaluate to `false`; rows of these
	/// tables raise [`Error::NoIndexFoundForMatch`] when the expression is
	/// unregistered or unresolvable.
	executor_tables: Arc<[TableName]>,
	/// Per-row-table resolution cache.
	resolution: tokio::sync::Mutex<HashMap<TableName, Arc<MatchTarget>>>,
}

impl MatchesOp {
	/// Create a new MatchesOp.
	pub(crate) fn new(
		left: Arc<dyn PhysicalExpr>,
		right: Arc<dyn PhysicalExpr>,
		operator: MatchesOperator,
		idiom: Idiom,
		query: String,
		registered: bool,
		executor_tables: Arc<[TableName]>,
	) -> Self {
		Self {
			left,
			right,
			operator,
			idiom,
			query,
			registered,
			executor_tables,
			resolution: tokio::sync::Mutex::new(HashMap::new()),
		}
	}

	/// The legacy "executor exists but no entry for this expression" outcome.
	fn no_index_error(&self) -> FlowResult<Value> {
		Err(anyhow::Error::new(Error::NoIndexFoundForMatch {
			exp: self.to_sql(),
		})
		.into())
	}

	/// Get (or lazily compute) the evaluation strategy for a row table.
	async fn resolve_for_table(
		&self,
		ctx: &EvalContext<'_>,
		table: &TableName,
	) -> Result<Arc<MatchTarget>, anyhow::Error> {
		{
			let cache = self.resolution.lock().await;
			if let Some(t) = cache.get(table) {
				return Ok(Arc::clone(t));
			}
		}
		let resolved = Arc::new(self.resolve_uncached(ctx, table).await?);
		self.resolution.lock().await.insert(table.clone(), Arc::clone(&resolved));
		Ok(resolved)
	}

	/// Resolve the full-text index for `table`, mirroring the legacy tree's
	/// `resolve_idiom`: first a local index on the table itself, then a
	/// record-link traversal via the table's field definitions.
	async fn resolve_uncached(
		&self,
		ctx: &EvalContext<'_>,
		table: &TableName,
	) -> Result<MatchTarget, anyhow::Error> {
		use crate::catalog::providers::TableProvider;

		let frozen = ctx.exec_ctx.ctx();
		let root = ctx.exec_ctx.root();
		let opt = root
			.options
			.as_ref()
			.ok_or_else(|| anyhow::anyhow!("MatchesOp requires Options context"))?;
		let tx = ctx.txn();

		let db_ctx = ctx
			.exec_ctx
			.database()
			.map_err(|e| anyhow::anyhow!("MatchesOp requires database context: {}", e))?;
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;
		let version = ctx.exec_ctx.version_stamp();

		// SECURITY: when permissions are enforced, refuse indexes whose
		// columns touch a field with a restrictive SELECT permission (legacy
		// `Tree::resolve_indexes` guard) — otherwise the MATCHES outcome
		// would leak whether a hidden field matches the query.
		let check_perms = crate::exec::permission::should_check_perms(db_ctx, Action::View)?;
		let fields = tx.all_tb_fields(ns_id, db_id, table, version).await?;

		// 1. Full-text index on the row's own table.
		let indexes = tx.all_tb_indexes(ns_id, db_id, table, version).await?;
		let indexes = if version.is_none() {
			// MATCHES must not read a full-text index until durable state
			// has published it as queryable.
			filter_online_indexes(tx.as_ref(), ns_id, db_id, indexes).await?
		} else {
			indexes
		};
		let local = indexes.iter().find(|idx| {
			matches!(&idx.index, Index::FullText(_))
				&& idx.cols.contains(&self.idiom)
				&& !(check_perms && index_columns_touch_restricted(&idx.cols, &fields))
		});
		if let Some(index_def) = local {
			let (fti, qt) =
				self.open_index(ctx, table.clone(), index_def, frozen, opt, &tx).await?;
			return Ok(MatchTarget::Local(fti, qt));
		}

		// 2. Record-link traversal: the first field definition of `Record`
		// kind whose name prefixes the idiom decides (legacy
		// `resolve_record_field`); auto-defined `field[*]` children make
		// `ts[*].name` resolvable while `ts.name` is not.
		if self.idiom.0.len() > 1 {
			for field in fields.iter() {
				let Some(Kind::Record(targets)) = &field.field_kind else {
					continue;
				};
				if !self.idiom.starts_with(&field.name.0) {
					continue;
				}
				let remote_field = &self.idiom.0[field.name.0.len()..];
				if remote_field.is_empty() {
					break;
				}
				// Walk the link targets in declaration order; keep the last
				// resolvable target seen before the first unresolvable one
				// (the legacy executor inserts one entry per resolved target
				// and stops at the first failure — the last insert wins).
				let mut resolved = None;
				for target in targets {
					match self
						.resolve_remote_target(
							ctx,
							target,
							remote_field,
							check_perms,
							frozen,
							opt,
							&tx,
							ns_id,
							db_id,
						)
						.await?
					{
						Some(r) => resolved = Some(r),
						None => break,
					}
				}
				return Ok(match resolved {
					Some((fti, qt)) => MatchTarget::Remote(fti, qt),
					None => MatchTarget::Unresolved,
				});
			}
		}

		Ok(MatchTarget::Unresolved)
	}

	/// Resolve a full-text index on one record-link target table whose first
	/// column is the remaining idiom path.
	#[expect(clippy::too_many_arguments)]
	async fn resolve_remote_target(
		&self,
		ctx: &EvalContext<'_>,
		target: &TableName,
		remote_field: &[crate::expr::part::Part],
		check_perms: bool,
		frozen: &crate::ctx::FrozenContext,
		opt: &crate::dbs::Options,
		tx: &Arc<crate::kvs::Transaction>,
		ns_id: crate::catalog::NamespaceId,
		db_id: crate::catalog::DatabaseId,
	) -> Result<Option<(FullTextIndex, QueryTerms)>, anyhow::Error> {
		use crate::catalog::providers::TableProvider;

		let version = ctx.exec_ctx.version_stamp();
		let fields = tx.all_tb_fields(ns_id, db_id, target, version).await?;
		// No online-index filter here: the value path only uses the index's
		// analyzer configuration and query tokens, never its document
		// bitmaps, so build state cannot affect results (and the legacy
		// planner registers remote indexes regardless of build state).
		let indexes = tx.all_tb_indexes(ns_id, db_id, target, version).await?;
		let remote_idiom = Idiom::from(remote_field.to_vec());
		let index_def = indexes.iter().find(|idx| {
			matches!(&idx.index, Index::FullText(_))
				&& !idx.prepare_remove
				&& idx.cols.first().is_some_and(|col| col == &remote_idiom)
				&& !(check_perms && index_columns_touch_restricted(&idx.cols, &fields))
		});
		match index_def {
			Some(def) => {
				let (fti, qt) = self.open_index(ctx, target.clone(), def, frozen, opt, tx).await?;
				Ok(Some((fti, qt)))
			}
			None => Ok(None),
		}
	}

	/// Open a full-text index and extract the query terms for it.
	async fn open_index(
		&self,
		ctx: &EvalContext<'_>,
		table: TableName,
		index_def: &crate::catalog::IndexDefinition,
		frozen: &crate::ctx::FrozenContext,
		opt: &crate::dbs::Options,
		tx: &Arc<crate::kvs::Transaction>,
	) -> Result<(FullTextIndex, QueryTerms), anyhow::Error> {
		// Reject a full-text index whose on-disk format predates the shared
		// table-level doc-ID space; it must be rebuilt before it can serve
		// MATCHES. Mirrors the legacy plan-time gate in idx/planner/tree.rs
		// (`lookup_index_option`), which hard-errors for local and remote
		// (record-link) resolution alike.
		index_def.ensure_current_format()?;

		let ft_params = match &index_def.index {
			Index::FullText(params) => params,
			_ => unreachable!("Caller checked for FullText"),
		};

		let db_ctx = ctx
			.exec_ctx
			.database()
			.map_err(|e| anyhow::anyhow!("MatchesOp requires database context: {}", e))?;
		let ikb = IndexKeyBase::new(
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			table,
			index_def.index_id,
		);

		let fti = FullTextIndex::new(
			frozen.get_index_stores(),
			tx.as_ref(),
			ikb,
			ft_params,
			&frozen.config.idx.file_allowlist,
		)
		.await?;

		let query_terms = {
			let mut stack = reblessive::TreeStack::new();
			stack
				.enter(|stk| fti.extract_querying_terms(stk, frozen, opt, self.query.clone()))
				.finish()
				.await?
		};

		Ok((fti, query_terms))
	}
}

impl PhysicalExpr for MatchesOp {
	fn name(&self) -> &'static str {
		"MatchesOp"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// Need Root context for transaction and index store access,
		// plus whatever the child expressions need.
		let children = self.left.required_context().max(self.right.required_context());
		children.max(ContextLevel::Root)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			// Rows without a record id never match and never error (legacy
			// `ExecutorOption::None`).
			let Some(rid) = extract_record_id(ctx.current_value) else {
				return Ok(Value::Bool(false));
			};

			// Rows of tables without a legacy `QueryExecutor` — record-id
			// sources, graph/link hops — evaluate to `false` even when the
			// expression sits in the WHERE condition.
			if !self.executor_tables.contains(&rid.table) {
				return Ok(Value::Bool(false));
			}

			// Unregistered expressions (projection / fn-arg position, a
			// different expression than the WHERE's, or no WHERE at all)
			// error on executor-table rows, exactly like the legacy
			// executor's missing-entry path.
			if !self.registered {
				return self.no_index_error();
			}

			let target = self.resolve_for_table(&ctx, &rid.table).await?;
			match target.as_ref() {
				MatchTarget::Unresolved => self.no_index_error(),
				MatchTarget::Local(fti, qt) => {
					// Empty query terms → no possible matches
					if qt.is_empty() {
						return Ok(Value::Bool(false));
					}
					let tx = ctx.txn();
					// Resolve RecordId → DocId, then bitmap check. This
					// mirrors the legacy `fulltext_matches_with_doc_id`.
					let matches = match fti.get_doc_id(&tx, &rid).await? {
						Some(doc_id) => qt.contains_doc(doc_id),
						// Record not in the index → doesn't match
						None => false,
					};
					Ok(Value::Bool(matches))
				}
				MatchTarget::Remote(fti, qt) => {
					if qt.is_empty() {
						return Ok(Value::Bool(false));
					}
					// Evaluate the idiom side (traverses the record link),
					// then analyze the value with the remote index's
					// analyzer and compare token sets. This mirrors the
					// legacy `fulltext_matches_with_value`.
					let value = self.left.evaluate(ctx.clone()).await?;
					let frozen = ctx.exec_ctx.ctx();
					let root = ctx.exec_ctx.root();
					let opt = root
						.options
						.as_ref()
						.ok_or_else(|| anyhow::anyhow!("MatchesOp requires Options context"))?;
					let matches = {
						let mut stack = reblessive::TreeStack::new();
						stack
							.enter(|stk| {
								fti.matches_value(
									stk,
									frozen,
									opt,
									qt,
									self.operator.operator,
									value,
								)
							})
							.finish()
							.await?
					};
					Ok(Value::Bool(matches))
				}
			}
		})
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
			left: Arc::clone(&self.left),
			right: Arc::clone(&self.right),
			operator: self.operator.clone(),
			idiom: self.idiom.clone(),
			query: self.query.clone(),
			registered: self.registered,
			executor_tables: Arc::clone(&self.executor_tables),
			// The cache is not Clone — the clone lazily re-resolves.
			resolution: tokio::sync::Mutex::new(HashMap::new()),
		}
	}
}

impl std::fmt::Debug for MatchesOp {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("MatchesOp")
			.field("idiom", &self.idiom)
			.field("query", &self.query)
			.field("operator", &self.operator)
			.field("registered", &self.registered)
			.finish()
	}
}

/// Returns true when any index column is governed by a field definition with
/// a non-`Full` SELECT permission (an ancestor field definition also governs
/// the column). Mirrors the legacy `Tree::index_columns_select_full` /
/// `idiom_touches_restricted_field` pair.
fn index_columns_touch_restricted(cols: &[Idiom], fields: &[FieldDefinition]) -> bool {
	cols.iter().any(|col| {
		fields.iter().any(|field| {
			col.starts_with(&field.name.0) && !matches!(field.select_permission, Permission::Full)
		})
	})
}

/// Extract the RecordId from the current row value, if it has one.
fn extract_record_id(value: Option<&Value>) -> Option<crate::val::RecordId> {
	match value? {
		Value::Object(obj) => match obj.get("id") {
			Some(Value::RecordId(rid)) => Some(rid.clone()),
			_ => None,
		},
		Value::RecordId(rid) => Some(rid.clone()),
		_ => None,
	}
}
