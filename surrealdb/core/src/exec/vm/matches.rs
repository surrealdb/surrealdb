//! Physical expression for evaluating MATCHES (`@@` / `@N@`) operators.
//!
//! Evaluation follows a per-row decision tree:
//!
//! 1. Rows without a record id never match and never error (array/value sources, documents without
//!    `id`).
//! 2. Rows of tables the enclosing SELECT does not iterate as a full table source evaluate to
//!    `false` — record-id sources and graph/link hops register no full-text index. See
//!    [`MatchesScope`].
//! 3. On executor-table rows, an expression that was **not** registered from the SELECT's WHERE
//!    condition (projection position, function arguments, a different expression than the one in
//!    WHERE, or no WHERE at all) raises [`Error::NoIndexFoundForMatch`]. A WHERE condition carrying
//!    such an expression is refused before execution instead, with the same error, so the refusal
//!    does not depend on which rows the condition reaches — see
//!    [`first_unregistered_cond_matches`].
//! 4. A registered expression resolves an index for the row's table:
//!    - a full-text index on the row's own table whose first column is the idiom → `RecordId →
//!      DocId` + bitmap check (`get_doc_id()` + `contains_doc()`);
//!    - otherwise a record-link traversal (`t.name @@ 'x'` where `t` is a `record<..>`-typed field)
//!      to a full-text index on the link target table → the idiom's value is evaluated and analyzed
//!      with the remote index's analyzer, then compared against the query terms
//!      (`FullTextIndex::matches_value`);
//!    - if neither resolves, the outcome follows rule 3 ([`Error::NoIndexFoundForMatch`]).
//!
//! SECURITY: when permissions are enforced for the session, indexes whose
//! columns are governed by a field definition with a non-`Full` SELECT
//! permission are never used, so a record user cannot learn whether a
//! restricted field matches a search query. Such expressions fall through to
//! rule 3 and error on table sources.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::catalog::{FieldDefinition, Index, Permission};
use crate::exec::EvalContext;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::expr::idiom::Idiom;
use crate::expr::logical::{
	FieldPath, LogicalEval, LogicalExplain, LogicalExpr, LogicalForeach, LogicalLet, LogicalOp,
	LogicalReturn, LogicalSequence, LogicalSleep, LogicalThrow,
};
use crate::expr::operator::{BinaryOperator, MatchesOperator, PrefixOperator};
use crate::expr::{FlowResult, Kind};
use crate::iam::Action;
use crate::idx::ft::fulltext::{FullTextIndex, QueryTerms};
use crate::idx::{Error, IndexKeyBase};
use crate::kvs::index::filter_online_indexes;
use crate::val::{TableName, Value};

/// The tables whose rows MATCHES is evaluated against a registered full-text
/// index for.
///
/// Every full table source registers its index when the statement runs, so a
/// table only learned of then — a `type::table(..)` call or a parameter in the
/// `FROM` — is included too. The planner knows the tables written as sources
/// (`known`) and the tables of record-id sources, which register none
/// (`excluded`); when a source resolves at run time the set is `open`, and a
/// row of any table not excluded is evaluated against an index. A row
/// of an excluded table that a run-time source produced is treated as a
/// record-id source's row, since the two cannot be told apart by name.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExecutorTables {
	/// Tables written as full table sources.
	known: Arc<[TableName]>,
	/// Tables written as record-id sources.
	excluded: Arc<[TableName]>,
	/// Whether a source's table is only known when the statement runs.
	open: bool,
}

impl Default for ExecutorTables {
	fn default() -> Self {
		Self::none()
	}
}

impl ExecutorTables {
	/// The set for a statement with the given source tables.
	pub(crate) fn new(known: Vec<TableName>, excluded: Vec<TableName>, open: bool) -> Self {
		Self {
			known: Arc::from(known),
			excluded: Arc::from(excluded),
			open,
		}
	}

	/// No executor at all: the set outside any SELECT.
	pub(crate) fn none() -> Self {
		Self::new(Vec::new(), Vec::new(), false)
	}

	/// Whether a row of `table` is evaluated by an executor.
	pub(crate) fn has_executor(&self, table: &TableName) -> bool {
		self.known.contains(table) || (self.open && !self.excluded.contains(table))
	}

	/// Whether any source is known at plan time to get an executor, which is
	/// when an unregistered WHERE MATCHES can be refused before any row is
	/// read.
	pub(crate) fn any_known(&self) -> bool {
		!self.known.is_empty()
	}
}

/// Collect MATCHES expressions reachable from `expr` through binary nodes and
/// logical-NOT prefix nodes.
///
/// The traversal walks binary nodes only and treats every other node
/// (function calls, arrays, idioms, prefixes) as an opaque leaf. The
/// bitmap-fusion planner additionally turns a negated
/// full-text branch (`... AND !(body @@ 'x')`) into a `BitmapAndNot` over the
/// term's posting bitmap and keeps the negated MATCHES in the residual `Filter`
/// for per-row re-evaluation — so the inner MATCHES must be registered too, or
/// it would raise [`Error::NoIndexFoundForMatch`]. Descending through the `Not`
/// prefix registers it.
pub(crate) fn collect_cond_matches(expr: &LogicalExpr, out: &mut HashSet<LogicalExpr>) {
	match expr {
		LogicalExpr::Binary {
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
		LogicalExpr::Prefix {
			op: PrefixOperator::Not,
			expr: inner,
		} => {
			collect_cond_matches(inner, out);
		}
		_ => {}
	}
}

/// The first MATCHES expression in `cond` that `allowlist` does not hold, or
/// `None` when every MATCHES the condition evaluates is registered.
///
/// [`collect_cond_matches`] describes which MATCHES a SELECT registers; this is
/// the complement, used to refuse such a condition at plan time with the same
/// [`Error::NoIndexFoundForMatch`] raised per row. The two
/// walks answer different questions, so the boundary this one draws is its own
/// contract:
///
/// - A `BinaryOperator::Matches` node absent from `allowlist` is reported and the walk stops. A
///   node the allowlist holds is registered, so the walk continues past it.
/// - A [`LogicalExpr::Subquery`] holding a statement — a block, `RETURN`, `LET`, `FOR` — is
///   descended into: it evaluates against this row, so a MATCHES inside it is this SELECT's to
///   register. One holding a relational plan is not: a nested `SELECT` is planned in its own scope
///   and registers its own WHERE. A [`LogicalExpr::Closure`] is not descended into either: a
///   closure body is evaluated wherever the closure is finally called, which is not necessarily
///   this row.
/// - The predicate of a [`LogicalExpr::Filter`] and the clauses of a [`LogicalExpr::Lookup`] are
///   not descended into (their bases are). A path filter and a graph hop evaluate their condition
///   against values other than the SELECT's iteration row, where the outcome is decided by the
///   per-row rules in the module docs (rules 1–2) rather than by registration.
/// - Every other shape is walked, so a MATCHES in a function argument, a literal container, an
///   index expression, a method argument, a destructure alias, a recursion body, or an IF/ELSE
///   branch is reported.
pub(crate) fn first_unregistered_cond_matches(
	cond: &LogicalExpr,
	allowlist: &HashSet<LogicalExpr>,
) -> Option<LogicalExpr> {
	fn walk_expr(e: &LogicalExpr, allowlist: &HashSet<LogicalExpr>) -> Option<LogicalExpr> {
		match e {
			LogicalExpr::Binary {
				op: BinaryOperator::Matches(_),
				..
			} if !allowlist.contains(e) => return Some(e.clone()),
			LogicalExpr::Subquery(op) => return walk_statement(op, allowlist),
			LogicalExpr::Closure(_) => return None,
			LogicalExpr::Filter {
				base,
				..
			}
			| LogicalExpr::Lookup {
				base,
				..
			} => return walk_expr(base, allowlist),
			_ => {}
		}
		e.children().into_iter().find_map(|child| walk_expr(child, allowlist))
	}

	/// The scalars of a statement evaluated against this row. A relational
	/// plan or a GQL `MATCH` is its own scope and is not entered.
	fn walk_statement(op: &LogicalOp, allowlist: &HashSet<LogicalExpr>) -> Option<LogicalExpr> {
		match op {
			LogicalOp::Eval(LogicalEval {
				expr,
			}) => walk_expr(expr, allowlist),
			LogicalOp::Sequence(LogicalSequence {
				stmts,
			}) => stmts.iter().find_map(|stmt| walk_statement(stmt, allowlist)),
			LogicalOp::Let(LogicalLet {
				value,
				..
			}) => walk_statement(value, allowlist),
			LogicalOp::Return(LogicalReturn {
				what,
				fetch,
			}) => walk_statement(what, allowlist)
				.or_else(|| fetch.iter().find_map(|e| walk_expr(e, allowlist))),
			LogicalOp::Foreach(LogicalForeach {
				range,
				body,
				..
			}) => walk_expr(range, allowlist).or_else(|| walk_statement(body, allowlist)),
			LogicalOp::Explain(LogicalExplain {
				input,
				..
			}) => walk_statement(input, allowlist),
			LogicalOp::Sleep(LogicalSleep {
				duration,
			}) => walk_expr(duration, allowlist),
			// In evaluation order: each condition, then its arm.
			LogicalOp::If(ifs) => ifs
				.branches()
				.find_map(|(condition, arm)| {
					walk_expr(condition, allowlist).or_else(|| walk_statement(arm, allowlist))
				})
				.or_else(|| ifs.otherwise().and_then(|arm| walk_statement(arm, allowlist))),
			LogicalOp::Throw(LogicalThrow {
				value,
			}) => walk_expr(value, allowlist),
			LogicalOp::Break(_) | LogicalOp::Continue(_) => None,
			LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Match(_)
			| LogicalOp::Write(_)
			| LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_) => None,
		}
	}

	walk_expr(cond, allowlist)
}

/// A resolved evaluation strategy for one row table.
enum MatchTarget {
	/// Full-text index on the row's own table: check via `RecordId → DocId`
	/// and the query-term bitmaps.
	Local(FullTextIndex, QueryTerms),
	/// Full-text index on a record-link target table: evaluate the field's
	/// value and analyze it with the remote index's analyzer.
	Remote(FullTextIndex, QueryTerms),
	/// No usable full-text index for this table.
	Unresolved,
}

/// The field and query string a MATCHES needs to probe a full-text index.
///
/// Present only when the source expression can denote an index probe at all:
/// a path on the left, and a right operand that names a value, whose
/// `to_raw_string()` is the query. The query is the right operand's value
/// where the statement runs: resolved at plan time where the plan holds it,
/// and read when the operator is evaluated where the operand is a parameter
/// bound around the plan — a closure's argument, a `FOR` variable — whose
/// value is the binding's, not the planning context's.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MatchProbe {
	/// The declared position read on the left side. A path reading no
	/// declared position resolves to no index.
	pub(crate) field: Option<FieldPath>,
	/// The search query, rendered from the right side's value at plan time;
	/// `None` where the right side is a parameter bound around the plan, whose
	/// value the caller of [`MatchesEval::evaluate`] reads.
	pub(crate) query: Option<String>,
}

/// The per-row decision tree of a MATCHES operator, apart from the
/// expressions that read the operands: what the operator is, what it probes,
/// and which rows are evaluated against a registered index. The left
/// operand is read only on the record-link path, by the caller's `left`.
pub(crate) struct MatchesEval {
	/// The MATCHES operator (`@@`, `@1@`, `@AND@`, …; carries the boolean
	/// operator used on the record-link value path).
	pub(crate) operator: MatchesOperator,
	/// The index probe this expression denotes, when it denotes one.
	///
	/// `None` for a left operand that is not a path, or a right operand that
	/// does not resolve to a value at plan time. Neither shape can name an
	/// index, so both resolve to [`MatchTarget::Unresolved`].
	pub(crate) probe: Option<MatchProbe>,
	/// Whether this expression was registered from the enclosing SELECT's
	/// WHERE condition (see [`MatchesScope`]).
	registered: bool,
	/// The rows MATCHES is evaluated against an index for. Rows of any other
	/// table evaluate to `false`; rows evaluated against an index raise
	/// [`Error::NoIndexFoundForMatch`] when the expression is unregistered or
	/// unresolvable.
	executor_tables: ExecutorTables,
	/// The expression as the error names it: `left operator right`.
	sql: String,
	/// Per-row-table resolution cache.
	resolution: tokio::sync::Mutex<HashMap<TableName, Arc<MatchTarget>>>,
	/// Per-row-table resolution cache of a probe whose query is read when
	/// the operator is evaluated: the last query resolved for each table,
	/// which the evaluations of one run of the statement share.
	resolved_queries: tokio::sync::Mutex<HashMap<TableName, (String, Arc<MatchTarget>)>>,
}

impl MatchesEval {
	/// A decision tree for `operator`, whose error names the expression as
	/// `sql` writes it.
	pub(crate) fn new(
		operator: MatchesOperator,
		probe: Option<MatchProbe>,
		registered: bool,
		executor_tables: ExecutorTables,
		sql: String,
	) -> Self {
		Self {
			operator,
			probe,
			registered,
			executor_tables,
			sql,
			resolution: tokio::sync::Mutex::new(HashMap::new()),
			resolved_queries: tokio::sync::Mutex::new(HashMap::new()),
		}
	}

	/// The outcome for a row evaluated against an index when this expression has
	/// no entry in it.
	fn no_index_error(&self) -> FlowResult<Value> {
		Err(anyhow::Error::new(Error::NoIndexFoundForMatch {
			exp: self.sql.clone(),
		})
		.into())
	}

	/// Get (or lazily compute) the evaluation strategy for a row table, for
	/// a probe whose query was resolved at plan time.
	async fn resolve_for_table(
		&self,
		ctx: &EvalContext<'_>,
		table: &TableName,
		probe: &MatchProbe,
		query: &str,
	) -> Result<Arc<MatchTarget>, anyhow::Error> {
		{
			let cache = self.resolution.lock().await;
			if let Some(t) = cache.get(table) {
				return Ok(Arc::clone(t));
			}
		}
		let resolved = Arc::new(self.resolve_uncached(ctx, table, probe, query).await?);
		self.resolution.lock().await.insert(table.clone(), Arc::clone(&resolved));
		Ok(resolved)
	}

	/// [`Self::resolve_for_table`] for a probe whose query, `query`, is read
	/// when the operator is evaluated: the strategy is kept for the last
	/// query each table was resolved for.
	async fn resolve_for_query(
		&self,
		ctx: &EvalContext<'_>,
		table: &TableName,
		probe: &MatchProbe,
		query: String,
	) -> Result<Arc<MatchTarget>, anyhow::Error> {
		{
			let cache = self.resolved_queries.lock().await;
			if let Some((resolved_for, t)) = cache.get(table)
				&& *resolved_for == query
			{
				return Ok(Arc::clone(t));
			}
		}
		let resolved = Arc::new(self.resolve_uncached(ctx, table, probe, &query).await?);
		self.resolved_queries.lock().await.insert(table.clone(), (query, Arc::clone(&resolved)));
		Ok(resolved)
	}

	/// Resolve the full-text index for `table`: first a local index on the
	/// table itself, then a record-link traversal via the table's field
	/// definitions.
	///
	/// A probe whose path reads no declared position names no column, so
	/// there is nothing to resolve against and no catalog read is issued.
	async fn resolve_uncached(
		&self,
		ctx: &EvalContext<'_>,
		table: &TableName,
		probe: &MatchProbe,
		query: &str,
	) -> Result<MatchTarget, anyhow::Error> {
		use crate::catalog::providers::TableProvider;

		let Some(path) = probe.field.as_ref() else {
			return Ok(MatchTarget::Unresolved);
		};

		let frozen = ctx.exec_ctx.ctx();
		let tx = ctx.txn();

		let db_ctx = ctx
			.exec_ctx
			.database()
			.map_err(|e| anyhow::anyhow!("MatchesOp requires database context: {}", e))?;
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;
		let version = ctx.exec_ctx.version_stamp();

		// SECURITY: when permissions are enforced, refuse indexes whose
		// columns touch a field with a restrictive SELECT permission —
		// otherwise the MATCHES outcome would leak whether a hidden field
		// matches the query.
		let check_perms = crate::exec::permission::should_check_perms(db_ctx, Action::View)?;
		let fields =
			crate::exec::vm::descriptor::object(tx.all_tb_fields(ns_id, db_id, table, version))
				.await?;

		// 1. Full-text index on the row's own table.
		let indexes =
			crate::exec::vm::descriptor::object(tx.all_tb_indexes(ns_id, db_id, table, version))
				.await?;
		let indexes = if version.is_none() {
			// MATCHES must not read a full-text index until durable state
			// has published it as queryable. Which ones are is a catalog
			// fact, loaded as one.
			crate::exec::vm::descriptor::object(filter_online_indexes(
				tx.as_ref(),
				ns_id,
				db_id,
				indexes,
			))
			.await?
		} else {
			indexes
		};
		let local = indexes.iter().find(|idx| {
			matches!(&idx.index, Index::FullText(_))
				&& idx.cols.iter().any(|col| column_path(col).as_ref() == Some(path))
				&& !(check_perms && index_columns_touch_restricted(&idx.cols, &fields))
		});
		if let Some(index_def) = local {
			let (fti, qt) =
				self.open_index(ctx, table.clone(), index_def, query, frozen, &tx).await?;
			return Ok(MatchTarget::Local(fti, qt));
		}

		// 2. Record-link traversal: the first field definition of `Record`
		// kind whose name prefixes the path decides; auto-defined `field[*]` children make
		// `ts[*].name` resolvable while `ts.name` is not.
		if path.segments().len() > 1 {
			for field in fields.iter() {
				let Some(Kind::Record(targets)) = &field.field_kind else {
					continue;
				};
				let Some(link) = column_path(&field.name) else {
					continue;
				};
				let Some(remote_field) = path.segments().strip_prefix(link.segments()) else {
					continue;
				};
				if remote_field.is_empty() {
					break;
				}
				let remote_field = FieldPath::new(remote_field.to_vec());
				// Walk the link targets in declaration order; keep the last
				// resolvable target seen before the first unresolvable one
				// (one entry is inserted per resolved target and resolution
				// stops at the first failure — the last insert wins).
				let mut resolved = None;
				for target in targets {
					match self
						.resolve_remote_target(
							ctx,
							target,
							&remote_field,
							query,
							check_perms,
							frozen,
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
	/// column is the remaining path.
	#[expect(clippy::too_many_arguments)]
	async fn resolve_remote_target(
		&self,
		ctx: &EvalContext<'_>,
		target: &TableName,
		remote_field: &FieldPath,
		query: &str,
		check_perms: bool,
		frozen: &crate::ctx::FrozenContext,
		tx: &Arc<crate::kvs::Transaction>,
		ns_id: crate::catalog::NamespaceId,
		db_id: crate::catalog::DatabaseId,
	) -> Result<Option<(FullTextIndex, QueryTerms)>, anyhow::Error> {
		use crate::catalog::providers::TableProvider;

		let version = ctx.exec_ctx.version_stamp();
		let fields =
			crate::exec::vm::descriptor::object(tx.all_tb_fields(ns_id, db_id, target, version))
				.await?;
		// No online-index filter here: the value path only uses the index's
		// analyzer configuration and query tokens, never its document
		// bitmaps, so build state cannot affect results.
		let indexes =
			crate::exec::vm::descriptor::object(tx.all_tb_indexes(ns_id, db_id, target, version))
				.await?;
		let index_def = indexes.iter().find(|idx| {
			matches!(&idx.index, Index::FullText(_))
				&& !idx.prepare_remove
				&& idx
					.cols
					.first()
					.is_some_and(|col| column_path(col).as_ref() == Some(remote_field))
				&& !(check_perms && index_columns_touch_restricted(&idx.cols, &fields))
		});
		match index_def {
			Some(def) => {
				let (fti, qt) =
					self.open_index(ctx, target.clone(), def, query, frozen, tx).await?;
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
		query: &str,
		frozen: &crate::ctx::FrozenContext,
		tx: &Arc<crate::kvs::Transaction>,
	) -> Result<(FullTextIndex, QueryTerms), anyhow::Error> {
		// Indexes without shared doc ids resolve postings through their
		// historical private doc-ID layout until explicitly rebuilt; the
		// format version selects the resolver used below.
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

		let fti = crate::exec::vm::descriptor::object(FullTextIndex::new(
			frozen.get_index_stores(),
			tx.as_ref(),
			ikb,
			ft_params,
			&frozen.config.idx.file_allowlist,
			index_def.format_version,
		))
		.await?;

		let query_terms = {
			let az_fn = StreamingAnalyzerFunction::new(ctx.exec_ctx);
			fti.extract_querying_terms(frozen, &az_fn, query.to_owned()).await?
		};

		Ok((fti, query_terms))
	}
}

impl MatchesEval {
	/// Evaluate the operator against the row of `ctx`. `left` reads the
	/// operator's left operand, and is called only on the record-link path;
	/// `right` reads its right operand, and is called only for a probe whose
	/// query is read when the operator is evaluated ([`MatchProbe::query`]).
	pub(crate) async fn evaluate<L, F, R, G>(
		&self,
		ctx: &EvalContext<'_>,
		left: L,
		right: R,
	) -> FlowResult<Value>
	where
		L: FnOnce() -> F,
		F: std::future::Future<Output = FlowResult<Value>>,
		R: FnOnce() -> G,
		G: std::future::Future<Output = FlowResult<Value>>,
	{
		// Rows without a record id never match and never error.
		let Some(rid) = extract_record_id(ctx.value_in_scope().read()?) else {
			return Ok(Value::Bool(false));
		};

		// Rows of tables with no registered index — record-id sources,
		// graph/link hops — evaluate to `false` even when the expression
		// sits in the WHERE condition.
		if !self.executor_tables.has_executor(&rid.table) {
			return Ok(Value::Bool(false));
		}

		// Unregistered expressions (projection / fn-arg position, a
		// different expression than the WHERE's, or no WHERE at all)
		// error on executor-table rows.
		if !self.registered {
			return self.no_index_error();
		}

		let target = match &self.probe {
			None => return self.no_index_error(),
			Some(
				probe @ MatchProbe {
					query: Some(query),
					..
				},
			) => self.resolve_for_table(ctx, &rid.table, probe, query).await?,
			Some(probe) => {
				let query = right().await?.to_raw_string();
				self.resolve_for_query(ctx, &rid.table, probe, query).await?
			}
		};
		match target.as_ref() {
			MatchTarget::Unresolved => self.no_index_error(),
			MatchTarget::Local(fti, qt) => {
				// Empty query terms → no possible matches
				if qt.is_empty() {
					return Ok(Value::Bool(false));
				}
				let tx = ctx.txn();
				// Resolve RecordId → DocId, then bitmap check.
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
				// analyzer and compare token sets.
				let value = left().await?;
				let matches = {
					let az_fn = StreamingAnalyzerFunction::new(ctx.exec_ctx);
					fti.matches_value(&az_fn, qt, self.operator.operator, value).await?
				};
				Ok(Value::Bool(matches))
			}
		}
	}
}

impl Clone for MatchesEval {
	fn clone(&self) -> Self {
		Self {
			operator: self.operator.clone(),
			probe: self.probe.clone(),
			registered: self.registered,
			executor_tables: self.executor_tables.clone(),
			sql: self.sql.clone(),
			// The caches are not Clone — the clone lazily re-resolves.
			resolution: tokio::sync::Mutex::new(HashMap::new()),
			resolved_queries: tokio::sync::Mutex::new(HashMap::new()),
		}
	}
}

/// The declared position a catalog index column or field name declares.
///
/// Index columns and field names are plain paths — field names and `[*]` —
/// so a probe path compares equal to one exactly when it reads the same
/// position; a catalog name holding any other part declares no position.
fn column_path(catalog: &Idiom) -> Option<FieldPath> {
	FieldPath::from_idiom(catalog)
}

/// Returns true when any index column is governed by a field definition with
/// a non-`Full` SELECT permission (an ancestor field definition also governs
/// the column).
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
