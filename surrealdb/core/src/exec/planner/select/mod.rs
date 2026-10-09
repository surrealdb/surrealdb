//! Compiling a relational pipeline to a source and the operators above it.
//!
//! The fold ([`super::fold`]) walks a relational tree down to its leaf,
//! collecting the clauses above it into a [`PipelineClauses`], and hands both
//! to [`Planner::compile_pipeline`]. Everything here is on the logical types:
//! predicates and paths are [`LogicalExpr`], projections are
//! [`LogicalFields`] and orderings are [`LogicalOrdering`]. Nothing in this
//! module lowers a statement or converts a scalar back into a statement
//! expression.
//!
//! `compile_pipeline` decides how the leaf is read:
//!
//! - The fast paths in [`fast_path`] compile a whole pipeline to one scan when its shape allows — a
//!   bare or indexed `count()`, or a single record read.
//! - Otherwise [`Planner::plan_sources`] chooses an access path for each source with plan-time
//!   index analysis (table scan, B-tree index, full-text, KNN, index union, bitmap fusion) and
//!   records what the access path consumed — the predicate, or the paging — so the pipeline
//!   assembled over it by [`pipeline`] does not apply them twice.
//! - A node the fold does not collect (`PermissionFilter`, a projection layer, `Distinct`, a
//!   statement) is compiled in place and becomes an opaque source: every clause collected above it
//!   applies in full, and no filter, paging or threshold is pushed beneath it. The predicate and
//!   ordering are still offered to the access path beneath a chain of `PermissionFilter` and layer
//!   nodes over a table; the fold states what that offer withholds.
//!
//! Projection planning is in [`projection`]; the plan-time folding of
//! row-independent record paths in `WHERE` is in [`record_fold`].

mod fast_path;
mod pipeline;
mod projection;
mod record_fold;

use std::collections::HashSet;
use std::sync::Arc;

#[cfg(test)]
pub(crate) use pipeline::{
	FilterAction, PlannedSource, TopKPushdownRequest, compute_topk_pushdown_request,
	filter_action_for_predicate,
};
pub(crate) use pipeline::{SelectPipelineConfig, WhereClauseState};
pub(crate) use projection::output_field_path;
use surrealdb_strand::TableName;
use surrealdb_types::ToSql;

use self::fast_path::{
	CountAll, IndexedCountAll, match_count_all, match_indexed_count_all, permission_filtered_source,
};
#[cfg(test)]
use self::fast_path::{SingleRecord, match_single_record};
#[cfg(test)]
use super::fold::{GroupClause, ProjectClause};
use super::fold::{PipelineClauses, PipelineStop, is_layer_chain_over_table};
use super::row_scope::{PathRoot, classify_path_root};
#[cfg(test)]
use super::util::{
	BruteForceKnnVector, extract_bruteforce_knn, extract_matches_context,
	get_effective_limit_literal, has_knn_ktree_operator, index_covers_ordering,
	strip_knn_and_matches_from_condition,
};
use super::util::{
	exact_knn_by, expr_contains_matches, extract_record_id_point_lookup, field_path_from_expr,
	fold_expressions, has_knn_operator, order_is_scan_compatible, resolve_param_value,
	resolve_projection_field_idioms, source_field_reads_are_inert, strip_index_conditions,
	strip_knn_from_condition,
};
#[cfg(test)]
use super::util::{
	extract_version, has_knn_k_operator, has_top_level_or, is_bounded_topk_downstream,
	strip_fts_condition, strip_union_index_conditions,
};
use super::{PlanTimeVersion, Planner};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{Index, IndexDefinition};
#[cfg(test)]
use crate::err::EngineError;
use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::exec::function::FunctionRegistry;
use crate::exec::index::access_path::{
	AccessPath, BTreeAccess, BitmapPlan, IndexRef, KnnPrefilterPlan, select_access_path_paged,
};
use crate::exec::index::analysis::IndexAnalyzer;
use crate::exec::logical::named_reads;
use crate::exec::logical::reads::{
	declared_read_prefix, paths_overlap, reads_at_or_beneath, reads_row_opaquely,
};
#[cfg(test)]
use crate::exec::operators::FetchStep;
#[cfg(test)]
use crate::exec::operators::fetch::{FetchPart, fetch_parts};
use crate::exec::operators::scan::MergeMode;
use crate::exec::operators::scan::common::DecodeFields;
use crate::exec::operators::scan::pipeline::{
	determine_scan_direction, table_read_restricted_fields,
};
#[cfg(test)]
use crate::exec::operators::scan::resolved::resolve_table_context;
#[cfg(test)]
use crate::exec::operators::scan::resolved::{EnforcementMode, ResolvedTableContext};
use crate::exec::operators::{BitmapNode, CountOutput, SortDirection};
#[cfg(test)]
use crate::exec::operators::{
	BitmapResolve, DynamicScan, Fetch, KnnTopK, KnnVectorSource, LightweightEdgeScan, RecordIdScan,
	SourceExpr, TableScan, Timeout, UnionIndexScan, UnwrapExactlyOne, VersionScope,
};
#[cfg(test)]
use crate::exec::operators::{Filter, Limit, Union};
#[cfg(test)]
use crate::exec::pre_decode_filter::pre_decode_filter_status_at_plan_time;
#[cfg(test)]
use crate::exec::{ExecOperator, PhysicalExpr};
use crate::expr::logical::{
	LogicalDestructureField, LogicalExpr, LogicalField, LogicalFields, LogicalLiteral, LogicalOp,
	LogicalOrdering, LogicalRecordIdKey, LogicalRecurseInstruction, LogicalRecursePath,
	LogicalSource, LogicalUnionAll, LogicalValues, OutputPathPart, SourceRef, lower_expr,
};
#[cfg(test)]
use crate::expr::logical::{LogicalMatch, LogicalRecordIdLit, LogicalSubquery};
use crate::expr::with::With;
use crate::expr::{Function, Idiom};
use crate::kvs::index::filter_online_indexes;
use crate::kvs::{Direction, Transaction};

#[cfg(test)]
/// A `VERSION` or `TIMEOUT` the plan does not apply.
///
/// An absent clause has two spellings: no operator in the plan, and the
/// `NONE` literal a fold leaves behind on the operator. Neither bounds
/// anything.
fn clause_is_absent(clause: Option<&LogicalExpr>) -> bool {
	matches!(clause, None | Some(LogicalExpr::Literal(LogicalLiteral::None)))
}

/// The table a `Source` leaf reads whole, if it is one.
fn leaf_table(op: &LogicalOp) -> Option<&TableName> {
	match op {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(table),
			..
		}) => Some(table),
		_ => None,
	}
}

/// Every table read whole somewhere in `op`'s subtree, in plan order. A nested
/// statement's sources are its own and are not listed.
pub(crate) fn tables_beneath<'a>(op: &'a LogicalOp, out: &mut Vec<&'a TableName>) {
	if let Some(table) = leaf_table(op) {
		out.push(table);
	}
	if matches!(op, LogicalOp::Subquery(_)) {
		return;
	}
	for child in op.children() {
		tables_beneath(child, out);
	}
}

#[cfg(test)]
/// The expression a [`SourceRef`] classified, recovered. The inverse of
/// [`SourceRef::classify`]: every source kind has one, so re-classifying the
/// result yields the source back.
fn source_ref_expr(source: SourceRef) -> LogicalExpr {
	match source {
		SourceRef::Table(table) => LogicalExpr::Table(table),
		SourceRef::RecordId(rid) => LogicalExpr::RecordId(rid),
		SourceRef::Param(param) => LogicalExpr::Param(param),
		SourceRef::Mock(mock) => LogicalExpr::Mock(mock),
		SourceRef::Dynamic(expr) => expr,
	}
}

#[cfg(test)]
/// The tables of the record-id sources among `sources`: rows MATCHES evaluates
/// to `false` for, since a record-id source registers no full-text index.
fn record_id_source_tables(sources: &[LogicalOp]) -> Vec<TableName> {
	sources
		.iter()
		.filter_map(|source| match source {
			LogicalOp::Source(LogicalSource {
				source: SourceRef::RecordId(rid),
				..
			}) => Some(rid.table.clone()),
			_ => None,
		})
		.collect()
}

#[cfg(test)]
/// Whether a source among `sources` names its table only when the statement
/// runs: a `type::table(..)` call, a method call, or a parameter the passes
/// could not resolve.
fn has_runtime_table_source(sources: &[LogicalOp]) -> bool {
	sources.iter().any(|source| {
		matches!(
			source,
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Param(_)
					| SourceRef::Dynamic(
						LogicalExpr::Call(_)
							| LogicalExpr::Postfix { .. }
							| LogicalExpr::Invoke { .. }
					),
				..
			})
		)
	})
}

/// Whether `source` carries the `FOR UPDATE` flag: a `Source` the clause
/// locks, or a `Values` the clause was written over, which it cannot lock.
fn locks_records(source: &LogicalOp) -> bool {
	matches!(
		source,
		LogicalOp::Source(LogicalSource {
			for_update: true,
			..
		}) | LogicalOp::Values(LogicalValues {
			for_update: true,
			..
		})
	)
}

/// Reject every `FOR UPDATE` target that is not a specific record.
///
/// `FOR UPDATE` locks the records a read returns, so a target must name
/// specific records: a non-range record id, or a parameter or call whose
/// shape is only known when the statement runs and is re-validated by the
/// scan that resolves it. A table, a mock set, a key range, a literal value
/// and a nested statement — which lowering carries under the clause as a
/// `Dynamic` source holding the nested plan, so the flag has a node to ride
/// on — are rejected, and so is a literal value (a `Values`), which lowering
/// and the passes flag like any other source so that a `FROM [1] FOR UPDATE`
/// or a parameter that resolved to a value is caught wherever the statement
/// appears. Applied when any source in the `FROM` carries the flag, to every
/// source in it.
fn check_for_update_sources(sources: &[LogicalOp]) -> Result<(), Error> {
	let locks = sources.iter().any(locks_records);
	if !locks {
		return Ok(());
	}
	for source in sources {
		let ok = match source {
			LogicalOp::Source(LogicalSource {
				source,
				..
			}) => match source {
				SourceRef::RecordId(rid) => !matches!(rid.key, LogicalRecordIdKey::Range(_)),
				SourceRef::Param(_) => true,
				SourceRef::Dynamic(expr) => {
					matches!(expr, LogicalExpr::Call(_) | LogicalExpr::Invoke { .. })
				}
				SourceRef::Table(_) | SourceRef::Mock(_) => false,
			},
			_ => false,
		};
		if !ok {
			return Err(ExecError::Query {
				message: crate::dbs::FOR_UPDATE_TARGETS_ERROR.to_string(),
			}
			.into());
		}
	}
	Ok(())
}

/// The clause rules of a `FOR UPDATE` read over `sources`, the leaves of one
/// pipeline whose clauses are `clauses`; `versioned` is whether the pipeline
/// reads at a `VERSION`, its own or one inherited from an enclosing scope.
///
/// A locked read is of current record state, so a version makes the lock
/// meaningless; grouping and splitting produce rows that are not records to
/// lock; and a limit over several targets stops reading before the later
/// targets are registered for conflict detection. The targets are then
/// classified ([`check_for_update_sources`]): a target that resolved to a
/// non-record source is rejected, while targets that stay dynamic are
/// re-validated by the scan operators at execution time.
pub(crate) fn check_locked_read(
	sources: &[LogicalOp],
	clauses: &PipelineClauses,
	versioned: bool,
) -> Result<(), Error> {
	if sources.iter().any(locks_records) {
		if versioned {
			return Err(ExecError::Query {
				message: crate::dbs::FOR_UPDATE_VERSION_ERROR.to_string(),
			}
			.into());
		}
		if clauses.group.is_some() || clauses.split.is_some() {
			return Err(ExecError::Query {
				message: crate::dbs::FOR_UPDATE_GROUP_ERROR.to_string(),
			}
			.into());
		}
		if clauses.limit.is_some() && sources.len() > 1 {
			return Err(ExecError::Query {
				message: crate::dbs::FOR_UPDATE_LIMIT_ERROR.to_string(),
			}
			.into());
		}
	}
	check_for_update_sources(sources)
}

impl<'ctx> Planner<'ctx> {
	/// Resolve a parameter to its value at plan time.
	///
	/// Delegates to [`resolve_param_value`] with the planner's context and
	/// namespace/database IDs (looked up from the transaction when available).
	async fn resolve_param(&self, name: &str) -> Option<crate::val::Value> {
		let ns_db = self.definition_ids().await;
		resolve_param_value(name, &self.ctx, self.txn.as_ref(), ns_db, &self.row_scoped()).await
	}

	#[cfg(test)]
	/// Compile a `FROM` source's [`SourceRef`] to the expression a scan
	/// evaluates for its rows.
	pub(crate) async fn source_physical_expr(
		&self,
		source: SourceRef,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		self.physical_expr(source_ref_expr(source)).await
	}

	#[cfg(test)]
	/// Plan a `FETCH` clause over `input`. No fetches leave `input` as it is.
	pub(crate) async fn plan_fetch(
		&self,
		fetches: Vec<LogicalExpr>,
		input: Arc<dyn ExecOperator>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		if fetches.is_empty() {
			return Ok(input);
		}
		let mut fields = Vec::with_capacity(fetches.len());
		for fetch in fetches {
			let mut paths = self.resolve_field_paths(fetch).await?;
			fields.append(&mut paths);
		}
		let (fields_sql, physical_fields) = self.convert_fetch_paths(fields).await?;
		Ok(Arc::new(Fetch::new(input, fields_sql, physical_fields)) as Arc<dyn ExecOperator>)
	}

	/// Convert resolved FETCH paths into the [`FetchStep`]s the `Fetch`
	/// operator walks, in the order the steps apply. A field read, `.*`,
	/// `[0]` and `[$]` are walked as written; an index expression (`refs[0]`,
	/// `refs[$i]`) is compiled once here rather than on every row. The first
	/// step of any other kind — a filter, a lookup, a method, a path starting
	/// from an expression — ends the walk there, so nothing after it is
	/// converted.
	///
	/// The returned string is the paths' SQL rendering, captured up front for
	/// EXPLAIN (`Fetch::attrs`).
	#[cfg(test)]
	pub(crate) async fn convert_fetch_paths(
		&self,
		paths: Vec<LogicalExpr>,
	) -> Result<(String, Vec<Vec<FetchStep>>), Error> {
		let fields_sql = paths.iter().map(|f| f.to_sql()).collect::<Vec<_>>().join(", ");
		let mut physical_fields = Vec::with_capacity(paths.len());
		for path in &paths {
			let mut steps = Vec::new();
			for part in fetch_parts(path) {
				steps.push(match part {
					FetchPart::Field(name) => FetchStep::Field(name.clone()),
					FetchPart::All => FetchStep::All,
					FetchPart::First => FetchStep::First,
					FetchPart::Last => FetchStep::Last,
					FetchPart::Index(index) => {
						FetchStep::Index(self.physical_expr(index.clone()).await?)
					}
					FetchPart::Unsupported => FetchStep::Unsupported,
				});
			}
			physical_fields.push(steps);
		}
		Ok((fields_sql, physical_fields))
	}

	// ========================================================================
	// Field Resolution Helpers
	// ========================================================================

	/// Resolve an `OMIT` or `FETCH` expression to the paths it names: a path
	/// as written, a parameter holding a path as text, or a projection
	/// function whose arguments name paths.
	pub(crate) async fn resolve_field_paths(
		&self,
		expr: LogicalExpr,
	) -> Result<Vec<LogicalExpr>, Error> {
		match expr {
			path if path.is_path() => Ok(vec![path]),
			// A parameter holds a path as text. The error names the value it
			// held rather than the parameter.
			LogicalExpr::Param(param) => {
				let value =
					self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
				let invalid = || ExecError::InvalidFetch {
					value: value.clone().into_literal().to_sql(),
				};
				let s = value.clone().coerce_to::<String>().map_err(|_| invalid())?;
				let path = parse_field_path(&s).ok_or_else(invalid)?;
				Ok(vec![path])
			}
			LogicalExpr::Call(ref call) => match &call.receiver {
				Function::Normal(name) if self.function_registry().is_projection(name) => {
					// Generic projection function handling: resolve each argument
					// as a string (single field) or array of strings (multiple fields)
					// and parse each as a path.
					let mut paths = Vec::new();
					for arg in &call.args {
						match self.resolve_expr_to_string(arg).await {
							Ok(s) => {
								paths.push(parse_field_path(&s).ok_or_else(|| {
									ExecError::Query {
										message: format!("Failed to parse field path '{s}'"),
									}
								})?);
							}
							Err(_) => {
								// Try resolving as an array of strings
								let strings = self
									.resolve_expr_to_string_array(arg)
									.await
									.map_err(|_| ExecError::Query {
										message: format!(
											"Projection function '{}' argument could not \
												 be resolved to a field path",
											name
										),
									})?;
								for s in strings {
									paths.push(parse_field_path(&s).ok_or_else(|| {
										ExecError::Query {
											message: format!("Failed to parse field path '{s}'"),
										}
									})?);
								}
							}
						}
					}
					if paths.is_empty() {
						return Err(ExecError::Query {
							message: format!(
								"Projection function '{}' requires at least one argument",
								name
							),
						}
						.into());
					}
					Ok(paths)
				}
				_ => Err(ExecError::InvalidFetch {
					value: expr.to_sql(),
				}
				.into()),
			},
			other => Err(ExecError::InvalidFetch {
				value: other.to_sql(),
			}
			.into()),
		}
	}

	async fn resolve_expr_to_string(&self, expr: &LogicalExpr) -> Result<String, Error> {
		match expr {
			LogicalExpr::Literal(LogicalLiteral::String(s)) => Ok(s.as_str().to_owned()),
			LogicalExpr::Param(param) => {
				let value =
					self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
				value.coerce_to::<String>().map_err(|_| {
					ExecError::Query {
						message: "OMIT/FETCH parameter did not resolve to a string".to_string(),
					}
					.into()
				})
			}
			_ => Err(ExecError::Query {
				message: "OMIT/FETCH with computed expressions not yet supported".to_string(),
			}
			.into()),
		}
	}

	async fn resolve_expr_to_string_array(&self, expr: &LogicalExpr) -> Result<Vec<String>, Error> {
		match expr {
			LogicalExpr::Array(items) => {
				let mut result = Vec::with_capacity(items.len());
				for item in items {
					result.push(self.resolve_expr_to_string(item).await?);
				}
				Ok(result)
			}
			LogicalExpr::Param(param) => {
				let value =
					self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
				value.coerce_to::<Vec<String>>().map_err(|_| {
					ExecError::Query {
						message: "OMIT/FETCH parameter did not resolve to an array of strings"
							.to_string(),
					}
					.into()
				})
			}
			_ => Err(ExecError::Query {
				message: "OMIT/FETCH with computed expressions not yet supported".to_string(),
			}
			.into()),
		}
	}

	/// Extract the set of root field names a pipeline reads from its rows.
	///
	/// Returns `None` if all fields are needed: no projection, a wildcard in
	/// the projection, or an opaque expression (a parameter, a computed path
	/// start) that prevents static analysis. Returns `Some(set)` with the root
	/// field names needed by projections, `WHERE`, `ORDER`, `GROUP`, `SPLIT`.
	/// `knn` says what a KNN operator in those clauses reads from a row.
	pub(crate) fn extract_needed_fields(
		fields: Option<&LogicalFields>,
		omit: &[LogicalExpr],
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		group: Option<&[LogicalExpr]>,
		split: Option<&[LogicalExpr]>,
		knn: KnnRowReads,
	) -> Option<HashSet<String>> {
		// No projection, or a wildcard in it: every field is needed.
		let fields = fields?;
		if fields.has_all_selection() {
			return None;
		}

		let mut extractor = NeededFieldExtractor {
			fields: HashSet::new(),
			has_opaque: false,
			filter_depth: 0,
			knn,
		};

		// Walk projection expressions
		match fields {
			LogicalFields::Value {
				expr,
				..
			} => extractor.visit_expr(expr),
			LogicalFields::Select(field_list) => {
				for field in field_list {
					if let LogicalField::Single {
						expr,
						alias,
						..
					} = field
					{
						extractor.visit_expr(expr);
						// An alias is walked as a path from the row: its
						// leading field counts as read, so a later clause
						// naming the alias finds the field it shadows.
						if let Some(alias) = alias
							&& let Some(OutputPathPart::Field(name)) = alias.parts().first()
						{
							extractor.fields.insert(name.clone());
						}
					}
				}
			}
		}

		// Walk OMIT fields (they may reference computed fields that need evaluation)
		for expr in omit {
			extractor.visit_expr(expr);
		}

		// Walk WHERE condition
		if let Some(cond) = cond {
			extractor.visit_expr(cond);
		}

		// Walk ORDER BY
		if let Some(LogicalOrdering::Order(order_list)) = order {
			for order in order_list {
				extractor.visit_expr(&order.value);
			}
		}

		// Walk GROUP BY
		for key in group.into_iter().flatten() {
			extractor.visit_expr(key);
		}

		// Walk SPLIT
		for key in split.into_iter().flatten() {
			extractor.visit_expr(key);
		}

		if extractor.has_opaque {
			None
		} else {
			Some(extractor.fields)
		}
	}

	/// The top-level fields a scan fetching this pipeline's records decodes,
	/// or `None` when it decodes whole records.
	///
	/// Every root field a clause reads off a row, by [`named_reads`]: `None`
	/// when any clause reads the row other than by a field's name (a row
	/// parameter, the document, a nested statement, a call evaluated against
	/// the row, a closure). A `*` projection reads every field but the ones it
	/// omits, so it decodes all but the top-level `OMIT` targets no clause
	/// reads; a nested `OMIT` target keeps its root field decoded, and one that
	/// names no field, or a `*` that leaves nothing out, decodes whole records.
	/// `search::highlight` and `search::offsets` read the column of the
	/// full-text index their match reference names, which only a MATCHES in
	/// this pipeline's condition names, so a pipeline calling either without
	/// one decodes whole records.
	pub(crate) fn extract_decode_fields(
		registry: &FunctionRegistry,
		fields: Option<&LogicalFields>,
		omit: &[LogicalExpr],
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		group: Option<&[LogicalExpr]>,
		split: Option<&[LogicalExpr]>,
	) -> Option<DecodeFields> {
		let mut roots: HashSet<String> = HashSet::new();
		let mut exprs: Vec<&LogicalExpr> = Vec::new();
		let mut wildcard = false;
		match fields? {
			LogicalFields::Value {
				expr,
				..
			} => exprs.push(expr),
			LogicalFields::Select(list) => {
				for field in list {
					match field {
						LogicalField::All => wildcard = true,
						// An alias names an output field, not a stored one: a
						// clause reading it is walked like any other read.
						LogicalField::Single {
							expr,
							..
						} => exprs.push(expr),
					}
				}
			}
		}
		// Under `*` an OMIT target is a field not to decode; otherwise it is a
		// field the projection reads to remove.
		if !wildcard {
			exprs.extend(omit);
		}
		exprs.extend(cond);
		if let Some(LogicalOrdering::Order(list)) = order {
			exprs.extend(list.iter().map(|o| &o.value));
		}
		exprs.extend(group.into_iter().flatten());
		exprs.extend(split.into_iter().flatten());

		let reads_index_column = exprs.iter().any(|e| {
			e.any(|e| {
				matches!(e, LogicalExpr::Call(call) if matches!(
					&call.receiver,
					Function::Normal(name) if name == "search::highlight" || name == "search::offsets"
				))
			})
		});
		if reads_index_column && !cond.is_some_and(expr_contains_matches) {
			return None;
		}
		for expr in exprs {
			roots.extend(named_reads(expr, registry)?);
		}
		if !wildcard {
			return Some(DecodeFields::new(roots));
		}
		// A nested OMIT target keeps its root field decoded, for the projection
		// to cut the nested part from.
		for expr in omit {
			if !matches!(expr, LogicalExpr::Field(_)) {
				roots.extend(named_reads(expr, registry)?);
			}
		}
		let mut excluded = Vec::with_capacity(omit.len());
		for expr in omit {
			if let LogicalExpr::Field(name) = expr
				&& !roots.contains(name.as_str())
			{
				excluded.push(name.to_string());
			}
		}
		// Excluding only `id`, which is set from the storage key, excludes
		// nothing stored: that is a whole-record decode.
		DecodeFields::excluding(excluded)
	}

	#[cfg(test)]
	/// Apply the plan's `LIMIT`/`START` over an operator.
	///
	/// A fast path replaces the rows below it but still has to page what it
	/// produces: `GROUP ALL` yields one row, and `LIMIT 0` or a `START` past
	/// it returns none of that row.
	async fn apply_paging(
		&self,
		op: Arc<dyn ExecOperator>,
		limit: Option<LogicalExpr>,
		start: Option<LogicalExpr>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		if limit.is_none() && start.is_none() {
			return Ok(op);
		}
		let limit_expr = match limit {
			Some(l) => Some(self.physical_expr(l).await?),
			None => None,
		};
		let start_expr = match start {
			Some(s) => Some(self.physical_expr(s).await?),
			None => None,
		};
		Ok(Arc::new(Limit::new(op, limit_expr, start_expr)))
	}

	#[cfg(test)]
	/// Wrap a compiled pipeline with the standard tail layers: `Timeout` (if
	/// a TIMEOUT clause), `VersionScope` (if a VERSION expression), and
	/// `UnwrapExactlyOne` (if `ONLY`).
	///
	/// Every pipeline closes with this helper so the tail is built in exactly
	/// one place.
	pub(crate) async fn wrap_select_tail(
		&self,
		op: Arc<dyn ExecOperator>,
		timeout: Option<LogicalExpr>,
		version: Option<Arc<dyn PhysicalExpr>>,
		only: bool,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let timed = if clause_is_absent(timeout.as_ref()) {
			op
		} else if let Some(te) = timeout {
			let tp = self.physical_expr(te).await?;
			Arc::new(Timeout::new(op, Some(tp))) as Arc<dyn ExecOperator>
		} else {
			op
		};
		let versioned: Arc<dyn ExecOperator> = match version {
			Some(v) => Arc::new(VersionScope::new(timed, v)),
			None => timed,
		};
		if only {
			Ok(Arc::new(UnwrapExactlyOne::new(versioned)))
		} else {
			Ok(versioned)
		}
	}

	// ========================================================================
	// Pipeline compilation with plan-time index resolution
	// ========================================================================

	/// Compile a relational pipeline: the clauses the fold collected, over the
	/// leaf or node it stopped at.
	///
	/// `stop` is either the pipeline's leaf — one source, or the `UnionAll` of
	/// several — or a node the fold compiles in place, which becomes an opaque
	/// source (see the module docs). The clauses are applied in pipeline
	/// order: access path and filter → split → aggregate → sort (with
	/// elimination) → limit → project → fetch → timeout → version → `ONLY`.
	#[cfg(test)]
	pub(crate) async fn compile_pipeline(
		&self,
		stop: PipelineStop,
		clauses: PipelineClauses,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		// `VERSION` is resolved up front, because a fast path has to refuse a
		// versioned read as well as wrap one.
		let version = match clauses.version.clone() {
			Some(version) => extract_version(version, self).await?,
			None => None,
		};
		// The stamp the pipeline's sources are read at, for the catalog reads
		// planning makes; see `PlanTimeVersion`.
		let plan_time_version = self.plan_time_version_of(clauses.version.as_ref()).await;

		// The sources a `FROM` lowered to, or the node that stands in for
		// them. A `UnionAll` is the `FROM a, b` form; its inputs are planned
		// one by one and combined.
		let (sources, node): (Vec<LogicalOp>, Option<LogicalOp>) = match stop {
			PipelineStop::Leaf(LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			})) => (inputs, None),
			PipelineStop::Leaf(leaf) => (vec![leaf], None),
			PipelineStop::Node(node) => (vec![], Some(node)),
		};

		// The clause rules of a FOR UPDATE read, checked on the pipeline so
		// they hold wherever the statement appears — a top-level statement is
		// also checked on its surface form by the executor, a nested one only
		// here — with the version the statement reads at, its own or one
		// inherited from an enclosing scope (only known here).
		check_locked_read(&sources, &clauses, version.is_some() || self.version.is_some())?;

		match &node {
			None => {
				if let Some(compiled) =
					self.compile_fast_path(&sources, &clauses, version.clone()).await?
				{
					return Ok(compiled);
				}
			}
			// A count over a table whose permission the analyzer placed above
			// it is still one scan: the count operator resolves that permission
			// itself, so it stands in for the node (see
			// [`fast_path::permission_filtered_source`]). Only the count shapes
			// may do so — a single record read enforces nothing of its own.
			Some(node) => {
				if let Some(source) = permission_filtered_source(node)
					&& let Some(compiled) = self
						.compile_count_fast_path(
							std::slice::from_ref(source),
							&clauses,
							version.clone(),
						)
						.await?
				{
					return Ok(compiled);
				}
			}
		}

		let PipelineClauses {
			only,
			version: _,
			timeout,
			fetch,
			project,
			limit,
			start,
			order,
			tempfiles,
			group,
			split,
			cond,
		} = clauses;

		// A grouped pipeline projects inside its aggregation, so its fields
		// come from the `Aggregate`; the `Project` above it, when present,
		// carries only `OMIT`. An ungrouped pipeline's fields come from its
		// `Project`; with neither there is no projection to apply.
		let (group_keys, fields, omit): (
			Option<Vec<LogicalExpr>>,
			Option<LogicalFields>,
			Vec<LogicalExpr>,
		) = match (group, project) {
			(
				Some(GroupClause {
					group,
					fields,
				}),
				project,
			) => (Some(group), Some(fields), project.map(|p| p.omit).unwrap_or_default()),
			(
				None,
				Some(ProjectClause {
					fields,
					omit,
				}),
			) => (None, Some(fields), omit),
			(None, None) => (None, None, vec![]),
		};

		// The tables whose rows the predicate is evaluated against: the leaves
		// of the `FROM`, or the tables beneath the node the walk stopped at.
		let predicate_tables: Vec<TableName> = {
			let mut tables = Vec::new();
			for source in &sources {
				tables_beneath(source, &mut tables);
			}
			if let Some(node) = &node {
				tables_beneath(node, &mut tables);
			}
			tables.into_iter().cloned().collect()
		};
		let primary_table = predicate_tables.first().cloned();
		let has_knn_early = cond.as_ref().is_some_and(has_knn_operator);

		// The MATCHES entries and KNN slot this pipeline's own WHERE declares.
		// Either being present replaces what the enclosing planner had; what
		// this pipeline does not declare is inherited below.
		let own_matches_context = cond.as_ref().and_then(|c| {
			let mut mc =
				extract_matches_context(c, Some(&self.ctx), |name| self.param_is_shadowed(name));
			if mc.is_empty() {
				return None;
			}
			if let Some(ref t) = primary_table {
				mc.set_table(t.clone());
			}
			Some(Arc::new(mc))
		});

		// The nested planner is this planner's own scope — transaction,
		// namespace and database, auth, cycle guard, catalog id cache and the
		// path of function bodies being compiled — under the pipeline's
		// version and its own planning scopes. Inheriting the cycle guard
		// means a self-referential permission or computed-field body whose
		// subquery flows through this nested planner sees the parent's
		// in-progress tables and falls back; inheriting the function path
		// means a recursive call inside a nested `SELECT` is the recursive
		// edge `plan_function_body` leaves to the runtime rather than a fresh
		// body to pre-plan; inheriting auth keeps fast-path eligibility
		// decisions consistent with the outer statement. The depth is the
		// enclosing planner's: this is the same statement, not a re-entry.
		// The version the nested planner reads under is this pipeline's own
		// or, where it has none, the one already in force: a pipeline nested
		// two deep under a `VERSION` is still a versioned read, and the
		// decisions gated on a version — bitmap fusion, the record-path fold,
		// pre-planning a function body — are made against the planner's own
		// field, not the enclosing statement's.
		let mut pp = self
			.own_scope()
			.with_version(version.clone().or_else(|| self.version.clone()))
			.with_plan_time_version(plan_time_version)
			.with_depth(self.current_depth());
		pp.set_planning_scopes(own_matches_context, has_knn_early, self);

		// A `<|k, DIST|>` form is served by the brute-force source, which
		// scores each row's vector field; every other KNN form is answered by
		// an index and only tests the row's id against its result.
		let knn_reads = if cond.as_ref().is_some_and(has_knn_k_operator) {
			KnnRowReads::Operands
		} else {
			KnnRowReads::RecordId
		};
		let needed_fields = Self::extract_needed_fields(
			fields.as_ref(),
			&omit,
			cond.as_ref(),
			order.as_ref(),
			group_keys.as_deref(),
			split.as_deref(),
			knn_reads,
		)
		// A clause reading the row other than by a field's name —
		// `type::field`, `@`, `$this`, a nested plan, a closure — reaches fields
		// no name in it shows. An index-backed KNN may build its rows from
		// record ids alone, so there every field is then needed.
		.filter(|_| {
			!(has_knn_early
				&& knn_reads == KnnRowReads::RecordId
				&& clauses_read_row_opaquely(
					fields.as_ref(),
					&omit,
					cond.as_ref(),
					order.as_ref(),
					group_keys.as_deref(),
					split.as_deref(),
					self.function_registry(),
				))
		});
		let decode_fields = Self::extract_decode_fields(
			self.function_registry(),
			fields.as_ref(),
			&omit,
			cond.as_ref(),
			order.as_ref(),
			group_keys.as_deref(),
			split.as_deref(),
		);
		// One source whose rows come from one scan, so a compiled predicate
		// and paging can be offered to that scan. A parameter is not one: it
		// resolves to a value when the statement runs.
		let source_is_single_scan = sources.len() == 1
			&& matches!(
				&sources[0],
				LogicalOp::Source(LogicalSource {
					source: SourceRef::Table(_)
						| SourceRef::Dynamic(
							LogicalExpr::Call(_)
								| LogicalExpr::Postfix { .. }
								| LogicalExpr::Invoke { .. }
						),
					..
				})
			);

		// Collect the MATCHES expressions registered by this pipeline's WHERE
		// condition (see `MatchesScope`). Walked
		// again after the record fold below so that both the original nodes
		// (as projections reference them) and the rewritten nodes (as the
		// residual scan predicate contains them) are in the allowlist.
		let mut cond_matches = HashSet::new();
		if let Some(ref c) = cond {
			crate::exec::physical_expr::collect_cond_matches(c, &mut cond_matches);
		}

		let cond = self
			.fold_pipeline_cond(
				cond,
				version.is_some(),
				fields.as_ref(),
				&sources,
				node.as_ref(),
				&predicate_tables,
			)
			.await;

		// A read at a stamp ranks the nearest rows among the records it reads at
		// the stamp: an ANN index describes the present.
		let cond = match (cond, predicate_tables.as_slice()) {
			(Some(cond), [table]) if sources.len() <= 1 => {
				Some(pp.exact_knn_at_stamp(table, cond).await)
			}
			(cond, _) => cond,
		};

		// Finalize the MATCHES registration scope on the inner planner:
		// allowlist from both condition forms, executor tables from the full
		// table sources (a record-id source registers no full-text index, so
		// its rows evaluate MATCHES to `false`). A table beneath an in-place node is
		// still a full table source: the MATCHES above the node is evaluated
		// per row against the index, so its rows must resolve.
		{
			if let Some(ref c) = cond {
				crate::exec::physical_expr::collect_cond_matches(c, &mut cond_matches);
			}
			let executor_tables = crate::exec::physical_expr::ExecutorTables::new(
				predicate_tables.clone(),
				record_id_source_tables(&sources),
				has_runtime_table_source(&sources),
			);
			// A WHERE-clause MATCHES the registration walk does not reach —
			// a function argument, a literal container, an IF/ELSE branch, a
			// block — binds to no full-text index, which is reported per row
			// as `NoIndexFoundForMatch`. Raise the same refusal here so the
			// outcome does not depend on which rows the condition reaches.
			// Only a full table source registers an index at all: with none
			// known at plan time, MATCHES evaluates to `false` per row, or is
			// judged per row once a run-time
			// source names its table (see `MatchesScope`), and there is
			// nothing to refuse here.
			if executor_tables.any_known()
				&& let Some(ref c) = cond
				&& let Some(found) =
					crate::exec::physical_expr::first_unregistered_cond_matches(c, &cond_matches)
			{
				return Err(crate::idx::Error::NoIndexFoundForMatch {
					exp: found.to_sql(),
				}
				.into());
			}
			pp.set_matches_scope(Arc::new(crate::exec::physical_expr::MatchesScope {
				allowlist: cond_matches,
				executor_tables,
			}));
		}

		// KNN handling
		let has_knn = cond.as_ref().is_some_and(has_knn_operator);
		let brute_force_knn = if has_knn {
			cond.as_ref().and_then(extract_bruteforce_knn)
		} else {
			None
		};

		let (cond_for_index, cond_for_filter) = if has_knn {
			let stripped = cond.as_ref().and_then(strip_knn_from_condition);
			if let Some(c) = stripped.as_ref()
				&& has_knn_operator(c)
			{
				// `strip_knn_from_condition` removes `K` and `Approximate` from
				// the top-level AND chain; whatever is left is either a KTree
				// variant (no longer backed by any index) or a `K`/`Approximate`
				// nested under OR/NOT. Disambiguate so the message is actionable.
				let message = if has_knn_ktree_operator(c) {
					"The `<|k|>` KNN operator (KTree / M-Tree) is no longer supported. \
					 Use `<|k, EF|>` against an HNSW index (e.g. \
					 `DEFINE INDEX … HNSW DIMENSION N`), or `<|k, DISTANCE|>` for a \
					 brute-force KNN with an explicit distance metric."
						.to_string()
				} else {
					"KNN operators must appear at the top level of the WHERE clause \
					 (joined with AND); nesting `<|k, …|>` inside OR or NOT is not \
					 supported."
						.to_string()
				};
				return Err(ExecError::Query {
					message,
				}
				.into());
			}
			if brute_force_knn.is_some() {
				(stripped.clone(), stripped)
			} else if cond.as_ref().is_some_and(has_knn_k_operator) {
				// A `K` form whose right-hand side is not plan-time
				// computable (idiom, subquery, …). No KNN source is
				// registered for such an expression and it evaluates to
				// `false` per row; keep the conjunct in the filter so
				// `BinaryOp`, which yields `false` for KNN, applies it.
				(cond.clone(), cond)
			} else {
				(cond, stripped)
			}
		} else {
			let c = cond;
			(c.clone(), c)
		};

		let scan_predicate = if source_is_single_scan {
			match cond_for_filter.as_ref() {
				Some(c) => Some(pp.physical_expr(c.clone()).await?),
				None => None,
			}
		} else {
			None
		};

		// Check prerequisites for limit pushdown that don't depend on the
		// access path. The per-access-path decision (whether the scan
		// ordering covers the ORDER BY) is made inside plan_source().
		let can_push_limit = source_is_single_scan
			&& brute_force_knn.is_none()
			&& !has_top_level_or(cond_for_filter.as_ref())
			&& limit.is_some()
			&& split.is_none()
			&& group_keys.is_none();

		let can_soft_push_limit = !can_push_limit
			&& source_is_single_scan
			&& brute_force_knn.is_none()
			&& limit.is_some()
			&& split.is_some()
			&& group_keys.is_none();

		// A chain of layers over a table takes the pipeline's paging on the
		// same terms as a single scan, provided nothing of the pipeline's own
		// stands between the chain and its `Limit`: no predicate left above
		// the chain (every user conjunct moved beneath the layers), no split,
		// no grouping. `fold_uncollected` withholds it beneath a permission
		// node.
		let chain_can_push_limit = node.as_ref().is_some_and(is_layer_chain_over_table)
			&& cond_for_filter.is_none()
			&& brute_force_knn.is_none()
			&& limit.is_some()
			&& split.is_none()
			&& group_keys.is_none();

		// A batch-size hint for a scan that keeps its `Limit` operator above
		// it, taken from the clause only where the clause is a literal. The
		// hint sizes a read and never changes a row, so an unresolvable clause
		// simply yields none — and a clause that is not a literal must not be
		// evaluated a second time here, since the operator above evaluates it.
		let limit_hint = get_effective_limit_literal(start.as_ref(), limit.as_ref());

		let (scan_limit, scan_start) = if can_push_limit || chain_can_push_limit {
			(
				match limit.as_ref() {
					Some(l) => Some(pp.physical_expr(l.clone()).await?),
					None => None,
				},
				match start.as_ref() {
					Some(s) => Some(pp.physical_expr(s.clone()).await?),
					None => None,
				},
			)
		} else if can_soft_push_limit {
			(
				match limit.as_ref() {
					Some(l) => Some(pp.physical_expr(l.clone()).await?),
					None => None,
				},
				None,
			)
		} else {
			(None, None)
		};

		// Keep a clone of the scan predicate so we can reuse it as a
		// precompiled predicate for the pipeline Filter when the source
		// does not consume it (FilterAction::UseOriginal). This avoids
		// compiling the same expression into a PhysicalExpr twice.
		let scan_predicate_for_reuse = scan_predicate.clone();

		// Tell source planning whether the downstream pipeline contains a
		// bounded top-k sort. Used by UnionIndexScan to skip eager
		// per-sub-stream prefetch — the heap discards most rows anyway,
		// so prefetching only wastes memory under high concurrency.
		let max_priority_queue_size =
			self.ctx.config.exec.max_order_limit_priority_queue_size as usize;
		let downstream_topk = is_bounded_topk_downstream(
			order.as_ref(),
			start.as_ref(),
			limit.as_ref(),
			tempfiles,
			max_priority_queue_size,
		);

		// TopK threshold pushdown analysis: when the downstream sort will be
		// a bounded heap on a raw first key, a table scan can reject rows
		// against the heap's worst entry before record decode. SPLIT, GROUP
		// BY, and brute-force KNN duplicate, regroup, or rank rows between
		// scan and sort, so they disqualify the request outright.
		let topk_request = if self.ctx.config.exec.topk_threshold_pushdown_enabled {
			compute_topk_pushdown_request(
				order.as_ref(),
				start.as_ref(),
				limit.as_ref(),
				fields.as_ref(),
				tempfiles,
				split.is_some() || group_keys.is_some() || brute_force_knn.is_some(),
				max_priority_queue_size,
			)
		} else {
			TopKPushdownRequest::NotApplicable
		};

		// Source resolution with plan-time index analysis. The result tracks
		// whether the predicate and limit/start were consumed by the source
		// operator, so we can avoid duplicating them in the outer pipeline.
		// A node compiled in place is opaque to the pipeline: nothing it
		// consumed is recorded, so every clause applies in full above it. The
		// predicate and ordering are still offered to the access path beneath
		// a chain of `PermissionFilter` and layer nodes (see
		// `fold_uncollected`); paging and the top-k threshold are not,
		// since a row the nodes remove must not have been counted.
		let source_count = sources.len();
		// A blocking GROUP BY consumes the whole source stream, so its LIMIT
		// never terminates the source early and must not disqualify bitmap
		// fusion.
		let has_query_limit = limit.is_some() && group_keys.is_none();
		let mut planned = match node {
			Some(node) => {
				pp.fold_uncollected(
					node,
					version.clone(),
					cond_for_index.as_ref(),
					order.as_ref(),
					scan_limit,
					scan_start,
					limit_hint,
					has_query_limit,
					downstream_topk,
				)
				.await?
			}
			None => {
				pp.plan_sources(
					sources,
					version.clone(),
					cond_for_index.as_ref(),
					order.as_ref(),
					needed_fields,
					decode_fields,
					scan_predicate,
					scan_limit,
					scan_start,
					limit_hint,
					has_query_limit,
					downstream_topk,
					&topk_request,
				)
				.await?
			}
		};

		if can_soft_push_limit {
			planned.limit_pushed = false;
		}

		// Detach the TopK pushdown handle before `planned.operator` moves into
		// the pipeline below; `plan_pipeline` hands it to sort planning.
		let topk_handle = planned.topk_pushdown.take();

		// Resolve the pipeline's WHERE state from the source's filter action.
		// - FullyConsumed: source handles the entire predicate, no Filter.
		// - Residual: only the residual part needs a Filter.
		// - UseOriginal: source didn't analyze the predicate; reuse the already-compiled predicate
		//   when available to avoid recompiling.
		let where_clause = match planned.filter_action {
			FilterAction::FullyConsumed => WhereClauseState::None,
			FilterAction::Residual(residual) => WhereClauseState::Original(residual),
			FilterAction::UseOriginal => {
				if let Some(pred) = scan_predicate_for_reuse {
					WhereClauseState::Precompiled(pred)
				} else if let Some(c) = cond_for_filter {
					WhereClauseState::Original(c)
				} else {
					WhereClauseState::None
				}
			}
		};

		// KNN conjuncts are stripped from `cond_for_filter` on the assumption
		// a KNN source operator enforces them. When none fired — an
		// `Approximate` form with no usable HNSW index, or a source the KNN
		// could not be pushed into — restore the full condition so the
		// conjunct is evaluated per row (as membership in the empty KNN
		// result set, i.e. `false`) instead of silently dropping the
		// conjunct and letting every row through. A multi-source FROM always
		// restores: a KnnScan in one source's subtree says nothing about the
		// other sources' rows, which the conjunct filters out. Membership is stable under
		// re-evaluation, so restoring over a fired KnnScan is harmless, and this is also
		// safe for sources that resolve their access path at run time
		// (`DynamicScan`).
		let where_clause = if has_knn
			&& brute_force_knn.is_none()
			&& (source_count > 1 || !source_contains_knn(&planned.operator))
			&& let Some(c) = cond_for_index.as_ref()
		{
			WhereClauseState::Original(c.clone())
		} else {
			where_clause
		};

		// KNN wrapping. Residual predicates (non-KNN WHERE conditions) must
		// be applied BEFORE ranking by distance. Otherwise rows that don't
		// satisfy the WHERE clause can consume top-K slots and push out
		// valid rows.
		let (source, where_clause) = if let Some(kp) = brute_force_knn {
			let input = match &where_clause {
				WhereClauseState::Original(c) => {
					let pred = pp.physical_expr(c.clone()).await?;
					Arc::new(Filter::new(planned.operator, pred)) as Arc<dyn ExecOperator>
				}
				WhereClauseState::Precompiled(predicate) => {
					Arc::new(Filter::new(planned.operator, Arc::clone(predicate)))
						as Arc<dyn ExecOperator>
				}
				WhereClauseState::None => planned.operator,
			};
			let knn_ctx = pp.knn_context.clone();
			let vector = match kp.vector {
				BruteForceKnnVector::Literal(v) => KnnVectorSource::Literal(v),
				BruteForceKnnVector::Deferred(expr) => {
					KnnVectorSource::Deferred(pp.physical_expr(expr).await?)
				}
			};
			let wrapped = Arc::new(
				KnnTopK::new(input, kp.field, vector, kp.k as usize, kp.distance)
					.with_knn_context(knn_ctx),
			) as Arc<dyn ExecOperator>;
			(wrapped, WhereClauseState::None)
		} else {
			(planned.operator, where_clause)
		};

		// Build pipeline.
		// When limit/start were pushed, omit them to avoid double application.
		// ORDER BY is always passed through — sort elimination via
		// `can_eliminate_sort()` in `plan_pipeline()` handles it independently.
		let config = SelectPipelineConfig {
			where_clause,
			split,
			group: group_keys,
			order,
			limit: if planned.limit_pushed {
				None
			} else {
				limit
			},
			start: if planned.limit_pushed {
				None
			} else {
				start
			},
			omit,
			tempfiles,
			topk_pushdown: topk_handle,
		};

		let projected = pp.plan_pipeline(source, fields, config).await?;
		let fetched = pp.plan_fetch(fetch, projected).await?;
		pp.wrap_select_tail(fetched, timeout, version, only).await
	}

	#[cfg(test)]
	/// Compile the pipeline to one scan when its shape allows (see
	/// [`fast_path`]). `None` when no fast path applies, or when the one that
	/// matched finds no index to answer it.
	async fn compile_fast_path(
		&self,
		sources: &[LogicalOp],
		clauses: &PipelineClauses,
		version: Option<Arc<dyn PhysicalExpr>>,
	) -> Result<Option<Arc<dyn ExecOperator>>, Error> {
		if let Some(compiled) =
			self.compile_count_fast_path(sources, clauses, version.clone()).await?
		{
			return Ok(Some(compiled));
		}

		// A single key read. The match proved there is no predicate, ordering,
		// grouping or split, so the projection is the only thing that decides
		// which fields the scan has to decode.
		if let Some(SingleRecord {
			record,
			for_update,
		}) = match_single_record(sources, clauses)
		{
			return self
				.compile_single_record(sources, record, for_update, clauses)
				.await
				.map(Some);
		}

		Ok(None)
	}

	#[cfg(test)]
	/// The count fast paths: a whole key range or an indexed predicate, each
	/// answered without materialising a row. The scans they compile to resolve
	/// the table's `SELECT` permission themselves, which is what lets them
	/// stand in for a permission node the analyzer placed over the source.
	async fn compile_count_fast_path(
		&self,
		sources: &[LogicalOp],
		clauses: &PipelineClauses,
		version: Option<Arc<dyn PhysicalExpr>>,
	) -> Result<Option<Arc<dyn ExecOperator>>, Error> {
		let versioned = version.is_some() || self.version.is_some();
		let scan: Arc<dyn ExecOperator> =
			match self.decide_count(sources, clauses, versioned).await? {
				None => return Ok(None),
				Some(CountDecision::All {
					source,
					output,
				}) => {
					use crate::exec::operators::CountScan;
					let source = self.source_physical_expr(source).await?;
					Arc::new(CountScan::new(source, version.clone(), output))
				}
				Some(CountDecision::Indexed {
					table,
					cond,
					output,
					btree_access,
					bitmap_plan,
				}) => {
					use crate::exec::operators::scan::index_count::IndexCountScan;
					let table_expr = self.physical_expr(LogicalExpr::Table(table)).await?;
					let predicate = self.physical_expr(cond.clone()).await?;
					Arc::new(
						IndexCountScan::new(table_expr, predicate, cond, version.clone(), output)
							.with_btree_access(btree_access)
							.with_bitmap_plan(bitmap_plan.map(bitmap_plan_to_node)),
					)
				}
			};
		let paged = self.apply_paging(scan, clauses.limit.clone(), clauses.start.clone()).await?;
		self.wrap_select_tail(paged, clauses.timeout.clone(), version, clauses.only).await.map(Some)
	}

	/// The count fast path that answers the pipeline `op` heads, with the
	/// `LIMIT` and `START` that page the counted row, as the pipeline planner
	/// chooses it: over the pipeline's leaf, or over the table beneath the
	/// permission nodes the count stands in for. A read that locks its
	/// records takes none. `versioned` says the pipeline has a `VERSION`.
	pub(crate) async fn count_offer(
		&self,
		op: &LogicalOp,
		versioned: bool,
	) -> Result<Option<(CountDecision, Option<LogicalExpr>, Option<LogicalExpr>)>, Error> {
		let (clauses, stop) = super::fold::collect_pipeline(op.clone());
		let sources = match stop {
			PipelineStop::Leaf(LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			})) => inputs,
			PipelineStop::Leaf(leaf) => vec![leaf],
			PipelineStop::Node(node) => match permission_filtered_source(&node) {
				Some(source) => vec![source.clone()],
				None => return Ok(None),
			},
		};
		if sources.iter().any(locks_records) {
			return Ok(None);
		}
		let versioned = versioned || self.version.is_some();
		Ok(self
			.decide_count(&sources, &clauses, versioned)
			.await?
			.map(|decision| (decision, clauses.limit, clauses.start)))
	}

	/// Which count fast path answers the pipeline of `clauses` over `sources`,
	/// if one does: the decision, which builds nothing. `versioned` says the
	/// read is under a `VERSION`, the statement's own or an enclosing one.
	///
	/// A count of a whole key range takes any source but a lightweight
	/// relation, which has no record range to count. An indexed predicate is
	/// counted from the index only where an index answers it — a `COUNT`
	/// index with that guard, a covering B-tree walk, or an exact bitmap
	/// fusion — and never for a versioned read: a `COUNT` index's deltas, a
	/// B-tree's entries and the doc-ID mappings a fused bitmap reads are all
	/// current state, with no stamp to read them at, so an index-assisted
	/// count would answer for now rather than for the version asked for.
	///
	/// SECURITY: an index-assisted count reads index entries directly, so it
	/// never applies the document-level field reduction that hides values a
	/// non-`Full` SELECT permission withholds. A predicate over such a field
	/// would leak the cardinality of values the actor may not select, and
	/// takes the full pipeline instead. `WITH NOINDEX` forbids index-assisted
	/// execution outright.
	pub(crate) async fn decide_count(
		&self,
		sources: &[LogicalOp],
		clauses: &PipelineClauses,
		versioned: bool,
	) -> Result<Option<CountDecision>, Error> {
		if let Some(CountAll {
			source,
			output,
		}) = match_count_all(sources, clauses)
		{
			let static_table = match source {
				SourceRef::Table(table) => Some(table),
				SourceRef::RecordId(rid) => Some(&rid.table),
				SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => None,
			};
			let lightweight = match static_table {
				Some(table) => self.table_is_lightweight(table).await?,
				None => false,
			};
			if !lightweight {
				return Ok(Some(CountDecision::All {
					source: source.clone(),
					output,
				}));
			}
		}
		let Some(IndexedCountAll {
			table,
			cond,
			with,
			output,
		}) = match_indexed_count_all(sources, clauses)
		else {
			return Ok(None);
		};
		if versioned
			|| matches!(with, Some(With::NoIndex))
			|| self.cond_touches_restricted_select_field_for_table(table, cond).await
		{
			return Ok(None);
		}
		// A dedicated COUNT index first, then a covering B-tree walk, then an
		// exact bitmap fusion over several indexes.
		let has_count_idx = self.has_matching_count_index(table, cond).await;
		let btree_access = if has_count_idx {
			None
		} else {
			self.resolve_count_btree_access(table, cond, with).await
		};
		let bitmap_plan = if !has_count_idx && btree_access.is_none() {
			self.resolve_count_bitmap_plan(table, cond, with).await
		} else {
			None
		};
		if !has_count_idx && btree_access.is_none() && bitmap_plan.is_none() {
			return Ok(None);
		}
		Ok(Some(CountDecision::Indexed {
			table: table.clone(),
			cond: cond.clone(),
			output,
			btree_access,
			bitmap_plan,
		}))
	}

	#[cfg(test)]
	/// A single key read: the projection is the only thing that decides which
	/// fields the scan has to decode, and the match declined a versioned read,
	/// so there is no version to scope the result to.
	async fn compile_single_record(
		&self,
		sources: &[LogicalOp],
		record: &LogicalRecordIdLit,
		for_update: bool,
		clauses: &PipelineClauses,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let (fields, omit) = match &clauses.project {
			Some(ProjectClause {
				fields,
				omit,
			}) => (fields.clone(), omit.clone()),
			None => (LogicalFields::all(), vec![]),
		};
		let needed_fields = Self::extract_needed_fields(
			Some(&fields),
			&omit,
			None,
			None,
			None,
			None,
			KnnRowReads::Operands,
		);
		let table = record.table.clone();
		let rid_expr = self.physical_expr(LogicalExpr::RecordId(Box::new(record.clone()))).await?;
		let mode = EnforcementMode::of_sources(sources);
		let resolved_table_ctx = self.try_resolve_table_ctx(&table, mode).await?;
		let pdf = self.pre_decode_filter_status_for(
			resolved_table_ctx.as_ref(),
			None,
			needed_fields.as_ref(),
		);
		let mut scan = RecordIdScan::new(rid_expr, None, needed_fields, None)
			.with_for_update(for_update)
			.with_enforcement_mode(mode);
		if let Some(tc) = resolved_table_ctx {
			scan = scan.with_resolved(tc);
		}
		scan = scan.with_pre_decode_filter(pdf);
		let paged =
			self.apply_paging(Arc::new(scan), clauses.limit.clone(), clauses.start.clone()).await?;
		let projected = self.plan_projections(fields, omit, paged).await?;
		self.wrap_select_tail(projected, clauses.timeout.clone(), None, clauses.only).await
	}

	/// Plan the sources of a `FROM` with plan-time index resolution.
	///
	/// One source is planned on its own; several are planned one by one and
	/// concatenated with a `Union`, into which nothing is pushed.
	///
	/// `has_query_limit` reports a LIMIT the pipeline can terminate the source
	/// early on — see [`Self::plan_source`] for the contract.
	#[allow(clippy::too_many_arguments)]
	#[cfg(test)]
	pub(crate) async fn plan_sources(
		&self,
		sources: Vec<LogicalOp>,
		version: Option<Arc<dyn PhysicalExpr>>,
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		needed_fields: Option<HashSet<String>>,
		decode_fields: Option<DecodeFields>,
		scan_predicate: Option<Arc<dyn PhysicalExpr>>,
		scan_limit: Option<Arc<dyn PhysicalExpr>>,
		scan_start: Option<Arc<dyn PhysicalExpr>>,
		limit_hint: Option<usize>,
		has_query_limit: bool,
		downstream_topk: bool,
		topk_request: &TopKPushdownRequest,
	) -> Result<PlannedSource, Error> {
		if sources.is_empty() {
			return Err(ExecError::Query {
				message: "SELECT requires at least one source".to_string(),
			}
			.into());
		}
		// Multi-source FROM combines via Union: rows from one scan compete in
		// the sort heap with rows from the others, so a per-scan threshold
		// probe would be misleading in EXPLAIN (its cell can never be
		// installed — sort planning receives a single handle). Drop the
		// request before fanning out.
		let topk_request = if sources.len() > 1 {
			&TopKPushdownRequest::NotApplicable
		} else {
			topk_request
		};
		let mut plans = Vec::with_capacity(sources.len());
		for source in sources {
			let p = self
				.plan_source(
					source,
					version.clone(),
					cond,
					order,
					needed_fields.clone(),
					decode_fields.clone(),
					scan_predicate.clone(),
					scan_limit.clone(),
					scan_start.clone(),
					limit_hint,
					has_query_limit,
					downstream_topk,
					topk_request,
				)
				.await?;
			plans.push(p);
		}
		if plans.len() == 1 {
			Ok(plans.pop().expect("verified non-empty"))
		} else {
			// Multiple sources are combined via Union; pushdowns are not
			// applicable because source_is_single_scan is false when there is
			// more than one source, so scan_predicate/scan_limit are always None.
			let operators = plans.into_iter().map(|p| p.operator).collect();
			Ok(PlannedSource::opaque(Arc::new(Union::new(operators))))
		}
	}

	/// Plan a single `FROM` source.
	///
	/// A `Source` reading a table with a transaction at hand resolves its
	/// access path at plan time and creates the concrete operator (IndexScan,
	/// FullTextScan, KnnScan, …) directly, which avoids redundant index
	/// analysis at execution time and enables sort elimination via
	/// `output_ordering()`. Without a transaction, a `DynamicScan` resolves
	/// its access path at execution time. The source's own `WITH` hint and
	/// `FOR UPDATE` flag are read from the node.
	///
	/// A `Values` leaf produces its rows from its expression; a `Subquery` or
	/// a `Match` is compiled whole. Any other node is compiled in place and is
	/// opaque: the predicate, ordering and paging stay above it.
	#[allow(clippy::too_many_arguments)]
	#[cfg(test)]
	pub(crate) async fn plan_source(
		&self,
		source: LogicalOp,
		version: Option<Arc<dyn PhysicalExpr>>,
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		needed_fields: Option<HashSet<String>>,
		// The fields a fetching scan decodes; see [`Self::extract_decode_fields`].
		// Set only where the scan's rows reach the pipeline directly: a chain of
		// permission or layer nodes above the scan passes `None`.
		decode_fields: Option<DecodeFields>,
		scan_predicate: Option<Arc<dyn PhysicalExpr>>,
		scan_limit: Option<Arc<dyn PhysicalExpr>>,
		scan_start: Option<Arc<dyn PhysicalExpr>>,
		// A per-batch read-size hint for a scan that keeps its `Limit`
		// operator, set only where the clause is a literal; see the caller.
		limit_hint: Option<usize>,
		// Whether the enclosing SELECT carries a LIMIT the pipeline can
		// terminate the source early on. A LIMIT above a blocking stage
		// (GROUP BY) consumes the whole stream first and does not count.
		// Bitmap fusion declines under such a LIMIT so streaming plans keep
		// pull-based early termination; independent of `scan_limit`, which
		// only carries a limit the scan itself may consume (never set for a
		// top-level OR).
		has_query_limit: bool,
		downstream_topk: bool,
		topk_request: &TopKPushdownRequest,
	) -> Result<PlannedSource, Error> {
		let (source, mode, with, for_update) = match source {
			// A resolved schema means the analyzer put the read's permission
			// and field nodes above this source; the scan then yields rows as
			// stored and is told so through its resolved table context.
			LogicalOp::Source(LogicalSource {
				source,
				schema,
				with,
				for_update,
			}) => (source, EnforcementMode::for_source(schema.as_ref()), with, for_update),
			LogicalOp::Values(LogicalValues {
				expr,
				for_update,
				written,
			}) => {
				// A value is not a record: there is nothing to lock.
				if for_update {
					return Err(ExecError::Query {
						message: crate::dbs::FOR_UPDATE_TARGETS_ERROR.to_string(),
					}
					.into());
				}
				let phys_expr = self.physical_expr(expr).await?;
				let source = if written {
					SourceExpr::written_array(phys_expr)
				} else {
					SourceExpr::new(phys_expr)
				};
				return Ok(PlannedSource::opaque(Arc::new(source)));
			}
			// A relational source is compiled from the plan the passes
			// produced. The outer predicate, ordering and limit stay outside
			// it: what the subquery may absorb was already moved in by the
			// pushdown pass, and what remains outside is what could not
			// soundly move.
			LogicalOp::Subquery(LogicalSubquery {
				input,
			}) => {
				if version.is_some() {
					return Err(ExecError::Query {
						message: "VERSION clause cannot be used with a subquery source. \
								  Place the VERSION clause inside the subquery instead."
							.to_string(),
					}
					.into());
				}
				return Ok(PlannedSource::opaque(Box::pin(self.fold(*input)).await?));
			}
			LogicalOp::Match(LogicalMatch {
				plan,
			}) => return Ok(PlannedSource::opaque(Box::pin(self.plan_match(*plan)).await?)),
			other => {
				return Box::pin(self.fold_uncollected(
					other,
					version,
					cond,
					order,
					scan_limit,
					scan_start,
					limit_hint,
					has_query_limit,
					downstream_topk,
				))
				.await;
			}
		};
		let with = with.as_ref();

		// A call source over statically known arguments names its rows at
		// plan time: `type::table($name)` with `$name` bound denotes one
		// table, and planning it as that table gives it the same access-path
		// selection, limit pushdown and sort elimination as a literal name.
		// The analyzer attached no schema to such a source, so the scan it
		// becomes enforces its permissions at runtime. A locked source stays
		// dynamic: `DynamicScan` performs the locked lookup and rejects every
		// value shape but a record id.
		let source = if for_update {
			source
		} else {
			self.resolve_call_source(source)
		};

		// Optimisation: WHERE id = <RecordId> -> point lookup, avoiding index
		// analysis and full-table iteration entirely.
		if let SourceRef::Table(ref table_name) = source
			&& let Some(rid_expr) = self.record_point_lookup(table_name, cond)
		{
			let filter_action = filter_action_for_predicate(&scan_predicate);
			let record_id_expr = self.physical_expr(rid_expr).await?;
			let resolved_table_ctx: Option<ResolvedTableContext> =
				self.try_resolve_table_ctx(table_name, mode).await?;
			let pdf = self.pre_decode_filter_status_for(
				resolved_table_ctx.as_ref(),
				scan_predicate.as_ref(),
				needed_fields.as_ref(),
			);
			let mut scan =
				RecordIdScan::new(record_id_expr, version, needed_fields, scan_predicate.clone())
					.with_for_update(for_update)
					.with_enforcement_mode(mode);
			if let Some(tc) = resolved_table_ctx {
				scan = scan.with_resolved(tc);
			}
			scan = scan.with_pre_decode_filter(pdf);
			return Ok(PlannedSource {
				operator: Arc::new(scan) as Arc<dyn ExecOperator>,
				filter_action,
				limit_pushed: false,
				topk_pushdown: None,
			});
		}

		// When we have a txn and the source is a table, resolve the
		// access path and table context at plan time.
		if let SourceRef::Table(ref table_name) = source
			&& let (Some(_), Some(_), Some(_)) = (&self.txn, &self.ns, &self.db)
		{
			// Resolve table context at plan time, under the mode the source
			// asks for. This eliminates runtime KV lookups in the operator's
			// execute().
			//
			// A source the analyzer resolved is enforced by the plan above
			// it, and every access path below chooses an index from a table
			// context it reads. Without one, the only scan that yields rows as
			// stored is the table scan, which resolves its table when it runs.
			let table_ctx: Option<ResolvedTableContext> =
				match self.resolve_table_ctx(table_name, mode).await {
					Ok(ctx) => ctx,
					Err(_) if mode == EnforcementMode::Runtime => None,
					Err(_) => {
						return self
							.plan_table_scan_source(
								table_name.clone(),
								Direction::Forward,
								order,
								true,
								scan_predicate,
								scan_limit,
								scan_start,
								needed_fields,
								version,
								None,
								mode,
								&TopKPushdownRequest::NotApplicable,
							)
							.await;
					}
				};
			let decided = self
				.decide_access_path(
					table_name,
					cond,
					order,
					with,
					has_query_limit,
					// A statement-level VERSION or an enclosing version
					// context both disqualify bitmap fusion: doc-ID mappings
					// and index entries are not time-travel-aware.
					version.is_some() || self.version.is_some(),
				)
				.await;
			if let Some(AccessDecision {
				path: access_path,
				direction,
				order_withheld,
				restricted_select,
			}) = decided
			{
				let order = if order_withheld {
					None
				} else {
					order
				};
				let table = table_name.clone();
				let knn_ctx = self.knn_context.clone();
				match access_path {
					AccessPath::BTreeScan {
						index_ref,
						access,
						direction,
					} => {
						return self
							.plan_btree_scan_source(
								table,
								index_ref,
								access,
								direction,
								cond,
								order,
								order_withheld,
								scan_predicate,
								scan_limit,
								scan_start,
								limit_hint,
								needed_fields,
								decode_fields,
								version,
								table_ctx,
							)
							.await;
					}
					AccessPath::FullTextSearch {
						index_ref,
						query,
						operator,
					} => {
						return self
							.plan_fulltext_search_source(
								table,
								index_ref,
								query,
								operator,
								cond,
								needed_fields,
								decode_fields,
								version,
								table_ctx,
							)
							.await;
					}
					AccessPath::KnnSearch {
						index_ref,
						vector,
						k,
						ef,
						prefilter,
					} => {
						return self
							.plan_knn_search_source(
								table,
								index_ref,
								vector,
								k,
								ef,
								prefilter,
								cond,
								needed_fields,
								version,
								table_ctx,
								knn_ctx,
							)
							.await;
					}
					AccessPath::TableScan => {
						return self
							.plan_table_scan_source(
								table,
								direction,
								order,
								order_withheld,
								scan_predicate,
								scan_limit,
								scan_start,
								needed_fields,
								version,
								table_ctx,
								mode,
								topk_request,
							)
							.await;
					}
					AccessPath::Union {
						paths,
						dedupe,
					} => {
						return self
							.plan_union_index_source(
								table,
								paths,
								dedupe,
								order,
								limit_hint,
								cond,
								needed_fields,
								decode_fields,
								version,
								table_ctx,
								knn_ctx,
								downstream_topk,
								&restricted_select,
							)
							.await;
					}
					AccessPath::BitmapFusion {
						root,
						fallback,
					} => {
						return self
							.plan_bitmap_fusion_source(
								table,
								root,
								fallback,
								order,
								cond,
								needed_fields,
								decode_fields,
								version,
								table_ctx,
								knn_ctx,
								downstream_topk,
								&restricted_select,
							)
							.await;
					}
					AccessPath::EmptyScan => {
						return Ok(Self::plan_empty_source());
					}
				}
			}
		}

		// Fallback: create the appropriate operator (index resolved at runtime)
		let knn_ctx = self.knn_context.clone();

		// The scans built below resolve their table when they run and enforce
		// what they find per row. A source the analyzer resolved is enforced
		// by the plan above it, so such a scan would judge the rows a second
		// time, beneath the plan's nodes. A table the access paths could not
		// be chosen for is therefore scanned whole, as a table scan yielding
		// rows as stored; a record-id source takes its mode below.
		if mode == EnforcementMode::Planned
			&& let SourceRef::Table(table) = &source
		{
			return self
				.plan_table_scan_source(
					table.clone(),
					Direction::Forward,
					order,
					true,
					scan_predicate,
					scan_limit,
					scan_start,
					needed_fields,
					version,
					None,
					mode,
					&TopKPushdownRequest::NotApplicable,
				)
				.await;
		}

		if mode == EnforcementMode::Planned && !matches!(source, SourceRef::RecordId(_)) {
			return Err(EngineError::unreachable(
				"the analyzer resolved a source that is neither a table nor a record id",
			)
			.into());
		}

		match source {
			SourceRef::RecordId(rid) => {
				let table = rid.table.clone();
				let record_id_expr = self.physical_expr(LogicalExpr::RecordId(rid)).await?;
				// A lightweight relation's edges are not stored records, so a
				// record-id source over one is answered from the adjacency
				// walk too: the id it names bounds what the walk keeps. A
				// single id would be synthesized by a point read either way,
				// but a range has no record keys to scan between.
				if self.table_is_lightweight(&table).await? {
					let resolved = self.try_resolve_table_ctx(&table, mode).await?;
					Self::lightweight_needs_context(&table, resolved.as_ref(), mode)?;
					return Ok(PlannedSource::opaque(Arc::new(LightweightEdgeScan {
						table,
						record_id: Some(record_id_expr),
						needed_fields,
						predicate: scan_predicate,
						table_ctx: resolved,
						mode,
						metrics: Arc::new(crate::exec::OperatorMetrics::new()),
					})));
				}
				// A source the analyzer resolved is enforced by the plan; the
				// scan takes its table context, and its mode for a table the
				// catalog lacks. An unresolved one resolves its table when it
				// runs, as of the version it reads at.
				let resolved = if mode == EnforcementMode::Planned {
					self.try_resolve_table_ctx(&table, mode).await?
				} else {
					None
				};
				let mut scan = RecordIdScan::new(record_id_expr, version, needed_fields, None)
					.with_for_update(for_update)
					.with_enforcement_mode(mode);
				if let Some(tc) = resolved {
					scan = scan.with_resolved(tc);
				}
				Ok(PlannedSource::opaque(Arc::new(scan)))
			}
			// A parameter the passes could not resolve — bound by a `LET` or
			// `FOR` in the statement's own block, or shadowing a binding of
			// the enclosing context — is only known when the statement runs.
			// `DynamicScan` decides by the value it finds: a table is scanned
			// under its permissions, a record id is looked up (and, under FOR
			// UPDATE, locked; every other shape is rejected there), an array
			// yields its elements and any other value is one row.
			SourceRef::Param(param) => {
				self.plan_dynamic_scan(
					LogicalExpr::Param(param),
					None,
					version,
					cond,
					order,
					with,
					needed_fields,
					scan_predicate,
					scan_limit,
					scan_start,
					knn_ctx,
					for_update,
				)
				.await
			}
			SourceRef::Table(table) => {
				self.plan_dynamic_scan(
					LogicalExpr::Table(table.clone()),
					Some(&table),
					version,
					cond,
					order,
					with,
					needed_fields,
					scan_predicate,
					scan_limit,
					scan_start,
					knn_ctx,
					for_update,
				)
				.await
			}
			SourceRef::Dynamic(
				expr @ (LogicalExpr::Call(_)
				| LogicalExpr::Postfix {
					..
				}
				| LogicalExpr::Invoke {
					..
				}),
			) => {
				self.plan_dynamic_scan(
					expr,
					None,
					version,
					cond,
					order,
					with,
					needed_fields,
					scan_predicate,
					scan_limit,
					scan_start,
					knn_ctx,
					for_update,
				)
				.await
			}
			source @ (SourceRef::Dynamic(_) | SourceRef::Mock(_)) => {
				let phys_expr = self.physical_expr(source_ref_expr(source)).await?;
				Ok(PlannedSource::opaque(Arc::new(SourceExpr::new(phys_expr))))
			}
		}
	}

	// ========================================================================
	// Per-access-path source planners (called by plan_source)
	// ========================================================================

	#[cfg(test)]
	/// Build a `IndexScan` for [`AccessPath::BTreeScan`], computing the
	/// filter action (index-covered conditions stripped), limit/start
	/// pushdown when the index ordering covers the ORDER BY, and an
	/// optional batch-ceiling hint for residual filters.
	#[allow(clippy::too_many_arguments)]
	async fn plan_btree_scan_source(
		&self,
		table: TableName,
		index_ref: IndexRef,
		access: BTreeAccess,
		direction: Direction,
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		order_withheld: bool,
		scan_predicate: Option<Arc<dyn PhysicalExpr>>,
		scan_limit: Option<Arc<dyn PhysicalExpr>>,
		scan_start: Option<Arc<dyn PhysicalExpr>>,
		limit_hint: Option<usize>,
		needed_fields: Option<HashSet<String>>,
		decode_fields: Option<DecodeFields>,
		version: Option<Arc<dyn PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
	) -> Result<PlannedSource, Error> {
		use crate::exec::operators::IndexScan;

		// An index lookup narrows the candidates without proving the
		// conditions it answered: an index over an array field lists each
		// element, so a record found under `field = v` may hold `[v, w]`,
		// which is not equal to `v`. When the access covers every condition
		// the scan applies the whole predicate to what it fetches; otherwise
		// the Filter above it applies the whole predicate, not the conditions
		// the access left.
		let filter_action = if let Some(c) = cond {
			match strip_index_conditions(c, &access, &index_ref.cols) {
				None => FilterAction::FullyConsumed,
				Some(_) => FilterAction::Residual(c.clone()),
			}
		} else {
			FilterAction::FullyConsumed
		};
		// Push limit to IndexScan when the index ordering covers ORDER BY
		// (or there is no ORDER BY).
		//
		// IMPORTANT: only push when the filter is fully consumed by the
		// index. With a residual filter above the scan, pushing LIMIT
		// would make the scan return fewer rows than needed because the
		// post-filter may remove some of them.
		//
		// A column whose idiom flattens (`*`) holds one index entry per
		// element, so a key range that does not pin such a column to a
		// single value reaches the same record through several entries. The
		// scan yields records rather than entries, so those repeats have to
		// be collapsed below everything that counts rows — otherwise a
		// record is returned once per element, `count()` reports entries as
		// rows, and a mutating statement applies itself once per element.
		//
		// A column the prefix pins has no fan-out left; the column a
		// trailing range merely bounds still has, since several of its
		// elements can satisfy the bound.
		let fans_out = crate::exec::index::access_path::access_fans_out(&index_ref.cols, &access);
		// A plain column that may hold an array also reaches a record through
		// one entry per element, so its scan is collapsed as well.
		let repeats = crate::exec::index::access_path::access_repeats_records(&index_ref, &access);
		// `order_withheld` says an ORDER BY was taken away from access-path
		// selection for confidentiality. The index was then chosen without it
		// and says nothing about it, so the ordering is not covered.
		//
		// Nor is it covered when the scan fans out: the record's position in
		// key order is not one position but one per element, so a walk of the
		// range does not visit records in the order the index columns imply.
		let order_covered = || {
			!order_withheld
				&& !fans_out && order
				.is_none_or(|ord| index_covers_ordering(&index_ref, &access, direction, ord))
		};
		// LIMIT counts rows, and a fanning scan's entries are not rows, so it
		// cannot be answered by stopping the scan early.
		let push = scan_limit.is_some()
			&& !repeats
			&& matches!(filter_action, FilterAction::FullyConsumed)
			&& order_covered();
		// `scan_limit` flows to exactly one destination: pushed into the
		// scan when `push` is true (avoids reapplying LIMIT above),
		// otherwise used as a per-batch sizing hint when the index
		// already covers ORDER BY but a residual filter prevents real
		// LIMIT pushdown. Move it once instead of cloning.
		let (idx_limit, idx_start, limit_pushed, batch_ceiling) = if push {
			(scan_limit, scan_start, true, None)
		} else if scan_limit.is_some()
			&& matches!(filter_action, FilterAction::Residual(_))
			&& order_covered()
		{
			// The ceiling is a plan-time number, never the compiled LIMIT: the
			// `Limit` operator above this scan evaluates the statement's
			// clause, and a second evaluation here would run it twice. A
			// clause that is not a literal simply yields no hint.
			(None, None, false, limit_hint)
		} else {
			(None, None, false, None)
		};
		// Only thread the compiled WHERE into IndexScan when there is no
		// outer Filter for the same condition. For `Residual`, the
		// pipeline adds a Filter above the scan; applying `scan_predicate`
		// again here would duplicate work and skew EXPLAIN row accounting.
		let index_where_predicate = match &filter_action {
			FilterAction::FullyConsumed => scan_predicate,
			FilterAction::Residual(_) | FilterAction::UseOriginal => None,
		};
		let mut scan = IndexScan::new(
			index_ref,
			access,
			direction,
			table,
			idx_limit,
			idx_start,
			version,
			Some(needed_fields),
			index_where_predicate,
		)
		.with_batch_ceiling(batch_ceiling);
		if let Some(tc) = table_ctx {
			scan = scan.with_resolved(tc);
		}
		if let Some(fields) = decode_fields {
			scan = scan.with_decode_fields(fields);
		}
		// The dedupe sits directly on the scan, below any residual Filter, so
		// every operator above it sees each matching record exactly once.
		let operator: Arc<dyn ExecOperator> = if repeats {
			Arc::new(crate::exec::operators::DistinctRecords::new(Arc::new(scan)))
		} else {
			Arc::new(scan)
		};
		Ok(PlannedSource {
			operator,
			filter_action,
			limit_pushed,
			topk_pushdown: None,
		})
	}

	#[cfg(test)]
	/// Build a `FullTextScan` for [`AccessPath::FullTextSearch`]. The
	/// MATCHES conjunct the scan answers is stripped from the WHERE clause;
	/// every other condition, including any other MATCHES, is returned in the
	/// filter action.
	#[allow(clippy::too_many_arguments)]
	async fn plan_fulltext_search_source(
		&self,
		table: TableName,
		index_ref: IndexRef,
		query: String,
		operator: crate::expr::operator::MatchesOperator,
		cond: Option<&LogicalExpr>,
		needed_fields: Option<HashSet<String>>,
		decode_fields: Option<DecodeFields>,
		version: Option<Arc<dyn PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
	) -> Result<PlannedSource, Error> {
		use crate::exec::operators::FullTextScan;

		// A full-text index has exactly one column (`DEFINE INDEX … FULLTEXT`
		// takes one), which is the column the scan's probe reads. Without one
		// nothing is known to be answered, and the whole condition is kept.
		let filter_action = match (cond, index_ref.cols.first()) {
			(None, _) => FilterAction::FullyConsumed,
			(Some(c), Some(column)) => match strip_fts_condition(c, column, &operator, &query) {
				None => FilterAction::FullyConsumed,
				Some(residual) => FilterAction::Residual(residual),
			},
			(Some(c), None) => FilterAction::Residual(c.clone()),
		};
		let mut scan =
			FullTextScan::new(index_ref, query, operator, table, version, Some(needed_fields));
		if let Some(tc) = table_ctx {
			scan = scan.with_resolved(tc);
		}
		if let Some(fields) = decode_fields {
			scan = scan.with_decode_fields(fields);
		}
		Ok(PlannedSource {
			operator: Arc::new(scan) as Arc<dyn ExecOperator>,
			filter_action,
			limit_pushed: false,
			topk_pushdown: None,
		})
	}

	#[cfg(test)]
	/// Build a `KnnScan` for [`AccessPath::KnnSearch`]. KNN operators are
	/// stripped from the condition; the residual non-KNN predicates are
	/// pushed into the HNSW search so non-matching rows don't consume
	/// top-K slots.
	#[allow(clippy::too_many_arguments)]
	async fn plan_knn_search_source(
		&self,
		table: TableName,
		index_ref: IndexRef,
		vector: Vec<crate::val::Number>,
		k: u32,
		ef: u32,
		prefilter: Option<KnnPrefilterPlan>,
		cond: Option<&LogicalExpr>,
		needed_fields: Option<HashSet<String>>,
		version: Option<Arc<dyn PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
		knn_ctx: Option<Arc<crate::exec::function::KnnContext>>,
	) -> Result<PlannedSource, Error> {
		use crate::exec::operators::{KnnPrefilter, KnnScan};

		// The in-traversal residual excludes MATCHES conjuncts: they are not
		// evaluable inside the ANN search (no query executor — they compute
		// to `false` and would reject every candidate). The outer Filter
		// (`FilterAction::UseOriginal`) enforces them through their physical
		// operator instead.
		let residual_cond = match cond.and_then(strip_knn_and_matches_from_condition) {
			Some(c) => Some(self.physical_expr(c).await?),
			None => None,
		};
		// The plan-level prefilter is compiled into its operator-side form —
		// the bitmap node tree evaluated at execute time, plus the true
		// residual compiled once for the in-traversal filter and the
		// graph-free exact tier.
		// The MATCHES-aware condition is compiled for an index without shared
		// doc ids — which can never use the bitmap prefilter — and for any
		// index whose KNN-stripped condition contains a MATCHES the prefilter
		// may fail to cover: one served by a full-text index that is not
		// bitmap-capable, or an allow-list build that exceeds its runtime
		// budget.
		let stripped_cond = cond.and_then(strip_knn_from_condition);
		let needs_matches_condition = !index_ref.definition().uses_shared_doc_ids()
			|| stripped_cond.as_ref().is_some_and(expr_contains_matches);
		let matches_condition = if needs_matches_condition {
			match stripped_cond {
				Some(c) => Some(self.physical_expr(c).await?),
				None => None,
			}
		} else {
			None
		};
		let prefilter = match prefilter {
			Some(p) => {
				let residual = match p.residual {
					Some(c) => Some(self.physical_expr(c).await?),
					None => None,
				};
				let node = bitmap_plan_to_node(p.root);
				Some(KnnPrefilter {
					node_dyn: Arc::clone(&node) as Arc<dyn ExecOperator>,
					node,
					residual,
					uncovered_matches: p.uncovered_matches,
				})
			}
			None => None,
		};
		// Rows needing nothing but their id may be built from the search's
		// record ids when every field of the table reads as stored. A
		// versioned read always reads its records, so it never asks.
		let fields_read_as_stored = if version.is_none()
			&& needed_fields.as_ref().is_some_and(|needed| needed.iter().all(|f| f == "id"))
		{
			self.fields_read_as_stored(&table).await
		} else {
			false
		};
		let mut scan = KnnScan::new(
			index_ref,
			vector,
			k,
			ef,
			table,
			version,
			knn_ctx,
			residual_cond,
			prefilter,
			Some(needed_fields),
		)
		.with_matches_condition(matches_condition)
		.with_fields_read_as_stored(fields_read_as_stored);
		if let Some(tc) = table_ctx {
			scan = scan.with_resolved(tc.with_candidate_guard(self).await?);
		}
		Ok(PlannedSource::opaque(Arc::new(scan)))
	}

	/// Whether every field of `table` reads as stored for every caller: none
	/// is `COMPUTED`, and none carries a field `SELECT` permission other than
	/// `FULL`. `false` without catalog access, or when the field list cannot
	/// be read.
	async fn fields_read_as_stored(&self, table: &TableName) -> bool {
		let Some(txn) = self.txn.as_ref() else {
			return false;
		};
		let Some((ns_id, db_id)) = self.ns_db_ids().await else {
			return false;
		};
		match txn.all_tb_fields(ns_id, db_id, table, None).await {
			Ok(fields) => fields.iter().all(|fd| {
				fd.computed.is_none()
					&& matches!(fd.select_permission, crate::catalog::Permission::Full)
			}),
			Err(e) => {
				tracing::warn!(
					table = %table,
					error = %e,
					"plan-time field list failed in fields_read_as_stored; \
					 KNN rows will be read from their records",
				);
				false
			}
		}
	}

	/// The top-level fields a scan of `table` reads from each record it
	/// fetches for a pipeline with these clauses, or `None` when it decodes
	/// whole records; see [`Self::extract_decode_fields`].
	///
	/// A table that defines a computed field decodes whole records, since the
	/// field's body may read any stored field, and so does one with a field
	/// whose `SELECT` permission is not `FULL` when the caller's reads are
	/// checked, since that guard reads the stored record. The table's own
	/// permission and the layers above the scan are the caller's to judge.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn scan_decode_fields(
		&self,
		table: &TableName,
		fields: Option<&LogicalFields>,
		omit: &[LogicalExpr],
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		group: Option<&[LogicalExpr]>,
	) -> Option<DecodeFields> {
		let decode = Self::extract_decode_fields(
			self.function_registry(),
			fields,
			omit,
			cond,
			order,
			group,
			None,
		)?;
		let txn = self.txn.as_ref()?;
		let (ns_id, db_id) = self.ns_db_ids().await?;
		let defined = match txn.all_tb_fields(ns_id, db_id, table, None).await {
			Ok(defined) => defined,
			Err(e) => {
				tracing::warn!(
					table = %table,
					error = %e,
					"plan-time field list failed in scan_decode_fields; \
					 records will be decoded whole",
				);
				return None;
			}
		};
		let checked = self.reads_are_checked();
		defined
			.iter()
			.all(|fd| {
				fd.computed.is_none()
					&& (!checked
						|| matches!(fd.select_permission, crate::catalog::Permission::Full))
			})
			.then_some(decode)
	}

	/// Whether the rows of an index-backed KNN scan of `table` need nothing
	/// of their records but the id, given the clauses of the statement that
	/// reads them.
	///
	/// That holds when the clauses read no field but `id` (an index-backed
	/// KNN operator reads only the id it tests for membership), none of them
	/// reads the row other than by a field's name, and every field of
	/// `table` reads as stored for every caller. The table's permission and
	/// the version of the read are the caller's to judge. `fields` is the
	/// statement's selection, `None` for a read that selects the whole row.
	pub(crate) async fn knn_rows_need_only_ids(
		&self,
		table: &TableName,
		fields: Option<&LogicalFields>,
		omit: &[LogicalExpr],
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		group: Option<&[LogicalExpr]>,
	) -> bool {
		let ids_only = Self::extract_needed_fields(
			fields,
			omit,
			cond,
			order,
			group,
			None,
			KnnRowReads::RecordId,
		)
		.is_some_and(|needed| needed.iter().all(|f| f == "id"));
		ids_only
			&& !clauses_read_row_opaquely(
				fields,
				omit,
				cond,
				order,
				group,
				None,
				self.function_registry(),
			) && self.fields_read_as_stored(table).await
	}

	#[cfg(test)]
	/// Build a `BitmapResolve` for [`AccessPath::BitmapFusion`].
	///
	/// The whole WHERE clause stays as the residual filter
	/// (`FilterAction::UseOriginal`, as for KNN scans): the bitmap
	/// intersection is a candidate pre-filter, and re-evaluating the full
	/// predicate per surviving row keeps the plan's results identical to the
	/// streaming plans by construction — including when a range branch is
	/// dropped at runtime for exceeding its drained-entry budget.
	///
	/// For OR fusion, `fallback` carries the streaming union the fused plan
	/// replaced. It is planned into a `UnionIndexScan` here and handed to the
	/// `BitmapResolve`, which streams it instead of the bitmap when a union
	/// branch overflows its budget at execute time. The union's own filter
	/// action is discarded — this source stays `FilterAction::UseOriginal`,
	/// so the full WHERE above re-filters either path.
	#[allow(clippy::too_many_arguments)]
	async fn plan_bitmap_fusion_source(
		&self,
		table: TableName,
		root: BitmapPlan,
		fallback: Option<Box<AccessPath>>,
		order: Option<&LogicalOrdering>,
		cond: Option<&LogicalExpr>,
		needed_fields: Option<HashSet<String>>,
		decode_fields: Option<DecodeFields>,
		version: Option<Arc<dyn PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
		knn_ctx: Option<Arc<crate::exec::function::KnnContext>>,
		downstream_topk: bool,
		restricted_select: &RestrictedPrefixes,
	) -> Result<PlannedSource, Error> {
		let fallback_op = match fallback.map(|f| *f) {
			Some(AccessPath::Union {
				paths,
				dedupe,
			}) => Some(
				self.plan_union_index_source(
					table.clone(),
					paths,
					dedupe,
					order,
					// A LIMIT is never pushed into a fused source, so there is
					// no per-batch ceiling to hand the union's sub-scans.
					None,
					cond,
					needed_fields.clone(),
					decode_fields.clone(),
					version,
					table_ctx.clone(),
					knn_ctx,
					downstream_topk,
					restricted_select,
				)
				.await?
				.operator,
			),
			Some(other) => {
				// Server-side log carries the Debug-formatted access path for
				// diagnosis; the client-facing message stays opaque so internal
				// access-path details don't leak.
				tracing::error!(
					path = ?other,
					"BitmapFusion carried an unexpected fallback access path"
				);
				return Err(EngineError::Internal(
					"BitmapFusion carried an unexpected fallback access path; \
					 only Union is valid here"
						.into(),
				)
				.into());
			}
			None => None,
		};
		let mut scan = BitmapResolve::new(table, bitmap_plan_to_node(root), needed_fields);
		if let Some(op) = fallback_op {
			scan = scan.with_overflow_fallback(op);
		}
		if let Some(tc) = table_ctx {
			scan = scan.with_resolved(tc);
		}
		if let Some(fields) = decode_fields {
			scan = scan.with_decode_fields(fields);
		}
		Ok(PlannedSource::opaque(Arc::new(scan)))
	}

	#[cfg(test)]
	/// Build a `EmptyScan` for [`AccessPath::EmptyScan`] — used when the
	/// analyzer proved the WHERE clause cannot match any rows (e.g. a
	/// contradictory range or empty `IN []`). Returns a `PlannedSource`
	/// that reports the predicate as fully consumed and the limit as
	/// pushed, so the outer pipeline does not add a Filter or Limit.
	fn plan_empty_source() -> PlannedSource {
		use crate::exec::operators::EmptyScan;
		PlannedSource {
			operator: Arc::new(EmptyScan::new()) as Arc<dyn ExecOperator>,
			filter_action: FilterAction::FullyConsumed,
			limit_pushed: true,
			topk_pushdown: None,
		}
	}

	#[cfg(test)]
	/// Build a `TableScan` for [`AccessPath::TableScan`] — the fallback
	/// when no index access path covers the query. Pushes LIMIT/START
	/// only when ORDER BY is compatible with the natural KV scan
	/// direction (i.e. plain `id` ASC/DESC or absent).
	/// A lightweight relation's edge scan judges a synthesized edge by the
	/// table context it is given. Under [`EnforcementMode::Planned`] the plan
	/// above the scan enforces, and a scan without a context would enforce a
	/// second time beneath it, so the plan is refused rather than built.
	fn lightweight_needs_context(
		table: &TableName,
		resolved: Option<&ResolvedTableContext>,
		mode: EnforcementMode,
	) -> Result<(), Error> {
		if mode == EnforcementMode::Planned && resolved.is_none() {
			return Err(ExecError::Query {
				message: format!(
					"the lightweight relation `{table}` cannot be scanned: its table context \
					 could not be resolved when the statement was planned"
				),
			}
			.into());
		}
		Ok(())
	}

	#[cfg(test)]
	#[allow(clippy::too_many_arguments)]
	async fn plan_table_scan_source(
		&self,
		table: TableName,
		direction: Direction,
		order: Option<&LogicalOrdering>,
		order_withheld: bool,
		scan_predicate: Option<Arc<dyn PhysicalExpr>>,
		scan_limit: Option<Arc<dyn PhysicalExpr>>,
		scan_start: Option<Arc<dyn PhysicalExpr>>,
		needed_fields: Option<HashSet<String>>,
		version: Option<Arc<dyn PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
		mode: EnforcementMode,
		topk_request: &TopKPushdownRequest,
	) -> Result<PlannedSource, Error> {
		use crate::exec::topk_pushdown::{
			TopKPushdownHandle, TopKPushdownReason, TopKPushdownStatus, TopKThresholdProbe,
			field_path_wire_segments, topk_pushdown_status_at_plan_time,
		};

		// A lightweight relation keeps no records, so there is no record range
		// to open a cursor over: its edges are enumerated from the adjacency
		// of the vertex tables its `IN` clause names, and the record for each
		// is synthesized from the edge's own id. Every access path below reads
		// a record range, so none of them applies.
		let lightweight = match &table_ctx {
			Some(tc) => {
				crate::kvs::lightweight::lightweight_relation(&tc.table_def.table_type).is_some()
			}
			None => self.table_is_lightweight(&table).await?,
		};
		if lightweight {
			Self::lightweight_needs_context(&table, table_ctx.as_ref(), mode)?;
			return Ok(PlannedSource::opaque(Arc::new(LightweightEdgeScan {
				table,
				record_id: None,
				needed_fields,
				predicate: scan_predicate,
				table_ctx,
				mode,
				metrics: Arc::new(crate::exec::OperatorMetrics::new()),
			})));
		}

		let filter_action = filter_action_for_predicate(&scan_predicate);
		// A withheld ORDER BY is an ordering the scan does not produce, not an
		// absent one: reading it as absent would truncate the scan to the LIMIT
		// and leave the sort above with the wrong rows.
		let push = scan_limit.is_some() && !order_withheld && order_is_scan_compatible(order);
		let (tbl_limit, tbl_start, limit_pushed) = if push {
			(scan_limit, scan_start, true)
		} else {
			(None, None, false)
		};
		let pdf = self.pre_decode_filter_status_for(
			table_ctx.as_ref(),
			scan_predicate.as_ref(),
			needed_fields.as_ref(),
		);
		// TopK threshold pushdown: build the scan-side probe for an eligible
		// request, under a fresh pushdown id that the sort shares. The plan
		// holds no threshold: each execution of the sort makes its own (see
		// `crate::exec::topk_pushdown`). The field eligibility check runs
		// against the FULL plan-time field state (not the projection-filtered
		// view) so a computed ORDER BY field can't slip through when the
		// projection drops its definition. The handle travels to
		// `plan_sort_consolidated`, which installs the pushdown only if the
		// sort plan it builds matches `expected_first_key`.
		let (topk_status, topk_handle) = match topk_request {
			TopKPushdownRequest::NotApplicable => (TopKPushdownStatus::NotApplicable, None),
			TopKPushdownRequest::Ineligible(reason) => {
				(TopKPushdownStatus::Ineligible(*reason), None)
			}
			TopKPushdownRequest::Eligible(spec) => {
				match field_path_wire_segments(&spec.first_key.path) {
					// The request analysis already vets the path; this re-check
					// keeps the probe constructor's invariant local.
					None => {
						(TopKPushdownStatus::Ineligible(TopKPushdownReason::UnsupportedOrder), None)
					}
					Some(segments) => {
						let pushdown = crate::exec::topk_pushdown::next_id();
						let probe = Arc::new(TopKThresholdProbe::new(
							segments,
							spec.first_key.direction,
							spec.key_count == 1,
							pushdown,
							self.ctx.config.exec.idiom_recursion_limit,
						));
						let full_state = table_ctx.as_ref().map(|tc| tc.field_state());
						let status =
							topk_pushdown_status_at_plan_time(probe, full_state.as_deref());
						let handle = matches!(
							status,
							TopKPushdownStatus::Active(_) | TopKPushdownStatus::Deferred(_)
						)
						.then(|| TopKPushdownHandle {
							pushdown,
							expected_first_key: spec.first_key.clone(),
							expected_key_count: spec.key_count,
						});
						(status, handle)
					}
				}
			}
		};
		let mut scan = TableScan::new(
			table,
			direction,
			version,
			scan_predicate,
			tbl_limit,
			tbl_start,
			needed_fields,
		)
		.with_enforcement_mode(mode);
		if let Some(tc) = table_ctx {
			scan = scan.with_resolved(tc);
		}
		scan = scan.with_pre_decode_filter(pdf);
		scan = scan.with_topk_pushdown(topk_status);
		Ok(PlannedSource {
			operator: Arc::new(scan) as Arc<dyn ExecOperator>,
			filter_action,
			limit_pushed,
			topk_pushdown: topk_handle,
		})
	}

	#[cfg(test)]
	/// Build a `UnionIndexScan` for [`AccessPath::Union`] — one
	/// sub-operator per OR branch (or per IN-expansion / containment
	/// branch).  When every sub-path is an equality B-tree scan and
	/// ORDER BY is `id ASC/DESC` only, enables k-way merge-by-id so the
	/// Sort operator can be eliminated.
	///
	/// `dedupe` records whether branches can overlap on the same record
	/// — see [`AccessPath::Union`] for the contract. Drives the
	/// `ByIndexKey` vs `ByIndexKeyDedup` merge-mode selection when an
	/// ordered k-way merge is active.
	#[allow(clippy::too_many_arguments)]
	async fn plan_union_index_source(
		&self,
		table: TableName,
		paths: Vec<AccessPath>,
		dedupe: bool,
		order: Option<&LogicalOrdering>,
		limit_hint: Option<usize>,
		cond: Option<&LogicalExpr>,
		needed_fields: Option<HashSet<String>>,
		// Handed to each index and full-text sub-scan, which gates it against
		// the table as a scan of its own would; see [`DecodeFields`].
		decode_fields: Option<DecodeFields>,
		version: Option<Arc<dyn PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
		knn_ctx: Option<Arc<crate::exec::function::KnnContext>>,
		downstream_topk: bool,
		restricted_select: &RestrictedPrefixes,
	) -> Result<PlannedSource, Error> {
		let (paths, merge) = decide_union_merge(paths, dedupe, order);

		// `dedupe` is the explicit contract set by the analyser: `true`
		// for OR-union and CONTAINS-on-array (branches can overlap on
		// the same record), `false` for scalar `IN`-expansion (each
		// row's field equals at most one literal). Drives the merge
		// variant choice below.

		// When merge mode is active and a downstream LIMIT exists, pass it as
		// a batch-ceiling hint to each sub-scan so the merge terminates
		// quickly. The hint is a plan-time number, never the compiled LIMIT:
		// the union keeps `limit_pushed` false and so retains the outer
		// `Limit`, which evaluates the statement's clause itself — passing the
		// expression down would run it once per sub-scan as well.
		let merge_active = merge.is_some();
		let merge_batch_ceiling = if merge_active {
			limit_hint
		} else {
			None
		};

		// Strip CONTAINSANY / ANYINSIDE leaves whose literal value set
		// contains every one of the branches' prefix values — only then
		// does each branch imply the leaf.  `cond` is the whole WHERE
		// clause, which may hold conjuncts the union does not cover
		// (see `try_and_nested_or_union`), so the check is per leaf and
		// never assumes a leaf produced this union.  If everything is
		// covered, the residual Filter goes away and LIMIT can be
		// pushed into the sub-scans (when merge is active).
		// CONTAINSALL / ALLINSIDE leaves are NOT stripped (intersection
		// semantics; see `strip_union_index_conditions` rustdoc).
		//
		// SECURITY: stripping is also disabled when the WHERE clause
		// references a field whose SELECT permission is not `Full`.
		// `UnionIndexScan` sub-operators run a `ScanPipeline` with no
		// predicate (the union dedupes/merges and only the outer Filter
		// re-applies the WHERE). Field-level permissions then wipe the
		// restricted value from each document before it would have been
		// matched — but a stripped leaf is never re-evaluated, so the
		// index entries themselves become a membership oracle for the
		// hidden value. Leaving the leaf in the residual filter forces
		// the post-permission recheck and closes that channel.
		let filter_action = if let Some(c) = cond {
			// Reuses the prefixes resolved once in `plan_source` for this same
			// table, so the union path performs no extra `all_tb_fields` lookup.
			let strip_safe = !restricted_select.cond_touches(c);
			let stripped = if strip_safe {
				strip_union_index_conditions(c, &paths)
			} else {
				Some(c.clone())
			};
			match stripped {
				None => FilterAction::FullyConsumed,
				Some(residual) => FilterAction::Residual(residual),
			}
		} else {
			FilterAction::FullyConsumed
		};

		let mut sub_operators: Vec<Arc<dyn ExecOperator>> = Vec::with_capacity(paths.len());
		for path in paths {
			sub_operators.push(
				self.build_union_sub_operator(
					path,
					&table,
					cond,
					version.as_ref(),
					table_ctx.as_ref(),
					knn_ctx.as_ref(),
					merge_batch_ceiling,
					decode_fields.as_ref(),
				)
				.await?,
			);
		}

		// UnionIndexScan handles field-level permissions and computed-field
		// materialization internally; the outer pipeline handles Filter,
		// Sort, and Limit.
		let mut union_scan =
			UnionIndexScan::new(table, sub_operators, needed_fields).with_merge(merge.clone());
		if merge.is_none() && downstream_topk {
			// No merge available — but a bounded top-k sort is downstream.
			// Skip eager per-sub-stream prefetch so the heap drives demand.
			union_scan = union_scan.with_downstream_topk();
		}
		if let Some(tc) = table_ctx {
			union_scan = union_scan.with_resolved(tc);
		}

		// `limit_pushed` stays `false` for unions: the per-sub-scan
		// `merge_batch_ceiling` only sizes the per-batch fetch, not the
		// total row count.  Cancellation is driven by the outer `Limit`
		// operator pulling N rows and dropping the stream — the
		// streaming pipeline cancels the union and its sub-scans on
		// drop.  Removing the outer `Limit` (as `limit_pushed: true`
		// would do) drops LIMIT semantics entirely on this path.
		Ok(PlannedSource {
			operator: Arc::new(union_scan) as Arc<dyn ExecOperator>,
			filter_action,
			limit_pushed: false,
			topk_pushdown: None,
		})
	}

	#[cfg(test)]
	/// Build one sub-operator for a `UnionIndexScan`. Each sub-operator
	/// is a per-OR-branch scan without computed-field materialization or
	/// `needed_fields` — those are handled at the union level.
	///
	/// `select_access_path` only emits `BTreeScan` / `FullTextSearch` /
	/// `KnnSearch` as union sub-paths; anything else is a planner bug
	/// and surfaces as `EngineError::Internal` rather than silently returning a
	/// full table scan.
	#[allow(clippy::too_many_arguments)]
	async fn build_union_sub_operator(
		&self,
		path: AccessPath,
		table: &TableName,
		cond: Option<&LogicalExpr>,
		version: Option<&Arc<dyn PhysicalExpr>>,
		table_ctx: Option<&ResolvedTableContext>,
		knn_ctx: Option<&Arc<crate::exec::function::KnnContext>>,
		merge_batch_ceiling: Option<usize>,
		decode_fields: Option<&DecodeFields>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		use crate::exec::operators::{FullTextScan, IndexScan, KnnScan};

		match path {
			AccessPath::BTreeScan {
				index_ref,
				access,
				direction,
			} => {
				let fans_out =
					crate::exec::index::access_path::access_repeats_records(&index_ref, &access);
				let mut scan = IndexScan::new(
					index_ref,
					access,
					direction,
					table.clone(),
					None,
					None,
					version.cloned(),
					None,
					None,
				);
				if merge_batch_ceiling.is_some() {
					scan = scan.with_batch_ceiling(merge_batch_ceiling);
				}
				if let Some(tc) = table_ctx {
					scan = scan.with_resolved(tc.clone());
				}
				if let Some(fields) = decode_fields {
					scan = scan.with_decode_fields(fields.clone());
				}
				// A branch whose own key range leaves a fanning column open
				// reaches a record through several entries. The union's merge
				// answers for records appearing in more than one branch, not
				// for one branch reaching a record twice.
				if fans_out {
					return Ok(Arc::new(crate::exec::operators::DistinctRecords::new(Arc::new(
						scan,
					))));
				}
				Ok(Arc::new(scan))
			}
			AccessPath::FullTextSearch {
				index_ref,
				query,
				operator,
			} => {
				let mut scan = FullTextScan::new(
					index_ref,
					query,
					operator,
					table.clone(),
					version.cloned(),
					None,
				);
				if let Some(tc) = table_ctx {
					scan = scan.with_resolved(tc.clone());
				}
				if let Some(fields) = decode_fields {
					scan = scan.with_decode_fields(fields.clone());
				}
				Ok(Arc::new(scan))
			}
			AccessPath::KnnSearch {
				index_ref,
				vector,
				k,
				ef,
				// Union branches carry no prefilter: a branch is a single
				// predicate, so there is nothing to cover.
				prefilter: _,
			} => {
				let residual_cond = match cond.and_then(strip_knn_and_matches_from_condition) {
					Some(c) => Some(self.physical_expr(c).await?),
					None => None,
				};
				let mut scan = KnnScan::new(
					index_ref,
					vector,
					k,
					ef,
					table.clone(),
					version.cloned(),
					knn_ctx.cloned(),
					residual_cond,
					None,
					None,
				);
				if let Some(tc) = table_ctx {
					scan = scan.with_resolved(tc.clone().with_candidate_guard(self).await?);
				}
				Ok(Arc::new(scan))
			}
			other => {
				// Server-side log carries the Debug-formatted access path
				// for diagnosis; the client-facing message stays opaque
				// so internal access-path details don't leak.
				tracing::error!(
					path = ?other,
					"UnionIndexScan sub-path produced an unexpected access path"
				);
				Err(EngineError::Internal(
					"UnionIndexScan sub-path produced an unexpected access path; \
					 only BTreeScan / FullTextSearch / KnnSearch are valid here"
						.into(),
				)
				.into())
			}
		}
	}

	#[cfg(test)]
	/// Plan a `DynamicScan` source that resolves its access path at runtime.
	///
	/// Used for `FROM type::table(...)`, `FROM $param` under `FOR UPDATE`,
	/// and the `FROM tablename` fallback when plan-time catalog context is
	/// unavailable. `table` names the table when the source is one, so its
	/// context can still be resolved for the pre-decode filter. Handles
	/// filter-action, limit pushdown with ORDER BY compatibility, and KNN
	/// context in a single place.
	#[allow(clippy::too_many_arguments)]
	async fn plan_dynamic_scan(
		&self,
		expr: LogicalExpr,
		table: Option<&TableName>,
		version: Option<Arc<dyn PhysicalExpr>>,
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		with: Option<&With>,
		needed_fields: Option<HashSet<String>>,
		scan_predicate: Option<Arc<dyn PhysicalExpr>>,
		scan_limit: Option<Arc<dyn PhysicalExpr>>,
		scan_start: Option<Arc<dyn PhysicalExpr>>,
		knn_ctx: Option<Arc<crate::exec::function::KnnContext>>,
		for_update: bool,
	) -> Result<PlannedSource, Error> {
		let filter_action = filter_action_for_predicate(&scan_predicate);
		let push = scan_limit.is_some() && order_is_scan_compatible(order);
		let (dyn_limit, dyn_start, limit_pushed) = if push {
			(scan_limit, scan_start, true)
		} else {
			(None, None, false)
		};
		// `DynamicScan` enforces at runtime whatever the source asked for, so
		// the pre-decode filter is judged against the field state it will run
		// with.
		let resolved_table_ctx: Option<ResolvedTableContext> = match table {
			Some(table_name) => {
				self.try_resolve_table_ctx(table_name, EnforcementMode::Runtime).await?
			}
			None => None,
		};
		let source_expr = self.physical_expr(expr).await?;
		let pdf = self.pre_decode_filter_status_for(
			resolved_table_ctx.as_ref(),
			scan_predicate.as_ref(),
			needed_fields.as_ref(),
		);
		Ok(PlannedSource {
			operator: Arc::new(
				DynamicScan::new(
					source_expr,
					version,
					cond.cloned(),
					order.cloned(),
					with.cloned(),
					needed_fields,
					scan_predicate,
					dyn_limit,
					dyn_start,
				)
				.with_knn_context(knn_ctx)
				.with_pre_decode_filter(pdf)
				.with_for_update(for_update),
			) as Arc<dyn ExecOperator>,
			filter_action,
			limit_pushed,
			topk_pushdown: None,
		})
	}

	#[cfg(test)]
	/// Compute the plan-time [`PreDecodeFilterStatus`] for a KV scan from an optional resolved
	/// table context, the scan's WHERE predicate (if any), and the scan's projected fields.
	///
	/// Centralises the field-state projection and call into
	/// [`pre_decode_filter_status_at_plan_time`] used by [`TableScan`], [`DynamicScan`] and
	/// [`RecordIdScan`] planning paths.
	fn pre_decode_filter_status_for(
		&self,
		table_ctx: Option<&ResolvedTableContext>,
		predicate: Option<&Arc<dyn PhysicalExpr>>,
		needed_fields: Option<&HashSet<String>>,
	) -> crate::exec::pre_decode_filter::PreDecodeFilterStatus {
		let projected = table_ctx.map(|tc| tc.field_state_for_projection(needed_fields));
		// `idiom_recursion_limit` is shared with the SurrealQL `RECURSE` /
		// `@@` planner (`compute_idiom_recursion`); reusing it caps both
		// the SurrealQL-level recursion and the pre-decode walker descent
		// at a single, user-configurable limit (default 256).
		pre_decode_filter_status_at_plan_time(
			predicate,
			projected.as_ref(),
			self.ctx.config.exec.idiom_recursion_limit,
		)
	}

	/// The table or record a call source denotes, when that is known at plan
	/// time; any other source is returned unchanged.
	///
	/// The call is folded under the constant-folding rule: a pure,
	/// deterministic, synchronous built-in the caller's capabilities permit,
	/// over literal arguments. A denied call stays a call and fails at
	/// evaluation with the error the unfolded path raises. A result that is
	/// neither a table nor a record id (an array of values, a string) stays a
	/// dynamic source whose rows the call yields when the statement runs.
	pub(crate) fn resolve_call_source(&self, source: SourceRef) -> SourceRef {
		let SourceRef::Dynamic(mut expr @ LogicalExpr::Call(_)) = source else {
			return source;
		};
		let capabilities = self.ctx.get_capabilities();
		fold_expressions(
			&mut expr,
			self.function_registry(),
			&capabilities,
			None,
			false,
			self.folds_clock(),
		);
		match expr {
			folded @ (LogicalExpr::Table(_) | LogicalExpr::RecordId(_)) => {
				SourceRef::classify(folded)
			}
			other => SourceRef::Dynamic(other),
		}
	}

	#[cfg(test)]
	/// Resolve a [`ResolvedTableContext`] for `table_name`, planned under
	/// `mode` and read as of the planner's [`PlanTimeVersion`].
	///
	/// A decline — no database selected, a plan-time cycle, a catalog error, a
	/// table not in the catalog, a `VERSION` only the runtime can evaluate — is
	/// `Ok(None)` under either mode, and the scan resolves its table when it
	/// runs, at the stamp it reads at.
	///
	/// [`EnforcementMode::Runtime`] is for a source the analyzer did not
	/// resolve: the scan enforces per row, and a context spares it catalog
	/// reads without deciding anything.
	///
	/// [`EnforcementMode::Planned`] is for a source the analyzer resolved,
	/// whose permission and field nodes stand above the scan in the plan. Such
	/// a scan must yield rows as stored, which a table scan and a record-id
	/// scan do under that mode with or without a context. A caller whose scan
	/// cannot ([`LightweightEdgeScan`]) checks for the context itself
	/// ([`Self::lightweight_needs_context`]).
	async fn try_resolve_table_ctx(
		&self,
		table_name: &TableName,
		mode: EnforcementMode,
	) -> Result<Option<ResolvedTableContext>, Error> {
		Ok(self.resolve_table_ctx(table_name, mode).await.unwrap_or(None))
	}

	#[cfg(test)]
	/// The context [`Self::try_resolve_table_ctx`] resolves — `None` for a
	/// table the catalog lacks at the plan-time version, which the scan then
	/// resolves when it runs — or the reason none can be: no catalog to read,
	/// a `VERSION` only known at runtime, a plan-time cycle, a failed read. A
	/// failed catalog read is logged here, once, whichever mode asked.
	async fn resolve_table_ctx(
		&self,
		table_name: &TableName,
		mode: EnforcementMode,
	) -> Result<Option<ResolvedTableContext>, String> {
		let (Some(ns), Some(db)) = (self.ns(), self.db()) else {
			return Err("no database is selected".to_owned());
		};
		// Single cached catalog read for (ns_id, db_id); `ns_db_ids`
		// memoises across `resolve_param`, plan-time predicate folding,
		// and the per-source resolution path that this function backs.
		let Some((ns_id, db_id)) = self.ns_db_ids().await else {
			return Err("the namespace or database is not in the catalog".to_owned());
		};
		let version = match self.plan_time_version {
			PlanTimeVersion::Current => None,
			PlanTimeVersion::At(stamp) => Some(stamp),
			PlanTimeVersion::Unknown => {
				return Err("the VERSION is only known when the statement runs".to_owned());
			}
		};

		// Cycle check: if this table is already being resolved on a parent
		// stack frame (typically a self-referential permission predicate
		// like `WHERE (SELECT FROM same_table) != NONE`), the subtree is left
		// to the runtime. The entry's `Drop` impl pops the table when this
		// function returns.
		let Some(_entry) = self.cycle_guard().try_enter(ns_id, db_id, table_name.clone()) else {
			tracing::debug!(
				ns = %ns,
				db = %db,
				table = %table_name,
				"plan-time cycle detected; the table context is not resolved",
			);
			return Err("the table is being resolved by an enclosing frame".to_owned());
		};

		match resolve_table_context(self, ns, db, ns_id, db_id, table_name, mode, version).await {
			Ok(ctx) => Ok(ctx),
			Err(e) => {
				tracing::warn!(
					ns = %ns,
					db = %db,
					table = %table_name,
					error = %e,
					"plan-time table-context resolution failed",
				);
				Err(format!("the catalog could not be read: {e}"))
			}
		}
	}

	/// Check at plan time whether a matching COUNT index exists for the query.
	///
	/// Returns `true` when plan-time catalog access is available (txn, ns, db)
	/// and `table_name` has an online `DEFINE INDEX ... COUNT WHERE <cond>`
	/// whose condition is the same predicate as `cond`. The index's condition
	/// is held in expression form and the query's in logical form; both
	/// render as the same SurrealQL, so the comparison is on the rendering.
	async fn has_matching_count_index(&self, table_name: &TableName, cond: &LogicalExpr) -> bool {
		let Some(txn) = self.txn.as_ref() else {
			return false;
		};
		let Some((ns_id, db_id)) = self.ns_db_ids().await else {
			return false;
		};
		let indexes = match txn.all_tb_indexes(ns_id, db_id, table_name, None).await {
			Ok(idx) => idx,
			Err(e) => {
				tracing::warn!(
					table = %table_name,
					error = %e,
					"plan-time index list failed in has_matching_count_index; assuming no count index",
				);
				return false;
			}
		};
		// COUNT fast paths must not use an index until the durable build
		// protocol has published it as online.
		let Ok(indexes) = filter_online_indexes(txn, ns_id, db_id, indexes).await else {
			return false;
		};
		let cond_sql = cond.to_sql();
		indexes.iter().any(|ix| {
			matches!(&ix.index, Index::Count(Some(_)))
				&& ix.count_cond.as_ref().is_some_and(|c| c.0.to_sql() == cond_sql)
		})
	}

	/// Returns true when permissions are being enforced for this plan AND
	/// `cond` references a field whose SELECT permission on `table_name` is
	/// not `Full`.
	///
	/// The indexed COUNT fast paths (`IndexCountScan` over a dedicated
	/// `Index::Count` or a covering B-tree index) count index entries
	/// without going through the scan pipeline that applies field-level
	/// SELECT permissions. If the predicate references a restricted field,
	/// returning the materialised count leaks the cardinality of values the
	/// current user is not permitted to read.
	///
	/// The `UnionIndexScan` planner makes the equivalent decision (whether
	/// `strip_union_index_conditions` is safe — the union sub-operators
	/// don't re-evaluate the WHERE leaf after field-level permissions, so
	/// stripping a leaf on a restricted field would turn the index entries
	/// themselves into a membership oracle) by calling
	/// [`RestrictedPrefixes::cond_touches`] directly on prefixes it already
	/// has on hand, avoiding a redundant catalog lookup.
	async fn cond_touches_restricted_select_field_for_table(
		&self,
		table_name: &TableName,
		cond: &LogicalExpr,
	) -> bool {
		self.resolve_restricted_select_prefixes(table_name).await.cond_touches(cond)
	}

	/// Resolve which field-path prefixes on `table_name` are governed by a
	/// non-`Full` SELECT permission for the current actor. Shared by the
	/// WHERE-clause and ORDER BY plan-time guards.
	async fn resolve_restricted_select_prefixes(
		&self,
		table_name: &TableName,
	) -> RestrictedPrefixes {
		let (Some(ns_name), Some(db_name)) = (self.ns.as_deref(), self.db.as_deref()) else {
			// No catalog access at plan time — conservatively assume
			// permissions could apply.
			return RestrictedPrefixes::AssumeRestricted;
		};
		if !self.should_check_perms_for_view(ns_name, db_name) {
			return RestrictedPrefixes::None;
		}
		let Some(txn) = self.txn.as_ref() else {
			return RestrictedPrefixes::AssumeRestricted;
		};
		let Some((ns_id, db_id)) = self.ns_db_ids().await else {
			return RestrictedPrefixes::AssumeRestricted;
		};
		let fields = match txn.all_tb_fields(ns_id, db_id, table_name, None).await {
			Ok(fs) => fs,
			Err(e) => {
				tracing::warn!(
					table = %table_name,
					error = %e,
					"plan-time field list failed in \
					 resolve_restricted_select_prefixes; \
					 conservatively disabling index fast paths",
				);
				return RestrictedPrefixes::AssumeRestricted;
			}
		};
		// Collect the field paths that are not unconditionally SELECT-able. A
		// declared name that is not a field path names no position a read can
		// be matched against, so restriction is assumed rather than missed.
		let mut restricted_prefixes = Vec::new();
		for f in fields
			.iter()
			.filter(|f| !matches!(f.select_permission, crate::catalog::Permission::Full))
		{
			match crate::expr::logical::FieldPath::from_idiom(&f.name) {
				Some(path) => restricted_prefixes.push(path),
				None => return RestrictedPrefixes::AssumeRestricted,
			}
		}
		if restricted_prefixes.is_empty() {
			RestrictedPrefixes::None
		} else {
			RestrictedPrefixes::Some(restricted_prefixes)
		}
	}

	/// `indexes` with every b-tree column that stores array elements spelled
	/// as an element column, resolved against `table_name`'s declared fields;
	/// see [`with_array_columns_as_elements`](crate::exec::index::access_path::with_array_columns_as_elements).
	///
	/// `None` when the field list cannot be read: without it a column's
	/// fan-out is unknown, so the caller declines its plan-time index path
	/// rather than plan one that could count or order element entries as
	/// rows. A declined select plan falls back to `DynamicScan`, which plans
	/// without b-tree indexes when the list is unreadable there too.
	async fn with_array_columns_as_elements(
		&self,
		txn: &Transaction,
		ns_id: crate::catalog::NamespaceId,
		db_id: crate::catalog::DatabaseId,
		table_name: &TableName,
		indexes: Arc<[IndexDefinition]>,
	) -> Option<crate::exec::index::access_path::ElementColumns> {
		use crate::exec::index::access_path::ElementColumns;
		if !crate::exec::index::access_path::may_have_array_columns(&indexes) {
			return Some(ElementColumns::unchanged(indexes));
		}
		let fields = match txn.all_tb_fields(ns_id, db_id, table_name, None).await {
			Ok(fields) => fields,
			Err(e) => {
				tracing::warn!(
					table = %table_name,
					error = %e,
					"plan-time field list failed in with_array_columns_as_elements; \
					 falling back to runtime",
				);
				return None;
			}
		};
		Some(crate::exec::index::access_path::with_array_columns_as_elements(indexes, &fields))
	}

	/// Resolve a B-tree index access path covering the WHERE condition for
	/// key-only counting.  Returns `Some((IndexRef, BTreeAccess))` when the
	/// index analysis finds a B-tree index that fully covers the predicate
	/// (no residual filter), allowing `IndexCountScan` to count index keys
	/// instead of deserializing records.
	///
	/// Counting keys is exact only when each record has one key: an array
	/// value is indexed once per element, so a record holding `[3, 4]` is
	/// listed under `3` although `[3, 4] = 3` is false. The walk is therefore
	/// chosen only for an index every column of which is declared a kind
	/// that holds no array; any other count reads the records.
	async fn resolve_count_btree_access(
		&self,
		table_name: &TableName,
		cond: &LogicalExpr,
		with: Option<&With>,
	) -> Option<(IndexRef, BTreeAccess)> {
		let txn = self.txn.as_ref()?;

		let (ns_id, db_id) = self.ns_db_ids().await?;
		let indexes = match txn.all_tb_indexes(ns_id, db_id, table_name, None).await {
			Ok(idx) => idx,
			Err(e) => {
				tracing::warn!(
					table = %table_name,
					error = %e,
					"plan-time index list failed in resolve_count_btree_access; \
					 falling back to runtime",
				);
				return None;
			}
		};
		// Key-only count scans read index data directly, so restrict candidates
		// to durable-online indexes.
		let indexes = filter_online_indexes(txn, ns_id, db_id, indexes).await.ok()?;

		if indexes.is_empty() {
			return None;
		}
		let columns =
			self.with_array_columns_as_elements(txn, ns_id, db_id, table_name, indexes).await?;

		let fields = txn.all_tb_fields(ns_id, db_id, table_name, None).await.ok()?;
		let one_key_per_record = |cols: &[Idiom]| {
			cols.iter().all(|col| {
				fields.iter().find(|fd| &fd.name == col).is_some_and(field_def_excludes_arrays)
			})
		};

		let analyzer = columns.analyzer(with);
		let candidates = analyzer.analyze(Some(cond), None);

		// Look for a candidate that fully covers the WHERE condition
		// (no residual filter needed).
		for candidate in &candidates {
			// A key-only count counts index entries, so it is only a row
			// count while each matching record owns exactly one entry in the
			// range. A column not declared a kind that excludes arrays can hold
			// an array, which is listed once per element, and a scan that fans
			// out counts a record once per element.
			if !one_key_per_record(&candidate.index_ref.cols) {
				continue;
			}
			if crate::exec::index::access_path::access_fans_out(
				&candidate.index_ref.cols,
				&candidate.access,
			) {
				continue;
			}
			// Check: does this index access fully cover the condition?
			// If strip_index_conditions returns None, the index
			// consumed the entire WHERE clause.
			if strip_index_conditions(cond, &candidate.access, &candidate.index_ref.cols).is_none()
			{
				return Some((candidate.index_ref.clone(), candidate.access.clone()));
			}
		}

		None
	}

	/// Resolve an exact bitmap fusion plan for an index-only COUNT: an AND of
	/// two or more index-backed predicates whose every conjunct is exactly
	/// represented by a candidate bitmap. The count is then the fused
	/// bitmap's cardinality — zero record fetches.
	///
	/// Exactness needs the declared field kinds (array values fan out to one
	/// index entry per element, which would inflate the count), so the
	/// table's fields are resolved here; see
	/// [`IndexAnalyzer::try_bitmap_count_fusion`] for the structural rules.
	async fn resolve_count_bitmap_plan(
		&self,
		table_name: &TableName,
		cond: &LogicalExpr,
		with: Option<&With>,
	) -> Option<BitmapPlan> {
		let txn = self.txn.as_ref()?;

		let (ns_id, db_id) = self.ns_db_ids().await?;
		let indexes = txn.all_tb_indexes(ns_id, db_id, table_name, None).await.ok()?;
		// Bitmap count drains index data directly, so restrict candidates to
		// durable-online indexes.
		let indexes = filter_online_indexes(txn, ns_id, db_id, indexes).await.ok()?;
		if indexes.len() < 2 {
			return None;
		}

		let fields = txn.all_tb_fields(ns_id, db_id, table_name, None).await.ok()?;
		let exact_col = |col: &Idiom| -> bool {
			fields.iter().find(|fd| &fd.name == col).is_some_and(field_def_excludes_arrays)
		};
		let columns =
			crate::exec::index::access_path::with_array_columns_as_elements(indexes, &fields);

		let analyzer = columns.analyzer(with);
		analyzer.try_bitmap_count_fusion(cond, &exact_col)
	}

	/// The pipeline's predicate `cond` as access-path choice and the
	/// pipeline's filter read it: row-independent record-path traversals
	/// folded to literals and the condition simplified again, where that is
	/// sound (see `record_fold` and [`super::util::fold_expressions`]).
	/// `versioned` says the pipeline has a `VERSION` of its own;
	/// `predicate_tables` are the tables whose rows the predicate is
	/// evaluated against.
	pub(crate) async fn fold_pipeline_cond(
		&self,
		cond: Option<LogicalExpr>,
		versioned: bool,
		fields: Option<&LogicalFields>,
		sources: &[LogicalOp],
		node: Option<&LogicalOp>,
		predicate_tables: &[TableName],
	) -> Option<LogicalExpr> {
		let ns_db = self.ns_db_ids().await;

		// A fold that discards a subtree must not discard a field read that
		// would have fetched a record or evaluated something on one, so the
		// folder is told what these sources can put in a row and what reading
		// a row of them runs.
		let has_table_source = !predicate_tables.is_empty();
		// A node the walk stopped at has a table's rows only as a chain of
		// `PermissionFilter` and layer nodes over that table; any other node's
		// rows are unknown here, so no field read off them is inert.
		let node_is_inert = node.is_none_or(is_layer_chain_over_table);
		let restricted_fields = if !source_field_reads_are_inert(sources) || !node_is_inert {
			None
		} else if versioned || self.version.is_some() {
			// A versioned read resolves its stamp at evaluation, so the
			// definitions that will apply are not known here.
			(!has_table_source).then(HashSet::new)
		} else {
			match (self.txn(), ns_db) {
				(Some(txn), Some((ns_id, db_id))) => {
					let check_perms = match (self.ns.as_deref(), self.db.as_deref()) {
						(Some(ns), Some(db)) => self.should_check_perms_for_view(ns, db),
						// Without names to check against, assume enforcement.
						_ => true,
					};
					let mut restricted = HashSet::new();
					let mut resolved = true;
					for name in predicate_tables {
						match table_read_restricted_fields(
							txn,
							ns_id,
							db_id,
							name,
							None,
							check_perms,
						)
						.await
						{
							Some(names) => restricted.extend(names),
							None => {
								resolved = false;
								break;
							}
						}
					}
					resolved.then_some(restricted)
				}
				// Without catalog access a table's field definitions are unknown.
				_ => (!has_table_source).then(HashSet::new),
			}
		};

		// Fold row-independent record-path traversals (`(scan:one).field`
		// after param substitution) to literals so index analysis can match
		// them. Versioned reads are excluded — a statement-level VERSION or
		// an enclosing version context both disqualify, because the plan-time
		// walk reads current state, not the versioned snapshot. Pipelines
		// with write side effects anywhere (projections, sources, condition)
		// are excluded: a write can change a record between the plan-time
		// read and the per-row evaluation the fold replaces. See
		// `record_fold` for the runtime-parity contract.
		match cond {
			Some(mut c) => {
				if !versioned
					&& self.version.is_none()
					&& fields.is_none_or(LogicalFields::read_only)
					&& sources.iter().all(LogicalOp::read_only)
					&& node.is_none_or(LogicalOp::read_only)
					&& c.read_only()
				{
					self.fold_constant_record_paths(&mut c).await;
					// Substitution can leave an operand constant that was not
					// constant during the first pass, so simplify again under
					// the same rules. A literal operand is inert, which is what
					// lets the enclosing comparison collapse.
					let capabilities = self.ctx.get_capabilities();
					fold_expressions(
						&mut c,
						self.function_registry(),
						&capabilities,
						restricted_fields.as_ref(),
						false,
						self.folds_clock(),
					);
				}
				Some(c)
			}
			None => None,
		}
	}

	/// The record id a table source's predicate pins with `id = <record id>`
	/// in its top-level `AND` chain, read by a point lookup in place of any
	/// scan; `None` when the predicate pins none, or holds a KNN operator,
	/// whose access path only [`Self::decide_access_path`] resolves.
	pub(crate) fn record_point_lookup(
		&self,
		table_name: &TableName,
		cond: Option<&LogicalExpr>,
	) -> Option<LogicalExpr> {
		if cond.is_some_and(has_knn_operator) {
			return None;
		}
		// Projection-function field references like `type::field("id")`
		// are normalized to plain paths, as the index analyzer normalizes
		// them, on a clone: the residual predicate keeps the original.
		let mut normalized = cond?.clone();
		resolve_projection_field_idioms(&mut normalized, self.function_registry());
		extract_record_id_point_lookup(&normalized, table_name)
	}

	/// `cond` for a read of `table_name` at the stamp this planner reads at:
	/// each approximate KNN operator whose field an ANN index declared at the
	/// stamp covers becomes the brute-force operator over that index's
	/// distance, so the nearest rows are ranked among the records read at the
	/// stamp rather than listed by an index that describes the present.
	///
	/// `cond` is returned as it is under any other version, or where the
	/// catalog cannot be read.
	pub(crate) async fn exact_knn_at_stamp(
		&self,
		table_name: &TableName,
		mut cond: LogicalExpr,
	) -> LogicalExpr {
		let PlanTimeVersion::At(stamp) = self.plan_time_version else {
			return cond;
		};
		if !has_knn_operator(&cond) {
			return cond;
		}
		let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db) else {
			return cond;
		};
		let Ok(Some(ns_def)) = txn.get_ns_by_name(ns, None).await else {
			return cond;
		};
		let Ok(Some(db_def)) = txn.get_db_by_name(ns, db, None).await else {
			return cond;
		};
		let Ok(indexes) = txn
			.all_tb_indexes(ns_def.namespace_id, db_def.database_id, table_name, Some(stamp))
			.await
		else {
			return cond;
		};
		let distance_of = |field: &LogicalExpr| {
			indexes.iter().find_map(|index| {
				let distance = match &index.index {
					Index::Hnsw(params) => &params.distance,
					Index::DiskAnn(params) => &params.distance,
					_ => return None,
				};
				let column = index.cols.first()?;
				(!index.prepare_remove && super::util::idiom_matches_column(field, column))
					.then(|| distance.clone())
			})
		};
		exact_knn_by(&mut cond, &distance_of);
		cond
	}

	/// The access path for a table source at plan time, with the predicate
	/// `cond` and ordering `order` offered to it: the decision, which builds
	/// nothing. `None` when the planner has no transaction, namespace or
	/// database, or the catalog cannot answer; the source is then resolved
	/// when it runs.
	///
	/// SECURITY (value-ordering oracle): an ordering touching a field whose
	/// `SELECT` permission is not `Full` for the current actor is withheld
	/// from the choice ([`AccessDecision::order_withheld`]). Otherwise the
	/// choice could walk an index over the restricted field in true value
	/// order; the value is later reduced to `NULL`, but the row order,
	/// preserved through the stable sort above, would reveal the hidden
	/// values' relative order across other users' records. Withholding keeps
	/// the source in an order the field does not decide and leaves the sort
	/// to run over the reduced keys.
	///
	/// Withholding says which access path may be chosen, not that the
	/// statement needs no ordering: a caller reads an absent ordering as
	/// licence to truncate at the source, which beneath a withheld ordering
	/// would hand the sort an already-truncated prefix. The runtime fallback
	/// (`operators/scan/dynamic.rs`) applies the same guard when it runs,
	/// against the actor's resolved field permissions.
	pub(crate) async fn decide_access_path(
		&self,
		table_name: &TableName,
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		with: Option<&With>,
		has_query_limit: bool,
		versioned: bool,
	) -> Option<AccessDecision> {
		let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db) else {
			return None;
		};
		let restricted_select = self.resolve_restricted_select_prefixes(table_name).await;
		let (order, order_withheld) = match order {
			Some(o) if restricted_select.order_touches(o) => (None, true),
			other => (other, false),
		};
		let (path, direction) = self
			.resolve_access_path(
				txn,
				ns,
				db,
				table_name,
				cond,
				order,
				with,
				has_query_limit,
				versioned,
				&restricted_select,
			)
			.await
			.ok()??;
		Some(AccessDecision {
			path,
			direction,
			order_withheld,
			restricted_select,
		})
	}

	/// Resolve the optimal access path for a table at plan time.
	///
	/// Performs index analysis using the WHERE condition and ORDER BY clause.
	/// Returns the selected `AccessPath` and scan direction, or `None` if
	/// the namespace/database cannot be resolved.
	#[allow(clippy::too_many_arguments)]
	async fn resolve_access_path(
		&self,
		txn: &Transaction,
		ns_name: &str,
		db_name: &str,
		table_name: &TableName,
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		with: Option<&With>,
		has_limit: bool,
		has_version: bool,
		restricted_select: &RestrictedPrefixes,
	) -> Result<Option<(AccessPath, Direction)>, Error> {
		let direction = determine_scan_direction(order);

		// If the entire WHERE clause folded to `false` (e.g. `field IN []`
		// short-circuited by `fold_condition_expressions`) the SELECT can
		// produce no rows. Skip index lookup entirely.
		if matches!(cond, Some(LogicalExpr::Literal(LogicalLiteral::Bool(false)))) {
			return Ok(Some((AccessPath::EmptyScan, direction)));
		}

		if matches!(with, Some(With::NoIndex)) {
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		// SECURITY: `AssumeRestricted` records that plan-time permission context
		// was unavailable, not that a restricted field exists. Resolving here
		// would withhold every index on the table and pin a table scan, which
		// also turns a `MATCHES` on a fully readable column into
		// `NoIndexFoundForMatch` because no full-text index can be named.
		// Decline the plan-time path instead, exactly as the catalog lookups
		// below do when they cannot resolve: the `DynamicScan` fallback applies
		// the same guard at execute time against the actor's resolved field
		// permissions, so it is fail-closed without over-applying.
		if matches!(restricted_select, RestrictedPrefixes::AssumeRestricted) {
			return Ok(None);
		}

		// Look up namespace and database to get IDs
		let ns_def = match txn.get_ns_by_name(ns_name, None).await {
			Ok(Some(ns)) => ns,
			_ => return Ok(None),
		};
		let db_def = match txn.get_db_by_name(ns_name, db_name, None).await {
			Ok(Some(db)) => db,
			_ => return Ok(None),
		};

		// Fetch queryable indexes for the table. Building or errored durable
		// indexes stay in the catalog for write admission, but the planner must
		// ignore them until the durable phase is `Online`.
		//
		// A read under a `VERSION` that names a stamp chooses only among the
		// indexes the table declared at that stamp and still declares: an
		// index defined since holds no entries for it, and one dropped since
		// is not one to read. Of those, none is chosen; see below.
		let indexes = match txn
			.all_tb_indexes(ns_def.namespace_id, db_def.database_id, table_name, None)
			.await
		{
			Ok(idx) => idx,
			Err(_) => return Ok(None),
		};
		let indexes = match self.plan_time_version {
			PlanTimeVersion::At(stamp) => {
				let then = match txn
					.all_tb_indexes(
						ns_def.namespace_id,
						db_def.database_id,
						table_name,
						Some(stamp),
					)
					.await
				{
					Ok(then) => then,
					Err(_) => return Ok(None),
				};
				let kept: Vec<_> = indexes
					.iter()
					.filter(|now| then.iter().any(|was| was.index_id == now.index_id))
					.cloned()
					.collect();
				Arc::from(kept)
			}
			PlanTimeVersion::Current | PlanTimeVersion::Unknown => indexes,
		};
		// An index entry carries no version stamp: it holds the state the last
		// write left, so a path answered from one answers for now. That covers
		// a key lookup or range, the order an index supplies, a limit cut in
		// an index scan and an ANN search alike, so a versioned read chooses
		// none of them and scans the table at its stamp. A `MATCHES` has no
		// evaluation but its full-text index, so a full-text index stays a
		// candidate: the records it names are read at the stamp, and the match
		// itself is the index's current answer.
		let indexes = if self.reads_versioned(has_version) {
			indexes.iter().filter(|ix| matches!(ix.index, Index::FullText(_))).cloned().collect()
		} else {
			indexes
		};
		let indexes = match filter_online_indexes(
			txn,
			ns_def.namespace_id,
			db_def.database_id,
			indexes,
		)
		.await
		{
			Ok(idx) => idx,
			Err(_) => return Ok(None),
		};

		if indexes.is_empty() {
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		// Rewrite projection function calls (e.g. type::field("name")) →
		// path in a cloned condition so the index analyzer can match
		// against indexed columns.
		let rewritten_cond = cond.map(|c| {
			let mut c = c.clone();
			resolve_projection_field_idioms(&mut c, self.function_registry());
			c
		});
		let analysis_cond = rewritten_cond.as_ref();

		// SECURITY: an index whose columns carry a non-`Full` SELECT permission
		// is withheld from access-path selection entirely. Every scan built
		// from a candidate answers its predicate from index entries and drops
		// the covered condition, so the document-level reduction never gets to
		// hide the value the decision was made from — the full-text, compound
		// B-tree and HNSW paths all become oracles over a field the actor
		// cannot read. Withholding the index leaves the predicate to be
		// answered from the reduced document instead.
		let indexes = drop_restricted_indexes(indexes, restricted_select);
		if indexes.is_empty() {
			return Ok(Some((AccessPath::TableScan, direction)));
		}
		let Some(columns) = self
			.with_array_columns_as_elements(
				txn,
				db_def.namespace_id,
				db_def.database_id,
				table_name,
				indexes,
			)
			.await
		else {
			return Ok(None);
		};

		let analyzer = columns.analyzer(with);
		let candidates = analyzer.analyze(analysis_cond, order);

		// Bitmap candidate fusion: when the WHERE clause is an AND of several
		// index-backed predicates, intersect per-branch candidate bitmaps
		// over the table's shared doc-ID space instead of driving from one
		// index and filtering the rest per row. Chosen conservatively — the
		// streaming plans keep every case where they have a structural
		// advantage:
		// - an index covers ORDER BY → keep sort elimination / sorted-merge early termination;
		// - LIMIT without ORDER BY → any streaming plan terminates early, a bitmap plan
		//   materializes every branch first;
		// - VERSION queries → doc-ID mappings are not time-travel-aware.
		let order_covered = candidates.iter().any(|c| c.covers_order);
		if with.is_none()
			&& !has_version
			&& !order_covered
			&& (order.is_some() || !has_limit)
			&& let Some(root) = self
				.try_bitmap_fusion_plan(
					txn,
					ns_def.namespace_id,
					db_def.database_id,
					table_name,
					&analyzer,
					analysis_cond,
					&candidates,
				)
				.await
		{
			return Ok(Some((
				AccessPath::BitmapFusion {
					root,
					fallback: None,
				},
				direction,
			)));
		}

		// Top-level OR fusion (issue #550): union the per-branch candidate
		// bitmaps over the shared doc-ID space instead of k-way-merging
		// RecordId streams, whose dedupe set grows with the result size.
		// Gated like the AND fusion above: LIMIT without ORDER BY keeps
		// streaming early termination (a bitmap plan materializes every
		// branch first), an index-covered ORDER BY keeps the union scan's
		// sorted-merge and sort-elimination plans, and ORDER BY id keeps
		// the union's merge-by-id. An ordering nothing covers installs a
		// blocking Sort above either plan, so fusion is allowed there even
		// under LIMIT.
		if with.is_none()
			&& !has_version
			&& ((order.is_none() && !has_limit)
				|| (order.is_some() && !order_covered && !order_is_scan_compatible(order)))
			&& let Some((root, fallback)) =
				analyzer.try_bitmap_union_fusion(analysis_cond, direction)
		{
			// `order_covered` only sees whole-condition candidates, which for
			// an OR root never include the branches' index accesses — so it
			// misses the union's composite k-way merge: when every branch
			// pins the same composite-index prefix and the ordering is that
			// index's next column, the union satisfies the ORDER BY with no
			// Sort. Keep the streaming union for that shape.
			let union_covers_order = matches!(
				&fallback,
				AccessPath::Union {
					paths,
					..
				} if detect_order_for_composite_union(order, paths).is_some()
			);
			if !union_covers_order {
				return Ok(Some((
					AccessPath::BitmapFusion {
						root,
						fallback: Some(Box::new(fallback)),
					},
					direction,
				)));
			}
		}

		if candidates.is_empty() {
			if let Some(path) = analyzer.try_or_union(analysis_cond, direction) {
				return Ok(Some((path, direction)));
			}
			// A nested OR conjunct can be the only indexable part of an AND
			// when every sibling conjunct is unindexed (e.g. an unindexed
			// `category = $c AND (title @1@ $q OR body @2@ $q)`). `analyze`
			// ignores OR subtrees, so there is no single-index candidate and
			// we reach here before the score-gated check further below. With
			// no single-index driver the union always beats the table-scan
			// fallback, so take it unconditionally.
			if let Some((path, _)) = analyzer.try_and_nested_or_union(analysis_cond, direction) {
				return Ok(Some((path, direction)));
			}
			// Try expanding IN operators into union of equality lookups
			if let Some(path) = analyzer.try_in_expansion(analysis_cond, direction) {
				return Ok(Some((path, direction)));
			}
			// Try expanding CONTAINSALL/CONTAINSANY into union of equality lookups
			if let Some(path) = analyzer.try_containment_expansion(analysis_cond, direction) {
				return Ok(Some((path, direction)));
			}
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		// Rank of the best single-index driver, captured before
		// `select_access_path_paged` consumes `candidates`. Used below to
		// decide whether a nested-OR multi-index union would be a better
		// driver.
		let best_single_score = candidates.iter().map(|c| c.rank(has_limit)).max().unwrap_or(0);

		let path = select_access_path_paged(candidates, with, direction, has_limit);

		// When the chosen index covers ORDER BY, derive the correct scan
		// direction from the ORDER BY clause rather than the default
		// `determine_scan_direction` (which only handles ORDER BY id).
		// This enables LIMIT pushdown and sort elimination for queries like
		// `ORDER BY metadata.payload_metadata.modified DESC LIMIT 25`.
		let (mut path, direction) = adjust_direction_for_order(path, order, direction);

		// Pre-filtered vector search: evaluate the KNN query's exactly
		// index-coverable conjuncts into an allow-list bitmap before the ANN
		// search, instead of fetching every visited candidate's record.
		// Gated like bitmap fusion: no WITH hints (the user pinned the plan)
		// and no VERSION (doc-ID mappings are not time-travel-aware).
		if let AccessPath::KnnSearch {
			prefilter,
			..
		} = &mut path
			&& with.is_none()
			&& !has_version
			&& *surrealdb_cnf::KNN_PREFILTER_ENABLED
		{
			*prefilter = self
				.try_knn_prefilter_plan(
					txn,
					ns_def.namespace_id,
					db_def.database_id,
					table_name,
					&analyzer,
					analysis_cond,
				)
				.await;
		}

		// When the best single-index path is a full-range scan (ORDER BY
		// only, no WHERE selectivity), also try a multi-index union for
		// OR conditions. The union reads only matching rows from each
		// branch, which is typically far better than scanning every row
		// in the index. The outer pipeline adds a Sort when the union
		// does not satisfy ORDER BY.
		if path.is_full_range_scan()
			&& let Some(union_path) = analyzer.try_or_union(analysis_cond, direction)
		{
			return Ok(Some((union_path, direction)));
		}
		// Same logic for containment expansion (CONTAINSANY / ANYINSIDE
		// against an array-element index): the full-range scan would
		// walk every indexed entry — typically one per array element
		// per row — and filter post-hoc. A Union of per-value Compound
		// prefix scans reads only the matching prefix ranges. The k-way
		// merge with `MergeMode::ByIndexKeyDedup` preserves the trailing
		// ORDER BY column's sort order and dedupes rows that match
		// multiple branches.
		if path.is_full_range_scan()
			&& let Some(union_path) = analyzer.try_containment_expansion(analysis_cond, direction)
		{
			return Ok(Some((union_path, direction)));
		}
		// `try_in_expansion` is deliberately not tried here. The full-range
		// scan covers ORDER BY, enabling sort elimination and early
		// termination with the batch ceiling. Replacing it with a Union of
		// prefix scans would require an expensive Sort of ALL matching
		// records, which is far worse for ORDER BY + LIMIT queries. IN
		// expansion is only helpful in the candidates.is_empty() fallback
		// above when no index covers ORDER BY at all.

		// When the WHERE clause is a conjunction and one conjunct is a pure
		// OR whose every branch is independently indexable, consider driving
		// from that OR's multi-index union and applying the remaining
		// conjuncts as a residual filter. Without this, a nested OR of
		// full-text matches like `type = $t AND (title @1@ $q OR body @2@ $q)`
		// drives from the low-selectivity `type` equality and evaluates the
		// OR as a per-row filter — re-scoring the FT index for every `type`
		// row. Switch only when the union's weakest branch is more selective
		// than the chosen single-index driver (higher score); this keeps a
		// selective unique/compound equality (or an ORDER BY-covering scan,
		// which carries the +100 order bonus) as the driver.
		if with.is_none()
			&& let Some((union_path, union_score)) =
				analyzer.try_and_nested_or_union(analysis_cond, direction)
			&& union_score > best_single_score
		{
			return Ok(Some((union_path, direction)));
		}

		Ok(Some((path, direction)))
	}

	/// Attempt to build the bitmap fusion plan for `resolve_access_path`.
	///
	/// Resolves the table's declared field kinds so `NOT` subtraction can be
	/// restricted to columns that provably never hold arrays (see
	/// [`IndexAnalyzer::bitmap_exact_plan`]); the structural work is delegated
	/// to [`IndexAnalyzer::try_bitmap_fusion`]. Any catalog error simply
	/// disables the fusion — this is an optimization, never a correctness
	/// gate.
	#[allow(clippy::too_many_arguments)]
	async fn try_bitmap_fusion_plan(
		&self,
		txn: &Transaction,
		ns_id: crate::catalog::NamespaceId,
		db_id: crate::catalog::DatabaseId,
		table_name: &TableName,
		analyzer: &IndexAnalyzer<'_>,
		cond: Option<&LogicalExpr>,
		candidates: &[crate::exec::index::analysis::IndexCandidate],
	) -> Option<BitmapPlan> {
		// Cheap structural pre-check before touching the catalog: fusion
		// needs a conjunction and at least one NOT for the field-kind lookup
		// to matter — but `try_bitmap_fusion` re-checks everything, so only
		// skip the obviously-impossible case here.
		let cond = cond?;
		if !matches!(
			cond,
			LogicalExpr::Binary {
				op: crate::expr::BinaryOperator::And,
				..
			}
		) {
			return None;
		}
		// Field kinds gate NOT subtraction exactness. Fetch lazily: only
		// when the WHERE clause actually contains a negated conjunct.
		let has_not = {
			let mut found = false;
			let mut stack = vec![cond];
			while let Some(e) = stack.pop() {
				match e {
					LogicalExpr::Binary {
						left,
						op: crate::expr::BinaryOperator::And,
						right,
					} => {
						stack.push(left);
						stack.push(right);
					}
					e if IndexAnalyzer::as_negated_expr(e).is_some() => {
						found = true;
						break;
					}
					_ => {}
				}
			}
			found
		};
		let fields = if has_not {
			txn.all_tb_fields(ns_id, db_id, table_name, None).await.ok()
		} else {
			None
		};
		let exact_col = |col: &Idiom| -> bool {
			let Some(fields) = &fields else {
				return false;
			};
			fields.iter().find(|fd| &fd.name == col).is_some_and(field_def_excludes_arrays)
		};
		analyzer.try_bitmap_fusion(Some(cond), candidates, &exact_col)
	}

	/// Attempt to build the pre-filter plan for a KNN access path.
	///
	/// The KNN-stripped WHERE is split per conjunct: conjuncts whose truth
	/// set is exactly a candidate bitmap join the allow-list plan, the rest
	/// stay as the true residual (see [`IndexAnalyzer::try_knn_prefilter`]).
	/// The table's declared field kinds are resolved for the array-free
	/// exactness gate — unlike [`Self::try_bitmap_fusion_plan`]'s lazy fetch,
	/// every b-tree leaf needs the gate here, not only NOT subtractions. Any
	/// catalog error simply disables the optimization — this is never a
	/// correctness gate.
	async fn try_knn_prefilter_plan(
		&self,
		txn: &Transaction,
		ns_id: crate::catalog::NamespaceId,
		db_id: crate::catalog::DatabaseId,
		table_name: &TableName,
		analyzer: &IndexAnalyzer<'_>,
		cond: Option<&LogicalExpr>,
	) -> Option<KnnPrefilterPlan> {
		let stripped = cond.and_then(strip_knn_from_condition)?;
		let fields = txn.all_tb_fields(ns_id, db_id, table_name, None).await.ok()?;
		let exact_col = |col: &Idiom| -> bool {
			fields
				.iter()
				.find(|fd| &fd.name == col)
				.and_then(|fd| fd.field_kind.as_ref())
				.is_some_and(crate::exec::index::access_path::field_kind_excludes_arrays)
		};
		analyzer.try_knn_prefilter(&stripped, &exact_col)
	}
}

/// Parse a field path given as text (an `OMIT`/`FETCH` parameter, a
/// projection function argument) into a logical path. `None` when the text
/// is not a path.
fn parse_field_path(text: &str) -> Option<LogicalExpr> {
	let idiom = crate::syn::idiom(text).ok()?;
	let path = lower_expr(crate::expr::Expr::Idiom(idiom)).ok()?;
	path.is_path().then_some(path)
}

/// Collects the root field names a pipeline reads from its rows, and detects
/// the expressions that make that set unknowable.
///
/// Path roots determine whose row a field access belongs to, via
/// [`classify_path_root`]:
///
/// - `ThisRow` (a field, a leading lookup, `$this.x`, `$self.x`): the leading field read is a
///   column of the current row's table — add it to the needed set.
/// - `OuterRow` (`$parent.x`): the field reads belong to the outer row, NOT the current table. Skip
///   them; only walk nested expressions (which may reference `$this` legitimately).
/// - `Opaque` (a path from a computed value or another parameter): mark `has_opaque` and bail to
///   "all fields needed."
///
/// A parameter anywhere is opaque: it could hold any field. A nested
/// statement's own expressions are walked as part of this pipeline's, since
/// they are evaluated against rows this pipeline produces.
struct NeededFieldExtractor {
	fields: HashSet<String>,
	has_opaque: bool,
	/// Depth of `[WHERE …]` predicates currently being walked.
	///
	/// At depth 0 (top-level) the row scope follows the natural SELECT
	/// semantics: `$this` is the current row, `$parent` is the outer
	/// SELECT's row.
	///
	/// At depth 1 (inside one `[WHERE …]`) the runtime rebinds scope:
	/// `current_value` is the iteration element and `$parent` is bound to
	/// the enclosing `document_root`. So `.x` and `$this.x` reference the
	/// iteration element (NOT this row's columns), while `$parent.x`
	/// references the current row.
	///
	/// At depth ≥ 2 the chain is not modelled and the walk conservatively
	/// falls back to "all fields needed" via `has_opaque`.
	filter_depth: u32,
	/// What a KNN operator reads from a row.
	knn: KnnRowReads,
}

/// Whether an expression of the clauses reads the row other than by a
/// field's name ([`reads_row_opaquely`]).
fn clauses_read_row_opaquely(
	fields: Option<&LogicalFields>,
	omit: &[LogicalExpr],
	cond: Option<&LogicalExpr>,
	order: Option<&LogicalOrdering>,
	group: Option<&[LogicalExpr]>,
	split: Option<&[LogicalExpr]>,
	registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	let projected = match fields {
		Some(LogicalFields::Value {
			expr,
			..
		}) => reads_row_opaquely(expr, registry),
		Some(LogicalFields::Select(list)) => list.iter().any(|field| match field {
			LogicalField::Single {
				expr,
				..
			} => reads_row_opaquely(expr, registry),
			LogicalField::All => false,
		}),
		None => false,
	};
	let ordered = || match order {
		Some(LogicalOrdering::Order(list)) => {
			list.iter().any(|o| reads_row_opaquely(&o.value, registry))
		}
		Some(LogicalOrdering::Random) | None => false,
	};
	projected
		|| omit.iter().any(|expr| reads_row_opaquely(expr, registry))
		|| cond.is_some_and(|expr| reads_row_opaquely(expr, registry))
		|| ordered()
		|| group.into_iter().flatten().any(|expr| reads_row_opaquely(expr, registry))
		|| split.into_iter().flatten().any(|expr| reads_row_opaquely(expr, registry))
}

/// What a KNN operator (`<|k, …|>`) reads from the rows it is evaluated
/// against.
// Constructed or read only by the operator-tree oracle, which test builds compile.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KnnRowReads {
	/// Its operands: the brute-force source scores the vector field of every
	/// row.
	Operands,
	/// Only the row's `id`. An index-backed KNN source ranks the candidates,
	/// and the operator is evaluated as membership of the row's id in that
	/// result (`KnnMembershipOp`), without evaluating either operand.
	RecordId,
}

impl NeededFieldExtractor {
	/// Walk a path: decide from its root whether its leading field read is a
	/// column of this row, then walk its steps in the order they apply.
	fn visit_path(&mut self, path: &LogicalExpr) {
		let mut take_leading = match (self.filter_depth, classify_path_root(path)) {
			// Top-level, rooted at the current row: the first field read is
			// a column name. The `$this`/`$self` anchor itself is structural
			// and is not visited — as a parameter it would read as opaque
			// and lose the selective scan entirely.
			(0, PathRoot::ThisRow) => true,
			// `$parent.X.Y…` at the top level: the fields belong to the outer
			// row, not this row's table. Only the nested expressions (filter
			// predicates, method arguments) may still reference this row.
			(0, PathRoot::OuterRow) => false,
			// Inside one filter predicate the scope flips: `$parent` is
			// rebound to the enclosing row, so `X` is a column of *this*
			// row's table.
			(1, PathRoot::OuterRow) => true,
			// `.X` or `$this.X` inside the filter reads the iteration
			// element, not this row's columns.
			(1, PathRoot::ThisRow) => false,
			// An opaque root, or any path at depth ≥ 2 (nested filters whose
			// scope chain is not modelled). The `2..` lower bound on the
			// catch-all means a new `PathRoot` variant added later trips the
			// exhaustiveness checker instead of silently short-circuiting.
			(0 | 1, PathRoot::Opaque) | (2.., _) => {
				self.has_opaque = true;
				return;
			}
		};
		self.visit_steps(path, &mut take_leading);
	}

	/// Walk the steps of `path` from its root outwards, visiting the
	/// expressions they hold. `take_leading` is whether the next field read
	/// met names a column of this row; it is cleared once that field is
	/// taken. A root that is not a path form (the expression a path starts
	/// from) is not visited: whose fields it reads was decided by the root
	/// classification.
	fn visit_steps(&mut self, path: &LogicalExpr, take_leading: &mut bool) {
		match path {
			LogicalExpr::Field(name) => self.take_field(name, take_leading),
			LogicalExpr::Doc | LogicalExpr::Current => {}
			LogicalExpr::Access {
				base,
				field,
			} => {
				self.visit_steps(base, take_leading);
				self.take_field(field, take_leading);
			}
			LogicalExpr::Each(base)
			| LogicalExpr::First(base)
			| LogicalExpr::Last(base)
			| LogicalExpr::RepeatRecurse(base) => self.visit_steps(base, take_leading),
			LogicalExpr::Flatten {
				base,
				..
			} => self.visit_steps(base, take_leading),
			LogicalExpr::Index {
				base,
				index,
			} => {
				self.visit_steps(base, take_leading);
				self.visit_expr(index);
			}
			LogicalExpr::Filter {
				base,
				predicate,
			} => {
				self.visit_steps(base, take_leading);
				self.filter_depth = self.filter_depth.saturating_add(1);
				self.visit_expr(predicate);
				self.filter_depth = self.filter_depth.saturating_sub(1);
			}
			LogicalExpr::Method {
				base,
				args,
				..
			} => {
				self.visit_steps(base, take_leading);
				args.iter().for_each(|a| self.visit_expr(a));
			}
			LogicalExpr::Destructure {
				base,
				fields,
			} => {
				self.visit_steps(base, take_leading);
				self.visit_destructure(fields);
			}
			LogicalExpr::Lookup {
				base,
				..
			} => {
				self.visit_steps(base, take_leading);
				// A lookup's clauses are evaluated against the rows it reaches,
				// but its subjects may hold expressions of this row (a key
				// range bound) and its predicate may reference `$parent`. The
				// first child is the base, walked above.
				for clause in path.children().into_iter().skip(1) {
					self.visit_expr(clause);
				}
			}
			LogicalExpr::Recurse(recurse) => {
				self.visit_steps(&recurse.base, take_leading);
				match &recurse.path {
					// A written body is applied to each depth's value.
					Some(LogicalRecursePath::Nested(body)) => self.visit_body(body),
					// The rest of the path continues the sequence of steps.
					Some(LogicalRecursePath::Trailing(body)) => {
						self.visit_steps(body, take_leading)
					}
					None => {}
				}
				if let Some(LogicalRecurseInstruction::Shortest {
					expects,
					..
				}) = &recurse.instruction
				{
					self.visit_expr(expects);
				}
			}
			LogicalExpr::Optional {
				base,
				then,
			} => {
				self.visit_steps(base, take_leading);
				self.visit_steps(then, take_leading);
			}
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => {
				self.visit_steps(base, take_leading);
				self.visit_steps(body, take_leading);
			}
			// The expression a path starts from.
			_ => {}
		}
	}

	/// Record a field read as a column of this row when it is the path's
	/// leading one.
	fn take_field(&mut self, name: &crate::val::Strand, take_leading: &mut bool) {
		if *take_leading {
			self.fields.insert(name.as_str().to_owned());
			*take_leading = false;
		}
	}

	/// A body applied to a value other than the row (a destructure alias, a
	/// written recursion body): its field reads are positions inside that
	/// value, not columns of this row, so only the expressions its steps hold
	/// are walked. A body starting from an expression is walked from that
	/// expression.
	fn visit_body(&mut self, body: &LogicalExpr) {
		let root = body.path_root();
		if !matches!(root, LogicalExpr::Current | LogicalExpr::Field(_) | LogicalExpr::Doc) {
			self.visit_expr(root);
		}
		self.visit_steps(body, &mut false);
	}

	fn visit_destructure(&mut self, fields: &[LogicalDestructureField]) {
		for field in fields {
			match field {
				LogicalDestructureField::All(_)
				| LogicalDestructureField::Field(_)
				| LogicalDestructureField::Rest => {}
				LogicalDestructureField::Aliased(_, body) => self.visit_body(body),
				LogicalDestructureField::Destructure(_, nested) => self.visit_destructure(nested),
			}
		}
	}

	fn visit_expr(&mut self, expr: &LogicalExpr) {
		match expr {
			// Parameters could reference any field
			LogicalExpr::Param(_) => self.has_opaque = true,
			LogicalExpr::Binary {
				op: crate::expr::operator::BinaryOperator::NearestNeighbor(_),
				..
			} if self.knn == KnnRowReads::RecordId => {
				self.fields.insert("id".to_owned());
			}
			path if path.is_path() => self.visit_path(path),
			// A nested statement's expressions are evaluated with this row in
			// scope; walk them at the current depth.
			LogicalExpr::Subquery(op) => {
				let _ = op.visit_scalars_deep(&mut |e| {
					self.visit_expr(e);
					Ok::<(), std::convert::Infallible>(())
				});
			}
			_ => {
				for child in expr.children() {
					self.visit_expr(child);
				}
			}
		}
	}
}

/// Convert a plan-level [`BitmapPlan`] tree into the executable
/// [`BitmapNode`] operator tree.
pub(crate) fn bitmap_plan_to_node(plan: BitmapPlan) -> Arc<BitmapNode> {
	match plan {
		BitmapPlan::BTree {
			index_ref,
			access,
		} => BitmapNode::btree(index_ref, access),
		BitmapPlan::FullText {
			index_ref,
			query,
			operator,
		} => BitmapNode::fulltext(index_ref, query, operator),
		BitmapPlan::Graph {
			source,
			direction,
			edge_tables,
		} => BitmapNode::graph(source, direction, edge_tables),
		BitmapPlan::And(children) => {
			BitmapNode::and(children.into_iter().map(bitmap_plan_to_node).collect())
		}
		BitmapPlan::Or(children) => {
			BitmapNode::or(children.into_iter().map(bitmap_plan_to_node).collect())
		}
		BitmapPlan::AndNot {
			base,
			subtract,
		} => BitmapNode::and_not(bitmap_plan_to_node(*base), bitmap_plan_to_node(*subtract)),
	}
}

/// Like [`crate::exec::index::access_path::field_kind_excludes_arrays`], but starting from a field
/// definition's declared kind. A definition with no declared `TYPE` is
/// conservatively not exact.
pub(crate) fn field_def_excludes_arrays(fd: &crate::catalog::FieldDefinition) -> bool {
	let Some(kind) = fd.field_kind.as_ref() else {
		return false;
	};
	crate::exec::index::access_path::field_kind_excludes_arrays(kind)
}

/// Removes every index whose columns are governed by a non-`Full` SELECT
/// permission for the current actor.
///
/// See [`RestrictedPrefixes::index_touches`] for why such an index must not
/// reach access-path selection. Returns the input untouched when nothing is
/// restricted, so the common case allocates nothing.
fn drop_restricted_indexes(
	indexes: Arc<[IndexDefinition]>,
	restricted_select: &RestrictedPrefixes,
) -> Arc<[IndexDefinition]> {
	if matches!(restricted_select, RestrictedPrefixes::None) {
		return indexes;
	}
	if !indexes.iter().any(|ix| restricted_select.index_touches(&ix.cols)) {
		return indexes;
	}
	indexes
		.iter()
		.filter(|ix| !restricted_select.index_touches(&ix.cols))
		.cloned()
		.collect::<Vec<_>>()
		.into()
}

/// The count fast path [`Planner::decide_count`] chose.
#[derive(Debug, Clone, PartialEq)]
#[expect(clippy::large_enum_variant, reason = "one decision is held per statement planned")]
pub(crate) enum CountDecision {
	/// Every record `source` names: a table, a record id or a record range.
	All {
		source: SourceRef,
		output: CountOutput,
	},
	/// The records of `table` that `cond` admits, counted from a `COUNT`
	/// index guarded by `cond` when the table has one, else by walking
	/// `btree_access`'s keys, else as `bitmap_plan`'s exact cardinality.
	Indexed {
		table: TableName,
		cond: LogicalExpr,
		output: CountOutput,
		btree_access: Option<(IndexRef, BTreeAccess)>,
		bitmap_plan: Option<BitmapPlan>,
	},
}

/// The access path [`Planner::decide_access_path`] chose for a table source.
// Constructed or read only by the operator-tree oracle, which test builds compile.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct AccessDecision {
	pub(crate) path: AccessPath,
	pub(crate) direction: Direction,
	/// Whether the ordering offered was withheld from the choice because it
	/// touches a restricted field: the path then says nothing about it, so
	/// it does not cover the ordering, and nothing may be truncated at the
	/// source beneath it.
	pub(crate) order_withheld: bool,
	/// The table's restricted fields, as the choice read them.
	pub(crate) restricted_select: RestrictedPrefixes,
}

/// Outcome of resolving which fields on a table carry a non-`Full` SELECT
/// permission for the current actor.
pub(crate) enum RestrictedPrefixes {
	/// Plan-time catalog/permission context is unavailable; callers must
	/// conservatively assume a restricted field could be referenced.
	AssumeRestricted,
	/// The actor sees the table with full field permissions, or no field is
	/// restricted — index fast paths are safe.
	None,
	/// These declared fields are governed by a non-`Full` SELECT
	/// permission; a read at, beneath or above one of them is restricted —
	/// reading `acl` reads the restricted elements `acl.*` holds.
	Some(Vec<crate::expr::logical::FieldPath>),
}

impl RestrictedPrefixes {
	/// Returns `true` when the WHERE `cond` references a field governed by a
	/// non-`Full` SELECT permission for the current actor (or when restriction
	/// must be conservatively assumed because plan-time context is missing).
	///
	/// Used to keep an index fast path from turning index entries into a
	/// membership oracle for a value the actor cannot read. A read touches a
	/// restricted field at, beneath or above it: a read of an ancestor
	/// reads the restricted descendant along with it, and an index over the
	/// ancestor holds the descendant's values as entries (`acl CONTAINS 'x'`
	/// over an index on `acl` seeks `acl.*` elements). A nested statement is
	/// not descended into: it operates against its own tables and performs
	/// its own permission resolution.
	fn cond_touches(&self, cond: &LogicalExpr) -> bool {
		let prefixes = match self {
			RestrictedPrefixes::AssumeRestricted => return true,
			RestrictedPrefixes::None => return false,
			RestrictedPrefixes::Some(p) => p,
		};
		/// `enclosing` is the position the enclosing path reads, when `expr`
		/// is part of that path: a base of `meta.public` reads `meta`, which
		/// is not a read of the whole `meta` and so of nothing beneath it.
		fn walk(
			expr: &LogicalExpr,
			prefixes: &[crate::expr::logical::FieldPath],
			enclosing: Option<&[crate::expr::logical::FieldSegment]>,
		) -> bool {
			if matches!(expr, LogicalExpr::Subquery(_)) {
				return false;
			}
			let read = declared_read_prefix(expr);
			if let Some(read) = read.as_ref() {
				let read = read.segments();
				let whole = !enclosing.is_some_and(|e| e.len() > read.len() && e.starts_with(read));
				if prefixes.iter().any(|p| {
					read.starts_with(p.segments()) || (whole && p.segments().starts_with(read))
				}) {
					return true;
				}
			}
			let enclosing = read.as_ref().map(|r| r.segments());
			expr.children().into_iter().any(|child| walk(child, prefixes, enclosing))
		}
		walk(cond, prefixes, None)
	}

	/// SECURITY: returns `true` when any column of an index is governed by a
	/// non-`Full` SELECT permission for the current actor (or when restriction
	/// must be conservatively assumed because plan-time context is missing).
	///
	/// An index scan answers a predicate from index entries rather than from
	/// the document, so the field-level reduction that hides the value never
	/// runs on the data the decision was made from. Whether a record is
	/// returned then reports something about a value the actor may not read:
	/// which records contain a guessed value or search term, or which
	/// restricted vectors lie nearest a supplied one. Callers drop such an
	/// index from the candidate set, so the predicate is answered from the
	/// reduced document instead.
	///
	/// An ancestor field definition governs the column too — reducing an
	/// ancestor implicitly reduces its descendants — and so does a restricted
	/// descendant, whose values a column over the ancestor stores: an index on
	/// `acl` holds the elements a restricted `acl.*` hides.
	fn index_touches(&self, cols: &[Idiom]) -> bool {
		let prefixes = match self {
			RestrictedPrefixes::AssumeRestricted => return true,
			RestrictedPrefixes::None => return false,
			RestrictedPrefixes::Some(p) => p,
		};
		cols.iter().any(|col| {
			// A column that is not a field path names no position a declared
			// field can be compared against, so it is taken as touching.
			crate::expr::logical::FieldPath::from_idiom(col).is_none_or(|col| {
				prefixes.iter().any(|prefix| paths_overlap(col.segments(), prefix.segments()))
			})
		})
	}

	/// SECURITY (value-ordering oracle): returns `true` when any top-level
	/// `ORDER BY` path references a field whose SELECT permission is not
	/// `Full` for the current actor (or when restriction must be conservatively
	/// assumed because plan-time context is missing).
	///
	/// An index scan walks the B-tree in the indexed field's true value order
	/// and emits records in that order, while field-level reduction nulls the
	/// restricted value in the projected output. A stable post-reduce `Sort`
	/// over the (now all-`NULL`) keys preserves the source order, so the row
	/// order would still encode the hidden values' relative ordering. Callers
	/// use this to withhold a restricted `ORDER BY` from access-path selection,
	/// keeping the source in record-id order so no ordering oracle is exposed.
	///
	/// The match is intentionally shallow: each `ORDER BY` key is a plain
	/// top-level path, matched by prefix. An index-ordering leak requires
	/// the index to cover the field directly, so a restricted field
	/// referenced only *inside* a path filter (e.g. `ORDER BY foo[WHERE code =
	/// …]`) is not an indexable ordering and cannot leak through this vector.
	/// This is the same shallow-match property as the WHERE guard
	/// ([`Self::cond_touches`]).
	///
	/// OUT OF SCOPE (parent/child nested-field gap): the prefix match catches
	/// ordering by a restricted field *or a descendant of it* (`ORDER BY meta`
	/// when `meta` is restricted, or `ORDER BY meta.sub` when `meta` is
	/// restricted), but NOT ordering by a *parent* of a restricted child
	/// (`ORDER BY meta` when only `meta.secret` is restricted). Ordering by
	/// the parent object can, with a covering index, still encode the child's
	/// relative ordering. This is a narrower, separate vector and is not
	/// addressed here.
	fn order_touches(&self, order: &LogicalOrdering) -> bool {
		// `ORDER BY RAND()` references no field and cannot leak ordering.
		let LogicalOrdering::Order(order_list) = order else {
			return false;
		};
		let prefixes = match self {
			RestrictedPrefixes::AssumeRestricted => return true,
			RestrictedPrefixes::None => return false,
			RestrictedPrefixes::Some(p) => p,
		};
		order_list
			.iter()
			.any(|order| prefixes.iter().any(|prefix| reads_at_or_beneath(&order.value, prefix)))
	}
}

/// Adjust the scan direction and access path when the chosen index covers
/// the ORDER BY clause.
///
/// `determine_scan_direction` only flips to `Backward` for `ORDER BY id DESC`.
/// When an index covers a non-`id` ORDER BY (e.g. a nested field like
/// `metadata.payload_metadata.modified DESC`), the correct direction is
/// derived from the ORDER BY clause so that:
///
/// 1. `index_covers_ordering()` succeeds → LIMIT is pushed to the IndexScan
/// 2. `can_eliminate_sort()` succeeds → the Sort operator is eliminated
///
/// Otherwise the index is scanned forward, LIMIT cannot be pushed
/// (direction mismatch), and all rows are read + sorted in memory.
fn adjust_direction_for_order(
	path: AccessPath,
	order: Option<&LogicalOrdering>,
	default_direction: Direction,
) -> (AccessPath, Direction) {
	use crate::exec::field_path::FieldPath;
	use crate::exec::field_path_convert::field_path_from_idiom as field_path_from_column;

	// Only adjust for BTreeScan paths that cover ORDER BY
	let AccessPath::BTreeScan {
		ref index_ref,
		ref access,
		..
	} = path
	else {
		return (path, default_direction);
	};

	// Need an ORDER BY clause to determine direction
	let Some(LogicalOrdering::Order(order_list)) = order else {
		return (path, default_direction);
	};

	let ix_def = index_ref.definition();

	// Collect equality-pinned column paths so ORDER BY fields that reference
	// them can be skipped (those columns have a single constant value, so
	// any direction trivially satisfies the requirement).
	let equality_col_paths: Vec<FieldPath> = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => ix_def
			.cols
			.iter()
			.take(prefix.len())
			.filter_map(|s| field_path_from_column(s).ok())
			.collect(),
		BTreeAccess::Equality(_) => {
			ix_def.cols.iter().filter_map(|s| field_path_from_column(s).ok()).collect()
		}
		_ => vec![],
	};

	// Skip leading ORDER BY fields that match equality-pinned columns.
	let mut order_idx = 0;
	for field in order_list.iter() {
		if let Ok(fp) = field_path_from_expr(&field.value)
			&& equality_col_paths.contains(&fp)
		{
			order_idx += 1;
			continue;
		}
		break;
	}

	// Get the first non-constant ORDER BY field
	let Some(first_order) = order_list.get(order_idx) else {
		// All ORDER BY fields are constant — direction doesn't matter,
		// keep the default.
		return (path, default_direction);
	};

	let Ok(order_path) = field_path_from_expr(&first_order.value) else {
		return (path, default_direction);
	};

	// Determine which index column to match against.
	// For compound access with an equality prefix, match the column
	// immediately after the prefix.  For Equality access on a
	// single-column index, all index columns are skipped.
	let target_col_index = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => prefix.len(),
		BTreeAccess::Equality(_) => ix_def.cols.len(),
		_ => 0,
	};

	// If all index columns are equality-pinned, the effective ordering
	// is by record ID.  Check if the ORDER BY field is `id`.
	if target_col_index >= ix_def.cols.len() {
		// All columns are equality-pinned.  Match `ORDER BY id`.
		if order_path == FieldPath::field("id") {
			let new_direction = if first_order.direction {
				Direction::Forward // ASC
			} else {
				Direction::Backward // DESC
			};
			let new_path = AccessPath::BTreeScan {
				index_ref: index_ref.clone(),
				access: access.clone(),
				direction: new_direction,
			};
			return (new_path, new_direction);
		}
		return (path, default_direction);
	}

	let Some(target_col) = ix_def.cols.get(target_col_index) else {
		return (path, default_direction);
	};

	let Ok(col_path) = field_path_from_column(target_col) else {
		return (path, default_direction);
	};

	// If the target column matches the ORDER BY field,
	// set the direction based on the ORDER BY direction
	if order_path == col_path {
		let new_direction = if first_order.direction {
			Direction::Forward // ASC
		} else {
			Direction::Backward // DESC
		};

		let new_path = AccessPath::BTreeScan {
			index_ref: index_ref.clone(),
			access: access.clone(),
			direction: new_direction,
		};

		(new_path, new_direction)
	} else {
		(path, default_direction)
	}
}

/// Collect output field names from a SELECT field list.
///
/// These names are passed as the `reserved_names` argument of
/// `ExpressionRegistry::with_reserved_and_protected_names` so that
/// synthetic internal names (`_e0`, `_e1`, ...) do not collide with fields
/// the user explicitly selected.
pub(super) fn collect_field_names(fields: &LogicalFields) -> Vec<String> {
	match fields {
		LogicalFields::Value {
			..
		} => vec![], // SELECT VALUE has no object fields
		LogicalFields::Select(field_list) => {
			let mut names = Vec::with_capacity(field_list.len());
			for field in field_list {
				if let LogicalField::Single {
					expr,
					alias,
					..
				} = field
				{
					let name = match alias {
						Some(alias) => alias.name(),
						None => expr.output_path().name(),
					};
					names.push(name);
				}
			}
			names
		}
	}
}

/// Collect simple source fields read by SELECT projections.
///
/// These names must not be used as Compute internal names, even when a
/// computed expression has the same alias. Compute runs before projection, so
/// using one of these names internally would overwrite the source value before
/// another projection can read it.
pub(super) fn collect_simple_source_field_names(fields: &LogicalFields) -> Vec<String> {
	match fields {
		LogicalFields::Value {
			expr,
			..
		} => simple_field_name(expr).into_iter().collect(),
		LogicalFields::Select(field_list) => field_list
			.iter()
			.filter_map(|field| match field {
				LogicalField::Single {
					expr,
					..
				} => simple_field_name(expr),
				LogicalField::All => None,
			})
			.collect(),
	}
}

/// The name of a bare field read, or `None` for any other expression.
fn simple_field_name(expr: &LogicalExpr) -> Option<String> {
	match expr {
		LogicalExpr::Field(name) => Some(name.as_str().to_owned()),
		_ => None,
	}
}

/// Whether the expression is exactly a read of the `id` field.
fn is_id_field(expr: &LogicalExpr) -> bool {
	matches!(expr, LogicalExpr::Field(name) if name.as_str() == "id")
}

/// How an index union over `paths` combines its branches for the pipeline's
/// ordering `order`: the branches, rewritten to scan in the merge's direction
/// where a merge needs it, and the merge, or `None` to read them in branch
/// order. `dedupe` says whether branches can list the same record.
///
/// A merge by record id applies to `ORDER BY id` alone over equality scans,
/// each of which lists its records in id order. A merge by an index key
/// applies where every branch pins the same composite-index prefix and the
/// ordering is that index's next column.
pub(crate) fn decide_union_merge(
	paths: Vec<AccessPath>,
	dedupe: bool,
	order: Option<&LogicalOrdering>,
) -> (Vec<AccessPath>, Option<MergeMode>) {
	// Enable merge-sort by record ID when ORDER BY is `id ASC/DESC`
	// only and every sub-path is an equality B-tree scan (each one
	// already produces records in record-ID order).
	let merge_dir = detect_order_by_id_only(order).filter(|_| {
		paths.iter().all(|p| {
			matches!(
				p,
				AccessPath::BTreeScan {
					access: BTreeAccess::Equality(_),
					..
				}
			)
		})
	});

	// If we couldn't merge-by-id, try the composite-index k-way merge:
	// every branch pins the same composite-index prefix and the ORDER
	// BY column is the next column of that index. Each branch is then
	// already sorted by the ORDER BY column (in the scan's direction).
	let merge_by_index_key = if merge_dir.is_none() {
		detect_order_for_composite_union(order, &paths)
	} else {
		None
	};

	// When the merge-by-index-key opportunity applies but a branch's
	// scan direction doesn't match the ORDER BY direction (this happens
	// when `try_in_expansion` used the default direction because the
	// chosen ORDER BY isn't on `id`), rewrite each branch to scan in
	// the correct direction. Each per-branch sub-scan must yield rows
	// already sorted by the suffix column in the merge's direction.
	let paths = if let Some((_, dir)) = &merge_by_index_key {
		let target = match dir {
			SortDirection::Asc => Direction::Forward,
			SortDirection::Desc => Direction::Backward,
		};
		paths
			.into_iter()
			.map(|p| match p {
				AccessPath::BTreeScan {
					index_ref,
					access,
					..
				} => AccessPath::BTreeScan {
					index_ref,
					access,
					direction: target,
				},
				other => other,
			})
			.collect::<Vec<_>>()
	} else {
		paths
	};

	let merge = match (merge_dir, merge_by_index_key) {
		(Some(direction), _) => Some(MergeMode::ById(direction)),
		// Branches that can list the same record are deduplicated as they
		// merge (see `AccessPath::Union::dedupe`).
		(None, Some((path, direction))) if dedupe => Some(MergeMode::ByIndexKeyDedup {
			path,
			direction,
		}),
		(None, Some((path, direction))) => Some(MergeMode::ByIndexKey {
			path,
			direction,
		}),
		(None, None) => None,
	};
	(paths, merge)
}

/// Check whether the ORDER BY clause is exactly `ORDER BY id ASC` or
/// `ORDER BY id DESC` with no additional columns.
///
/// Returns `Some(SortDirection)` when the condition is met, allowing
/// callers to enable optimisations that rely on record-ID ordering
/// (e.g. merge-sort in `UnionIndexScan`).
fn detect_order_by_id_only(order: Option<&LogicalOrdering>) -> Option<SortDirection> {
	if let Some(LogicalOrdering::Order(order_list)) = order
		&& let [first] = order_list.as_slice()
		&& is_id_field(&first.value)
		&& !first.collate
		&& !first.numeric
	{
		Some(if first.direction {
			SortDirection::Asc
		} else {
			SortDirection::Desc
		})
	} else {
		None
	}
}

/// Detect the pattern "every branch pins the same composite-index prefix
/// to an equality value, and ORDER BY is the next column of that index".
///
/// When this holds, each per-branch sub-scan is already sorted by the
/// ORDER BY column (in the scan's direction), so a k-way merge over the
/// branches by that column yields a globally-sorted stream — with
/// early-stop on a downstream `LIMIT`.
///
/// Returns the field path to merge on and the required scan direction
/// (the caller may need to rewrite per-branch scan directions to match).
/// Returns `None` when the pattern is not applicable (different indexes,
/// non-equality access, ORDER BY column not the next index column,
/// COLLATE/NUMERIC modifiers, single-column indexes, duplicate branch
/// prefixes, etc).
fn detect_order_for_composite_union(
	order: Option<&LogicalOrdering>,
	paths: &[AccessPath],
) -> Option<(crate::exec::field_path::FieldPath, SortDirection)> {
	use crate::exec::field_path_convert::field_path_from_idiom as field_path_from_column;

	// Must be ORDER BY a single, plain column with no collation modifiers.
	let Some(LogicalOrdering::Order(order_list)) = order else {
		return None;
	};
	let [order_field] = order_list.as_slice() else {
		return None;
	};
	if order_field.collate || order_field.numeric {
		return None;
	}
	let order_path = field_path_from_expr(&order_field.value).ok()?;
	let direction = if order_field.direction {
		SortDirection::Asc
	} else {
		SortDirection::Desc
	};

	// All branches must share the same index. Take the first branch's
	// index_ref as the reference and check every branch against it.
	let mut branches = paths.iter();
	let first = branches.next()?;
	let (first_index_ref, first_prefix_len) = match first {
		AccessPath::BTreeScan {
			index_ref,
			access: BTreeAccess::Compound {
				prefix,
				range: None,
			},
			..
		} => (index_ref.clone(), prefix.len()),
		// Single-column equality on the ORDER BY column itself wouldn't
		// give us a "next column" to merge on.
		_ => return None,
	};
	// Composite index must have an additional column after the prefix
	// that is exactly the ORDER BY target.
	let ix_def = first_index_ref.definition();
	if ix_def.cols.len() <= first_prefix_len {
		return None;
	}
	let sort_col_idiom = ix_def.cols.get(first_prefix_len)?;
	let sort_col_path = field_path_from_column(sort_col_idiom).ok()?;
	if sort_col_path != order_path {
		return None;
	}

	// Every remaining branch must use the same index and same prefix
	// length. Branch directions need not match — the caller rewrites
	// each branch's scan direction to align with ORDER BY before
	// constructing the sub-operators. Branch prefixes must be distinct
	// so no record appears twice.
	let first_prefix = match first {
		AccessPath::BTreeScan {
			access: BTreeAccess::Compound {
				prefix,
				..
			},
			..
		} => prefix.as_slice(),
		_ => return None,
	};
	let mut seen_prefixes: Vec<&[crate::val::Value]> = Vec::with_capacity(paths.len());
	seen_prefixes.push(first_prefix);
	for path in branches {
		let (idx, access) = match path {
			AccessPath::BTreeScan {
				index_ref,
				access: access @ BTreeAccess::Compound {
					range: None,
					..
				},
				..
			} => (index_ref, access),
			_ => return None,
		};
		if idx != &first_index_ref {
			return None;
		}
		let BTreeAccess::Compound {
			prefix,
			..
		} = access
		else {
			return None;
		};
		if prefix.len() != first_prefix_len {
			return None;
		}
		if seen_prefixes.iter().any(|p| p == &prefix.as_slice()) {
			return None;
		}
		seen_prefixes.push(prefix.as_slice());
	}

	Some((order_path, direction))
}

#[cfg(test)]
/// Whether the planned source tree contains a KNN source operator
/// (`KnnScan`), i.e. an `Approximate` KNN conjunct from the WHERE condition
/// was lowered into an index-backed search. Used to decide whether the
/// pre-stripped filter condition must be restored (see
/// [`Planner::compile_pipeline`]).
fn source_contains_knn(op: &Arc<dyn ExecOperator>) -> bool {
	op.name() == crate::exec::operators::KnnScan::NAME
		|| op.children().iter().any(|c| source_contains_knn(c))
}

#[cfg(test)]
mod tests {
	use super::super::fold::collect_pipeline;
	use super::*;
	use crate::expr::logical::lower_select;
	use crate::syn;

	/// The clauses of `sql` as the planner receives them: lowered to a plan
	/// and collected off it.
	fn clauses_of(sql: &str) -> PipelineClauses {
		let ast = syn::parse(sql).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement in {sql:?}");
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Select(select)) =
			exprs.remove(0)
		else {
			panic!("`{sql}` is not a SELECT");
		};
		collect_pipeline(lower_select(*select).expect("SELECT lowers")).0
	}

	/// The decode set `extract_decode_fields` derives for `sql`, as its
	/// listed fields (sorted) and whether they are excluded; `None` when the
	/// statement decodes whole records.
	fn decode_fields(sql: &str) -> Option<(Vec<String>, bool)> {
		let clauses = clauses_of(sql);
		Planner::extract_decode_fields(
			&FunctionRegistry::with_builtins(),
			clauses.project.as_ref().map(|p| &p.fields),
			clauses.project.as_ref().map(|p| p.omit.as_slice()).unwrap_or(&[]),
			clauses.cond.as_ref(),
			clauses.order.as_ref(),
			clauses.group.as_ref().map(|g| g.group.as_slice()),
			clauses.split.as_deref(),
		)
		.map(|d| d.listed())
	}

	fn listed(fields: &[&str], excluding: bool) -> Option<(Vec<String>, bool)> {
		Some((fields.iter().map(|f| f.to_string()).collect(), excluding))
	}

	#[test]
	fn extract_decode_fields_lists_every_field_a_clause_reads() {
		assert_eq!(
			decode_fields("SELECT id, title FROM post WHERE grp = 'a' ORDER BY n"),
			listed(&["grp", "n", "title"], false)
		);
	}

	#[test]
	fn extract_decode_fields_declines_a_read_it_cannot_name() {
		assert_eq!(decode_fields("SELECT id, type::field('emb') AS e FROM post"), None);
		assert_eq!(
			decode_fields("SELECT id, (SELECT VALUE $parent.emb FROM [1]) AS e FROM post"),
			None
		);
		assert_eq!(decode_fields("SELECT * FROM post"), None);
	}

	#[test]
	fn extract_decode_fields_excludes_the_omitted_fields_nothing_reads() {
		assert_eq!(decode_fields("SELECT * OMIT emb FROM post"), listed(&["emb"], true));
		assert_eq!(
			decode_fields(
				"SELECT *, search::score(1) AS s OMIT emb, meta FROM post WHERE title @1@ 'x'"
			),
			listed(&["emb", "meta"], true)
		);
		// An omitted field another clause reads is decoded.
		assert_eq!(
			decode_fields("SELECT * OMIT emb, meta FROM post WHERE emb[0] > 1"),
			listed(&["meta"], true)
		);
		assert_eq!(decode_fields("SELECT *, emb[0] AS e OMIT emb FROM post"), None);
		// A nested OMIT target keeps its root decoded, and the rest still apply.
		assert_eq!(decode_fields("SELECT * OMIT meta.x FROM post"), None);
		assert_eq!(decode_fields("SELECT * OMIT emb, meta.x FROM post"), listed(&["emb"], true));
		assert_eq!(decode_fields("SELECT * OMIT meta, meta.x FROM post"), None);
		// `id` is never decoded from the record, so omitting it excludes nothing.
		assert_eq!(decode_fields("SELECT * OMIT id FROM post"), None);
		assert_eq!(decode_fields("SELECT * OMIT id, emb FROM post"), listed(&["emb"], true));
	}

	#[test]
	fn extract_decode_fields_reads_no_stored_field_for_an_alias() {
		assert_eq!(
			decode_fields("SELECT 1 AS computed, title FROM post"),
			listed(&["title"], false)
		);
		assert_eq!(
			decode_fields("SELECT *, 1 AS secret OMIT secret FROM post"),
			listed(&["secret"], true)
		);
		// A clause reading the alias name reads it like any other field.
		assert_eq!(
			decode_fields("SELECT title AS t FROM post ORDER BY t"),
			listed(&["t", "title"], false)
		);
	}

	fn needed_fields(sql: &str) -> HashSet<String> {
		needed_fields_with(sql, KnnRowReads::Operands)
			.expect("non-wildcard projection should produce a Some(set)")
	}

	fn needed_fields_with(sql: &str, knn: KnnRowReads) -> Option<HashSet<String>> {
		let clauses = clauses_of(sql);
		Planner::extract_needed_fields(
			clauses.project.as_ref().map(|p| &p.fields),
			clauses.project.as_ref().map(|p| p.omit.as_slice()).unwrap_or(&[]),
			clauses.cond.as_ref(),
			clauses.order.as_ref(),
			clauses.group.as_ref().map(|g| g.group.as_slice()),
			clauses.split.as_deref(),
			knn,
		)
	}

	/// An index-backed KNN operator reads only the id it tests for
	/// membership: neither its vector field nor its query parameter, which
	/// would otherwise make the set opaque. The rest of the clause is read as
	/// usual.
	#[test]
	fn an_index_knn_reads_only_the_row_id() {
		let fields = |sql| needed_fields_with(sql, KnnRowReads::RecordId);
		let set = |names: &[&str]| Some(names.iter().map(|n| n.to_string()).collect());
		assert_eq!(fields("SELECT id FROM t WHERE v <|3,40|> $q"), set(&["id"]));
		assert_eq!(
			fields("SELECT id, name FROM t WHERE v <|3,40|> $q AND n > 1"),
			set(&["id", "name", "n"])
		);
		assert_eq!(fields("SELECT id FROM t WHERE v <|3,40|> $q AND n > $m"), None);
		// A brute-force KNN scores the vector of every row, so its operands are read.
		assert_eq!(
			needed_fields_with("SELECT id FROM t WHERE v <|3,40|> $q", KnnRowReads::Operands),
			None
		);
		assert_eq!(
			needed_fields_with(
				"SELECT id FROM t WHERE v <|3,EUCLIDEAN|> [1, 2]",
				KnnRowReads::Operands
			),
			set(&["id", "v"])
		);
	}

	/// `$parent.refs` reaches into the OUTER row's `refs` field, so the
	/// needed-fields set must not include `refs`; `$this.cat` reaches THIS
	/// row's `cat`, which must be in.
	#[test]
	fn extract_needed_fields_excludes_outer_row_paths() {
		let needed = needed_fields(
			"SELECT cat FROM users \
			 WHERE $parent.refs CONTAINS $this.cat",
		);
		assert!(
			needed.contains("cat"),
			"current row's `cat` should be in needed-fields: got {needed:?}",
		);
		assert!(
			!needed.contains("refs"),
			"$parent.refs is the OUTER row's `refs`, must NOT be in this row's \
			 needed-fields: got {needed:?}",
		);
	}

	/// A `$parent` path inside a function-call argument belongs to the outer
	/// row as well.
	#[test]
	fn extract_needed_fields_excludes_parent_paths_in_method_args() {
		let needed = needed_fields(
			"SELECT cat FROM users \
			 WHERE array::find($parent.refs, $this.cat) != NONE",
		);
		assert!(needed.contains("cat"), "got {needed:?}");
		assert!(
			!needed.contains("refs"),
			"`$parent.refs` inside a function-call argument must not leak \
			 into this row's needed-fields: got {needed:?}",
		);
	}

	/// `$this.x` paths propagate into the needed-fields set.
	#[test]
	fn extract_needed_fields_keeps_this_row_paths() {
		let needed = needed_fields("SELECT name FROM users WHERE $this.age > 18");
		assert!(needed.contains("name"), "got {needed:?}");
		assert!(needed.contains("age"), "got {needed:?}");
	}

	/// Filter-predicate scoping: in `refs[WHERE kind = 'tag']` the bare
	/// `kind` references the iteration element of `refs`, not this row's
	/// columns; only `refs` (the array being iterated) is in the set.
	#[test]
	fn extract_needed_fields_filter_predicate_scope_excluded() {
		let needed = needed_fields(
			"SELECT name FROM users \
			 WHERE refs[WHERE kind = 'tag'] != []",
		);
		assert!(needed.contains("name"), "got {needed:?}");
		assert!(needed.contains("refs"), "got {needed:?}");
		assert!(
			!needed.contains("kind"),
			"`kind` inside `[WHERE …]` references the iteration element, \
			 not this row: got {needed:?}",
		);
	}

	/// Filter-predicate scoping with `$parent`: inside the filter `$parent`
	/// is rebound to this row, so `cat` IS a current-row field. Sibling
	/// iteration-scope paths (`kind`) remain excluded.
	#[test]
	fn extract_needed_fields_filter_predicate_parent_promotes() {
		let needed = needed_fields(
			"SELECT name FROM users \
			 WHERE refs[WHERE $parent.cat = kind] != []",
		);
		assert!(needed.contains("name"), "got {needed:?}");
		assert!(needed.contains("refs"), "got {needed:?}");
		assert!(
			needed.contains("cat"),
			"`$parent.cat` inside `[WHERE …]` is rebound to this row: \
			 got {needed:?}",
		);
		assert!(!needed.contains("kind"), "got {needed:?}");
	}

	/// A bare field literally named `parent` (not the `$parent` parameter)
	/// is a real column.
	#[test]
	fn extract_needed_fields_treats_bare_parent_as_field() {
		let needed = needed_fields("SELECT parent.sub FROM table");
		assert!(
			needed.contains("parent"),
			"bare `parent.sub` is a real column path: got {needed:?}",
		);
	}

	/// A grouped pipeline's fields live on its aggregation; the set is read
	/// from there.
	#[test]
	fn extract_needed_fields_reads_a_grouped_projection() {
		let clauses = clauses_of("SELECT count(), a FROM t GROUP BY a");
		let needed = Planner::extract_needed_fields(
			clauses.group.as_ref().map(|g| &g.fields),
			&[],
			None,
			None,
			clauses.group.as_ref().map(|g| g.group.as_slice()),
			None,
			KnnRowReads::Operands,
		)
		.expect("a grouped projection is selective");
		assert!(needed.contains("a"), "got {needed:?}");
	}

	#[test]
	fn for_update_targets_must_be_records() {
		fn sources_of(sql: &str) -> Vec<LogicalOp> {
			let ast = syn::parse(sql).expect("parse");
			let mut exprs = ast.expressions;
			let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Select(select)) =
				exprs.remove(0)
			else {
				panic!("`{sql}` is not a SELECT");
			};
			match collect_pipeline(lower_select(*select).expect("SELECT lowers")).1 {
				PipelineStop::Leaf(LogicalOp::UnionAll(LogicalUnionAll {
					inputs,
				})) => inputs,
				PipelineStop::Leaf(leaf) => vec![leaf],
				PipelineStop::Node(node) => panic!("stopped at {}", node.name()),
			}
		}
		for sql in [
			"SELECT * FROM t:1 FOR UPDATE",
			"SELECT * FROM $p FOR UPDATE",
			"SELECT * FROM type::record($p) FOR UPDATE",
			"SELECT * FROM t:1, t:2 FOR UPDATE",
			"SELECT * FROM t:1",
			"SELECT * FROM t",
		] {
			assert!(check_for_update_sources(&sources_of(sql)).is_ok(), "`{sql}` should pass");
		}
		for sql in [
			"SELECT * FROM t FOR UPDATE",
			"SELECT * FROM t:1..5 FOR UPDATE",
			"SELECT * FROM |t:3| FOR UPDATE",
			"SELECT * FROM t:1, t FOR UPDATE",
			"SELECT * FROM (SELECT * FROM t:1) FOR UPDATE",
		] {
			assert!(check_for_update_sources(&sources_of(sql)).is_err(), "`{sql}` should fail");
		}
	}

	#[test]
	fn restricted_prefix_matches_by_leading_parts() {
		let secret: Idiom = crate::syn::idiom("meta.secret").expect("idiom");
		let restricted = RestrictedPrefixes::Some(vec![
			crate::expr::logical::FieldPath::from_idiom(&secret).expect("field path"),
		]);
		let clauses = clauses_of("SELECT * FROM t WHERE meta.secret.inner = 1 ORDER BY meta");
		assert!(restricted.cond_touches(clauses.cond.as_ref().expect("WHERE")));
		// Ordering by the parent of a restricted child is the documented gap.
		assert!(!restricted.order_touches(clauses.order.as_ref().expect("ORDER BY")));
		let clauses = clauses_of("SELECT * FROM t WHERE meta.public = 1 ORDER BY meta.secret");
		assert!(!restricted.cond_touches(clauses.cond.as_ref().expect("WHERE")));
		assert!(restricted.order_touches(clauses.order.as_ref().expect("ORDER BY")));
		// A read of the whole parent reads the restricted child with it.
		let clauses = clauses_of("SELECT * FROM t WHERE meta CONTAINS 'x'");
		assert!(restricted.cond_touches(clauses.cond.as_ref().expect("WHERE")));
		// A nested statement resolves its own permissions.
		let clauses = clauses_of("SELECT * FROM t WHERE (SELECT meta.secret FROM u) != NONE");
		assert!(!restricted.cond_touches(clauses.cond.as_ref().expect("WHERE")));
	}

	/// A `Source` the analyzer resolved (`schema: Some`), as the physical
	/// planner receives it from a bound plan.
	fn resolved_source(table: &str) -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(TableName::from(table)),
			schema: Some(Arc::new(crate::expr::logical::Schema::open())),
			with: None,
			for_update: false,
		})
	}

	fn select_all(input: LogicalOp) -> LogicalOp {
		LogicalOp::Project(crate::expr::logical::LogicalProject {
			input: Box::new(input),
			fields: LogicalFields::all(),
			omit: vec![],
		})
	}

	/// The operator names of a plan, from the root down.
	fn operator_names(plan: &Arc<dyn ExecOperator>, out: &mut Vec<&'static str>) {
		out.push(plan.name());
		for child in plan.children() {
			operator_names(child, out);
		}
	}

	/// A source the analyzer resolved is planned with a scan that yields rows
	/// as stored: from its table context, or from its mode alone for a table
	/// the catalog lacks. When no context can be resolved — the `VERSION` the
	/// table is read under is only known when the statement runs — the table
	/// is scanned whole by a scan that resolves its table when it runs and
	/// still yields rows as stored, rather than by one that enforces at
	/// runtime beneath the plan's own nodes.
	#[tokio::test]
	async fn a_resolved_source_without_a_table_context_is_a_table_scan() {
		use crate::exec::operators::test_util::TestDb;
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = db.exec_ctx().await;
		let planner = Planner::with_txn(
			ctx.ctx(),
			ctx.function_registry(),
			ctx.try_txn().expect("a transaction"),
			Some("test".to_owned()),
			Some("test".to_owned()),
		);

		// The table the analyzer saw is planned.
		assert!(planner.plan_logical(select_all(resolved_source("t"))).await.is_ok());
		// A table the catalog lacks is planned: the analyzer saw the same
		// absence, and the scan carries the plan's mode for the table it finds
		// when it runs.
		assert!(planner.plan_logical(select_all(resolved_source("ghost"))).await.is_ok());
		// A `VERSION` the planner cannot evaluate leaves no stamp to read the
		// table at: the table is scanned whole, and resolves when it runs.
		let versioned = LogicalOp::VersionScope(crate::expr::logical::LogicalVersionScope {
			input: Box::new(select_all(resolved_source("t"))),
			version: LogicalExpr::Param(crate::expr::Param::from("unbound".to_owned())),
		});
		let plan = planner.plan_logical(versioned).await.expect("the statement plans");
		let mut names = Vec::new();
		operator_names(&plan, &mut names);
		assert!(names.contains(&"TableScan"), "planned as {names:?}");
		// An unresolved source under the same `VERSION` is planned: its scan
		// resolves the table when it runs, at the stamp it evaluates.
		let runtime = LogicalOp::VersionScope(crate::expr::logical::LogicalVersionScope {
			input: Box::new(select_all(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(TableName::from("t")),
				schema: None,
				with: None,
				for_update: false,
			}))),
			version: LogicalExpr::Param(crate::expr::Param::from("unbound".to_owned())),
		});
		assert!(planner.plan_logical(runtime).await.is_ok());
	}
}
