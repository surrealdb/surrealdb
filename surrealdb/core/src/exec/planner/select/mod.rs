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

pub(crate) use projection::output_field_path;
use surrealdb_strand::TableName;
use surrealdb_types::ToSql;

use self::fast_path::{
	CountAll, IndexedCountAll, match_count_all, match_indexed_count_all, permission_filtered_source,
};
use super::fold::{PipelineClauses, PipelineStop, is_layer_chain_over_table};
use super::row_scope::{PathRoot, classify_path_root};
use super::util::{
	exact_knn_by, expr_contains_matches, extract_record_id_point_lookup, field_path_from_expr,
	fold_expressions, has_knn_operator, order_is_scan_compatible, resolve_param_value,
	resolve_projection_field_idioms, source_field_reads_are_inert, strip_index_conditions_in,
	strip_knn_from_condition,
};
use super::{PlanTimeVersion, Planner};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{Index, IndexDefinition, KeyStructure, table_key};
use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::exec::function::FunctionRegistry;
use crate::exec::index::access_path::{
	AccessPath, BTreeAccess, BitmapPlan, IndexRef, KnnPrefilterPlan, select_access_path_paged,
};
use crate::exec::index::analysis::{IndexAnalyzer, KnnQueryVectors};
use crate::exec::index::key_range::{DeliveredKeyOrder, KeyRangeAccess, plan_key_range};
use crate::exec::logical::reads::{
	declared_read_prefix, paths_overlap, reads_at_or_beneath, reads_row_opaquely,
};
use crate::exec::logical::{Descend, named_reads, visit_binding_pruning};
use crate::exec::operators::scan::MergeMode;
use crate::exec::operators::scan::common::DecodeFields;
use crate::exec::operators::scan::pipeline::{
	determine_scan_direction, table_read_restricted_fields,
};
use crate::exec::operators::{BitmapNode, CountOutput, SortDirection};
use crate::expr::logical::{
	LogicalDestructureField, LogicalExpr, LogicalField, LogicalFields, LogicalLiteral, LogicalOp,
	LogicalOrdering, LogicalRecordIdKey, LogicalRecurseInstruction, LogicalRecursePath,
	LogicalSource, LogicalUnionAll, LogicalValues, OutputPathPart, SourceRef, lower_expr,
};
use crate::expr::with::With;
use crate::expr::{Function, Idiom};
use crate::kvs::index::filter_online_indexes;
use crate::kvs::{Direction, Transaction};

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
	/// A name an enclosing `FOR`, closure, user-defined function or block
	/// `LET` binds has no plan-time value: the ambient one it shadows is not
	/// the one the statement reads.
	async fn resolve_param(&self, name: &str) -> Option<crate::val::Value> {
		let ns_db = self.definition_ids().await;
		resolve_param_value(name, &self.ctx, self.txn.as_ref(), ns_db, &self.row_scoped()).await
	}

	/// The query vectors of the KNN operators in `cond` whose right operand
	/// is a parameter holding an array of numbers, each resolved once. A
	/// closure body is not entered: its parameters are bound where it is
	/// invoked.
	async fn knn_query_vectors(&self, cond: Option<&LogicalExpr>) -> KnnQueryVectors {
		let mut names: HashSet<&str> = HashSet::new();
		if let Some(cond) = cond {
			visit_binding_pruning(cond, &mut |e| {
				if let LogicalExpr::Binary {
					op: crate::expr::operator::BinaryOperator::NearestNeighbor(_),
					right,
					..
				} = e && let LogicalExpr::Param(param) = right.as_ref()
				{
					names.insert(param.as_str());
				}
				Descend::Into
			});
		}
		let mut vectors = KnnQueryVectors::new();
		for name in names {
			if let Some(vector) = self.resolve_query_vector(name).await {
				vectors.insert(name.to_owned(), vector);
			}
		}
		vectors
	}

	/// The value of parameter `name` as a KNN query vector, when it is an
	/// array of numbers at plan time ([`Self::resolve_param`]).
	pub(crate) async fn resolve_query_vector(
		&self,
		name: &str,
	) -> Option<Arc<[crate::val::Number]>> {
		let Some(crate::val::Value::Array(array)) = self.resolve_param(name).await else {
			return None;
		};
		// As binding's own test of a query vector: a vector has at least one
		// component.
		if array.is_empty() {
			return None;
		}
		array
			.iter()
			.map(|v| match v {
				crate::val::Value::Number(n) => Some(*n),
				_ => None,
			})
			.collect()
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
	pub(crate) fn extract_needed_fields(
		fields: Option<&LogicalFields>,
		omit: &[LogicalExpr],
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		group: Option<&[LogicalExpr]>,
		split: Option<&[LogicalExpr]>,
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

	// ========================================================================
	// Pipeline compilation with plan-time index resolution
	// ========================================================================

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

	// ========================================================================
	// Per-access-path source planners (called by plan_source)
	// ========================================================================

	/// Whether `id` reads as stored on `table` for every caller: no definition
	/// that may cover it is `COMPUTED` or carries a field `SELECT` permission
	/// other than `FULL`. The rows a KNN search builds from record ids hold
	/// `id` and nothing else, so `id` is checked whatever the pipeline reads
	/// of them, `count()` included.
	///
	/// The plan enforces the table's schema on those rows, through the
	/// projection layers above the search, which the binding has pruned to
	/// what the pipeline reads. A definition named by a plain field path
	/// (fields and `[*]`) rooted elsewhere than `id` is evaluated for no row
	/// the pipeline reads only the `id` of, and is not consulted. Any other
	/// name is consulted.
	///
	/// `false` without catalog access, or when the field list cannot be read.
	async fn id_reads_as_stored(&self, table: &TableName) -> bool {
		use crate::expr::part::Part;
		let Some(txn) = self.txn.as_ref() else {
			return false;
		};
		let Some((ns_id, db_id)) = self.ns_db_ids().await else {
			return false;
		};
		match txn.all_tb_fields(ns_id, db_id, table, None).await {
			Ok(fields) => fields
				.iter()
				.filter(|fd| match fd.name.0.as_slice() {
					[Part::Field(root), rest @ ..]
						if rest.iter().all(|p| matches!(p, Part::Field(_) | Part::All)) =>
					{
						root.as_str() == "id"
					}
					_ => true,
				})
				.all(|fd| {
					fd.computed.is_none()
						&& matches!(fd.select_permission, crate::catalog::Permission::Full)
				}),
			Err(e) => {
				tracing::warn!(
					table = %table,
					error = %e,
					"plan-time field list failed in id_reads_as_stored; \
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
	///
	/// A field derived from a named record id component
	/// ([`crate::catalog::FieldDefinition::is_key_part`]) is not a computed
	/// field here: `id[i]` reads the id alone, which every decode sets from
	/// the key whatever fields it lists.
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
				(fd.computed.is_none() || fd.is_key_part())
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
	/// reads the row other than by a field's name, and `id` reads as stored
	/// for every caller ([`Self::id_reads_as_stored`]). The plan enforces the
	/// table's schema on the rows of every KNN search it maps, so a definition
	/// the clauses do not read is not consulted. The table's permission and
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
		let ids_only = Self::extract_needed_fields(fields, omit, cond, order, group, None)
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
			) && self.id_reads_as_stored(table).await
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
		let guards: Vec<_> = indexes
			.iter()
			.filter(|ix| matches!(&ix.index, Index::Count(Some(_))))
			.filter_map(|ix| ix.count_cond.as_ref())
			.collect();
		if guards.is_empty() {
			return false;
		}
		// Where the table's record ids name their components, a guard written
		// with a component's name is the condition a layer moved beneath
		// itself, which reads that component as an element of the id.
		let key = self.record_key_structure(txn, ns_id, db_id, table_name).await;
		guards.iter().any(|guard| {
			crate::exec::index::derived_columns::guard_counts(key.as_deref(), &guard.0, cond)
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
	/// as an element column, and every one that flattens a set withheld from
	/// containment, resolved against `table_name`'s declared fields and those
	/// of the tables they link to or a view reads; see
	/// [`with_array_columns_as_elements`](crate::exec::index::access_path::with_array_columns_as_elements)
	/// and [`foreign_set_prefixes`](crate::exec::index::access_path::foreign_set_prefixes).
	///
	/// `None` when the field list or the indexes' recorded shapes cannot be
	/// read: without them a column's fan-out is unknown, so the caller
	/// declines its plan-time index path rather than plan one that could
	/// count or order element entries as rows. A declined select plan falls back to `DynamicScan`,
	/// which plans without b-tree indexes when the list is unreadable there too.
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
		let sets = match crate::exec::index::access_path::foreign_set_prefixes(
			txn, ns_id, db_id, table_name, &indexes, &fields, None,
		)
		.await
		{
			Ok(sets) => sets,
			Err(e) => {
				tracing::warn!(
					table = %table_name,
					error = %e,
					"plan-time linked field list failed in with_array_columns_as_elements; \
					 falling back to runtime",
				);
				return None;
			}
		};
		let shapes = self.index_shapes(txn, ns_id, db_id, table_name, &indexes).await?;
		Some(
			crate::exec::index::access_path::with_array_columns_as_elements(
				indexes, &fields, &shapes,
			)
			.withholding_sets_at(&sets),
		)
	}

	/// The value shapes `table_name`'s B-tree indexes among `indexes` have
	/// stored, read for planning through the node's cache
	/// ([`Transaction::planning_index_shapes`]), or none for a planner that
	/// may not trust them ([`Planner::trusts_index_shapes`]). `None` when they
	/// cannot be read.
	async fn index_shapes(
		&self,
		txn: &Transaction,
		ns_id: crate::catalog::NamespaceId,
		db_id: crate::catalog::DatabaseId,
		table_name: &TableName,
		indexes: &[IndexDefinition],
	) -> Option<surrealdb_datastore::index_shapes::TableIndexShapes> {
		if !self.trusts_index_shapes() {
			return Some(Default::default());
		}
		let cache = self.ctx.get_cache();
		match txn.planning_index_shapes(cache.as_deref(), ns_id, db_id, table_name, indexes).await {
			Ok(shapes) => Some(shapes),
			Err(e) => {
				tracing::warn!(
					table = %table_name,
					error = %e,
					"plan-time index shapes failed; falling back to runtime",
				);
				None
			}
		}
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
	/// chosen only for an index every column of which holds one entry per
	/// record — the index has recorded storing no array in it, or it is
	/// declared a kind that holds none; any other count reads the records.
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

		// An element column (`tags.*`) whose element field is declared a
		// scalar kind (`DEFINE FIELD tags.* … TYPE string`, a field named as
		// the column is) holds one entry per element value, which the fan-out
		// check below admits for an access that pins it.
		// `IndexRef::one_entry_per_record` answers for plain columns only, so
		// this is the one route by which such a column counts from its keys.
		// The field list is the transaction's cached one.
		let fields = txn.all_tb_fields(ns_id, db_id, table_name, None).await.ok()?;
		let one_key_per_record = |index: &IndexRef| {
			index.cols.iter().enumerate().all(|(i, col)| {
				index.one_entry_per_record(i)
					|| fields
						.iter()
						.find(|fd| &fd.name == col)
						.is_some_and(field_def_excludes_arrays)
			})
		};

		// The key lets the analyzer match a column that addresses a component
		// of the record id, and widen a range over a datetime one.
		let key = self.record_key_structure(txn, ns_id, db_id, table_name).await;
		let analyzer = columns.analyzer(with).with_key(key.clone());
		let candidates = analyzer.analyze(Some(cond), None);

		// Look for a candidate that fully covers the WHERE condition
		// (no residual filter needed).
		for candidate in &candidates {
			// A key-only count counts index entries, so it is only a row
			// count while each matching record owns exactly one entry in the
			// range. A column not known to hold no array can hold one, which is
			// listed once per element, and a scan that fans out counts a record
			// once per element.
			if !one_key_per_record(&candidate.index_ref)
				|| !crate::exec::index::access_path::range_matches_comparison(&candidate.access)
			{
				continue;
			}
			if crate::exec::index::access_path::access_fans_out(
				&candidate.index_ref.cols,
				&candidate.access,
			) {
				continue;
			}
			// A range widened to the seconds of its bounds counts records its
			// predicate rejects.
			if crate::exec::index::derived_columns::range_is_widened(
				&candidate.index_ref,
				&candidate.access,
			) {
				continue;
			}
			// Check: does this index access fully cover the condition?
			// If strip_index_conditions returns None, the index
			// consumed the entire WHERE clause. A conjunct that reads a
			// component of the record id as `id[i]` is the column the index
			// holds for it.
			if strip_index_conditions_in(
				cond,
				&candidate.access,
				&candidate.index_ref.cols,
				key.as_deref(),
			)
			.is_none()
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
			self.with_array_columns_as_elements(txn, ns_id, db_id, table_name, indexes).await?;

		let key = self.record_key_structure(txn, ns_id, db_id, table_name).await;
		let analyzer = columns.analyzer(with).with_key(key);
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
	///
	/// `permissioned` says a table permission predicate is part of `cond`
	/// ([`super::AccessOffer::permissioned`]): a key range over the
	/// components of the record id then takes only literal bounds, since a
	/// bound evaluated when the scan opens is evaluated in the caller's
	/// frame, not the definer's.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn decide_access_path(
		&self,
		table_name: &TableName,
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		with: Option<&With>,
		has_query_limit: bool,
		versioned: bool,
		permissioned: bool,
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
				permissioned,
				&restricted_select,
			)
			.await
			.ok()??;
		Some(AccessDecision {
			path,
			direction,
			order_withheld,
		})
	}

	/// Resolve the optimal access path for a table at plan time.
	///
	/// Performs index analysis using the WHERE condition and ORDER BY clause.
	/// Returns the selected `AccessPath` and scan direction, or `None` if
	/// the namespace/database cannot be resolved.
	///
	/// Over a table whose record ids name their components, or an aggregated
	/// view, whose rows are keyed by their group values, a key range over
	/// the record keys ([`plan_key_range`]) competes with the index
	/// candidates
	/// ([`IndexCandidate::outranks_key_range`](crate::exec::index::IndexCandidate::outranks_key_range));
	/// it is not planned
	/// under a `VERSION`, a `WITH` hint, a `MATCHES` or a KNN operator.
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
		permissioned: bool,
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

		// The structure of the table's record ids, where they name their
		// components or the table is an aggregated view: what a key range is
		// planned against, and what the index analyzer matches a column
		// addressing a component by. An ordering reads it too, for an index
		// over a datetime component delivers no order within a second.
		let key = match (cond, order) {
			(None, None) => None,
			_ => {
				self.record_key_structure(txn, ns_def.namespace_id, db_def.database_id, table_name)
					.await
			}
		};
		// A read at a stamp plans no key range: the structure was checked
		// against the keys present when it was defined, not against the keys a
		// past stamp holds. A `WITH` hint chooses the read, and a `MATCHES` or
		// a KNN operator has no answer but its index.
		let key_range = match (&key, cond) {
			(Some(key), Some(cond))
				if with.is_none()
					&& !self.reads_versioned(has_version)
					&& !expr_contains_matches(cond)
					&& !has_knn_operator(cond) =>
			{
				// A conjunct that reads a field the caller may not read is
				// left to the reduced row, as it is for an index.
				let conjuncts: Vec<&LogicalExpr> = crate::exec::logical::conjuncts(cond)
					.into_iter()
					.filter(|c| !restricted_select.cond_touches(c))
					.collect();
				plan_key_range(key, &conjuncts, order, permissioned, self.function_registry())
			}
			_ => None,
		};
		let key_range_path = |access: KeyRangeAccess| {
			let direction = access.direction;
			(AccessPath::KeyRange(access), direction)
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
			return Ok(Some(key_range.map_or((AccessPath::TableScan, direction), key_range_path)));
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
			return Ok(Some(key_range.map_or((AccessPath::TableScan, direction), key_range_path)));
		}
		// Only an ANN index reads a KNN query vector; without one, a
		// brute-force source resolves its own.
		let knn_query_vectors =
			if indexes.iter().any(|ix| matches!(ix.index, Index::Hnsw(_) | Index::DiskAnn(_))) {
				self.knn_query_vectors(analysis_cond).await
			} else {
				KnnQueryVectors::new()
			};
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

		let analyzer =
			columns.analyzer(with).with_key(key).with_knn_query_vectors(&knn_query_vectors);
		let candidates = analyzer.analyze(analysis_cond, order);

		// A key range is chosen unless an index candidate outranks it, and only
		// a candidate that outranks it is chosen in its place.
		let candidates = match key_range {
			Some(access) => {
				let pinned = access.eq.len();
				let delivers = access.delivers == DeliveredKeyOrder::Exact;
				let outranking: Vec<_> = candidates
					.into_iter()
					.filter(|c| c.outranks_key_range(pinned, delivers, has_limit))
					.collect();
				if outranking.is_empty() {
					return Ok(Some(key_range_path(access)));
				}
				outranking
			}
			None => candidates,
		};

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

	/// The structure of `table`'s record ids that access-path selection reads:
	/// the one [`table_key`] derives, from the `id` field where it names its
	/// components and from the group expressions of an aggregated view.
	/// `None` for every other table.
	async fn record_key_structure(
		&self,
		txn: &Transaction,
		ns_id: crate::catalog::NamespaceId,
		db_id: crate::catalog::DatabaseId,
		table_name: &TableName,
	) -> Option<Arc<KeyStructure>> {
		let fields = txn.all_tb_fields(ns_id, db_id, table_name, None).await.ok()?;
		let definition = txn.get_tb(ns_id, db_id, table_name, None).await.ok()??;
		table_key(&definition, &fields)
	}

	/// The structure of `table`'s record ids where its `id` field names their
	/// components, read in the planner's transaction; `None` for every other
	/// table, and where the planner has no transaction or the catalog cannot
	/// answer.
	pub(crate) async fn named_key_structure(&self, table: &TableName) -> Option<Arc<KeyStructure>> {
		let txn = self.txn.as_ref()?;
		let (ns, db) = self.ns_db_ids().await?;
		self.record_key_structure(txn, ns, db, table)
			.await
			.filter(|key| key.origin == crate::catalog::KeyOrigin::IdField)
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
			} => {
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
/// [`BitmapNode`] tree.
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
pub(crate) struct AccessDecision {
	pub(crate) path: AccessPath,
	pub(crate) direction: Direction,
	/// Whether the ordering offered was withheld from the choice because it
	/// touches a restricted field: the path then says nothing about it, so
	/// it does not cover the ordering, and nothing may be truncated at the
	/// source beneath it.
	pub(crate) order_withheld: bool,
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
/// each of which lists its records in id order where the record ids order by
/// value ([`IndexRef::orders_by_id`]). A merge by an index key applies where
/// every branch pins the same composite-index prefix and the ordering is that
/// index's next column, which orders by value
/// ([`IndexRef::orders_coarsely`]).
pub(crate) fn decide_union_merge(
	paths: Vec<AccessPath>,
	dedupe: bool,
	order: Option<&LogicalOrdering>,
) -> (Vec<AccessPath>, Option<MergeMode>) {
	// Enable merge-sort by record ID when ORDER BY is `id ASC/DESC`
	// only and every sub-path is an equality B-tree scan (each one
	// already produces records in record-ID order, unless the record ids
	// order other than by value).
	let merge_dir = detect_order_by_id_only(order).filter(|_| {
		paths.iter().all(|p| {
			matches!(
				p,
				AccessPath::BTreeScan {
					index_ref,
					access: BTreeAccess::Equality(_),
					..
				} if index_ref.orders_by_id()
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
	// A column whose entries order only by the whole second leaves each
	// branch out of order within a second.
	if first_index_ref.orders_coarsely(first_prefix_len) {
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
/// A `VERSION` or `TIMEOUT` the plan does not apply.
///
/// An absent clause has two spellings: no operator in the plan, and the
/// `NONE` literal a fold leaves behind on the operator. Neither bounds
/// anything.
fn clause_is_absent(clause: Option<&LogicalExpr>) -> bool {
	matches!(clause, None | Some(LogicalExpr::Literal(LogicalLiteral::None)))
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

	/// The `OMIT` targets of the projection `sql` selects with.
	fn omit_of(sql: &str) -> Vec<LogicalExpr> {
		fn find(op: &LogicalOp) -> Option<Vec<LogicalExpr>> {
			match op {
				LogicalOp::Project(project) if !op.is_layer() => Some(project.omit.clone()),
				other => other.children().iter().find_map(find),
			}
		}
		let ast = syn::parse(sql).expect("parse");
		let mut exprs = ast.expressions;
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Select(select)) =
			exprs.remove(0)
		else {
			panic!("`{sql}` is not a SELECT");
		};
		find(&lower_select(*select).expect("SELECT lowers")).unwrap_or_default()
	}

	/// The decode set `extract_decode_fields` derives for `sql`, as its
	/// listed fields (sorted) and whether they are excluded; `None` when the
	/// statement decodes whole records.
	fn decode_fields(sql: &str) -> Option<(Vec<String>, bool)> {
		let clauses = clauses_of(sql);
		Planner::extract_decode_fields(
			&FunctionRegistry::with_builtins(),
			clauses.project.as_ref().map(|p| &p.fields),
			&omit_of(sql),
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
		needed_fields_of(sql).expect("non-wildcard projection should produce a Some(set)")
	}

	fn needed_fields_of(sql: &str) -> Option<HashSet<String>> {
		let clauses = clauses_of(sql);
		Planner::extract_needed_fields(
			clauses.project.as_ref().map(|p| &p.fields),
			&omit_of(sql),
			clauses.cond.as_ref(),
			clauses.order.as_ref(),
			clauses.group.as_ref().map(|g| g.group.as_slice()),
			clauses.split.as_deref(),
		)
	}

	/// An index-backed KNN operator reads only the id it tests for
	/// membership: neither its vector field nor its query parameter, which
	/// would otherwise make the set opaque. The rest of the clause is read as
	/// usual.
	#[test]
	fn an_index_knn_reads_only_the_row_id() {
		let fields = needed_fields_of;
		let set = |names: &[&str]| Some(names.iter().map(|n| n.to_string()).collect());
		assert_eq!(fields("SELECT id FROM t WHERE v <|3,40|> $q"), set(&["id"]));
		assert_eq!(
			fields("SELECT id, name FROM t WHERE v <|3,40|> $q AND n > 1"),
			set(&["id", "name", "n"])
		);
		assert_eq!(fields("SELECT id FROM t WHERE v <|3,40|> $q AND n > $m"), None);
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
}
