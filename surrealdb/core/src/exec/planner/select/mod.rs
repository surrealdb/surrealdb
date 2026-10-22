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

use std::sync::Arc;

pub(crate) use projection::output_field_path;
use surrealdb_strand::TableName;

use super::Planner;
use super::util::field_path_from_expr;
use crate::catalog::providers::TableProvider;
use crate::catalog::{IndexDefinition, KeyStructure, table_key};
use crate::exec::index::access_path::{
	AccessPath, BTreeAccess, BoundOperand, prefixes_are_disjoint,
};
use crate::exec::logical::reads::{declared_read_prefix, paths_overlap, reads_at_or_beneath};
use crate::exec::operators::SortDirection;
use crate::exec::operators::scan::MergeMode;
use crate::expr::Idiom;
use crate::expr::logical::{LogicalExpr, LogicalOrdering};
use crate::kvs::{Direction, Transaction};

impl<'ctx> Planner<'ctx> {
	// ========================================================================
	// Field Resolution Helpers
	// ========================================================================

	/// The namespace and database whose `DEFINE PARAM`s a parameter may be
	/// bound to at plan time: none where the statement inlines no catalog
	/// definition ([`super::CatalogWithin::calls_current`]), whose parameters are
	/// read when it runs.
	pub(crate) async fn definition_ids(
		&self,
	) -> Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)> {
		match self.within.calls_current {
			true => None,
			false => self.ns_db_ids().await,
		}
	}

	/// Resolve a parameter to its value at plan time.
	///
	/// Delegates to [`resolve_param_value`] with the planner's context and
	/// namespace/database IDs (looked up from the transaction when available).
	/// A name an enclosing `FOR`, closure, user-defined function or block
	/// `LET` binds has no plan-time value: the ambient one it shadows is not
	/// the one the statement reads.
	async fn resolve_param(&self, name: &str) -> Option<crate::val::Value> {
		let ns_db = self.definition_ids().await;
		super::util::resolve_param_value(
			name,
			&self.ctx,
			self.txn.as_ref(),
			ns_db,
			&self.row_scoped(),
		)
		.await
	}

	/// Resolve an `OMIT` or `FETCH` expression to the paths it names: a path
	/// as written, a parameter holding a path as text, or a projection
	/// function whose arguments name paths.
	pub(crate) async fn resolve_field_paths(
		&self,
		expr: LogicalExpr,
	) -> Result<Vec<LogicalExpr>, crate::err::Error> {
		if expr.is_path() {
			return Ok(vec![expr]);
		}
		if let LogicalExpr::Param(ref param) = expr {
			let value = self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
			let invalid = || crate::exec::Error::InvalidFetch {
				value: surrealdb_types::ToSql::to_sql(&value.clone().into_literal()),
			};
			let s = value.clone().coerce_to::<String>().map_err(|_| invalid())?;
			let path = parse_field_path(&s).ok_or_else(invalid)?;
			return Ok(vec![path]);
		}
		if let LogicalExpr::Call(ref call) = expr
			&& let crate::expr::Function::Normal(name) = &call.receiver
			&& self.function_registry().is_projection(name)
		{
			// Generic projection function handling: resolve each argument
			// as a string (single field) or array of strings (multiple fields)
			// and parse each as a path.
			let mut paths = Vec::new();
			for arg in &call.args {
				match self.resolve_expr_to_string(arg).await {
					Ok(s) => {
						paths.push(parse_field_path(&s).ok_or_else(|| {
							crate::exec::Error::Query {
								message: format!("Failed to parse field path '{s}'"),
							}
						})?);
					}
					Err(_) => {
						// Try resolving as an array of strings
						let strings =
							self.resolve_expr_to_string_array(arg).await.map_err(|_| {
								crate::exec::Error::Query {
									message: format!(
										"Projection function '{name}' argument could not \
										 be resolved to a field path",
									),
								}
							})?;
						for s in strings {
							paths.push(parse_field_path(&s).ok_or_else(|| {
								crate::exec::Error::Query {
									message: format!("Failed to parse field path '{s}'"),
								}
							})?);
						}
					}
				}
			}
			if paths.is_empty() {
				return Err(crate::exec::Error::Query {
					message: format!("Projection function '{name}' requires at least one argument",),
				}
				.into());
			}
			return Ok(paths);
		}
		Err(crate::exec::Error::InvalidFetch {
			value: surrealdb_types::ToSql::to_sql(&expr),
		}
		.into())
	}

	async fn resolve_expr_to_string(
		&self,
		expr: &LogicalExpr,
	) -> Result<String, crate::err::Error> {
		if let LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::String(s)) = expr {
			return Ok(s.as_str().to_owned());
		}
		if let LogicalExpr::Param(param) = expr {
			let value = self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
			return value.coerce_to::<String>().map_err(|_| {
				crate::exec::Error::Query {
					message: "OMIT/FETCH parameter did not resolve to a string".to_string(),
				}
				.into()
			});
		}
		Err(crate::exec::Error::Query {
			message: "OMIT/FETCH with computed expressions not yet supported".to_string(),
		}
		.into())
	}

	async fn resolve_expr_to_string_array(
		&self,
		expr: &LogicalExpr,
	) -> Result<Vec<String>, crate::err::Error> {
		if let LogicalExpr::Array(items) = expr {
			let mut result = Vec::with_capacity(items.len());
			for item in items {
				result.push(self.resolve_expr_to_string(item).await?);
			}
			return Ok(result);
		}
		if let LogicalExpr::Param(param) = expr {
			let value = self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
			return value.coerce_to::<Vec<String>>().map_err(|_| {
				crate::exec::Error::Query {
					message: "OMIT/FETCH parameter did not resolve to an array of strings"
						.to_string(),
				}
				.into()
			});
		}
		Err(crate::exec::Error::Query {
			message: "OMIT/FETCH with computed expressions not yet supported".to_string(),
		}
		.into())
	}

	// ========================================================================
	// Pipeline compilation with plan-time index resolution
	// ========================================================================

	// ========================================================================
	// Per-access-path source planners (called by plan_source)
	// ========================================================================

	/// Resolve which field-path prefixes on `table_name` are governed by a
	/// non-`Full` SELECT permission for the current actor. Shared by the
	/// WHERE-clause and ORDER BY plan-time guards.
	pub(crate) async fn resolve_restricted_select_prefixes(
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
	/// count or order element entries as rows. A declined select plan falls
	/// back to `DynamicScan`, which plans without b-tree indexes when the
	/// list is unreadable there too.
	pub(crate) async fn with_array_columns_as_elements(
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

	/// The structure of `table`'s record ids that access-path selection reads:
	/// the one [`table_key`] derives, from the `id` field where it names its
	/// components and from the group expressions of an aggregated view.
	/// `None` for every other table.
	pub(crate) async fn record_key_structure(
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
}

/// Removes every index whose columns are governed by a non-`Full` SELECT
/// permission for the current actor.
///
/// See [`RestrictedPrefixes::index_touches`] for why such an index must not
/// reach access-path selection. Returns the input untouched when nothing is
/// restricted, so the common case allocates nothing.
pub(crate) fn drop_restricted_indexes(
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
	pub(crate) fn cond_touches(&self, cond: &LogicalExpr) -> bool {
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
	pub(crate) fn index_touches(&self, cols: &[Idiom]) -> bool {
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
	/// Known gap: the prefix match catches ordering by a restricted field
	/// *or a descendant of it* (`ORDER BY meta` or `ORDER BY meta.sub` when
	/// `meta` is restricted). It does not catch ordering by a *parent* of a
	/// restricted child (`ORDER BY meta` when only `meta.secret` is
	/// restricted). With a covering index, that parent ordering can still
	/// encode the child's relative ordering. This vector is separate and is
	/// not guarded by this check.
	pub(crate) fn order_touches(&self, order: &LogicalOrdering) -> bool {
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
pub(crate) fn adjust_direction_for_order(
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
		BTreeAccess::Range {
			..
		}
		| BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => {
			vec![]
		}
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
		BTreeAccess::Range {
			..
		}
		| BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => 0,
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
			.map(|p| {
				if let AccessPath::BTreeScan {
					index_ref,
					access,
					..
				} = p
				{
					AccessPath::BTreeScan {
						index_ref,
						access,
						direction: target,
					}
				} else {
					p
				}
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
pub(crate) fn detect_order_for_composite_union(
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
	let AccessPath::BTreeScan {
		index_ref: first_index_ref,
		access: BTreeAccess::Compound {
			prefix: first_compound_prefix,
			range: None,
			..
		},
		..
	} = first
	else {
		return None;
	};
	let first_index_ref = first_index_ref.clone();
	let first_prefix_len = first_compound_prefix.len();
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
	let first_prefix = first_compound_prefix.as_slice();
	let mut seen_prefixes: Vec<&[BoundOperand]> = Vec::with_capacity(paths.len());
	seen_prefixes.push(first_prefix);
	for path in branches {
		let AccessPath::BTreeScan {
			index_ref: idx,
			access: BTreeAccess::Compound {
				prefix,
				range: None,
				..
			},
			..
		} = path
		else {
			return None;
		};
		if idx != &first_index_ref {
			return None;
		}
		if prefix.len() != first_prefix_len {
			return None;
		}
		if !seen_prefixes.iter().all(|earlier| prefixes_are_disjoint(earlier, prefix)) {
			return None;
		}
		seen_prefixes.push(prefix.as_slice());
	}

	Some((order_path, direction))
}

/// Parse a field path given as text (an `OMIT`/`FETCH` parameter, a
/// projection function argument) into a logical path. `None` when the text
/// is not a path.
fn parse_field_path(text: &str) -> Option<LogicalExpr> {
	let idiom = crate::syn::idiom(text).ok()?;
	let path = crate::expr::logical::lower_expr(crate::expr::Expr::Idiom(idiom)).ok()?;
	path.is_path().then_some(path)
}
