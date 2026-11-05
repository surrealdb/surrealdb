//! The access path of a table read: which physical op reads the rows of a source.
//!
//! [`select_access`] is a pure decision. It takes everything the decision reads from the catalog
//! ([`TableAnswers`]), the source's attributes, the predicate the analyzer reads
//! ([`Reconstructed`]) and the statement's ordering ([`SortKeys`]), and answers the physical op
//! that reads the source, the table enforcement that wraps it and the conjuncts of the predicate
//! that remain a residual of the filter above. It tries, in this order:
//!
//! 1. a record id source reads the record by its key;
//! 2. a locked read of a whole table is the refusal the planner raises;
//! 3. a table that is not defined reads nothing (`phys.vacant`);
//! 4. a conjunct `id = <record id>` reads the record by its key;
//! 5. a `VERSION` whose stamp only the run can evaluate scans the table, unless the predicate names
//!    an index operator;
//! 6. the planner's own decision after its catalog reads, [`decide_access`]: `WITH NOINDEX`, or a
//!    table with no index the actor may read, scans the table; otherwise the index analyzer's
//!    candidates, in the order the planner takes them. A decision that declines the plan-time path
//!    scans the table with every index withheld.
//!
//! Every conjunct of the predicate stays a residual, in its position, except those a `KNN` search
//! answers itself: the analysis decides which rows the access lists, never which rows the
//! statement returns. A conjunct an exact B-tree scan covers is dropped from its filter later, by
//! the physical rewrites (`answered_by_index`). A conjunct the analyzer cannot read is never
//! matched to an index; one that names an index operator (`MATCHES`, `KNN`) the reconstruction
//! could not read is an error, because no residual evaluates it.
//!
//! A read has one access path per plan. A choice at run time between access paths is built only by
//! [`super::choose::physical_choice`]; the decision here is made from catalog answers fixed at the
//! plan's snapshot, so it offers none.
//!
//! [`value_source`] answers the op that reads a source naming a value. [`convert_source`] is the
//! conversion pattern of `logical.source`: it reads the answers of the source's table read
//! ([`SourceKey`]) through the physical catalog the conversion driver hands it, the predicate and
//! ordering the statement offers ([`offer_of`]), and replaces the source by the op the decision
//! names. The one access path it does not build is a key range over the components of a record
//! id, which has no physical op; [`select_access`] answers it as an error.

#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use smallvec::SmallVec;
use surrealdb_datastore::index_shapes::TableIndexShapes;
use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql;

use super::access_expr::{Reconstructed, conjunct_values, expr_def, value_rebuilt_as};
use super::access_regions::{
	cone_is_expr, conjunct_region, conjunct_region_across, reads_region_args, region_copy,
	rowless_value_copy, value_copy,
};
use super::offer::{Offer, offer_across, offer_of};
use super::ops::{
	BitmapAttrs, BranchReadShape, FullTextScanAttrs, IdBoundsShape, IndexBranchShape,
	IndexScanAttrs, IndexUnionAttrs, KeyRangeScanAttrs, KnnFilterShape, KnnPermissionShape,
	KnnScanAttrs, PhysOp, ReadClauses, RecordScanAttrs, ScanProbes, TableScanAttrs,
};
use super::{keyed_record, knn};
use crate::catalog::{FieldDefinition, Index, IndexDefinition, KeyStructure};
use crate::exec::field_path::FieldPath;
use crate::exec::function::FunctionRegistry;
use crate::exec::index::access_path::{
	AccessInputs, AccessPath, BTreeAccess, ElementColumns, access_fans_out, admissible_indexes,
	analysis_condition, decide_access, with_array_columns_as_elements,
};
use crate::exec::index::analysis::KnnQueryVectors;
use crate::exec::index::id_range::plan_id_range;
use crate::exec::index::probe::probe_set;
use crate::exec::ir::build::phys::{
	bitmap_shape, id_bounds_shape, index_ref, key_range_shape, knn_filter_shape,
};
use crate::exec::ir::core::convert::{ConversionPattern, Converted};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::egraph::decl::{IndexCandidate as FactCandidate, IndexKindClass};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, KeyShape, RaiseKind, RecordIdAttrs};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::catalog::{
	CatalogAnswer, ExactAnswer, PhysCx, PhysicalCatalog, SemanticCatalog, SourceKey,
};
use crate::exec::ir::iface::choose::IndexRef;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::fold::FoldConsumer;
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{LogOp, LogTag, LoopKind, SourceAttrs, SourceRefShape};
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::logical::effects::Oracle;
use crate::exec::logical::{Descend, visit_binding_pruning};
use crate::exec::operators::scan::MergeMode;
use crate::exec::operators::scan::pipeline::determine_scan_direction;
use crate::exec::planner::util::{
	expr_contains_matches, extract_record_id_list_lookup, extract_record_id_point_lookup,
	field_path_from_expr, has_knn_operator, idiom_is_id, index_covers_ordering,
	is_record_id_list_lookup, strip_knn_and_matches_from_condition, strip_knn_from_condition,
};
use crate::exec::planner::{RestrictedPrefixes, decide_union_merge};
use crate::exec::vm::payload::{CountSelect, KnnFilter, KnnPrefilter, Refusal, ValuesRead};
use crate::exec::vm::plain::ValueData;
use crate::expr::logical::{
	LogicalExpr, LogicalLiteral, LogicalOrdering, LogicalRecordIdKey, Schema, effects_of,
};
use crate::expr::with::With;
use crate::expr::{BinaryOperator, Function};
use crate::kvs::Direction;
use crate::val::{Number, RecordId, RecordIdKey, Value};

/// Which fields of a table a non-`Full` `SELECT` permission governs for the actor.
///
/// An index over such a field is never a candidate, and an ordering by one is withheld from the
/// decision: an index answers a predicate, and orders its rows, from entries the field's
/// permission has not reduced. The analysis is [`RestrictedPrefixes`], the planner's own.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RestrictedSelect {
	/// The permission context of the table could not be resolved, so a restricted field may be
	/// named by any index or ordering. The access path is not decided from an index.
	Unresolved,
	/// No field is restricted.
	None,
	/// These declared fields are restricted; a read at, beneath or above one is a restricted read.
	Fields(Vec<crate::expr::logical::FieldPath>),
}

impl RestrictedSelect {
	/// The planner's form of the same answer.
	pub(crate) fn prefixes(&self) -> RestrictedPrefixes {
		match self {
			RestrictedSelect::Unresolved => RestrictedPrefixes::AssumeRestricted,
			RestrictedSelect::None => RestrictedPrefixes::None,
			RestrictedSelect::Fields(fields) => RestrictedPrefixes::Some(fields.clone()),
		}
	}
}

impl From<RestrictedPrefixes> for RestrictedSelect {
	fn from(prefixes: RestrictedPrefixes) -> Self {
		match prefixes {
			RestrictedPrefixes::AssumeRestricted => RestrictedSelect::Unresolved,
			RestrictedPrefixes::None => RestrictedSelect::None,
			RestrictedPrefixes::Some(fields) => RestrictedSelect::Fields(fields),
		}
	}
}

/// The version a read observes, as far as the plan knows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReadVersion {
	/// The read observes the current catalog and data.
	Current,
	/// The read carries a `VERSION` whose stamp is known at plan time; the table's indexes are
	/// those declared at the stamp. An index entry carries no version, so only a full-text index
	/// is a candidate.
	Stamped(u64),
	/// The read carries a `VERSION` whose stamp only the run can evaluate; no catalog names the
	/// indexes to choose from.
	Deferred,
}

/// Everything the decision reads from the catalog about the table a source names.
///
/// The catalog answers are read at the version the plan is bound to, by the readers the planner's
/// own access-path choice reads through. `indexes` holds only the indexes a read may be served
/// from: the ones that are online.
#[derive(Clone, Debug)]
pub(crate) struct TableAnswers {
	/// The table the answers describe.
	pub(crate) table: TableName,
	/// Whether the table is defined.
	pub(crate) defined: bool,
	/// Whether the table is a lightweight relation, which stores no record range to scan.
	pub(crate) lightweight: bool,
	/// Whether the table declares something a read must enforce on the rows it yields: a
	/// permission, a computed field or a field permission. The read enforces it itself unless a
	/// node above it does ([`Offer::enforced_above`]), so a row is judged once.
	pub(crate) enforces: bool,
	/// The online indexes of the table, at the stamp a `VERSION` read names.
	pub(crate) indexes: Arc<[IndexDefinition]>,
	/// The field definitions of the table: the kinds that decide which index columns store array
	/// elements and which accesses list exactly the records that satisfy their conjuncts.
	pub(crate) fields: Arc<[FieldDefinition]>,
	/// What the catalog declares for the rows of the table, as the planner resolved it; `None`
	/// reads as an open schema.
	pub(crate) schema: Option<Arc<Schema>>,
	/// The fields a non-`Full` `SELECT` permission governs.
	pub(crate) restricted: RestrictedSelect,
	/// The version the read observes.
	pub(crate) version: ReadVersion,
	/// The columns of the indexes a read may be served from, with every column that stores array
	/// elements spelled as an element column and every one that flattens a set withheld from
	/// containment: the answer the planner reads through
	/// [`with_array_columns_as_elements`](crate::exec::index::access_path::with_array_columns_as_elements).
	/// `None` when the field list or the indexes' recorded shapes could not be read, the case
	/// where the planner declines its plan-time index path.
	pub(crate) columns: Option<ElementColumns>,
	/// Under `WITH NOINDEX`, which leaves [`Self::indexes`] and [`Self::columns`] empty so that no
	/// index serves the read, the columns those would hold: read only for the selectivity probes
	/// of the table scan ([`crate::exec::index::probe`]), which decide how the scan is placed and
	/// never which rows it reads. `None` under any other hint.
	pub(crate) probe_columns: Option<ElementColumns>,
	/// The structure of the table's record ids, where they name their components or the table is
	/// an aggregated view.
	pub(crate) key: Option<Arc<KeyStructure>>,
	/// Whether a scan of the table delivers its rows in `id` order, the answer a read ordered by
	/// `id` alone takes: `false` where the record id names a component whose key bytes order its
	/// values coarsely, and for a versioned read, whose records may have been written under a
	/// structure the catalog no longer holds.
	pub(crate) id_order: bool,
	/// Whether a table permission predicate governs the caller's reads of the table, so that a
	/// key range over the components of the record id takes only literal bounds: a bound
	/// evaluated when the scan opens is evaluated in the caller's frame, not the definer's.
	pub(crate) permissioned: bool,
	/// Whether the caller may call `time::now()`, so that a comparison against it is an index
	/// bound.
	pub(crate) allows_clock: bool,
	/// What the table's `SELECT` permission does to a caller whose reads are checked, as a count
	/// of its records needs it: a table that is not defined is `Deny`.
	pub(crate) select: CountSelect,
	/// For a relation table that is not lightweight, what each adjacency entry of one of its edges
	/// carries of the edge's fields; `None` for any other table.
	pub(crate) inline: Option<Arc<InlineFields>>,
}

/// The `INLINE` fields of a relation table, as an adjacency entry carries them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InlineFields {
	/// The table's inline generation, which stamps every entry written under these fields.
	pub(crate) generation: u32,
	/// The raw name of every inline field, in the order an entry holds their values.
	pub(crate) names: Arc<[String]>,
}

impl TableAnswers {
	/// The answers for a table whose reads are not checked and which declares nothing: no index,
	/// no field, no key structure. `defined` says whether the catalog holds the table.
	pub(crate) fn plain(table: TableName, defined: bool) -> Self {
		let indexes: Arc<[IndexDefinition]> = Arc::from(Vec::new());
		Self {
			table,
			defined,
			lightweight: false,
			enforces: false,
			columns: Some(ElementColumns::unchanged(Arc::clone(&indexes))),
			probe_columns: None,
			indexes,
			fields: Arc::from(Vec::new()),
			schema: None,
			restricted: RestrictedSelect::None,
			version: ReadVersion::Current,
			key: None,
			id_order: true,
			permissioned: false,
			allows_clock: true,
			select: if defined {
				CountSelect::Allow
			} else {
				CountSelect::Deny
			},
			inline: None,
		}
	}

	/// The answers for a read of `table` at `version` planned with no knowledge of its
	/// definitions: the table may be defined, every row is enforced as the definitions the read
	/// meets declare, and no field, index or key structure is assumed.
	pub(crate) fn unknown(table: TableName, version: ReadVersion) -> Self {
		Self {
			enforces: true,
			restricted: RestrictedSelect::Unresolved,
			version,
			// The permission is the one the run meets, so a count runs the ordinary plan beside it.
			select: CountSelect::Conditional,
			..Self::plain(table, true)
		}
	}

	/// These answers with the indexes `indexes`, whose columns are read against the declared
	/// fields with no recorded shape and no foreign set.
	pub(crate) fn with_indexes(mut self, indexes: Vec<IndexDefinition>) -> Self {
		self.indexes = Arc::from(indexes);
		self.columns = Some(self.columns_over(&TableIndexShapes::default(), &[]));
		self
	}

	/// The reference to every index a read of the table may be served from, as the access-path
	/// decision carries it: the definition with its array columns read as elements, which columns
	/// hold no array, and which are coarse components of the record id. Empty when the columns
	/// could not be read, the case where the decision declines its plan-time path.
	pub(crate) fn index_refs(
		&self,
		registry: &FunctionRegistry,
	) -> Vec<crate::exec::index::access_path::IndexRef> {
		match self.probe_columns.as_ref().or(self.columns.as_ref()) {
			Some(columns) => {
				columns.analyzer(None, registry).with_key(self.key.clone()).index_refs()
			}
			None => Vec::new(),
		}
	}

	/// The columns of the indexes a read may be served from ([`access_path::admissible_indexes`]),
	/// read against the declared fields, the recorded `shapes` and the `foreign_sets` other tables
	/// declare.
	pub(crate) fn columns_over(
		&self,
		shapes: &TableIndexShapes,
		foreign_sets: &[Vec<crate::expr::Part>],
	) -> ElementColumns {
		let admissible = admissible_indexes(
			Arc::clone(&self.indexes),
			&self.restricted.prefixes(),
			self.version != ReadVersion::Current,
		);
		with_array_columns_as_elements(admissible, &self.fields, shapes)
			.withholding_sets_at(foreign_sets)
	}
}

/// What the statement asks of the order of the rows its read delivers.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SortKeys {
	/// The `ORDER BY` of the statement.
	pub(crate) ordering: Option<LogicalOrdering>,
	/// Whether a `LIMIT` can end the read early: the statement pages its rows and does not group
	/// them.
	pub(crate) limited: bool,
	/// The literal count `k` of a `LIMIT k` without `START`, when the statement states one.
	pub(crate) limit: Option<u32>,
}

/// The conjuncts a `KNN` scan judges itself, by position in the predicate.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct KnnConjuncts {
	/// The conjuncts without a `KNN` operator or a `MATCHES`, judged inside the search: the
	/// residual region of the scan.
	pub(crate) residual: Vec<usize>,
	/// The conjuncts without a `KNN` operator, judged where the allow-list cannot cover a
	/// `MATCHES`: the `MATCHES` condition region of the scan.
	pub(crate) matches_condition: Vec<usize>,
	/// The conjuncts the pre-filter's bitmap does not cover: the pre-filter's residual region.
	pub(crate) prefilter_residual: Vec<usize>,
}

/// The access path a source reads through.
#[derive(Clone, Debug)]
pub(crate) struct AccessChoice {
	/// The op that reads the rows.
	pub(crate) access: PhysOp,
	/// The index union a bitmap fusion of an `OR` replaced, which answers when a branch of the
	/// bitmap overflows: the region of the `phys.bitmap` op.
	pub(crate) fallback: Option<PhysOp>,
	/// The table whose enforcement wraps the read in a `phys.enforced`, when the read enforces.
	pub(crate) enforce: Option<TableName>,
	/// The positions of the conjuncts that remain in the filter above the read, in order.
	pub(crate) residual: Vec<usize>,
	/// The position of the conjunct `id = <record id>` whose record id a point read takes as its
	/// operand, or of the conjunct `id IN <list>` whose record ids it takes, when the read is one.
	pub(crate) point_lookup: Option<usize>,
	/// The conjuncts a `KNN` scan judges, when the read is one.
	pub(crate) knn: Option<KnnConjuncts>,
	/// The expressions of the deferred bounds of a key-range scan, in the order of its operand
	/// slots: the operands the conversion binds beside the scan, in the source's frame. Empty for
	/// every other read.
	pub(crate) key_operands: Vec<LogicalExpr>,
	/// The refusal the statement is raised as, when the read is one the planner refuses to run: a
	/// locked read of a whole table. The access op is then never installed.
	pub(crate) refusal: Option<Refusal>,
}

/// What the access-path decision reads besides the answers about the table.
pub(crate) struct DecisionCx<'a> {
	/// The function registry the analysis resolves projection function calls against.
	pub(crate) registry: &'a FunctionRegistry,
	/// The query vectors of the `KNN` operators whose right operand is a parameter that holds an
	/// array of numbers at plan time.
	pub(crate) knn_vectors: &'a KnnQueryVectors,
	/// Whether a table scan the decision chooses may be narrowed to the record ids the predicate
	/// bounds: the read is not the subject of a write, whose stages judge each stored record
	/// themselves.
	pub(crate) id_bounds: bool,
}

/// The decision before it is mapped to ops.
struct Decision {
	path: AccessPath,
	direction: Direction,
	/// What the read keeps of the source it is chosen for.
	read: ReadClauses,
	/// Whether the ordering was withheld from the decision because it names a restricted field.
	order_withheld: bool,
}

/// The positions `0..len`.
fn every_position(len: usize) -> Vec<usize> {
	(0..len).collect()
}

/// The access path of the table source `source` reads.
///
/// The decision after the catalog reads is [`decide_access`], the planner's own. Every source has
/// an answer:
///
/// - a locked read of a whole table is the refusal the planner raises ([`AccessChoice::refusal`]);
/// - a table that is not defined reads nothing;
/// - a source whose plan-time path the decision declines (an unresolved permission context, or
///   columns that could not be read) is read by a table scan that withholds every index and keeps
///   the whole predicate as a residual. The scan enforces the table where the answers say the read
///   does, and a source whose bind resolved it has the permission and layer nodes above it, so
///   every row passes the field guard exactly once. A `MATCHES` or `KNN` conjunct raises the error
///   of a table without that index.
///
/// The error is a source that reads no table ([`IrError::Stale`]). A `KNN` or `MATCHES` operator is
/// always in the predicate the decision reads, because the reconstruction reads every conjunct that
/// names one whole.
pub(crate) fn select_access(
	source: &SourceAttrs,
	table: &TableAnswers,
	predicate: &Reconstructed,
	sort: Option<&SortKeys>,
	cx: &DecisionCx<'_>,
) -> Result<AccessChoice, IrError> {
	// A table the catalog does not define, read where it may be defined by the time the read runs,
	// is read as the catalog then stands: the read enforces whatever the definition it finds
	// declares.
	let undefined_until_run = !table.defined && source.may_be_defined;
	let enforce = || {
		(table.enforces || undefined_until_run || source.may_meet_change)
			.then(|| table.table.clone())
	};
	let all = every_position(predicate.len());
	let finished = |access: PhysOp| AccessChoice {
		access,
		fallback: None,
		enforce: enforce(),
		residual: all.clone(),
		point_lookup: None,
		knn: None,
		refusal: None,
		key_operands: Vec::new(),
	};
	match &source.source_ref {
		SourceRefShape::RecordId => {
			return Ok(finished(PhysOp::RecordScan(Box::new(RecordScanAttrs {
				table: table.table.clone(),
				for_update: source.for_update,
				lightweight: table.lightweight && !source.for_update,
				read: read_clauses(table, source.with.as_ref()),
			}))));
		}
		SourceRefShape::Table(name) if *name == table.table => {}
		SourceRefShape::Table(_)
		| SourceRefShape::Mock
		| SourceRefShape::Param
		| SourceRefShape::Dynamic => {
			return Err(IrError::Stale("a source that reads another table or a value"));
		}
	}
	if source.for_update {
		return Ok(AccessChoice {
			enforce: None,
			refusal: Some(Refusal::Query {
				message: crate::dbs::FOR_UPDATE_TARGETS_ERROR.to_string(),
			}),
			..finished(PhysOp::Vacant {
				table: table.table.clone(),
			})
		});
	}
	if !table.defined && !source.may_be_defined {
		return Ok(AccessChoice {
			enforce: None,
			..finished(PhysOp::Vacant {
				table: table.table.clone(),
			})
		});
	}
	let cond = predicate.condition();
	// A composite key is read as written, so it names the stored record only where the table
	// declares no structure its record ids are coerced to.
	let names_record = |record: &LogicalExpr| {
		let composite = matches!(
			record,
			LogicalExpr::RecordId(record)
				if matches!(record.key, LogicalRecordIdKey::Array(_) | LogicalRecordIdKey::Object(_))
		);
		!composite || table.key.is_none()
	};
	if !cond.as_ref().is_some_and(has_knn_operator)
		&& let Some(at) = predicate.conjuncts.iter().find_map(|(at, conjunct)| {
			point_lookup_record(conjunct, &table.table, cx.registry)
				.filter(|record| names_record(record))
				.map(|_| *at)
		}) {
		return Ok(AccessChoice {
			point_lookup: Some(at),
			..finished(PhysOp::RecordScan(Box::new(RecordScanAttrs {
				table: table.table.clone(),
				for_update: false,
				lightweight: table.lightweight,
				read: read_clauses(table, source.with.as_ref()),
			})))
		});
	}
	// `id IN <list>` reads the listed records of the table by key, in key order, as the table scan
	// it replaces yields them. The whole predicate stays above the read. A lightweight relation
	// reads one record by its own source, the subjects of a write are read by a scan their stages
	// judge, and a table the catalog does not define, or a read that meets a catalog change, is
	// scanned as the definitions it meets when it runs require.
	if cx.id_bounds
		&& table.defined
		&& !source.may_meet_change
		&& !table.lightweight
		&& table.key.is_none()
		&& !cond.as_ref().is_some_and(has_knn_operator)
		&& let Some(at) = predicate.conjuncts.iter().find_map(|(at, conjunct)| {
			is_record_id_list_lookup(conjunct, &table.table).then_some(*at)
		}) {
		return Ok(AccessChoice {
			point_lookup: Some(at),
			..finished(PhysOp::RecordScan(Box::new(RecordScanAttrs {
				table: table.table.clone(),
				for_update: false,
				lightweight: false,
				read: read_clauses(table, source.with.as_ref()),
			})))
		});
	}
	let index_operator =
		cond.as_ref().is_some_and(|cond| expr_contains_matches(cond) || has_knn_operator(cond));
	if table.version == ReadVersion::Deferred && !index_operator {
		return Ok(finished(table_scan(
			table,
			Direction::Forward,
			read_clauses(table, source.with.as_ref()),
		)));
	}
	let offered = sort.and_then(|sort| sort.ordering.as_ref());
	let limited = sort.is_some_and(|sort| sort.limited);
	let limit = sort.and_then(|sort| sort.limit);
	let prefixes = table.restricted.prefixes();
	let (ordering, order_withheld) = match offered {
		Some(order) if prefixes.order_touches(order) => (None, true),
		other => (other, false),
	};
	let read = read_clauses(table, source.with.as_ref());
	// A read that meets a catalog change when it runs enforces definitions the answers do not
	// hold, and a read of a table the catalog does not define may find one defined.
	let id_bounds = cx.id_bounds && table.defined && !source.may_meet_change;
	match decide_with_limit(
		table,
		source.with.as_ref(),
		cond.as_ref(),
		ordering,
		limited,
		limit,
		cx,
	)? {
		Some((path, direction)) => {
			let mut choice = map_decision(
				Decision {
					path,
					direction,
					read,
					order_withheld,
				},
				table,
				predicate,
				cond.as_ref(),
				offered,
				id_bounds.then_some(cx.registry),
			)?;
			if let PhysOp::TableScan(attrs) = &mut choice.access
				&& !attrs.lightweight
			{
				attrs.probes = scan_probes(table, cond.as_ref(), cx.registry).map(Box::new);
			}
			Ok(choice)
		}
		None => Ok(finished(table_scan(table, determine_scan_direction(ordering), read))),
	}
}

/// The selectivity probes of a table scan of `table` under the predicate `cond`
/// ([`crate::exec::index::probe`]), or `None` where no conjunct is one an index answers.
///
/// The probes read the indexes the access-path decision reads, whatever the `WITH` hint, since a
/// hint chooses the read and not what the indexes say of the rows. They read only what the
/// catalog states: a conjunct over a field the caller may not read is given no probe, and a read
/// at a `VERSION` none, because an index entry holds the state the last write left rather than
/// the state at the stamp.
fn scan_probes(
	table: &TableAnswers,
	cond: Option<&LogicalExpr>,
	registry: &FunctionRegistry,
) -> Option<ScanProbes> {
	if table.version != ReadVersion::Current
		|| matches!(table.restricted, RestrictedSelect::Unresolved)
	{
		return None;
	}
	let columns = table.probe_columns.as_ref().or(table.columns.as_ref())?;
	let cond = analysis_condition(cond, registry)?;
	let prefixes = table.restricted.prefixes();
	let analyzer =
		columns.analyzer(None, registry).with_key(table.key.clone()).with_clock(table.allows_clock);
	let set = probe_set(&analyzer, &cond, &|conjunct| !prefixes.cond_touches(conjunct))?;
	set.try_map(|index| Ok::<_, std::convert::Infallible>(index_ref(index))).ok()
}

/// The record-id bounds a table scan of `table` takes from `predicate`, with the expressions of
/// its deferred bounds in operand-slot order, or `None` when it takes none.
///
/// A table that declares a key structure reads its predicate as key ranges, a lightweight relation
/// stores no record range, a read at a `VERSION` keeps every key the stamp may have written, and a
/// table that enforces something at run time (a permission, a field permission or a computed
/// field, [`TableAnswers::enforces`]) would judge rows the bounds skip, so a predicate that reads a
/// computed field is never planned against the bounds.
fn id_bounds_of(
	table: &TableAnswers,
	predicate: &Reconstructed,
	registry: &FunctionRegistry,
) -> Result<Option<(IdBoundsShape, Vec<LogicalExpr>)>, IrError> {
	if table.key.is_some()
		|| table.lightweight
		|| table.version != ReadVersion::Current
		|| table.permissioned
		|| table.enforces
	{
		return Ok(None);
	}
	let conjuncts: Vec<std::borrow::Cow<'_, LogicalExpr>> =
		predicate.conjuncts.iter().map(|(_, conjunct)| id_projected(conjunct, registry)).collect();
	let refs: Vec<&LogicalExpr> = conjuncts.iter().map(|conjunct| conjunct.as_ref()).collect();
	let Some(access) = plan_id_range(
		&table.table,
		&refs,
		!predicate.withheld.is_empty(),
		table.permissioned,
		registry,
	) else {
		return Ok(None);
	};
	let (shape, operands) = id_bounds_shape(&access)?;
	Ok(Some((shape, operands.into_iter().cloned().collect())))
}

/// `conjunct` with a side that reads `id` as `type::field('id')` ([`projected_field`]) written as
/// `id`, as the access path reads it.
fn id_projected<'a>(
	conjunct: &'a LogicalExpr,
	registry: &FunctionRegistry,
) -> std::borrow::Cow<'a, LogicalExpr> {
	let LogicalExpr::Binary {
		left,
		op,
		right,
	} = conjunct
	else {
		return std::borrow::Cow::Borrowed(conjunct);
	};
	let read = |side: &LogicalExpr| projected_field(side, registry).filter(idiom_is_id);
	let (read_left, read_right) = (read(left), read(right));
	if read_left.is_none() && read_right.is_none() {
		return std::borrow::Cow::Borrowed(conjunct);
	}
	std::borrow::Cow::Owned(LogicalExpr::Binary {
		left: Box::new(read_left.unwrap_or_else(|| (**left).clone())),
		op: op.clone(),
		right: Box::new(read_right.unwrap_or_else(|| (**right).clone())),
	})
}

/// The record id the conjunct `conjunct` reads `table` by, when it is `id = <record id>`
/// ([`extract_record_id_point_lookup`]) once a side that is a projection function call naming a
/// field by a string literal, `type::field('id')`, is read as that field ([`projected_field`]), as
/// the index analysis reads it. A composite key names one record for the whole statement only
/// when each of its elements is a literal or a parameter; a key that reads the row names none.
fn point_lookup_record(
	conjunct: &LogicalExpr,
	table: &TableName,
	registry: &FunctionRegistry,
) -> Option<LogicalExpr> {
	extract_record_id_point_lookup(conjunct, table)
		.or_else(|| {
			let LogicalExpr::Binary {
				left,
				op: op @ (BinaryOperator::Equal | BinaryOperator::ExactEqual),
				right,
			} = conjunct
			else {
				return None;
			};
			let (left, right) =
				match (projected_field(left, registry), projected_field(right, registry)) {
					(None, None) => return None,
					(read_left, read_right) => (
						read_left.unwrap_or_else(|| (**left).clone()),
						read_right.unwrap_or_else(|| (**right).clone()),
					),
				};
			let read = LogicalExpr::Binary {
				left: Box::new(left),
				op: op.clone(),
				right: Box::new(right),
			};
			extract_record_id_point_lookup(&read, table)
		})
		.filter(key_is_fixed)
}

/// The field `expr` names when it is a call of a projection function (`type::field`) whose one
/// argument is a string literal that spells a path.
fn projected_field(expr: &LogicalExpr, registry: &FunctionRegistry) -> Option<LogicalExpr> {
	let LogicalExpr::Call(call) = expr else {
		return None;
	};
	let Function::Normal(name) = &call.receiver else {
		return None;
	};
	if !registry.is_projection(name) {
		return None;
	}
	let [LogicalExpr::Literal(LogicalLiteral::String(text))] = call.args.as_slice() else {
		return None;
	};
	let idiom = crate::syn::idiom(text.as_str()).ok()?;
	crate::expr::logical::lower_expr(crate::expr::Expr::Idiom(idiom))
		.ok()
		.filter(LogicalExpr::is_path)
}

/// Whether the key of the record id `record` is fixed for the statement: a scalar key, or a
/// composite key whose every element is a literal or a parameter.
fn key_is_fixed(record: &LogicalExpr) -> bool {
	let fixed =
		|element: &LogicalExpr| matches!(element, LogicalExpr::Literal(_) | LogicalExpr::Param(_));
	let LogicalExpr::RecordId(record) = record else {
		return true;
	};
	match &record.key {
		LogicalRecordIdKey::Array(items) => items.iter().all(fixed),
		LogicalRecordIdKey::Object(entries) => entries.iter().all(|(_, item)| fixed(item)),
		LogicalRecordIdKey::Number(_)
		| LogicalRecordIdKey::String(_)
		| LogicalRecordIdKey::Uuid(_)
		| LogicalRecordIdKey::Generate(_)
		| LogicalRecordIdKey::Range(_) => true,
	}
}

/// What a read of `table` under `with` keeps of its source.
fn read_clauses(table: &TableAnswers, with: Option<&With>) -> ReadClauses {
	ReadClauses {
		schema: table.schema.clone(),
		with: with.cloned(),
	}
}

/// The table scan of `table` along `direction`.
fn table_scan(table: &TableAnswers, direction: Direction, read: ReadClauses) -> PhysOp {
	PhysOp::TableScan(Box::new(TableScanAttrs {
		probes: None,
		bounds: None,
		prefilter: None,
		table: table.table.clone(),
		direction,
		topk: None,
		lightweight: table.lightweight,
		id_order: table.id_order,
		read,
	}))
}

// ---------------------------------------------------------------------------------------------
// The decision
// ---------------------------------------------------------------------------------------------

/// The access path for a table with the condition `cond` and ordering `order` offered to it, the
/// decision [`decide_access`] makes from the table's answers. `order` is the ordering the decision
/// may use: an ordering over a restricted field is withheld by the caller.
///
/// A `LIMIT` without an `ORDER BY` and a read at a `VERSION` keep the streaming plans: a bitmap
/// plan materializes every branch first and its document ids are not versioned.
pub(crate) fn decide(
	table: &TableAnswers,
	with: Option<&With>,
	cond: Option<&LogicalExpr>,
	order: Option<&LogicalOrdering>,
	limited: bool,
	cx: &DecisionCx<'_>,
) -> Result<Option<(AccessPath, Direction)>, IrError> {
	decide_with_limit(table, with, cond, order, limited, None, cx)
}

fn decide_with_limit(
	table: &TableAnswers,
	with: Option<&With>,
	cond: Option<&LogicalExpr>,
	order: Option<&LogicalOrdering>,
	limited: bool,
	limit: Option<u32>,
	cx: &DecisionCx<'_>,
) -> Result<Option<(AccessPath, Direction)>, IrError> {
	let prefixes = table.restricted.prefixes();
	let inputs = AccessInputs {
		indexes: Arc::clone(&table.indexes),
		restricted: &prefixes,
		key: table.key.clone(),
		columns: table.columns.clone(),
		knn_vectors: cx.knn_vectors,
		allows_clock: table.allows_clock,
		reads_versioned: table.version != ReadVersion::Current,
		permissioned: table.permissioned,
		limit,
	};
	decide_access(&inputs, cond, order, with, limited, cx.registry).map_err(|_| {
		IrError::Invariant {
			what: "access::decide: the access-path decision failed",
		}
	})
}

/// The admissible access candidates of a table read, as the e-graph's physical facts carry them:
/// `None` when no index may be chosen for the read (an unresolved permission context, or columns
/// that could not be read), an empty set under `WITH NOINDEX`, otherwise one candidate per
/// admissible index with its array columns read as elements. Under `WITH INDEX` only the named
/// indexes are candidates, and every vector index, since a `KNN` operator has no answer but its
/// index.
///
/// The indexes are those [`decide_access`] chooses among, so the facts and the decision never
/// disagree on which indexes are candidates.
pub(crate) fn admissible_access_candidates(
	table: &TableAnswers,
	with: Option<&With>,
) -> Option<SmallVec<[FactCandidate; 4]>> {
	if matches!(with, Some(With::NoIndex)) {
		return Some(SmallVec::new());
	}
	if matches!(table.restricted, RestrictedSelect::Unresolved) {
		return None;
	}
	let columns = table.columns.as_ref()?;
	Some(
		columns
			.indexes
			.iter()
			.enumerate()
			.filter(|(_, ix)| match with {
				Some(With::Index(names)) => {
					matches!(ix.index, Index::Hnsw(_) | Index::DiskAnn(_))
						|| names.iter().any(|name| name.as_str() == ix.name.as_str())
				}
				Some(With::NoIndex) | None => true,
			})
			.map(|(at, ix)| FactCandidate {
				index: IndexRef::of(
					ix,
					at,
					columns
						.scalar
						.as_ref()
						.and_then(|flags| flags.get(at))
						.map_or(&[][..], |flags| &flags[..]),
				),
				columns: ix.cols.iter().map(|col| Strand::from(col.to_sql())).collect(),
				unique: matches!(ix.index, Index::Uniq),
				kind: index_kind_class(&ix.index),
			})
			.collect(),
	)
}

/// The family of an index. Every variant of [`Index`] names its class here.
fn index_kind_class(index: &Index) -> IndexKindClass {
	match index {
		Index::Idx | Index::Uniq => IndexKindClass::BTree,
		Index::FullText(_) => IndexKindClass::FullText,
		Index::Hnsw(_) => IndexKindClass::Hnsw,
		Index::DiskAnn(_) => IndexKindClass::DiskAnn,
		Index::Count(_) => IndexKindClass::Count,
		Index::Spatial => IndexKindClass::Spatial,
	}
}

// ---------------------------------------------------------------------------------------------
// The decision as ops
// ---------------------------------------------------------------------------------------------

/// Whether reading no row skips no evaluation the predicate owes: every conjunct the analysis
/// never read stands after a conjunct that is the literal `false`, which `AND` evaluates first and
/// which stops the conjuncts after it. A withheld conjunct before it is evaluated for every row and
/// may raise, so the rows are read for it.
fn empties_unobserved(predicate: &Reconstructed) -> bool {
	let first_false = predicate
		.conjuncts
		.iter()
		.filter(|(_, expr)| matches!(expr, LogicalExpr::Literal(LogicalLiteral::Bool(false))))
		.map(|(at, _)| *at)
		.min();
	match first_false {
		Some(at) => predicate.withheld.iter().all(|withheld| *withheld > at),
		None => true,
	}
}

/// `predicate` with each conjunct the physical catalog holds a record fold for replaced by it, or
/// `None` where it holds none.
fn record_folded(
	predicate: &Reconstructed,
	physical: &dyn PhysicalCatalog,
) -> Option<Reconstructed> {
	let mut folded = None;
	for (at, (_, conjunct)) in predicate.conjuncts.iter().enumerate() {
		if let Some(replaced) = physical.record_folded(conjunct) {
			folded.get_or_insert_with(|| predicate.clone()).conjuncts[at].1 = replaced;
		}
	}
	folded
}

/// Whether `predicate` holds the literal `false` and no conjunct before it is withheld: no row
/// passes it, and reading none raises nothing a conjunct judged first would.
fn settled_false(predicate: &Reconstructed) -> bool {
	predicate
		.conjuncts
		.iter()
		.any(|(_, expr)| matches!(expr, LogicalExpr::Literal(LogicalLiteral::Bool(false))))
		&& empties_unobserved(predicate)
}

/// Whether a write under `predicate` takes no subject: [`settled_false`], and every conjunct
/// before the first literal `false` only reads. A conjunct that does anything else, such as a
/// call that advances state or draws entropy, is evaluated for each subject the write reads, so
/// the subjects are read for it.
fn write_settled_false(predicate: &Reconstructed, registry: &FunctionRegistry) -> bool {
	let first_false = predicate
		.conjuncts
		.iter()
		.find(|(_, expr)| matches!(expr, LogicalExpr::Literal(LogicalLiteral::Bool(false))))
		.map(|(at, _)| *at);
	let Some(first_false) = first_false else {
		return false;
	};
	let oracle = Oracle::new(registry);
	settled_false(predicate)
		&& predicate
			.conjuncts
			.iter()
			.filter(|(at, _)| *at < first_false)
			.all(|(_, expr)| effects_of(expr, &oracle).effect_free())
}

/// The op of the decision, and the residual it leaves.
fn map_decision(
	decision: Decision,
	table: &TableAnswers,
	predicate: &Reconstructed,
	cond: Option<&LogicalExpr>,
	offered: Option<&LogicalOrdering>,
	id_bounds: Option<&FunctionRegistry>,
) -> Result<AccessChoice, IrError> {
	let Decision {
		path,
		direction,
		read,
		order_withheld,
	} = decision;
	let mut choice = AccessChoice {
		access: table_scan(table, direction, read.clone()),
		fallback: None,
		enforce: table.enforces.then(|| table.table.clone()),
		residual: every_position(predicate.len()),
		point_lookup: None,
		knn: None,
		refusal: None,
		key_operands: Vec::new(),
	};
	// A union is planned with the ordering the choice was made with.
	let union_order = if order_withheld {
		None
	} else {
		offered
	};
	match path {
		// A table scan under a predicate that bounds `id` reads the stretch of keys between the
		// bounds; the whole predicate stays above the scan.
		AccessPath::TableScan => {
			if let Some(registry) = id_bounds
				&& read.with.is_none()
				&& let Some((shape, operands)) = id_bounds_of(table, predicate, registry)?
				&& let PhysOp::TableScan(scan) = &mut choice.access
			{
				scan.bounds = Some(shape);
				choice.key_operands = operands;
			}
		}
		AccessPath::EmptyScan if empties_unobserved(predicate) => choice.access = PhysOp::Union,
		AccessPath::EmptyScan => {}
		// The whole predicate stays above the range, as the planner keeps it: the range lists a
		// superset of the rows the conjuncts admit.
		AccessPath::KeyRange(access) => {
			// A key structure comes from an `id` field, which a lightweight relation cannot
			// define, so the scan of its key range never meets one.
			if table.lightweight {
				return Err(invariant("a key-range scan over a lightweight relation"));
			}
			let (shape, operands) = key_range_shape(&access)?;
			choice.key_operands = operands.into_iter().cloned().collect();
			choice.access = PhysOp::KeyRangeScan(Box::new(KeyRangeScanAttrs {
				prefilter: None,
				table: table.table.clone(),
				read,
				access: shape,
				topk: None,
			}));
		}
		AccessPath::BTreeScan {
			index_ref: index,
			access,
			direction,
		} => {
			// A scan whose key range leaves a fanning column open reaches a record through
			// several entries, so it delivers no ordering.
			let fans_out = access_fans_out(&index.cols, &access);
			let claims_order = !order_withheld && !fans_out;
			let ordered_by = match offered {
				Some(order @ LogicalOrdering::Order(keys))
					if claims_order && index_covers_ordering(&index, &access, direction, order) =>
				{
					keys.iter().map(|key| field_path_from_expr(&key.value).ok()).collect()
				}
				Some(LogicalOrdering::Order(_) | LogicalOrdering::Random) | None => None,
			};
			let pinned = if !fans_out && answers_exactly(&index, &access) {
				pinned_paths(&index, &access)
			} else {
				None
			};
			let stated = (!crate::exec::index::derived_columns::range_is_widened(&index, &access))
				.then(|| index.cols.clone());
			choice.access = PhysOp::IndexScan(Box::new(IndexScanAttrs {
				table: table.table.clone(),
				index: index_ref(&index),
				access,
				direction,
				ordered_by,
				pinned,
				stated,
				rechecked: false,
				decode: None,
				read,
			}));
		}
		AccessPath::Union {
			paths,
			dedupe,
		} => choice.access = index_union(table, paths, dedupe, union_order, &read)?,
		AccessPath::BitmapFusion {
			root,
			fallback,
		} => {
			choice.fallback = match fallback.map(|fallback| *fallback) {
				Some(AccessPath::Union {
					paths,
					dedupe,
				}) => Some(index_union(table, paths, dedupe, union_order, &read)?),
				Some(AccessPath::KeyRange(_)) => {
					return Err(invariant("a key range as a bitmap fallback"));
				}
				Some(
					AccessPath::TableScan
					| AccessPath::EmptyScan
					| AccessPath::BTreeScan {
						..
					}
					| AccessPath::FullTextSearch {
						..
					}
					| AccessPath::KnnSearch {
						..
					}
					| AccessPath::BitmapFusion {
						..
					},
				) => {
					return Err(IrError::Stale("a bitmap fallback that is not a union"));
				}
				None => None,
			};
			choice.access = PhysOp::Bitmap(Box::new(BitmapAttrs {
				table: table.table.clone(),
				root: bitmap_shape(&root),
				decode: None,
				fallback: choice.fallback.is_some(),
				read,
			}));
		}
		AccessPath::FullTextSearch {
			index_ref: index,
			query,
			operator,
		} => {
			choice.access = PhysOp::FullTextScan(Box::new(FullTextScanAttrs {
				table: table.table.clone(),
				index: index_ref(&index),
				query,
				operator,
				column: index.cols.first().cloned(),
				decode: None,
				read,
				topk: None,
			}));
		}
		AccessPath::KnnSearch {
			index_ref: index,
			vector,
			k,
			ef,
			prefilter,
		} => {
			// The search judges every candidate against the statement's other conditions, so the
			// scan carries them in its regions and its `k` nearest are `k` the whole predicate
			// admits. The operator itself is answered by the search. The permissions it also
			// judges are given when the source is converted (`give_knn_conditions`), which reads
			// the permission filters above it.
			//
			// A withheld conjunct, which the analysis never read, is judged in every condition
			// region of the search: it names no index operator, so no allow-list covers it.
			let stripped = cond.and_then(strip_knn_from_condition);
			let residual = cond.and_then(strip_knn_and_matches_from_condition);
			let needs_matches = !index.definition().uses_shared_doc_ids()
				|| stripped.as_ref().is_some_and(expr_contains_matches);
			let matches_condition = if needs_matches {
				stripped
			} else {
				None
			};
			let with_withheld = |mut at: Vec<usize>| {
				at.extend_from_slice(&predicate.withheld);
				at.sort_unstable();
				at
			};
			let knn = KnnConjuncts {
				residual: with_withheld(kept_positions(
					predicate,
					strip_knn_and_matches_from_condition,
				)),
				matches_condition: if needs_matches {
					with_withheld(kept_positions(predicate, strip_knn_from_condition))
				} else {
					Vec::new()
				},
				prefilter_residual: match &prefilter {
					Some(plan) => with_withheld(uncovered_positions(predicate, &plan.covered)?),
					None => Vec::new(),
				},
			};
			let prefilter = prefilter.map(|plan| KnnPrefilter {
				root: plan.root,
				residual: plan.residual,
				uncovered_matches: plan.uncovered_matches,
			});
			let filter = (!knn.residual.is_empty()
				|| !knn.matches_condition.is_empty()
				|| prefilter.is_some())
			.then(|| {
				let mut shape = knn_filter_shape(&KnnFilter {
					permissions: Vec::new(),
					residual,
					matches_condition,
					prefilter,
				});
				shape.residual = !knn.residual.is_empty();
				shape.matches_condition = !knn.matches_condition.is_empty();
				if let Some(prefilter) = &mut shape.prefilter {
					prefilter.residual = !knn.prefilter_residual.is_empty();
				}
				shape
			});
			choice.residual = knn_residual(predicate)?;
			choice.knn = Some(knn);
			choice.access = PhysOp::KnnScan(Box::new(KnnScanAttrs {
				table: table.table.clone(),
				index: index_ref(&index),
				vector,
				vector_text: None,
				k,
				ef,
				filter,
				ids_only: false,
				read,
			}));
		}
	}
	Ok(choice)
}

/// The positions of the readable conjuncts of `predicate` that `strip` keeps. A stripper walks the
/// condition's `AND` chain and judges each conjunct on its own, so a conjunct is kept, at its own
/// position, exactly when stripping it alone leaves something.
fn kept_positions(
	predicate: &Reconstructed,
	strip: fn(&LogicalExpr) -> Option<LogicalExpr>,
) -> Vec<usize> {
	predicate
		.conjuncts
		.iter()
		.filter(|(_, conjunct)| strip(conjunct).is_some())
		.map(|(at, _)| *at)
		.collect()
}

/// The positions of the readable conjuncts of `predicate` that a `KNN` scan judges inside its
/// search beside the allow-list: the conjuncts it neither answers nor leaves to a `MATCHES`
/// condition, less those the allow-list `covered` covers.
///
/// `covered` indexes the readable conjuncts in order, as the access-path decision numbers the
/// conjuncts of the condition it reads ([`Reconstructed::condition`]); an index with no readable
/// conjunct is an [`IrError::Invariant`].
fn uncovered_positions(
	predicate: &Reconstructed,
	covered: &[usize],
) -> Result<Vec<usize>, IrError> {
	let mut judged = kept_positions(predicate, strip_knn_and_matches_from_condition);
	for index in covered {
		let Some((at, _)) = predicate.conjuncts.get(*index) else {
			return Err(IrError::Invariant {
				what: "access::select_access: a covered conjunct the predicate does not have",
			});
		};
		judged.retain(|kept| kept != at);
	}
	Ok(judged)
}

/// The conjuncts that remain in the filter above a `KNN` scan: the withheld ones and every
/// readable conjunct the search does not answer.
///
/// A conjunct is a leaf of the predicate's `AND` chain, which the split takes to its end, so the
/// search answers it whole or not at all. A conjunct that is itself an `AND` breaks that and is an
/// [`IrError::Invariant`].
fn knn_residual(predicate: &Reconstructed) -> Result<Vec<usize>, IrError> {
	let mut kept: Vec<usize> = predicate.withheld.clone();
	for (at, conjunct) in &predicate.conjuncts {
		if matches!(
			conjunct,
			LogicalExpr::Binary {
				op: BinaryOperator::And,
				..
			}
		) {
			return Err(IrError::Invariant {
				what: "access::select_access: a conjunct that is an AND chain",
			});
		}
		if strip_knn_from_condition(conjunct).is_some() {
			kept.push(*at);
		}
	}
	kept.sort_unstable();
	Ok(kept)
}

/// Whether the B-tree `access` over `index` lists exactly the records whose columns satisfy the
/// conjuncts it was chosen from: every column it pins is a plain field path the index holds at one
/// entry per record ([`IndexRef::one_entry_per_record`]), because the index has recorded storing no
/// array in it. An array value is listed once per
/// element, and an empty one under `NULL`, so such a column lists records its conjunct rejects.
///
/// A range whose bounds the keys do not order as the comparison does lists records the comparison
/// rejects too ([`bounds_admit_exactly`]).
///
/// [`IndexRef::one_entry_per_record`]: crate::exec::index::access_path::IndexRef::one_entry_per_record
fn answers_exactly(
	index: &crate::exec::index::access_path::IndexRef,
	access: &BTreeAccess,
) -> bool {
	bounds_admit_exactly(index, access)
		&& pinned_columns(index, access)
			.is_some_and(|columns| (0..columns.len()).all(|col| index.one_entry_per_record(col)))
}

/// Whether the range of `access` over `index` admits exactly the values its comparison does: no
/// number bounding it keys differently from how it compares (a float that is no decimal, or any
/// number over an index that keeps a decimal's trailing zeros), and no datetime bounds a column
/// that orders only by the whole second, which widens the range to the seconds of its bounds.
fn bounds_admit_exactly(
	index: &crate::exec::index::access_path::IndexRef,
	access: &BTreeAccess,
) -> bool {
	!crate::exec::index::derived_columns::range_is_widened(index, access)
		&& crate::exec::index::access_path::range_matches_comparison(index, access)
}

/// The columns of `index` that the B-tree `access` pins, in definition order: all of them for an
/// equality, the prefix and the ranged column of a compound access, the first for a range. `None`
/// when it pins none, when one of them is not a plain field path, and for a full-text or KNN
/// access, which pins no column.
fn pinned_columns<'a>(
	index: &'a crate::exec::index::access_path::IndexRef,
	access: &BTreeAccess,
) -> Option<&'a [crate::expr::Idiom]> {
	let pinned = match access {
		BTreeAccess::Equality(_) => index.cols.len(),
		BTreeAccess::Compound {
			prefix,
			range,
			..
		} => prefix.len() + usize::from(range.is_some()),
		BTreeAccess::Range {
			..
		} => 1,
		BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => return None,
	};
	let columns = &index.cols[..pinned.min(index.cols.len())];
	let plain = columns
		.iter()
		.all(|column| column.0.iter().all(|part| matches!(part, crate::expr::Part::Field(_))));
	(!columns.is_empty() && plain).then_some(columns)
}

/// The columns an exact B-tree `access` over `index` pins, as the field paths
/// [`IndexScanAttrs::pinned`] carries: [`pinned_columns`], each converted. `None` where
/// [`pinned_columns`] answers none.
pub(crate) fn pinned_paths(
	index: &crate::exec::index::access_path::IndexRef,
	access: &BTreeAccess,
) -> Option<Vec<FieldPath>> {
	pinned_columns(index, access)?
		.iter()
		.map(|column| crate::exec::field_path_convert::field_path_from_idiom(column).ok())
		.collect()
}

/// An index union over the scans `paths`, merged for `order`.
///
/// A branch's access holds only values the analysis read from the predicate, and the predicate is
/// rebuilt from literals that hold no closure (`try_value_to_logical_expr` declines one, at any
/// depth), so an access that holds a closure is an [`IrError::Invariant`].
fn index_union(
	table: &TableAnswers,
	paths: Vec<AccessPath>,
	dedupe: bool,
	order: Option<&LogicalOrdering>,
	read: &ReadClauses,
) -> Result<PhysOp, IrError> {
	let (paths, merge) = decide_union_merge(paths, dedupe, order);
	let branches = paths
		.into_iter()
		.map(|path| match path {
			AccessPath::BTreeScan {
				index_ref: index,
				access,
				direction,
			} => Ok(IndexBranchShape {
				index: index_ref(&index),
				read: BranchReadShape::BTree(
					ValueData::new(access, "an index union's access").map_err(|_| {
						IrError::Invariant {
							what: "access::select_access: an index union's access holds a closure the predicate cannot state",
						}
					})?,
				),
				direction,
			}),
			AccessPath::FullTextSearch {
				index_ref: index,
				query,
				operator,
			} => Ok(IndexBranchShape {
				index: index_ref(&index),
				read: BranchReadShape::FullText {
					query,
					operator,
					column: index.cols.first().cloned(),
					shared_doc_ids: index.uses_shared_doc_ids(),
				},
				direction: Direction::Forward,
			}),
			AccessPath::KeyRange(_) => Err(invariant("a key range as a union branch")),
			AccessPath::TableScan
			| AccessPath::EmptyScan
			| AccessPath::KnnSearch {
				..
			}
			| AccessPath::BitmapFusion {
				..
			}
			| AccessPath::Union {
				..
			} => Err(IrError::Stale(
				"a union branch that is neither a B-tree scan nor a full-text query",
			)),
		})
		.collect::<Result<Vec<_>, IrError>>()?;
	// A merge delivers its key's order in the direction the ordering asked for.
	let ordered_by = match &merge {
		Some(MergeMode::ById(_)) => Some(vec![FieldPath::field("id")]),
		Some(
			MergeMode::ByIndexKey {
				path,
				..
			}
			| MergeMode::ByIndexKeyDedup {
				path,
				..
			},
		) => Some(vec![path.clone()]),
		None => None,
	};
	Ok(PhysOp::IndexUnion(Box::new(IndexUnionAttrs {
		table: table.table.clone(),
		branches,
		merge,
		ordered_by,
		decode: None,
		read: read.clone(),
		topk: None,
	})))
}

/// The `phys.values` op that reads a source naming a value rather than a table or a record id, or
/// `None` for a source that names one.
///
/// A value source reads no index, and a table it names when the statement runs is read in full, so
/// `WITH` and the schema change nothing it yields. `call_shaped` says whether the dynamic
/// expression the source's operand computes is a call, an invocation or a postfix: such a value
/// resolves when the statement runs, any other dynamic expression is evaluated once when the
/// source opens. A locked read of a value is of the record id a parameter or a dynamic expression
/// resolves to; a locked read of a generated set is an error.
pub(crate) fn value_source(
	source: &SourceAttrs,
	call_shaped: bool,
) -> Result<Option<PhysOp>, IrError> {
	let read = match (&source.source_ref, source.for_update) {
		(SourceRefShape::Table(_) | SourceRefShape::RecordId, _) => return Ok(None),
		(SourceRefShape::Mock, true) => {
			return Err(IrError::Stale("a locked read of a generated record set"));
		}
		(SourceRefShape::Param | SourceRefShape::Dynamic, true) => ValuesRead::Locked,
		(SourceRefShape::Mock, false) => ValuesRead::Expression {
			written: false,
		},
		(SourceRefShape::Param, false) => ValuesRead::Dynamic,
		(SourceRefShape::Dynamic, false) if call_shaped => ValuesRead::Dynamic,
		(SourceRefShape::Dynamic, false) => ValuesRead::Expression {
			written: false,
		},
	};
	Ok(Some(PhysOp::Values {
		read,
	}))
}

// ---------------------------------------------------------------------------------------------
// The conversion pattern
// ---------------------------------------------------------------------------------------------

/// The conversion pattern of `logical.source`.
pub(crate) struct SourcePattern;

impl ConversionPattern for SourcePattern {
	fn name(&self) -> &'static str {
		LogTag::Source.mnemonic()
	}

	fn source(&self) -> LogTag {
		LogTag::Source
	}

	fn convert(
		&self,
		op: OpId,
		rw: &mut Rewriter<'_>,
		cx: &PhysCx<'_>,
	) -> Result<Converted, IrError> {
		convert_source(op, rw, cx)
	}
}

/// Converts a `logical.source` to the op that reads it.
///
/// A source naming a value is a `phys.values`. A table or record id source reads the answers the
/// physical catalog holds for its read ([`SourceKey`]), and decides its access path from them as
/// [`select_access`] does, with the predicate, ordering and paging the statement offers it
/// ([`offer_of`]). The access op replaces the source, wrapped in `phys.enforced` when the read
/// enforces the table. Every conjunct of the predicate stays in the filters above the read, and
/// the filters are the ones the statement wrote.
///
/// A source whose answers the memo does not hold answers [`IrError::NotLoaded`], and its key is
/// pending. After a catalog change the answers are `Unknown`: the statement is planned again when
/// it is reached ([`IrError::LateBind`]). A locked read of a whole table replaces the statement
/// with the refusal the planner raises.
pub(crate) fn convert_source(
	op: OpId,
	rw: &mut Rewriter<'_>,
	cx: &PhysCx<'_>,
) -> Result<Converted, IrError> {
	let Some(planned) = plan_source(rw.ir(), op, cx)? else {
		return Ok(Converted::NotApplicable);
	};
	apply_source(rw, op, planned)
}

/// The record id a point lookup reads, from the conjunct `id = <record id>`, or the record ids it
/// reads from the conjunct `id IN <list>`.
enum PointKey {
	/// A value known when the plan is made: the record ids of a list, in key order and without
	/// repeats, as one array literal, or a record id the planner folded from a record path.
	Constant(Value),
	/// A key with no operands, built beside the scan.
	Literal(TableName, KeyShape),
	/// The conjunct's own record id, whose key has operands: copied out of the region of the
	/// filter that holds the conjunct.
	Copied(RegionId, ValueId),
}

/// Whether the rows of the source `op` are the subjects of a write loop.
fn writes_subjects(ir: &Ir, op: OpId) -> bool {
	let Some(rows) = ir.op(op).and_then(|data| data.results.first().copied()) else {
		return false;
	};
	match ir.uses(rows) {
		[only] => {
			only.operand == 0
				&& matches!(
					ir.op(only.user).map(|user| &user.kind),
					Some(OpKind::Logical(LogOp::Loop {
						kind: LoopKind::Write,
						..
					}))
				)
		}
		_ => false,
	}
}

/// What converting one source creates.
struct Planned {
	choice: AccessChoice,
	/// The record id a point lookup reads.
	point: Option<PointKey>,
	/// The values of the deferred bounds of a key-range scan, in the order of its operand slots,
	/// each with the region of the filter that computes it.
	key_operands: Vec<(RegionId, ValueId)>,
	/// What the statement offered the read, whose filters a `KNN` scan takes conjuncts from.
	offer: Offer,
	/// The approximate `KNN` operators a read at a stamp ranks as brute-force operators.
	exact: Vec<knn::Exact>,
	/// For a `KNN` scan whose query vector is known only when the statement runs, the region of
	/// the stage that holds the operator and the operator's right operand, which the scan takes as
	/// its operand.
	knn_vector: Option<(RegionId, ValueId)>,
	/// For a record-id source over a table whose record ids name their components, the record id
	/// the scan reads in place of the source's and the comparisons its rows are judged by.
	keyed: Option<keyed_record::KeyedRecord>,
}

/// The decision for the source `op`: `None` when `op` is not a source.
///
/// A source whose rows are the subjects of a write that holds a `KNN` operator is refused unless
/// its access path is a `KNN` scan, which is the only search a write's operator is answered by
/// ([`knn::write_refusal`]).
fn plan_source(ir: &Ir, op: OpId, cx: &PhysCx<'_>) -> Result<Option<Planned>, IrError> {
	let Some(mut planned) = plan_source_choice(ir, op, cx)? else {
		return Ok(None);
	};
	if planned.choice.refusal.is_none()
		&& planned.choice.knn.is_none()
		&& !matches!(planned.choice.access, PhysOp::KnnScan(..))
		&& let Some(refusal) = knn::write_refusal(ir, op)?
	{
		planned.choice = AccessChoice {
			access: PhysOp::Union,
			fallback: None,
			enforce: None,
			residual: Vec::new(),
			point_lookup: None,
			knn: None,
			refusal: Some(refusal),
			key_operands: Vec::new(),
		};
		planned.point = None;
		planned.key_operands = Vec::new();
		planned.offer = Offer::default();
		planned.exact = Vec::new();
		planned.knn_vector = None;
		planned.keyed = None;
	}
	Ok(Some(planned))
}

/// The choice of the source `op`, before a write's `KNN` operator is weighed.
fn plan_source_choice(ir: &Ir, op: OpId, cx: &PhysCx<'_>) -> Result<Option<Planned>, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("source to convert"))?;
	let OpKind::Logical(LogOp::Source(attrs)) = &data.kind else {
		return Ok(None);
	};
	let planned = |choice| Planned {
		choice,
		point: None,
		key_operands: Vec::new(),
		offer: Offer::default(),
		exact: Vec::new(),
		knn_vector: None,
		keyed: None,
	};
	// A `KNN` operator no search can answer refuses the statement whatever its source would
	// answer, and a brute-force search hides every handled `KNN` operator from the access path.
	let stages = data.results.first().and_then(|rows| knn::stages_over(ir, *rows));
	let refusal = match stages.as_ref() {
		Some(stages) => knn::placement_refusal(ir, cx, stages)?,
		None => None,
	};
	if refusal.is_some() {
		return Ok(Some(planned(AccessChoice {
			access: PhysOp::Union,
			fallback: None,
			enforce: None,
			residual: Vec::new(),
			point_lookup: None,
			knn: None,
			refusal,
			key_operands: Vec::new(),
		})));
	}
	let call_shaped = data.operands.first().is_some_and(|operand| {
		matches!(
			producer_kind(ir, *operand),
			Some(ExprOp::Call(..) | ExprOp::Postfix { .. } | ExprOp::Invoke)
		)
	});
	if let Some(values) = value_source(attrs, call_shaped)? {
		return Ok(Some(planned(AccessChoice {
			access: values,
			fallback: None,
			enforce: None,
			residual: Vec::new(),
			point_lookup: None,
			knn: None,
			refusal: None,
			key_operands: Vec::new(),
		})));
	}
	let key = source_key(ir, op, cx.effects.catalog, cx.physical)?
		.ok_or(IrError::Stale("a value source that names no value"))?;
	let answers = match cx.physical.table_answers(&key) {
		CatalogAnswer::Found(answers) => answers,
		CatalogAnswer::NotLoaded => {
			return Err(IrError::NotLoaded {
				what: "access::convert_source: the answers of a table read",
			});
		}
		// A read after a catalog change, in the compile no later one follows, knows nothing of the
		// table's definitions: it scans the table and enforces the definitions it finds when it
		// runs, through the table's version-keyed unit. Any other compile plans the statement
		// again late-bound when it is reached.
		CatalogAnswer::Unknown if cx.physical.plans_unknown_reads() => {
			Arc::new(TableAnswers::unknown(key.0.clone(), key.2))
		}
		CatalogAnswer::Unknown => {
			return Err(IrError::LateBind {
				what: "access::convert_source: a table read after a catalog change",
			});
		}
		CatalogAnswer::Absent => {
			return Err(IrError::Invariant {
				what: "access::convert_source: the physical catalog answers a read as absent",
			});
		}
	};
	// A read at a stamp only the run evaluates ranks over the vector indexes the table declares
	// now, the only ones the plan can name.
	let exact = match (&stages, answers.version, &attrs.source_ref) {
		(
			Some(stages),
			ReadVersion::Stamped(_) | ReadVersion::Deferred,
			SourceRefShape::Table(_),
		) => knn::exact_at_stamp(ir, stages, &answers.indexes)?,
		// A current read of a table with a vector index that is not online yet (a concurrent
		// build still running) ranks the operators of that index's field by brute force: the
		// index cannot answer, and no search answering would admit no row.
		// `WITH NOINDEX` keeps its contract: no index, no answer.
		(Some(stages), ReadVersion::Current, SourceRefShape::Table(_))
			if !matches!(attrs.with, Some(With::NoIndex))
				&& !matches!(answers.restricted, RestrictedSelect::Unresolved) =>
		{
			match cx.physical.indexes(&key.0) {
				CatalogAnswer::Found(defined) => {
					let restricted = answers.restricted.prefixes();
					// A vector index already online over the same field answers the
					// operator; an unrelated build beside it must not change its ranking.
					let building: Vec<_> = defined
						.iter()
						.filter(|ix| {
							!ix.prepare_remove
								&& matches!(ix.index, Index::Hnsw(_) | Index::DiskAnn(_))
								&& !restricted.index_touches(&ix.cols)
								&& !answers.indexes.iter().any(|online| {
									online.name == ix.name
										|| (!online.prepare_remove
											&& matches!(
												online.index,
												Index::Hnsw(_) | Index::DiskAnn(_)
											) && !restricted.index_touches(&online.cols)
											&& online.cols.first() == ix.cols.first())
								})
						})
						.cloned()
						.collect();
					knn::exact_at_stamp(ir, stages, &building)?
				}
				CatalogAnswer::NotLoaded => {
					return Err(IrError::NotLoaded {
						what: "access::convert_source: the defined indexes of a table",
					});
				}
				CatalogAnswer::Unknown if cx.physical.plans_unknown_reads() => Vec::new(),
				CatalogAnswer::Unknown => {
					return Err(IrError::LateBind {
						what: "access::convert_source: the defined indexes of a table",
					});
				}
				CatalogAnswer::Absent => Vec::new(),
			}
		}
		_ => Vec::new(),
	};
	let ranks = match stages.as_ref() {
		Some(stages) => knn::ranks(ir, cx.effects.registry, stages, &exact)?,
		None => false,
	};
	let decide = |mut offer: Offer| -> Result<(Offer, AccessChoice), IrError> {
		if ranks {
			knn::withhold_handled(&mut offer.predicate);
		}
		// The `LIMIT k` is what lets a `KNN` scan answer `ORDER BY vector::distance::*(..)`, which
		// evaluates the query vector once where the sort evaluates it per row: a vector two
		// evaluations may disagree on keeps the sort.
		if let Some(sort) = offer.sort.as_mut()
			&& sort.limit.is_some()
			&& let Some(stages) = stages.as_ref()
			&& !knn::sort_vector_is_stable(ir, &cx.effects, stages)
		{
			sort.limit = None;
		}
		// The decision reads each conjunct with the record paths the planner folded from the
		// current records, unless the read is at another version; the filter keeps it as written.
		let folded = match answers.version {
			ReadVersion::Current => record_folded(&offer.predicate, cx.physical),
			ReadVersion::Stamped(_) | ReadVersion::Deferred => None,
		};
		let predicate = folded.as_ref().unwrap_or(&offer.predicate);
		let cond = predicate.condition();
		let knn_vectors = knn_vectors_of(cx, data, &answers, cond.as_ref(), offer.sort.as_ref())?;
		let decision = DecisionCx {
			registry: cx.effects.registry,
			knn_vectors: &knn_vectors,
			id_bounds: !offer.write_subjects && !offer.in_write,
		};
		let choice = select_access(attrs, &answers, predicate, offer.sort.as_ref(), &decision)?;
		Ok((offer, choice))
	};
	// A member of a union of sources whose predicate holds a `KNN` operator is searched when the
	// predicate over the union's rows gives it a `KNN` scan. The subjects of a write are read by
	// the access path the write's `WHERE` chooses, as a read under it would be; a write's `KNN`
	// scan must know its query vector. A `WHERE` that reads no row empties the subjects of a write
	// over a table whose definitions the plan holds. Over any other table the write's subjects stay
	// the table's: the write still meets the table it names, which raises where it is not defined
	// and answers for the definitions it finds where a catalog change precedes it. Any other is
	// read as no predicate reaches it.
	let write = writes_subjects(ir, op);
	let across = match stages.as_ref() {
		Some(stages) => (stages.united && !ranks) || write,
		None => write,
	};
	let across = match across {
		true => {
			let across = offer_across(ir, op)?;
			let condition = across.predicate.condition();
			match condition.as_ref().is_some_and(|cond| write || has_knn_operator(cond)) {
				true => Some(decide(across)?),
				false => None,
			}
		}
		false => None,
	};
	let holds_definitions = answers.defined
		&& !attrs.may_meet_change
		&& !matches!(answers.restricted, RestrictedSelect::Unresolved);
	let emptied = |predicate: &Reconstructed| {
		holds_definitions && write_settled_false(predicate, cx.effects.registry)
	};
	let searched = |(offer, choice): &(Offer, AccessChoice)| {
		if let PhysOp::KnnScan(attrs) = &choice.access {
			!offer.in_write || !attrs.vector.is_empty()
		} else {
			offer.in_write && (!matches!(choice.access, PhysOp::Union) || emptied(&offer.predicate))
		}
	};
	let (offer, mut choice) = match across {
		Some(decided) if searched(&decided) => decided,
		Some(_) | None => decide(united_offer(ir, op, write || ranks)?)?,
	};
	// A search judges the source's rows, and a conjunct past a resolver layer reads the rows the
	// layer resolves: it stays in the filter above the layer and out of the search's regions.
	if !offer.past_layers.is_empty()
		&& let Some(knn) = choice.knn.as_mut()
	{
		for positions in
			[&mut knn.residual, &mut knn.matches_condition, &mut knn.prefilter_residual]
		{
			positions.retain(|at| !offer.past_layers.contains(at));
		}
		if let PhysOp::KnnScan(attrs) = &mut choice.access
			&& let Some(filter) = attrs.filter.as_mut()
		{
			filter.residual &= !knn.residual.is_empty();
			filter.matches_condition &= !knn.matches_condition.is_empty();
			if let Some(prefilter) = filter.prefilter.as_mut() {
				prefilter.residual &= !knn.prefilter_residual.is_empty();
			}
			if !filter.residual && !filter.matches_condition && filter.prefilter.is_none() {
				attrs.filter = None;
			}
		}
	}
	// A permission over the source's own rows that admits no row leaves the read nothing to list:
	// the read is the empty scan, and the permission filter stays above it.
	if matches!(attrs.source_ref, SourceRefShape::Table(_))
		&& choice.refusal.is_none()
		&& choice.knn.is_none()
		&& offer.permissions.iter().any(|permission| admits_no_row(ir, *permission))
	{
		choice.access = PhysOp::Union;
		choice.fallback = None;
		choice.point_lookup = None;
		choice.key_operands.clear();
	}
	// A member of a union of sources under a predicate settled false, which the fold leaves only
	// where no member's rows can dereference a record, lists nothing: every row of the union is
	// refused, so the table's read is the empty scan. The subjects of a write under a `WHERE`
	// settled false are no rows either, where the plan holds the table's definitions.
	if matches!(attrs.source_ref, SourceRefShape::Table(_))
		&& choice.refusal.is_none()
		&& choice.knn.is_none()
		&& !matches!(choice.access, PhysOp::Union)
	{
		let across = offer_across(ir, op)?;
		let empty = match across.in_write {
			true => emptied(&across.predicate),
			false => across.united && settled_false(&across.predicate),
		};
		if empty {
			choice.access = PhysOp::Union;
			choice.fallback = None;
			choice.point_lookup = None;
			choice.key_operands.clear();
		}
	}
	// A source the bind resolved has its table's declarations enforced by the nodes above it, and
	// enforcing them again would judge a row twice. A resolved source with no such node above it
	// needs none: what the statement reads of it declares nothing the read must enforce. The
	// answers of a table the catalog does not define, or whose permission context is unresolved,
	// are not the ones the resolution read, so its read enforces what it meets.
	let resolved = attrs.resolved
		&& answers.defined
		&& !matches!(answers.restricted, RestrictedSelect::Unresolved);
	if (offer.enforced_above || resolved) && (offer.write_subjects || !attrs.may_meet_change) {
		choice.enforce = None;
	}
	// The decision read the conjunct with the record paths the planner folded (`decide`), so the
	// key is read from the same form when the conjunct as written names no literal record.
	let physical = match answers.version {
		ReadVersion::Current => Some(cx.physical),
		ReadVersion::Stamped(_) | ReadVersion::Deferred => None,
	};
	let point = match choice.point_lookup {
		Some(at) => Some(point_key(ir, &offer, &answers.table, at, physical, cx.effects.registry)?),
		None => None,
	};
	let key_operands = choice
		.key_operands
		.iter()
		.map(|bound| key_operand(ir, &offer, bound))
		.collect::<Result<Vec<_>, _>>()?;
	let run_time = matches!(&choice.access, PhysOp::KnnScan(attrs) if attrs.vector.is_empty());
	let knn_vector = if run_time {
		let found = stages.as_ref().and_then(|stages| knn::run_time_vector(ir, stages));
		let Some((region, value)) = found else {
			return Err(IrError::NotYet {
				what: "access::convert_source: a KNN scan whose run-time vector no stage holds",
			});
		};
		if let PhysOp::KnnScan(attrs) = &mut choice.access {
			attrs.vector_text = Some(knn::vector_text(ir, region, value)?);
		}
		found
	} else {
		None
	};
	// A read at a version keeps the record id as written: the structure the catalog holds now need
	// not be the one the records at the version were written under.
	let keyed = match (&attrs.source_ref, &choice.access, &answers.key, answers.version) {
		(SourceRefShape::RecordId, PhysOp::RecordScan(..), Some(key), ReadVersion::Current) => {
			data.operands.first().and_then(|record| keyed_record::keyed_record(ir, *record, key))
		}
		_ => None,
	};
	Ok(Some(Planned {
		choice,
		point,
		key_operands,
		offer,
		exact,
		knn_vector,
		keyed,
	}))
}

/// The offer a source's access path is chosen from where no search or write reads it across a
/// union: [`offer_of`], or, for a table member of a union of sources that nothing between it and
/// the union filters, the predicate the filters over the union's rows state ([`offer_across`]).
/// Such a predicate judges every row of the union, so a conjunct it holds holds of each row the
/// union yields: the access path lists a superset of the member's rows that survive it, and the
/// filters above the union keep every conjunct. A predicate with a `KNN` or `MATCHES` operator is
/// not offered this way, and `searched` withholds it from a source read for a `KNN` search or a
/// write, whose offers [`plan_source`] reads itself.
fn united_offer(ir: &Ir, op: OpId, searched: bool) -> Result<Offer, IrError> {
	let own = offer_of(ir, op)?;
	if searched || !own.predicate.is_empty() {
		return Ok(own);
	}
	let across = offer_across(ir, op)?;
	let unindexed = across
		.predicate
		.condition()
		.is_none_or(|cond| has_knn_operator(&cond) || expr_contains_matches(&cond));
	if !across.united || across.in_write || unindexed {
		return Ok(own);
	}
	Ok(across)
}

/// Whether the permission filter `permission` admits no row: its predicate is the literal `false`.
fn admits_no_row(ir: &Ir, permission: OpId) -> bool {
	let Some(region) = ir.op(permission).and_then(|data| data.regions.first().copied()) else {
		return false;
	};
	let Some(closing) = ops_in(ir, region).last().and_then(|last| ir.op(last)) else {
		return false;
	};
	let (OpKind::Expr(ExprOp::Yield), [value]) = (&closing.kind, closing.operands.as_slice())
	else {
		return false;
	};
	matches!(
		producer_kind(ir, *value),
		Some(ExprOp::Literal(literal)) if matches!(**literal, Value::Bool(false))
	)
}

/// The key of the table read the source `op` makes, or `None` when the source names a value and
/// reads no table.
///
/// The table is the one the source names, or the record id's; the version is the one the source's
/// snapshot names ([`ReadVersion`]).
pub(crate) fn source_key(
	ir: &Ir,
	op: OpId,
	catalog: &dyn SemanticCatalog,
	physical: &dyn PhysicalCatalog,
) -> Result<Option<SourceKey>, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("source"))?;
	let OpKind::Logical(LogOp::Source(attrs)) = &data.kind else {
		return Ok(None);
	};
	let table = match &attrs.source_ref {
		SourceRefShape::Table(table) => table.clone(),
		SourceRefShape::RecordId => data
			.operands
			.first()
			.and_then(|operand| match producer_kind(ir, *operand) {
				Some(ExprOp::RecordId(record)) => Some(record.table.clone()),
				_ => None,
			})
			.ok_or(IrError::Stale("the record id a source reads"))?,
		SourceRefShape::Mock | SourceRefShape::Param | SourceRefShape::Dynamic => {
			return Ok(None);
		}
	};
	let version = read_version(ir, data, catalog, physical)?;
	Ok(Some((table, attrs.with.clone(), version)))
}

/// The expression op that defines `value`, when one does.
fn producer_kind(ir: &Ir, value: ValueId) -> Option<&ExprOp> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	match &ir.op(op)?.kind {
		OpKind::Expr(kind) => Some(kind),
		OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// The version the read of the source `data` observes.
///
/// A read at a datetime the plan knows (a literal, or a name the compile binds to one) whose stamp
/// the datastore resolves is [`ReadVersion::Stamped`], and so is a read under no `VERSION` of its
/// own in a compile at a stamp ([`PhysicalCatalog::read_stamp`]); a read at any other version is
/// [`ReadVersion::Deferred`].
fn read_version(
	ir: &Ir,
	data: &OpData,
	catalog: &dyn SemanticCatalog,
	physical: &dyn PhysicalCatalog,
) -> Result<ReadVersion, IrError> {
	// A read under no `VERSION` of its own reads at the stamp the compile reads at, where it is
	// one.
	let version = match ir.snapshot(data.snapshot) {
		Some(Snapshot::Version(version)) => *version,
		Some(Snapshot::Current) | None => {
			return Ok(physical.read_stamp().map_or(ReadVersion::Current, ReadVersion::Stamped));
		}
	};
	let datetime = match producer_kind(ir, version) {
		Some(ExprOp::Literal(value)) => match &**value {
			Value::Datetime(datetime) => Some(*datetime),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => None,
		},
		Some(
			ExprOp::FreeParam {
				name,
			}
			| ExprOp::SessionParam {
				name,
			}
			| ExprOp::CatalogParam {
				name,
			},
		) => match catalog.free_value_exact(name, data.frame, FoldConsumer::IndexChoice) {
			ExactAnswer::Value(Value::Datetime(datetime)) => Some(datetime),
			ExactAnswer::NotLoaded => {
				return Err(IrError::NotLoaded {
					what: "access::convert_source: the version a read is made at",
				});
			}
			ExactAnswer::Value(_) | ExactAnswer::RunTime => None,
		},
		_ => None,
	};
	Ok(datetime
		.and_then(|datetime| physical.version_stamp(&datetime))
		.map_or(ReadVersion::Deferred, ReadVersion::Stamped))
}

/// The query vectors of the `KNN` operators of `cond` whose right operand is a parameter that
/// holds a non-empty array of numbers at plan time, read only when an approximate index is a
/// candidate.
fn knn_vectors_of(
	cx: &PhysCx<'_>,
	data: &OpData,
	answers: &TableAnswers,
	cond: Option<&LogicalExpr>,
	sort: Option<&SortKeys>,
) -> Result<KnnQueryVectors, IrError> {
	let mut vectors = KnnQueryVectors::new();
	if cond.is_none() && sort.is_none() {
		return Ok(vectors);
	}
	let Some(columns) = &answers.columns else {
		return Ok(vectors);
	};
	if !columns.indexes.iter().any(|ix| matches!(ix.index, Index::Hnsw(_) | Index::DiskAnn(_))) {
		return Ok(vectors);
	}
	let mut names: Vec<String> = Vec::new();
	if let Some(cond) = cond {
		visit_binding_pruning(cond, &mut |expr| {
			if let LogicalExpr::Binary {
				op: BinaryOperator::NearestNeighbor(_),
				right,
				..
			} = expr && let LogicalExpr::Param(param) = right.as_ref()
				&& !names.iter().any(|held| held == param.as_str())
			{
				names.push(param.as_str().to_owned());
			}
			Descend::Into
		});
	}
	if let Some(LogicalOrdering::Order(keys)) = sort.and_then(|s| s.ordering.as_ref())
		&& let [key] = keys.as_slice()
		&& let LogicalExpr::Call(call) = &key.value
		&& matches!(&call.receiver, Function::Normal(name) if name.starts_with("vector::distance::"))
	{
		for arg in call.args.iter().take(2) {
			if let LogicalExpr::Param(param) = arg
				&& !names.iter().any(|held| held == param.as_str())
			{
				names.push(param.as_str().to_owned());
			}
		}
	}
	for name in names {
		match cx.effects.catalog.free_value_exact(&name, data.frame, FoldConsumer::IndexChoice) {
			ExactAnswer::Value(Value::Array(array)) if !array.is_empty() => {
				let vector: Option<Arc<[Number]>> = array
					.iter()
					.map(|value| {
						if let Value::Number(number) = value {
							Some(number.clone())
						} else {
							None
						}
					})
					.collect();
				if let Some(vector) = vector {
					vectors.insert(name, vector);
				}
			}
			ExactAnswer::NotLoaded => {
				return Err(IrError::NotLoaded {
					what: "access::convert_source: a stored parameter a KNN query reads",
				});
			}
			// A vector known only when the statement runs is searched for then: the scan takes
			// it as its operand, which the empty vector stands for here.
			ExactAnswer::RunTime => {
				vectors.insert(name, Arc::from(Vec::<Number>::new()));
			}
			ExactAnswer::Value(_) => {}
		}
	}
	Ok(vectors)
}

/// The value of the deferred key-range bound `bound`, with the region of the filter that computes
/// it.
///
/// The bound is a sub-expression of a readable conjunct of the offer's predicate, which the
/// analysis read from that conjunct's value, so a value of the conjunct's cone rebuilds as it. The
/// bound is fixed per execution, so it reads no argument of its filter's region and can be
/// computed beside the scan. A bound no conjunct holds, or one that reads the row, is an
/// [`IrError::Invariant`].
fn key_operand(
	ir: &Ir,
	offer: &Offer,
	bound: &LogicalExpr,
) -> Result<(RegionId, ValueId), IrError> {
	for part in &offer.filters {
		let Some((leaves, _)) = conjunct_values(ir, part.region) else {
			continue;
		};
		for leaf in leaves {
			let Some(value) = value_rebuilt_as(ir, leaf, &offer.predicate.operands, bound) else {
				continue;
			};
			if reads_region_args(ir, part.region, value) {
				return Err(IrError::Invariant {
					what: "access::convert_source: a key-range bound that reads the row",
				});
			}
			return Ok((part.region, value));
		}
	}
	Err(IrError::Invariant {
		what: "access::convert_source: a key-range bound no conjunct holds",
	})
}

/// The record id the point lookup chosen for the conjunct at `at` of the offer's predicate reads,
/// or the record ids of the list it reads.
///
/// The conjunct is one [`extract_record_id_point_lookup`] or [`extract_record_id_list_lookup`]
/// accepts, as written or with the record paths `physical` folds (the form the decision read, where
/// a projection call such as `type::field('id')` reads as the field it names). A key with no
/// operands is built afresh, a folded record or list is its value, and any other key is the
/// conjunct's own record id, copied out of its filter's region. A conjunct that is not of
/// that form is an [`IrError::Invariant`].
fn point_key(
	ir: &Ir,
	offer: &Offer,
	table: &TableName,
	at: usize,
	physical: Option<&dyn PhysicalCatalog>,
	registry: &FunctionRegistry,
) -> Result<PointKey, IrError> {
	let conjunct =
		offer.predicate.conjuncts.iter().find(|(position, _)| *position == at).map(|(_, c)| c);
	let list = |conjunct: &LogicalExpr| {
		extract_record_id_list_lookup(conjunct, table).map(|records| {
			let records: Vec<Value> = records.into_iter().map(Value::RecordId).collect();
			PointKey::Constant(Value::Array(records.into()))
		})
	};
	if let Some(key) = conjunct.and_then(list) {
		return Ok(key);
	}
	let record = conjunct.and_then(|conjunct| extract_record_id_point_lookup(conjunct, table));
	if let Some((table, key)) = record.as_ref().and_then(record_key) {
		return Ok(PointKey::Literal(table, key));
	}
	let folded = match (conjunct, physical) {
		(Some(conjunct), Some(physical)) => physical.record_folded(conjunct),
		_ => None,
	};
	if let Some(folded) = folded {
		if let Some(key) = list(&folded) {
			return Ok(key);
		}
		if let Some(value) =
			point_lookup_record(&folded, table, registry).as_ref().and_then(record_value)
		{
			return Ok(PointKey::Constant(value));
		}
	}
	let not_a_point = || IrError::Invariant {
		what: "access::convert_source: a point lookup whose conjunct names no record id",
	};
	let grouped = by_filter(offer, &[at])?;
	let [(filter, local)] = grouped.as_slice() else {
		return Err(not_a_point());
	};
	let region = offer.filters.get(*filter).ok_or_else(not_a_point)?.region;
	let leaf = conjunct_values(ir, region)
		.and_then(|(leaves, _)| local.first().and_then(|local| leaves.get(*local).copied()))
		.ok_or_else(not_a_point)?;
	let Some((
		ExprOp::Binary {
			op: BinaryOperator::Equal | BinaryOperator::ExactEqual,
		},
		data,
	)) = expr_def(ir, leaf)
	else {
		return Err(not_a_point());
	};
	let names_record = |value: &ValueId| {
		let Some((kind, _)) = expr_def(ir, *value) else {
			return false;
		};
		if let ExprOp::RecordId(attrs) = kind {
			return attrs.table == *table;
		}
		if let ExprOp::Literal(literal) = kind {
			return matches!(&**literal, Value::RecordId(record) if record.table == *table);
		}
		false
	};
	let value = data.operands.iter().copied().find(names_record).ok_or_else(not_a_point)?;
	if reads_region_args(ir, region, value) {
		return Err(not_a_point());
	}
	Ok(PointKey::Copied(region, value))
}

/// The record id `record` names, when it is a record id whose key is built of literals alone.
fn record_value(record: &LogicalExpr) -> Option<Value> {
	let LogicalExpr::RecordId(record) = record else {
		return None;
	};
	let literal = crate::exec::planner::util::try_literal_to_value;
	let key = match &record.key {
		LogicalRecordIdKey::Number(number) => RecordIdKey::Number(*number),
		LogicalRecordIdKey::String(string) => RecordIdKey::String(string.clone()),
		LogicalRecordIdKey::Uuid(uuid) => RecordIdKey::Uuid(*uuid),
		LogicalRecordIdKey::Array(items) => {
			RecordIdKey::Array(items.iter().map(literal).collect::<Option<Vec<_>>>()?.into())
		}
		LogicalRecordIdKey::Object(entries) => RecordIdKey::Object(
			entries
				.iter()
				.map(|(name, item)| Some((name.clone(), literal(item)?)))
				.collect::<Option<std::collections::BTreeMap<_, _>>>()?
				.into(),
		),
		LogicalRecordIdKey::Generate(_) | LogicalRecordIdKey::Range(_) => return None,
	};
	Some(Value::RecordId(RecordId::new(record.table.clone(), key)))
}

/// The key shape of the record id `record`, when it is a literal key that fixes a record: a
/// number, a string or a uuid.
fn record_key(record: &LogicalExpr) -> Option<(TableName, KeyShape)> {
	let LogicalExpr::RecordId(record) = record else {
		return None;
	};
	let key = match &record.key {
		LogicalRecordIdKey::Number(number) => KeyShape::Number(*number),
		LogicalRecordIdKey::String(string) => KeyShape::String(string.clone()),
		LogicalRecordIdKey::Uuid(uuid) => KeyShape::Uuid(*uuid),
		LogicalRecordIdKey::Array(..)
		| LogicalRecordIdKey::Object(..)
		| LogicalRecordIdKey::Generate(..)
		| LogicalRecordIdKey::Range(..) => return None,
	};
	Some((record.table.clone(), key))
}

/// Replaces the source `op` by the ops of `planned`.
fn apply_source(rw: &mut Rewriter<'_>, op: OpId, planned: Planned) -> Result<Converted, IrError> {
	let Planned {
		choice,
		point,
		key_operands,
		offer,
		exact,
		knn_vector,
		keyed,
	} = planned;
	let data = rw.ir().op(op).ok_or(IrError::Stale("source to replace"))?;
	let (frame, snapshot, loc) = (data.frame, data.snapshot, data.loc.clone());
	let source_operands = data.operands.clone();
	let rows_type = data
		.results
		.first()
		.and_then(|value| rw.ir().value(*value))
		.map(|value| value.ty.clone())
		.ok_or(IrError::Stale("source result"))?;
	if let Some(refusal) = choice.refusal {
		return refuse_statement(rw, op, refusal);
	}
	rw.derived(&[op], |rw| {
		knn::apply_exact(rw, &exact)?;
		let spec = |kind: OpKind, operands: SmallVec<[ValueId; 4]>, ty: TypeKnowledge| OpSpec {
			kind,
			operands,
			results: smallvec::smallvec![ty],
			frame,
			snapshot,
			loc: loc.clone(),
		};
		let mut operands: SmallVec<[ValueId; 4]> = SmallVec::new();
		match (&choice.access, point) {
			(PhysOp::RecordScan(..), Some(PointKey::Copied(region, value))) => {
				operands.push(value_copy(rw, region, value, op)?);
			}
			(PhysOp::RecordScan(..), Some(PointKey::Constant(records))) => {
				let kind = ExprOp::Literal(Box::new(records));
				let ty = result_type(&kind, &[]);
				let list = rw.create(
					InsertPoint::Before(op),
					spec(OpKind::Expr(kind), SmallVec::new(), ty),
				)?;
				operands.push(first_result(rw.ir(), list)?);
			}
			(PhysOp::RecordScan(..), Some(PointKey::Literal(table, key))) => {
				let kind = ExprOp::RecordId(Box::new(RecordIdAttrs {
					table,
					key_shape: key,
				}));
				let ty = result_type(&kind, &[]);
				let record = rw.create(
					InsertPoint::Before(op),
					spec(OpKind::Expr(kind), SmallVec::new(), ty),
				)?;
				operands.push(first_result(rw.ir(), record)?);
			}
			(PhysOp::RecordScan(..), None) if let Some(keyed) = &keyed => {
				let site = keyed_record::Site {
					before: op,
					frame,
					snapshot,
					loc: loc.clone(),
				};
				operands.push(keyed_record::record(rw, &site, keyed)?);
			}
			(PhysOp::RecordScan(..), None)
			| (
				PhysOp::Values {
					..
				},
				_,
			) => operands.clone_from(&source_operands),
			(PhysOp::KeyRangeScan(..), _) => {
				for (region, value) in key_operands {
					operands.push(value_copy(rw, region, value, op)?);
				}
			}
			(PhysOp::TableScan(attrs), _) if attrs.bounds.is_some() => {
				for (region, value) in key_operands {
					operands.push(value_copy(rw, region, value, op)?);
				}
			}
			// A query vector known only when the statement runs is copied out of the stage that
			// holds it. One computed by ops the conversion has yet to convert is copied once every
			// op of the statement is ([`knn::attach_scan_vectors`]): until then the scan reads the
			// `NONE` that stands for it.
			(PhysOp::KnnScan(..), _) => match knn_vector {
				Some((region, vector)) if cone_is_expr(rw.ir(), region, vector) => {
					operands.push(rowless_value_copy(rw, region, vector, op)?);
				}
				Some(_) => {
					let none = rw.create(
						InsertPoint::Before(op),
						spec(
							OpKind::Expr(ExprOp::Literal(Box::new(Value::None))),
							SmallVec::new(),
							TypeKnowledge::Unknown,
						),
					)?;
					operands.push(first_result(rw.ir(), none)?);
				}
				None => {}
			},
			_ => {}
		}
		let scan = rw.create(
			InsertPoint::Before(op),
			spec(OpKind::Phys(choice.access), operands, rows_type.clone()),
		)?;
		if let Some(union) = choice.fallback {
			let (_, block) = rw.create_region(scan, RegionKind::Pipeline, &[])?;
			let read = rw.create(
				InsertPoint::End(block),
				spec(OpKind::Phys(union), SmallVec::new(), rows_type.clone()),
			)?;
			let fallback_rows = first_result(rw.ir(), read)?;
			rw.create(
				InsertPoint::End(block),
				OpSpec {
					kind: OpKind::Phys(PhysOp::Yield),
					operands: smallvec::smallvec![fallback_rows],
					results: SmallVec::new(),
					frame,
					snapshot,
					loc: loc.clone(),
				},
			)?;
		}
		if let Some(knn) = &choice.knn {
			give_knn_conditions(rw, scan, op, knn, &choice.residual, &offer)?;
		}
		let mut rows = first_result(rw.ir(), scan)?;
		if let Some(keyed) = &keyed {
			let site = keyed_record::Site {
				before: op,
				frame,
				snapshot,
				loc: loc.clone(),
			};
			rows = keyed_record::judge(rw, &site, keyed, rows, &rows_type)?;
		}
		if let Some(table) = choice.enforce {
			let enforced = rw.create(
				InsertPoint::Before(op),
				spec(
					OpKind::Phys(PhysOp::Enforced {
						table,
					}),
					smallvec::smallvec![rows],
					TypeKnowledge::Unknown,
				),
			)?;
			rows = first_result(rw.ir(), enforced)?;
		}
		rw.replace_op(op, &[rows])?;
		if keyed.is_some()
			&& let Some(record) = source_operands.first()
		{
			keyed_record::retire(rw, *record)?;
		}
		Ok(Converted::Replaced)
	})
}

/// Gives the `KNN` scan `scan` the permission and condition regions its filter declares, and
/// leaves the filter above it with the conjuncts `keep` names.
///
/// The search gates each candidate on the table's permissions, so the `k` records it returns are
/// `k` the caller may see: the predicate of each permission filter over the source's rows is
/// copied whole into a `PermissionFrame` region of the scan, innermost first, and the filter
/// states its guard, `AUTH LIMIT` and computed reads. A search with no condition gets a filter of
/// the permissions alone. The permission filters stay above the scan. A permission above a node
/// that reshapes the rows is an internal error: the bind places a table's permission directly
/// above its source.
///
/// The conjuncts the search judges ([`KnnConjuncts`]) are copied into the scan's regions after the
/// permissions, in the order the scan declares them: the residual, the `MATCHES` condition, the
/// pre-filter's residual.
/// A conjunct the search answers itself is dropped from the filter it stands in, which is replaced
/// by one over the conjuncts that remain, or by its own input when none does; evaluating an
/// operator the search answers as a residual would be an error. A filter above a union of sources
/// ([`Offer::united`]) judges the rows of the other members too and keeps every conjunct: its
/// `KNN` operators hold for the rows a search yielded. The conjuncts may stand in several
/// filters, which judge the rows one after another: a region of the scan takes the conjuncts it
/// judges from each, in the order the filters apply them.
fn give_knn_conditions(
	rw: &mut Rewriter<'_>,
	scan: OpId,
	source: OpId,
	knn: &KnnConjuncts,
	keep: &[usize],
	offer: &Offer,
) -> Result<(), IrError> {
	let kind = rw.ir().op(scan).ok_or(IrError::Stale("scan"))?.kind.clone();
	let OpKind::Phys(PhysOp::KnnScan(attrs)) = kind else {
		return Err(IrError::Invariant {
			what: "access::convert_source: a KNN choice that is not a KNN scan",
		});
	};
	if offer.permission_past_rows {
		return Err(IrError::Invariant {
			what: "access::convert_source: a KNN search under a permission above a layer",
		});
	}
	let mut permissions = Vec::with_capacity(offer.permissions.len());
	for filter in &offer.permissions {
		let (shape, regions) = knn_permission(rw.ir(), *filter)?;
		permissions.push(shape);
		for region in regions {
			region_copy(rw, scan, source, region)?;
		}
	}
	if !permissions.is_empty() {
		rw.update_kind(scan, |kind| {
			if let OpKind::Phys(PhysOp::KnnScan(attrs)) = kind {
				attrs
					.filter
					.get_or_insert_with(|| KnnFilterShape {
						permissions: Vec::new(),
						residual: false,
						matches_condition: false,
						prefilter: None,
					})
					.permissions = permissions;
			}
		})?;
	}
	let total = offer.predicate.len();
	if let Some(filter) = &attrs.filter {
		let mut wanted: Vec<(bool, &[usize])> = vec![
			(filter.residual, &knn.residual),
			(filter.matches_condition, &knn.matches_condition),
		];
		if let Some(prefilter) = &filter.prefilter {
			wanted.push((prefilter.residual, &knn.prefilter_residual));
		}
		for (declared, positions) in wanted {
			if declared == positions.is_empty() {
				return Err(IrError::Invariant {
					what: "access::convert_source: a KNN region and its conjuncts disagree",
				});
			}
			if declared {
				let grouped = by_filter(offer, positions)?;
				let parts: Vec<(RegionId, &[usize])> = grouped
					.iter()
					.map(|(at, local)| (offer.filters[*at].region, local.as_slice()))
					.collect();
				conjunct_region_across(rw, scan, source, &parts)?;
			}
		}
	}
	// Filters read across a union of sources judge the other members' rows as well, and a write
	// judges its `WHERE` on each subject itself: both keep every conjunct.
	if keep.len() == total || offer.united || offer.in_write {
		return Ok(());
	}
	let kept = by_filter(offer, keep)?;
	for (at, part) in offer.filters.iter().enumerate() {
		let local: &[usize] = kept
			.iter()
			.find(|(held, _)| *held == at)
			.map(|(_, local)| local.as_slice())
			.unwrap_or_default();
		// A permission's predicate judges every row the access lists, whichever conjuncts chose it.
		if part.permission || local.len() == part.positions.len() {
			continue;
		}
		let held = rw.ir().op(part.op).ok_or(IrError::Stale("filter"))?;
		let (operands, frame, snapshot, loc) =
			(held.operands.clone(), held.frame, held.snapshot, held.loc.clone());
		let rows = *operands.first().ok_or(IrError::Stale("filter input"))?;
		if local.is_empty() {
			rw.replace_op(part.op, &[rows])?;
			continue;
		}
		let ty = held
			.results
			.first()
			.and_then(|value| rw.ir().value(*value))
			.map(|value| value.ty.clone())
			.ok_or(IrError::Stale("filter result"))?;
		let narrowed = rw.create(
			InsertPoint::Before(part.op),
			OpSpec {
				kind: OpKind::Phys(PhysOp::Filter),
				operands,
				results: smallvec::smallvec![ty],
				frame,
				snapshot,
				loc,
			},
		)?;
		conjunct_region(rw, narrowed, part.op, part.region, local)?;
		let produced = first_result(rw.ir(), narrowed)?;
		rw.replace_op(part.op, &[produced])?;
	}
	Ok(())
}

/// The conjuncts at the positions `positions` of the offer's predicate, grouped by the filter they
/// stand in: the filter's place in [`Offer::filters`] and the positions counted from the first
/// conjunct of that filter, in order. A position no filter holds is an [`IrError::Invariant`].
fn by_filter(offer: &Offer, positions: &[usize]) -> Result<Vec<(usize, Vec<usize>)>, IrError> {
	let mut grouped: Vec<(usize, Vec<usize>)> = Vec::new();
	for at in positions {
		let (filter, part) =
			offer.filters.iter().enumerate().find(|(_, part)| part.positions.contains(at)).ok_or(
				IrError::Invariant {
					what: "access::convert_source: a KNN conjunct outside its filter",
				},
			)?;
		let local = at - part.positions.start;
		match grouped.last_mut() {
			Some((held, locals)) if *held == filter => locals.push(local),
			Some(_) | None => grouped.push((filter, vec![local])),
		}
	}
	Ok(grouped)
}

/// The permission a `KNN` search gates its candidates on for the permission filter `filter`, and
/// the regions that hold its predicate and the bodies of the computed fields it derives, in order.
///
/// The `AUTH LIMIT` is the one of the frame the predicate's ops run in.
fn knn_permission(
	ir: &Ir,
	filter: OpId,
) -> Result<(KnnPermissionShape, SmallVec<[RegionId; 2]>), IrError> {
	let data = ir.op(filter).ok_or(IrError::Stale("permission filter"))?;
	let OpKind::Logical(LogOp::PermissionFilter {
		guard,
		computed,
	}) = &data.kind
	else {
		return Err(IrError::Invariant {
			what: "access::convert_source: a permission of a KNN search that is no permission filter",
		});
	};
	let shape = KnnPermissionShape {
		auth: None,
		guard: *guard,
		computed: computed.as_deref().cloned(),
	};
	let Some(region) = data.regions.first().filter(|_| data.regions.len() == shape.regions())
	else {
		return Err(IrError::Invariant {
			what: "access::convert_source: a permission filter with other than its predicate and derived fields",
		});
	};
	let last = ops_in(ir, *region).last().ok_or(IrError::Stale("permission predicate"))?;
	let frame = ir.op(last).ok_or(IrError::Stale("op"))?.frame;
	let auth = ir.frame(frame).ok_or(IrError::Stale("frame"))?.eval.auth.clone();
	Ok((
		KnnPermissionShape {
			auth,
			..shape
		},
		data.regions.clone(),
	))
}

/// Whether the permission filter `data` is part of what the access path of the table read beneath
/// it is chosen from, so that the access path is the definer's to see: the predicate holds no
/// `MATCHES` or `KNN` operator, and the chain of single inputs beneath the filter (filters,
/// permission filters and resolver layers, read through the select loop that holds them) ends at
/// a read of a table's rows that no `MATCHES` or `KNN` operator of the statement answers. A caller
/// whose reads are checked is shown `<permission>` for every access path beneath such a filter.
pub(crate) fn permission_drives_access(ir: &Ir, data: &OpData) -> bool {
	let index_operator = |op: OpId| {
		matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Expr(ExprOp::Binary {
				op: BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_),
			}))
		)
	};
	let Some(predicate) = data.regions.first() else {
		return false;
	};
	let mut scoped = false;
	ir.walk(*predicate, crate::exec::ir::core::ir::WalkOrder::PreOrder, &mut |op| {
		if index_operator(op) {
			scoped = true;
			return crate::exec::ir::core::ir::Walk::Stop;
		}
		crate::exec::ir::core::ir::Walk::Continue
	});
	if scoped {
		return false;
	}
	let Some(mut rows) = data.operands.first().copied() else {
		return false;
	};
	// The chain is finite: each step moves to an operand of the op it stands at, or to the source
	// of the loop whose element it is.
	loop {
		let Some(def) = ir.value(rows).map(|value| value.def) else {
			return false;
		};
		let op = match def {
			ValueDef::OpResult {
				op,
				index: 0,
			} => op,
			ValueDef::BlockArg {
				block,
				index: 0,
			} => {
				let owner = ir
					.block(block)
					.and_then(|block| ir.region(block.parent))
					.and_then(|region| region.parent);
				let source = owner.and_then(|owner| ir.op(owner)).and_then(|owner| {
					matches!(
						owner.kind,
						OpKind::Logical(LogOp::Loop {
							kind: LoopKind::Select,
							..
						}) | OpKind::Phys(PhysOp::Loop {
							kind: LoopKind::Select,
							..
						})
					)
					.then(|| owner.operands.first().copied())
					.flatten()
				});
				match source {
					Some(source) => {
						rows = source;
						continue;
					}
					None => return false,
				}
			}
			ValueDef::OpResult {
				..
			}
			| ValueDef::BlockArg {
				..
			} => return false,
		};
		let Some(data) = ir.op(op) else {
			return false;
		};
		let through =
			match &data.kind {
				OpKind::Logical(
					LogOp::Filter
					| LogOp::PermissionFilter {
						..
					},
				)
				| OpKind::Phys(PhysOp::Filter | PhysOp::PermissionFilter(_) | PhysOp::Layer(_)) => true,
				OpKind::Logical(LogOp::Project(attrs)) => attrs.layer,
				OpKind::Logical(LogOp::Source(attrs)) => {
					return matches!(
						attrs.source_ref,
						SourceRefShape::Table(_) | SourceRefShape::RecordId
					) && offer_of(ir, op).is_ok_and(|offer| {
						!offer.predicate.condition().as_ref().is_some_and(|cond| {
							has_knn_operator(cond) || expr_contains_matches(cond)
						})
					});
				}
				OpKind::Phys(
					PhysOp::TableScan(_)
					| PhysOp::IndexScan(_)
					| PhysOp::IndexUnion(_)
					| PhysOp::Bitmap(_)
					| PhysOp::IndexLookup(_)
					| PhysOp::KeyRangeScan(_)
					| PhysOp::RecordScan(_)
					| PhysOp::Count(_),
				) => return true,
				OpKind::Logical(_) | OpKind::Phys(_) | OpKind::Expr(_) => false,
			};
		if !through {
			return false;
		}
		let Some(input) = data.operands.first().copied() else {
			return false;
		};
		rows = input;
	}
}

/// The first result of `op`.
fn first_result(ir: &Ir, op: OpId) -> Result<ValueId, IrError> {
	ir.op(op).and_then(|data| data.results.first().copied()).ok_or(IrError::Stale("result"))
}

/// Replaces the statement that holds the source `op` by the raise of `refusal`: every op of the
/// statement's region is erased, and the region ends in `expr.raise { Refused }`, the form a
/// statement the planner refuses to run takes.
pub(super) fn refuse_statement(
	rw: &mut Rewriter<'_>,
	op: OpId,
	refusal: Refusal,
) -> Result<Converted, IrError> {
	let ir = rw.ir();
	let mut region = ir.enclosing_region(op);
	let statement = loop {
		let data = ir.region(region).ok_or(IrError::Stale("region"))?;
		if matches!(data.kind, RegionKind::Statement(_)) {
			break region;
		}
		let owner = data.parent.ok_or(IrError::Invariant {
			what: "access::convert_source: a source outside every statement",
		})?;
		region = ir.enclosing_region(owner);
	};
	let block = ir
		.region(statement)
		.and_then(|data| data.blocks.first().copied())
		.ok_or(IrError::Stale("statement block"))?;
	let held = ir.op(op).ok_or(IrError::Stale("source"))?;
	let (frame, snapshot, loc) = (held.frame, held.snapshot, held.loc.clone());
	let members: Vec<OpId> = ir.block(block).map(|data| data.ops.clone()).unwrap_or_default();
	rw.derived(&[op], |rw| {
		for member in members.into_iter().rev() {
			if rw.ir().op(member).is_some() {
				rw.erase(member, EliminationReason::Unreachable)?;
			}
		}
		let raise = OpKind::Expr(ExprOp::Raise(Box::new(RaiseKind::Refused(Box::new(refusal)))));
		rw.create(
			InsertPoint::End(block),
			OpSpec {
				kind: raise,
				operands: SmallVec::new(),
				results: SmallVec::new(),
				frame,
				snapshot,
				loc,
			},
		)?;
		Ok(Converted::Eliminated(EliminationReason::Unreachable))
	})
}

#[cfg(test)]
impl DecisionCx<'static> {
	/// The context of a decision made without a statement's own parameters: the full builtin
	/// function registry and no query vector. For tests.
	pub(crate) fn builtin() -> Self {
		static REGISTRY: std::sync::LazyLock<FunctionRegistry> =
			std::sync::LazyLock::new(FunctionRegistry::with_builtins);
		static NO_VECTORS: std::sync::LazyLock<KnnQueryVectors> =
			std::sync::LazyLock::new(KnnQueryVectors::new);
		Self {
			registry: &REGISTRY,
			knn_vectors: &NO_VECTORS,
			id_bounds: true,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{FullTextParams, IndexId, Scoring};
	use crate::exec::index::access_path::BitmapPlan;
	use crate::exec::index::key_range::KeyRangeAccess;
	use crate::exec::ir::iface::ids::ValueId;
	use crate::exec::ir::phys::ops::{KeyBoundShape, KeyEqShape, PhysTag, key_range_fixtures};
	use crate::expr::Idiom;
	use crate::expr::logical::{
		LogicalFilter, LogicalLiteral, LogicalOp, LogicalSort, LogicalSource, lower,
	};

	fn idx_def(id: u32, name: &str, cols: &[&str], kind: Index) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(id),
			name: Strand::from(name),
			table_name: "t".into(),
			cols: cols.iter().map(|c| Idiom::from_str(c).expect("valid idiom")).collect(),
			index: kind,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
			nulls_not_distinct: false,
		}
	}

	fn idx(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
		idx_def(id, name, cols, Index::Idx)
	}

	fn fulltext(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
		idx_def(
			id,
			name,
			cols,
			Index::FullText(FullTextParams {
				analyzer: "simple".into(),
				highlight: false,
				scoring: Scoring::Bm {
					k1: 1.2,
					b: 0.75,
				},
			}),
		)
	}

	fn answers(indexes: Vec<IndexDefinition>) -> TableAnswers {
		TableAnswers::plain("t".into(), true).with_indexes(indexes)
	}

	/// `table` with its columns read again after a change to its version or its restricted fields.
	fn refreshed(mut table: TableAnswers) -> TableAnswers {
		table.columns = Some(table.columns_over(&TableIndexShapes::default(), &[]));
		table
	}

	fn decision() -> DecisionCx<'static> {
		DecisionCx::builtin()
	}

	fn table_source() -> SourceAttrs {
		SourceAttrs {
			source_ref: SourceRefShape::Table("t".into()),
			with: None,
			for_update: false,
			may_be_defined: false,
			may_meet_change: false,
			resolved: false,
		}
	}

	/// The condition, ordering and `WITH` hint of `SELECT * FROM t <snippet>`.
	fn parts(snippet: &str) -> (Option<LogicalExpr>, Option<LogicalOrdering>, Option<With>) {
		let sql = format!("SELECT * FROM t {snippet}");
		let ast = crate::syn::parse(&sql).expect("parse");
		let mut exprs = ast.expressions;
		let surrealdb_expr::parser::TopLevelExpr::Expr(select @ crate::expr::Expr::Select(_)) =
			exprs.remove(0)
		else {
			panic!("expected SELECT");
		};
		let plan = lower(select).expect("SELECT lowers");
		let (mut cond, mut order, mut with) = (None, None, None);
		let mut stack = vec![&plan];
		while let Some(op) = stack.pop() {
			if let LogicalOp::Filter(LogicalFilter {
				predicate,
				..
			}) = op
			{
				cond = Some(predicate.clone());
			} else if let LogicalOp::Sort(LogicalSort {
				ordering,
				..
			}) = op
			{
				order = Some(ordering.clone());
			} else if let LogicalOp::Source(LogicalSource {
				with: hint,
				..
			}) = op
			{
				with = hint.clone();
			}
			stack.extend(op.children());
		}
		(cond, order, with)
	}

	/// The choice for `SELECT * FROM t <snippet>` over `table`.
	fn chosen(table: &TableAnswers, snippet: &str) -> Result<AccessChoice, IrError> {
		let (cond, order, with) = parts(snippet);
		let mut source = table_source();
		source.with = with;
		let predicate = cond.as_ref().map(Reconstructed::of_condition).unwrap_or_default();
		let sort = order.map(|ordering| SortKeys {
			ordering: Some(ordering),
			limited: false,
			limit: None,
		});
		select_access(&source, table, &predicate, sort.as_ref(), &decision())
	}

	fn tag(choice: &AccessChoice) -> PhysTag {
		choice.access.tag()
	}

	#[test]
	fn a_range_the_keys_do_not_order_as_the_comparison_does_is_no_exact_answer() {
		use crate::exec::index::access_path::{BoundOperand, IndexRef};
		use crate::expr::BinaryOperator;
		use crate::val::{Number, Value};

		let mut def = idx(1, "ix", &["a", "b"]);
		def.format_version = crate::catalog::INDEX_FORMAT_VERSION;
		let defs = [def];
		let compound = |prefix: i64, bound: Value| BTreeAccess::Compound {
			prefix: vec![BoundOperand::Literal(Value::from(prefix))],
			range: Some((BinaryOperator::MoreThan, BoundOperand::Literal(bound))),
			upper: None,
		};
		let plain = IndexRef::new(&defs, 0);
		assert!(bounds_admit_exactly(&plain, &compound(1, Value::from(2i64))));
		assert!(bounds_admit_exactly(&plain, &compound(1, Value::Number(Number::Float(1.5)))));
		assert!(
			!bounds_admit_exactly(&plain, &compound(1, Value::Number(Number::Float(0.1)))),
			"a float no decimal holds keys apart from where it compares"
		);
		assert!(bounds_admit_exactly(&plain, &BTreeAccess::literal_equality(Value::from(1i64))));
		// A datetime bounding a column that orders by the whole second reaches records outside it.
		let coarse = IndexRef::new(&defs, 0).with_coarse_columns(Box::new([false, true]));
		let datetime = Value::Datetime(crate::val::Datetime::now());
		assert!(!bounds_admit_exactly(&coarse, &compound(1, datetime.clone())));
		assert!(bounds_admit_exactly(&plain, &compound(1, datetime)));
		// An index that keeps a decimal's trailing zeros keys `2.0dec` apart from `2`.
		let mut older = defs[0].clone();
		older.format_version = crate::catalog::BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION - 1;
		let older = [older];
		let older = IndexRef::new(&older, 0);
		assert!(!bounds_admit_exactly(&older, &compound(1, Value::from(2i64))));
		assert!(!bounds_admit_exactly(&older, &BTreeAccess::literal_equality(Value::from(1i64))));
	}

	#[test]
	fn a_record_id_reads_the_record_by_its_key() {
		let source = SourceAttrs {
			source_ref: SourceRefShape::RecordId,
			with: None,
			for_update: false,
			may_be_defined: false,
			may_meet_change: false,
			resolved: false,
		};
		let mut table = answers(vec![idx(1, "ix", &["x"])]);
		table.lightweight = true;
		let choice = select_access(&source, &table, &Reconstructed::default(), None, &decision())
			.expect("a choice");
		assert!(matches!(
			&choice.access,
			PhysOp::RecordScan(attrs) if !attrs.for_update && attrs.lightweight
		));
	}

	#[test]
	fn a_locked_record_read_is_not_lightweight() {
		let source = SourceAttrs {
			source_ref: SourceRefShape::RecordId,
			with: None,
			for_update: true,
			may_be_defined: false,
			may_meet_change: false,
			resolved: false,
		};
		let mut table = answers(Vec::new());
		table.lightweight = true;
		let choice = select_access(&source, &table, &Reconstructed::default(), None, &decision())
			.expect("a choice");
		assert!(matches!(
			&choice.access,
			PhysOp::RecordScan(attrs) if attrs.for_update && !attrs.lightweight
		));
	}

	#[test]
	fn a_table_that_is_not_defined_reads_nothing() {
		let mut table = answers(vec![idx(1, "ix", &["x"])]);
		table.defined = false;
		table.enforces = true;
		let choice = chosen(&table, "WHERE x = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::Vacant);
		assert_eq!(choice.enforce, None);
	}

	#[test]
	fn no_index_scans_the_table() {
		let table = answers(vec![idx(1, "ix", &["x"])]);
		let choice = chosen(&table, "WITH NOINDEX WHERE x = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
		assert_eq!(choice.residual, vec![0]);
	}

	#[test]
	fn a_table_without_an_index_scans_the_table() {
		let table = answers(Vec::new());
		let choice = chosen(&table, "WHERE x = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
	}

	#[test]
	fn an_equality_on_an_indexed_field_scans_the_index() {
		let table = answers(vec![idx(1, "ix", &["x"])]);
		let choice = chosen(&table, "WHERE x = 1").expect("a choice");
		let PhysOp::IndexScan(attrs) = &choice.access else {
			panic!("an index scan, got {:?}", choice.access);
		};
		assert_eq!(attrs.index.index.as_str(), "ix");
		assert!(matches!(attrs.access, BTreeAccess::Equality(_)));
		assert_eq!(choice.residual, vec![0]);
	}

	/// The predicate's rebuild never states a closure, so no access an analysis builds from it
	/// holds one: a union branch's closure check ([`index_union`]) is an invariant.
	#[test]
	fn a_closure_is_never_stated_to_the_analysis() {
		use crate::val::{Closure, Value};
		let closure = Value::Closure(Box::new(Closure::Expr {
			args: Vec::new(),
			returns: None,
			body: crate::expr::Expr::Literal(crate::expr::Literal::None),
			captures: crate::expr::variables::Variables::new(),
		}));
		let nested = Value::Array(crate::val::Array(vec![Value::from(1_i64), closure.clone()]));
		for value in [closure, nested] {
			assert!(
				crate::exec::planner::util::try_value_to_logical_expr(value).is_none(),
				"a closure reaches no conjunct the analysis reads"
			);
		}
	}

	#[test]
	fn an_enforcing_table_wraps_the_read() {
		let mut table = answers(vec![idx(1, "ix", &["x"])]);
		table.enforces = true;
		let choice = chosen(&table, "WHERE x = 1").expect("a choice");
		assert_eq!(choice.enforce, Some(TableName::from("t")));
	}

	#[test]
	fn a_record_id_conjunct_reads_the_record() {
		let table = answers(vec![idx(1, "ix", &["x"])]);
		let choice = chosen(&table, "WHERE x = 1 AND id = t:5").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::RecordScan);
		assert_eq!(choice.point_lookup, Some(1));
		assert_eq!(choice.residual, vec![0, 1]);
	}

	#[test]
	fn a_composite_record_id_conjunct_reads_the_record() {
		let table = answers(Vec::new());
		for snippet in ["WHERE id = t:['a', 2026]", "WHERE id = t:{ k: 'a', y: 2026 }"] {
			let choice = chosen(&table, snippet).expect("a choice");
			assert_eq!(tag(&choice), PhysTag::RecordScan, "{snippet}");
			assert_eq!(choice.point_lookup, Some(0), "{snippet}");
		}
	}

	#[test]
	fn a_composite_record_id_of_a_structured_key_is_no_point_lookup() {
		let mut table = answers(Vec::new());
		table.key = Some(key_range_fixtures::pair());
		let choice = chosen(&table, "WHERE id = t:['a', 2026]").expect("a choice");
		assert_eq!(choice.point_lookup, None);
		assert_ne!(tag(&choice), PhysTag::RecordScan);
	}

	#[test]
	fn a_timestamp_derived_bound_stays_an_operand_and_a_residual() {
		let table = answers(vec![idx(1, "ix", &["at"])]);
		let bound = LogicalExpr::Binary {
			left: Box::new(LogicalExpr::Field(Strand::from("at"))),
			op: BinaryOperator::MoreThan,
			right: Box::new(LogicalExpr::Param(crate::expr::Param::from(String::from(
				"__operand0",
			)))),
		};
		let predicate = Reconstructed {
			conjuncts: vec![(0, bound)],
			withheld: Vec::new(),
			operands: vec![ValueId::default()],
		};
		let choice = select_access(&table_source(), &table, &predicate, None, &decision())
			.expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
		assert_eq!(choice.residual, vec![0]);
		assert!(!format!("{choice:?}").contains("Datetime"));
	}

	#[test]
	fn a_withheld_conjunct_stays_a_residual() {
		let table = answers(vec![idx(1, "ix", &["x"])]);
		let (cond, _, _) = parts("WHERE x = 1");
		let predicate = Reconstructed {
			conjuncts: vec![(0, cond.expect("a condition"))],
			withheld: vec![1],
			..Reconstructed::default()
		};
		let choice = select_access(&table_source(), &table, &predicate, None, &decision())
			.expect("a choice");
		assert_eq!(tag(&choice), PhysTag::IndexScan);
		assert_eq!(choice.residual, vec![0, 1]);
	}

	/// A binary node over `left` and `right`.
	fn binary(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Binary {
			left: Box::new(left),
			op,
			right: Box::new(right),
		}
	}

	/// `name = value`.
	fn field_is(name: &str, value: i64) -> LogicalExpr {
		binary(
			LogicalExpr::Field(Strand::from(name)),
			BinaryOperator::Equal,
			LogicalExpr::Literal(LogicalLiteral::Integer(value)),
		)
	}

	/// `(x = 1 AND true) OR y = 2`: a conjunct whose nested `AND` the planner's strippers collapse.
	fn collapsing_conjunct() -> LogicalExpr {
		binary(
			binary(
				field_is("x", 1),
				BinaryOperator::And,
				LogicalExpr::Literal(LogicalLiteral::Bool(true)),
			),
			BinaryOperator::Or,
			field_is("y", 2),
		)
	}

	/// A conjunct the strippers rewrite inside, not at the top of the chain, is still a residual
	/// of a `KNN` scan, and the pre-filter's residual is every such conjunct its bitmap does not
	/// cover, found by position.
	#[test]
	fn a_conjunct_a_stripper_collapses_inside_stays_a_residual() {
		let conjunct = collapsing_conjunct();
		let collapsed = strip_knn_from_condition(&conjunct).expect("a condition remains");
		assert_ne!(collapsed, conjunct);
		let predicate = Reconstructed::of_exprs([conjunct, field_is("z", 3)]);
		assert_eq!(knn_residual(&predicate).expect("a residual"), vec![0, 1]);
		assert_eq!(uncovered_positions(&predicate, &[1]).expect("positions"), vec![0]);
	}

	#[test]
	fn equal_conjuncts_are_covered_one_at_a_time() {
		let predicate =
			Reconstructed::of_exprs([field_is("x", 1), field_is("y", 2), field_is("x", 1)]);
		assert_eq!(uncovered_positions(&predicate, &[0]).expect("positions"), vec![1, 2]);
		assert_eq!(uncovered_positions(&predicate, &[2]).expect("positions"), vec![0, 1]);
		assert_eq!(uncovered_positions(&predicate, &[0, 2]).expect("positions"), vec![1]);
	}

	#[test]
	fn a_covered_conjunct_no_readable_conjunct_stands_at_is_an_invariant_error() {
		let predicate = Reconstructed::of_exprs([field_is("x", 1)]);
		assert!(matches!(uncovered_positions(&predicate, &[1]), Err(IrError::Invariant { .. })));
	}

	/// A pre-filtered search whose conjuncts include a projection function call, which the index
	/// analysis reads as the field it names, covers the indexed conjunct and leaves the call to
	/// the search's own judgement, at its own position. The tag index has recorded storing no
	/// array in its column, so it lists one entry per record.
	#[test]
	fn a_prefilter_residual_holds_a_projection_call_the_analysis_rewrites() {
		use crate::catalog::{
			BTREE_ENTRY_DOC_IDS_FORMAT_VERSION, Distance, HnswParams, VectorType,
		};
		use crate::exec::index::access_path::ColumnFlags;

		let hnsw = Index::Hnsw(HnswParams {
			dimension: 2,
			distance: Distance::Euclidean,
			vector_type: VectorType::F32,
			m: 12,
			m0: 24,
			ml: 0.4.into(),
			ef_construction: 150,
			extend_candidates: false,
			keep_pruned_connections: false,
			use_hashed_vector: false,
		});
		let mut by_tag = idx(1, "ix_tag", &["tag"]);
		by_tag.format_version = BTREE_ENTRY_DOC_IDS_FORMAT_VERSION;
		let mut table = answers(vec![by_tag, idx_def(2, "hn", &["emb"], hnsw)]);
		let columns = table.columns.as_mut().expect("the columns are read");
		let scalar: Vec<ColumnFlags> = vec![Box::new([true]), Box::default()];
		columns.scalar = Some(scalar.into());
		let choice = chosen(
			&table,
			"WHERE emb <|2,40|> [1.0, 2.0] AND tag = 'a' AND type::field('cat') = 'x'",
		)
		.expect("a choice");
		let knn = choice.knn.as_ref().expect("a KNN scan");
		let PhysOp::KnnScan(attrs) = &choice.access else {
			panic!("expected a KNN scan, got {:?}", tag(&choice));
		};
		assert!(attrs.filter.as_ref().is_some_and(|filter| filter.prefilter.is_some()));
		assert_eq!(knn.prefilter_residual, vec![2]);
		assert_eq!(knn.residual, vec![1, 2]);
		assert_eq!(choice.residual, vec![1, 2]);
	}

	#[test]
	fn a_conjunct_that_is_an_and_chain_is_an_invariant_error() {
		let chain = binary(field_is("x", 1), BinaryOperator::And, field_is("y", 2));
		let predicate = Reconstructed::of_exprs([chain]);
		assert!(matches!(knn_residual(&predicate), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn an_ordering_no_index_covers_keeps_one_access_path() {
		let table = answers(vec![idx(1, "ix", &["x"])]);
		let choice = chosen(&table, "WHERE x = 1 ORDER BY y").expect("a choice");
		let PhysOp::IndexScan(attrs) = &choice.access else {
			panic!("an index scan, got {:?}", choice.access);
		};
		assert_eq!(attrs.ordered_by, None);
		assert!(choice.fallback.is_none());
	}

	/// A key-range access over `[a: int, b: int]` that pins `a` to the parameter `$p`.
	fn key_range_over_a_deferred_bound() -> KeyRangeAccess {
		use crate::exec::index::key_range::{DeliveredKeyOrder, KeyBound, KeyEq};
		KeyRangeAccess {
			key: key_range_fixtures::pair(),
			eq: Box::new([KeyEq::One(KeyBound::Deferred(LogicalExpr::Param(
				crate::expr::Param::from("p".to_owned()),
			)))]),
			range: None,
			direction: Direction::Forward,
			delivers: DeliveredKeyOrder::None,
			ordered_by: Vec::new(),
		}
	}

	/// The op `path` decides for `SELECT * FROM t WHERE a = $p AND b > 1`.
	fn mapped_decision(
		table: &TableAnswers,
		path: AccessPath,
		direction: Direction,
	) -> Result<AccessChoice, IrError> {
		let (cond, ..) = parts("WHERE a = $p AND b > 1");
		let cond = cond.expect("a condition");
		let predicate = Reconstructed::of_condition(&cond);
		let decision = Decision {
			path,
			direction,
			read: ReadClauses::default(),
			order_withheld: false,
		};
		map_decision(decision, table, &predicate, Some(&cond), None, None)
	}

	#[test]
	fn a_key_range_reads_the_range_and_keeps_the_whole_predicate() {
		let mut table = answers(Vec::new());
		table.enforces = true;
		let access = key_range_over_a_deferred_bound();
		let choice = mapped_decision(&table, AccessPath::KeyRange(access), Direction::Forward)
			.expect("a choice");
		let PhysOp::KeyRangeScan(attrs) = &choice.access else {
			panic!("a key-range scan, got {:?}", choice.access);
		};
		assert_eq!(attrs.table, table.table);
		assert_eq!(attrs.access.holes(), 1);
		assert_eq!(attrs.access.eq, vec![KeyEqShape::One(KeyBoundShape::Operand(0))]);
		assert_eq!(attrs.topk, None);
		assert_eq!(choice.key_operands.len(), 1);
		assert_eq!(choice.residual, vec![0, 1]);
		assert_eq!(choice.enforce, Some(table.table));
		assert!(choice.fallback.is_none());
	}

	#[test]
	fn a_key_range_over_a_lightweight_relation_is_an_invariant_break() {
		let mut table = answers(Vec::new());
		table.lightweight = true;
		let access = key_range_over_a_deferred_bound();
		let broken = mapped_decision(&table, AccessPath::KeyRange(access), Direction::Forward)
			.expect_err("a lightweight relation holds no key structure");
		assert!(matches!(broken, IrError::Verify(_)), "{broken:?}");
	}

	#[test]
	fn a_key_range_is_never_a_union_branch_or_a_bitmap_fallback() {
		let table = answers(Vec::new());
		let range = || AccessPath::KeyRange(key_range_over_a_deferred_bound());
		let union = AccessPath::Union {
			paths: vec![range()],
			dedupe: true,
		};
		assert!(matches!(
			mapped_decision(&table, union, Direction::Forward),
			Err(IrError::Verify(_))
		));
		let root = BitmapPlan::Graph {
			source: crate::val::RecordId::new("t".into(), 1i64),
			direction: crate::expr::Dir::Out,
			edge_tables: Vec::new(),
		};
		let fused = AccessPath::BitmapFusion {
			root,
			fallback: Some(Box::new(range())),
		};
		assert!(matches!(
			mapped_decision(&table, fused, Direction::Forward),
			Err(IrError::Verify(_))
		));
	}

	#[test]
	fn a_table_scan_carries_the_id_order_the_catalog_answered() {
		let mut table = answers(Vec::new());
		table.id_order = false;
		let choice = chosen(&table, "ORDER BY id").expect("a choice");
		let PhysOp::TableScan(attrs) = &choice.access else {
			panic!("a table scan, got {:?}", choice.access);
		};
		assert!(!attrs.id_order);
	}

	#[test]
	fn a_descending_id_ordering_scans_backward() {
		let table = answers(Vec::new());
		let choice = chosen(&table, "ORDER BY id DESC").expect("a choice");
		let PhysOp::TableScan(attrs) = &choice.access else {
			panic!("a table scan, got {:?}", choice.access);
		};
		assert_eq!(attrs.direction, Direction::Backward);
	}

	/// The selectivity probes of the table scan `choice` names, if it names one.
	fn probes(choice: &AccessChoice) -> Option<&ScanProbes> {
		if let PhysOp::TableScan(attrs) = &choice.access {
			attrs.probes.as_deref()
		} else {
			None
		}
	}

	fn probed(set: &ScanProbes) -> Vec<&str> {
		set.probes.iter().map(|(index, _)| index.index.as_str()).collect()
	}

	/// A table scan carries a probe for each conjunct an index answers, whether the scan was
	/// chosen under `WITH NOINDEX` or because no index answers the whole predicate.
	#[test]
	fn a_table_scan_carries_probes_for_the_conjuncts_an_index_answers() {
		use crate::exec::index::probe::ProbeBound;
		let table = answers(vec![idx(1, "by_a", &["a"]), idx(2, "by_b", &["b"])]);
		let choice = chosen(&table, "WITH NOINDEX WHERE a = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
		let set = probes(&choice).expect("probes");
		assert_eq!(probed(set), ["by_a"]);
		assert!(matches!(set.probes[0].1, BTreeAccess::Equality(_)));
		assert_eq!(set.bound, ProbeBound::Probe(0));

		let choice =
			chosen(&table, "WITH NOINDEX WHERE a = 1 AND b = 2 AND c = 3").expect("a choice");
		let set = probes(&choice).expect("probes");
		assert_eq!(probed(set), ["by_a", "by_b"]);
		assert!(matches!(&set.bound, ProbeBound::All(parts) if parts.len() == 2));

		let choice = chosen(&table, "WITH NOINDEX WHERE a = 1 OR b = 2").expect("a choice");
		let set = probes(&choice).expect("probes");
		assert_eq!(set.bound, ProbeBound::Any(vec![ProbeBound::Probe(0), ProbeBound::Probe(1)]));

		// An OR with an unindexed arm bounds nothing, and its sibling bounds the conjunction.
		let choice =
			chosen(&table, "WITH NOINDEX WHERE (a = 1 OR c = 2) AND b = 2").expect("a choice");
		assert_eq!(probed(probes(&choice).expect("probes")), ["by_b"]);
		let choice = chosen(&table, "WHERE a = 1 OR c = 2").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
		assert_eq!(probes(&choice), None, "an OR with an unindexed arm bounds nothing");
	}

	#[test]
	fn an_index_scan_or_an_unindexed_predicate_carries_no_probes() {
		let table = answers(vec![idx(1, "by_a", &["a"])]);
		let choice = chosen(&table, "WHERE a = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::IndexScan);
		let choice = chosen(&table, "WHERE c = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
		assert_eq!(probes(&choice), None);
		let choice = chosen(&table, "").expect("a choice");
		assert_eq!(probes(&choice), None);
	}

	/// A bound the binder resolved is a literal of the probe, as it is of an index scan; one only
	/// the run evaluates, which no index access reads, has no probe.
	#[test]
	fn a_probe_reads_the_bounds_an_index_access_reads() {
		let table = answers(vec![idx(1, "ix", &["at"])]);
		let choice = chosen(&table, "WITH NOINDEX WHERE at = 10").expect("a choice");
		let set = probes(&choice).expect("probes");
		assert!(matches!(
			&set.probes[0].1,
			BTreeAccess::Equality(crate::exec::index::access_path::BoundOperand::Literal(_))
		));
		let operand = LogicalExpr::Binary {
			left: Box::new(LogicalExpr::Field(Strand::from("at"))),
			op: BinaryOperator::MoreThan,
			right: Box::new(LogicalExpr::Param(crate::expr::Param::from(String::from(
				"__operand0",
			)))),
		};
		let predicate = Reconstructed {
			conjuncts: vec![(0, operand)],
			withheld: Vec::new(),
			operands: vec![ValueId::default()],
		};
		let choice = select_access(&table_source(), &table, &predicate, None, &decision())
			.expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
		assert_eq!(probes(&choice), None);
	}

	/// A conjunct over a field the caller may not read, and a read at a `VERSION`, are probed
	/// through no index.
	#[test]
	fn no_probe_reads_a_restricted_field_or_an_unstamped_entry() {
		let mut table = answers(vec![idx(1, "by_x", &["x"]), idx(2, "by_y", &["y"])]);
		table.restricted = RestrictedSelect::Fields(vec![
			crate::expr::logical::FieldPath::from_idiom(&Idiom::from_str("x").expect("idiom"))
				.expect("a field path"),
		]);
		let table = refreshed(table);
		let choice = chosen(&table, "WITH NOINDEX WHERE x = 1 AND y = 2").expect("a choice");
		assert_eq!(probed(probes(&choice).expect("probes")), ["by_y"]);
		let choice = chosen(&table, "WITH NOINDEX WHERE x = 1").expect("a choice");
		assert_eq!(probes(&choice), None);

		let mut table = answers(vec![idx(1, "by_x", &["x"])]);
		table.version = ReadVersion::Stamped(7);
		let table = refreshed(table);
		let choice = chosen(&table, "WITH NOINDEX WHERE x = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
		assert_eq!(probes(&choice), None);
	}

	#[test]
	fn an_index_over_a_restricted_field_is_withheld() {
		let mut table = answers(vec![idx(1, "ix", &["x"])]);
		table.restricted = RestrictedSelect::Fields(vec![
			crate::expr::logical::FieldPath::from_idiom(&Idiom::from_str("x").expect("idiom"))
				.expect("a field path"),
		]);
		let table = refreshed(table);
		let choice = chosen(&table, "WHERE x = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
	}

	#[test]
	fn an_unresolved_permission_context_scans_the_table_with_no_index() {
		let mut table = answers(vec![idx(1, "ix", &["x"])]);
		table.restricted = RestrictedSelect::Unresolved;
		table.enforces = true;
		let choice = chosen(&table, "WHERE x = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
		assert_eq!(choice.enforce, Some(TableName::from("t")));
		assert_eq!(choice.residual, vec![0]);
		assert!(choice.refusal.is_none());
		// The nodes above a resolved source enforce, so the scan does not.
		table.enforces = false;
		let choice = chosen(&table, "WHERE x = 1").expect("a choice");
		assert_eq!(choice.enforce, None);
	}

	#[test]
	fn an_unresolved_permission_context_still_folds_a_false_predicate_and_obeys_noindex() {
		let mut table = answers(vec![idx(1, "ix", &["x"])]);
		table.restricted = RestrictedSelect::Unresolved;
		let choice = chosen(&table, "WITH NOINDEX WHERE x = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
		let predicate =
			Reconstructed::of_exprs([LogicalExpr::Literal(LogicalLiteral::Bool(false))]);
		let choice = select_access(&table_source(), &table, &predicate, None, &decision())
			.expect("a choice");
		assert_eq!(tag(&choice), PhysTag::Union);
	}

	#[test]
	fn columns_that_could_not_be_read_scan_the_table_with_no_index() {
		let mut table = answers(vec![idx(1, "ix", &["x"])]);
		table.columns = None;
		table.enforces = true;
		let choice = chosen(&table, "WHERE x = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
		assert_eq!(choice.enforce, Some(TableName::from("t")));
		assert!(admissible_access_candidates(&table, None).is_none());
	}

	#[test]
	fn noindex_admits_no_candidate() {
		let table = answers(vec![idx(1, "ix", &["x"])]);
		let candidates = admissible_access_candidates(&table, Some(&With::NoIndex));
		assert!(candidates.expect("a decided set").is_empty());
	}

	#[test]
	fn an_unresolved_context_has_no_candidates() {
		let mut table = answers(vec![idx(1, "ix", &["x"])]);
		table.restricted = RestrictedSelect::Unresolved;
		assert!(admissible_access_candidates(&table, None).is_none());
	}

	#[test]
	fn a_versioned_read_admits_only_full_text() {
		let mut table = answers(vec![idx(1, "ix", &["x"]), fulltext(2, "ft", &["body"])]);
		table.version = ReadVersion::Stamped(7);
		let table = refreshed(table);
		let candidates = admissible_access_candidates(&table, None).expect("a decided set");
		assert_eq!(candidates.len(), 1);
		assert_eq!(candidates[0].index.index, Strand::from("ft"));
		assert_eq!(candidates[0].kind, IndexKindClass::FullText);
	}

	#[test]
	fn a_restricted_column_drops_its_index() {
		let mut table = answers(vec![idx(1, "ix", &["x"]), idx(2, "iy", &["y"])]);
		table.restricted = RestrictedSelect::Fields(vec![
			crate::expr::logical::FieldPath::from_idiom(&Idiom::from_str("x").expect("idiom"))
				.expect("a field path"),
		]);
		let table = refreshed(table);
		let candidates = admissible_access_candidates(&table, None).expect("a decided set");
		assert_eq!(candidates.len(), 1);
		assert_eq!(candidates[0].index.index, Strand::from("iy"));
	}

	#[test]
	fn a_unique_index_is_a_unique_btree_candidate() {
		let table = answers(vec![
			idx_def(1, "u", &["x"], Index::Uniq),
			idx_def(2, "n", &["y"], Index::Idx),
		]);
		let candidates = admissible_access_candidates(&table, None).expect("a decided set");
		assert_eq!(candidates.len(), 2);
		assert!(candidates[0].unique);
		assert_eq!(candidates[0].kind, IndexKindClass::BTree);
		assert_eq!(candidates[0].index.table, TableName::from("t"));
		assert_eq!(candidates[0].columns.as_slice(), [Strand::from("x")]);
		assert!(!candidates[1].unique);
		assert_eq!(candidates[1].kind, IndexKindClass::BTree);
	}

	#[test]
	fn a_deferred_version_scans_the_table_unless_the_predicate_names_an_index_operator() {
		let mut table = answers(vec![idx(1, "ix", &["x"])]);
		table.version = ReadVersion::Deferred;
		let choice = chosen(&table, "WHERE x = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
	}

	#[test]
	fn a_stamped_version_reads_only_a_full_text_index() {
		let mut table = answers(vec![idx(1, "ix", &["x"]), fulltext(2, "ft", &["body"])]);
		table.version = ReadVersion::Stamped(7);
		let table = refreshed(table);
		let choice = chosen(&table, "WHERE x = 1").expect("a choice");
		assert_eq!(tag(&choice), PhysTag::TableScan);
	}

	#[test]
	fn a_locked_read_of_a_whole_table_is_the_refusal_the_planner_raises() {
		let table = answers(Vec::new());
		let mut source = table_source();
		source.for_update = true;
		let choice = select_access(&source, &table, &Reconstructed::default(), None, &decision())
			.expect("a refusal is an answer");
		assert!(matches!(
			choice.refusal,
			Some(Refusal::Query { ref message }) if message == crate::dbs::FOR_UPDATE_TARGETS_ERROR
		));
	}

	#[test]
	fn a_source_that_reads_a_value_has_no_access_path() {
		let table = answers(Vec::new());
		for source_ref in [SourceRefShape::Mock, SourceRefShape::Param, SourceRefShape::Dynamic] {
			let source = SourceAttrs {
				source_ref,
				with: None,
				for_update: false,
				may_be_defined: false,
				may_meet_change: false,
				resolved: false,
			};
			let error =
				select_access(&source, &table, &Reconstructed::default(), None, &decision())
					.expect_err("a value is not a table");
			assert!(matches!(error, IrError::Stale(_)));
		}
	}

	#[test]
	fn a_value_source_reads_values() {
		let source = |source_ref, for_update| SourceAttrs {
			source_ref,
			with: None,
			for_update,
			may_be_defined: false,
			may_meet_change: false,
			resolved: false,
		};
		let read = |source: &SourceAttrs, call_shaped| {
			if let Ok(Some(PhysOp::Values {
				read,
			})) = value_source(source, call_shaped)
			{
				Some(read)
			} else {
				None
			}
		};
		assert_eq!(
			read(&source(SourceRefShape::Mock, false), false),
			Some(ValuesRead::Expression {
				written: false
			})
		);
		assert_eq!(read(&source(SourceRefShape::Param, false), false), Some(ValuesRead::Dynamic));
		assert_eq!(read(&source(SourceRefShape::Dynamic, false), true), Some(ValuesRead::Dynamic));
		assert_eq!(
			read(&source(SourceRefShape::Dynamic, false), false),
			Some(ValuesRead::Expression {
				written: false
			})
		);
		assert_eq!(read(&source(SourceRefShape::Param, true), false), Some(ValuesRead::Locked));
		assert!(value_source(&source(SourceRefShape::Mock, true), false).is_err());
		assert!(matches!(value_source(&source(SourceRefShape::RecordId, false), false), Ok(None)));
	}

	#[test]
	fn a_false_predicate_reads_nothing() {
		let table = answers(vec![idx(1, "ix", &["x"])]);
		let predicate =
			Reconstructed::of_exprs([LogicalExpr::Literal(LogicalLiteral::Bool(false))]);
		let choice = select_access(&table_source(), &table, &predicate, None, &decision())
			.expect("a choice");
		assert_eq!(tag(&choice), PhysTag::Union);
	}

	#[test]
	fn the_source_pattern_is_named_by_its_tag() {
		assert_eq!(SourcePattern.name(), SourcePattern.source().mnemonic());
	}
}

#[cfg(test)]
mod convert_tests {
	use std::collections::HashMap;

	use super::*;
	use crate::catalog::{HnswParams, IndexId};
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::core::convert::{ConversionSet, convert};
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::core::ir::{IrScope, Walk, WalkOrder};
	use crate::exec::ir::core::location::{LocOrigin, Location};
	use crate::exec::ir::expr::ops::{CallAttrs, NestedStrategy, ShortCircuitOp};
	use crate::exec::ir::iface::catalog::{EffectCx, SemanticCatalog};
	use crate::exec::ir::iface::conversion::PHYSICAL_PHASE;
	use crate::exec::ir::iface::fold::NotYetFolder;
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::iface::ids::{BlockId, FrameId, SnapshotId};
	use crate::exec::ir::iface::region_kind::{ClauseRole, StatementCommit};
	use crate::exec::ir::logical::ops::{
		LoopKind, LoopSink, SourceOrder, WriteStageKind, WriteStages,
	};
	use crate::exec::ir::op_kind::OperandRule;
	use crate::exec::ir::phys::access_expr::predicate_of_region;
	use crate::exec::ir::phys::ops::PhysTag;
	use crate::expr::logical::GuardKind;
	use crate::expr::operator::NearestNeighbor;
	use crate::expr::{Function, Idiom};

	/// A semantic catalog that holds no definition.
	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	/// A physical catalog that answers the reads it is given, and a read it is given nothing for
	/// is not loaded. `unknown` answers every read `Unknown`, as after a catalog change.
	struct Held {
		answers: HashMap<SourceKey, Arc<TableAnswers>>,
		unknown: bool,
	}

	impl Held {
		fn of(answers: Vec<TableAnswers>) -> Self {
			Self {
				answers: answers
					.into_iter()
					.map(|answers| {
						((answers.table.clone(), None, ReadVersion::Current), Arc::new(answers))
					})
					.collect(),
				unknown: false,
			}
		}
	}

	impl PhysicalCatalog for Held {
		/// The defined indexes of a held table are the ones its answers carry, all online.
		fn indexes(&self, table: &TableName) -> CatalogAnswer<Arc<[IndexDefinition]>> {
			if self.unknown {
				return CatalogAnswer::Unknown;
			}
			match self.answers.values().find(|answers| &answers.table == table) {
				Some(answers) => CatalogAnswer::Found(Arc::clone(&answers.indexes)),
				None => CatalogAnswer::NotLoaded,
			}
		}

		fn table_answers(&self, key: &SourceKey) -> CatalogAnswer<Arc<TableAnswers>> {
			if self.unknown {
				return CatalogAnswer::Unknown;
			}
			match self.answers.get(key) {
				Some(answers) => CatalogAnswer::Found(Arc::clone(answers)),
				None => CatalogAnswer::NotLoaded,
			}
		}
	}

	fn index(id: u32, name: &str, col: &str, kind: Index) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(id),
			name: Strand::from(name),
			table_name: "t".into(),
			cols: vec![col.parse::<Idiom>().expect("an idiom")],
			index: kind,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
			nulls_not_distinct: false,
		}
	}

	fn hnsw() -> Index {
		Index::Hnsw(HnswParams {
			dimension: 2,
			distance: crate::catalog::Distance::Euclidean,
			vector_type: crate::catalog::VectorType::F32,
			m: 12,
			m0: 24,
			ml: 0.4.into(),
			ef_construction: 150,
			extend_candidates: false,
			keep_pruned_connections: false,
			use_hashed_vector: false,
		})
	}

	/// One conjunct of a `WHERE`.
	enum Conjunct {
		/// `<field> = <integer>`.
		Equals(&'static str, i64),
		/// `id = <table>:<integer>`.
		RecordId(&'static str, i64),
		/// `<field> <|k,ef|> [<floats>]`.
		Knn(&'static str, Vec<f64>, u32, u32),
		/// `<field> <|k,EUCLIDEAN|> [<floats>]`, a brute-force operator.
		Brute(&'static str, Vec<f64>, u32),
		/// `<field> = <integer> OR v <|2,40|> [1, 2]`: a `KNN` operator under an `OR`.
		KnnUnderOr(&'static str, i64),
		/// `<field> = <integer> OR <field> = <integer>`.
		Either(&'static str, i64, &'static str, i64),
		/// A predicate folded to `false`.
		Falsity,
		/// `<integer> = <integer>`, which no fold has reduced.
		Constants(i64, i64),
		/// `string::len(<field>) > 3 OR <field> @@ 'x'`: a `MATCHES` operator beside a call.
		MatchesOrCall(&'static str),
		/// `<field> = (<a statement that holds a MATCHES operator>)`.
		EqualsSubqueryOfMatches(&'static str),
		/// `<field> @@ 'x' OR <field> = (<a statement>)`.
		MatchesOrSubquery(&'static str),
		/// `<field> @@ 'x' OR deref(1)`: a form no binder emits.
		MatchesOrDeref(&'static str),
		/// `<loop over an array whose body holds a KNN operator> = 1`.
		LoopOfKnn,
		/// `<field> = $<param>`: a field against a parameter.
		IsParam(&'static str, &'static str),
	}

	fn spec(kind: OpKind, operands: &[ValueId], results: usize) -> OpSpec {
		OpSpec {
			kind,
			operands: operands.iter().copied().collect(),
			results: vec![TypeKnowledge::Unknown; results].into_iter().collect(),
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			loc: Location {
				stmt: 0,
				span: None,
				origin: LocOrigin::Synthesized,
			},
		}
	}

	fn emit(rw: &mut Rewriter<'_>, block: BlockId, kind: OpKind, operands: &[ValueId]) -> ValueId {
		let results = usize::from(!matches!(
			kind,
			OpKind::Expr(ExprOp::Yield) | OpKind::Logical(LogOp::Yield)
		));
		let op = rw
			.create(InsertPoint::End(block), spec(kind, operands, results))
			.expect("an op is created");
		rw.ir().op(op).and_then(|data| data.results.first().copied()).unwrap_or_default()
	}

	fn expr(rw: &mut Rewriter<'_>, block: BlockId, kind: ExprOp, operands: &[ValueId]) -> ValueId {
		emit(rw, block, OpKind::Expr(kind), operands)
	}

	/// The value of the conjunct `c` over `row`, in `block`.
	fn conjunct(rw: &mut Rewriter<'_>, block: BlockId, row: ValueId, c: &Conjunct) -> ValueId {
		let field = |rw: &mut Rewriter<'_>, name: &str| {
			expr(
				rw,
				block,
				ExprOp::Field {
					name: Strand::new(name),
				},
				&[row],
			)
		};
		match c {
			Conjunct::Equals(name, n) => {
				let left = field(rw, name);
				let right = expr(rw, block, ExprOp::Literal(Box::new(Value::from(*n))), &[]);
				expr(
					rw,
					block,
					ExprOp::Binary {
						op: BinaryOperator::Equal,
					},
					&[left, right],
				)
			}
			Conjunct::RecordId(table, n) => {
				let left = field(rw, "id");
				let right = expr(
					rw,
					block,
					ExprOp::RecordId(Box::new(RecordIdAttrs {
						table: (*table).into(),
						key_shape: KeyShape::Number(*n),
					})),
					&[],
				);
				expr(
					rw,
					block,
					ExprOp::Binary {
						op: BinaryOperator::Equal,
					},
					&[left, right],
				)
			}
			Conjunct::Knn(name, vector, k, ef) => {
				let left = field(rw, name);
				let vector = Value::Array(
					vector.iter().map(|float| Value::from(*float)).collect::<Vec<_>>().into(),
				);
				let right = expr(rw, block, ExprOp::Literal(Box::new(vector)), &[]);
				expr(
					rw,
					block,
					ExprOp::Binary {
						op: BinaryOperator::NearestNeighbor(Box::new(
							NearestNeighbor::Approximate(*k, *ef),
						)),
					},
					&[left, right],
				)
			}
			Conjunct::Brute(name, vector, k) => {
				let left = field(rw, name);
				let vector = Value::Array(
					vector.iter().map(|float| Value::from(*float)).collect::<Vec<_>>().into(),
				);
				let right = expr(rw, block, ExprOp::Literal(Box::new(vector)), &[]);
				expr(
					rw,
					block,
					ExprOp::Binary {
						op: BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::K(
							*k,
							crate::expr::index::Distance::Euclidean,
						))),
					},
					&[left, right],
				)
			}
			Conjunct::KnnUnderOr(name, n) => {
				let left = conjunct(rw, block, row, &Conjunct::Equals(name, *n));
				let either = rw
					.create(
						InsertPoint::End(block),
						spec(
							OpKind::Expr(ExprOp::ShortCircuit {
								op: ShortCircuitOp::Or,
							}),
							&[left],
							1,
						),
					)
					.expect("the disjunction is created");
				let (_, lazy) = rw
					.create_region(either, RegionKind::LazyOperand, &[])
					.expect("the right operand is created");
				let right = conjunct(rw, lazy, row, &Conjunct::Knn("v", vec![1.0, 2.0], 2, 40));
				emit(rw, lazy, OpKind::Expr(ExprOp::Yield), &[right]);
				rw.ir().op(either).expect("the disjunction exists").results[0]
			}
			Conjunct::Either(left, a, right, b) => {
				let first = conjunct(rw, block, row, &Conjunct::Equals(left, *a));
				let either = rw
					.create(
						InsertPoint::End(block),
						spec(
							OpKind::Expr(ExprOp::ShortCircuit {
								op: ShortCircuitOp::Or,
							}),
							&[first],
							1,
						),
					)
					.expect("the disjunction is created");
				let (_, lazy) = rw
					.create_region(either, RegionKind::LazyOperand, &[])
					.expect("the right operand is created");
				let second = conjunct(rw, lazy, row, &Conjunct::Equals(right, *b));
				emit(rw, lazy, OpKind::Expr(ExprOp::Yield), &[second]);
				rw.ir().op(either).expect("the disjunction exists").results[0]
			}
			Conjunct::Falsity => {
				expr(rw, block, ExprOp::Literal(Box::new(Value::Bool(false))), &[])
			}
			Conjunct::MatchesOrCall(name) => {
				let field = |rw: &mut Rewriter<'_>| {
					expr(
						rw,
						block,
						ExprOp::Field {
							name: Strand::new(name),
						},
						&[row],
					)
				};
				let measured = field(rw);
				let length = expr(
					rw,
					block,
					ExprOp::Call(Box::new(CallAttrs::of(Function::Normal(
						"string::len".to_owned(),
					)))),
					&[measured],
				);
				let bound = expr(rw, block, ExprOp::Literal(Box::new(Value::from(3_i64))), &[]);
				let longer = expr(
					rw,
					block,
					ExprOp::Binary {
						op: BinaryOperator::MoreThan,
					},
					&[length, bound],
				);
				let either = rw
					.create(
						InsertPoint::End(block),
						spec(
							OpKind::Expr(ExprOp::ShortCircuit {
								op: ShortCircuitOp::Or,
							}),
							&[longer],
							1,
						),
					)
					.expect("the disjunction is created");
				let (_, lazy) = rw
					.create_region(either, RegionKind::LazyOperand, &[])
					.expect("the right operand is created");
				let right = matches_conjunct(rw, lazy, row, name);
				emit(rw, lazy, OpKind::Expr(ExprOp::Yield), &[right]);
				rw.ir().op(either).expect("the disjunction exists").results[0]
			}
			Conjunct::EqualsSubqueryOfMatches(name) => {
				let field = expr(
					rw,
					block,
					ExprOp::Field {
						name: Strand::new(name),
					},
					&[row],
				);
				let nested = subquery(rw, block, true);
				expr(
					rw,
					block,
					ExprOp::Binary {
						op: BinaryOperator::Equal,
					},
					&[field, nested],
				)
			}
			Conjunct::MatchesOrSubquery(name) => {
				let first = matches_conjunct(rw, block, row, name);
				let either = rw
					.create(
						InsertPoint::End(block),
						spec(
							OpKind::Expr(ExprOp::ShortCircuit {
								op: ShortCircuitOp::Or,
							}),
							&[first],
							1,
						),
					)
					.expect("the disjunction is created");
				let (_, lazy) = rw
					.create_region(either, RegionKind::LazyOperand, &[])
					.expect("the right operand is created");
				let field = expr(
					rw,
					lazy,
					ExprOp::Field {
						name: Strand::new(name),
					},
					&[row],
				);
				let nested = subquery(rw, lazy, false);
				let equal = expr(
					rw,
					lazy,
					ExprOp::Binary {
						op: BinaryOperator::Equal,
					},
					&[field, nested],
				);
				emit(rw, lazy, OpKind::Expr(ExprOp::Yield), &[equal]);
				rw.ir().op(either).expect("the disjunction exists").results[0]
			}
			Conjunct::MatchesOrDeref(name) => {
				let first = matches_conjunct(rw, block, row, name);
				let either = rw
					.create(
						InsertPoint::End(block),
						spec(
							OpKind::Expr(ExprOp::ShortCircuit {
								op: ShortCircuitOp::Or,
							}),
							&[first],
							1,
						),
					)
					.expect("the disjunction is created");
				let (_, lazy) = rw
					.create_region(either, RegionKind::LazyOperand, &[])
					.expect("the right operand is created");
				let id = expr(rw, lazy, ExprOp::Literal(Box::new(Value::from(1_i64))), &[]);
				let read = expr(
					rw,
					lazy,
					ExprOp::Deref {
						table_hint: None,
					},
					&[id],
				);
				emit(rw, lazy, OpKind::Expr(ExprOp::Yield), &[read]);
				rw.ir().op(either).expect("the disjunction exists").results[0]
			}
			Conjunct::IsParam(name, param) => {
				let element = field(rw, name);
				let bound = expr(
					rw,
					block,
					ExprOp::FreeParam {
						name: Strand::new(*param),
					},
					&[],
				);
				expr(
					rw,
					block,
					ExprOp::Binary {
						op: BinaryOperator::Equal,
					},
					&[element, bound],
				)
			}
			Conjunct::LoopOfKnn => {
				let xs = expr(rw, block, ExprOp::Literal(Box::new(Value::from(1_i64))), &[]);
				let looped = rw
					.create(
						InsertPoint::End(block),
						spec(
							OpKind::Logical(LogOp::Loop {
								kind: LoopKind::ElementFilter,
								sink: LoopSink::Filter,
								order: SourceOrder::Positional,
								step: None,
							}),
							&[xs],
							1,
						),
					)
					.expect("the loop is created");
				let (_, body) = rw
					.create_region(
						looped,
						RegionKind::Loop,
						&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
					)
					.expect("the loop body is created");
				let element = rw.ir().block(body).expect("the body exists").args[0];
				expr(
					rw,
					body,
					ExprOp::Field {
						name: Strand::new("secret"),
					},
					&[element],
				);
				let ranked = conjunct(rw, body, row, &Conjunct::Knn("v", vec![1.0], 2, 40));
				emit(rw, body, OpKind::Logical(LogOp::Yield), &[ranked]);
				let kept = rw.ir().op(looped).expect("the loop exists").results[0];
				let one = expr(rw, block, ExprOp::Literal(Box::new(Value::from(1_i64))), &[]);
				expr(
					rw,
					block,
					ExprOp::Binary {
						op: BinaryOperator::Equal,
					},
					&[kept, one],
				)
			}
			Conjunct::Constants(left, right) => {
				let left = expr(rw, block, ExprOp::Literal(Box::new(Value::from(*left))), &[]);
				let right = expr(rw, block, ExprOp::Literal(Box::new(Value::from(*right))), &[]);
				expr(
					rw,
					block,
					ExprOp::Binary {
						op: BinaryOperator::Equal,
					},
					&[left, right],
				)
			}
		}
	}

	/// `<field> @@ 'x'`.
	fn matches_conjunct(
		rw: &mut Rewriter<'_>,
		block: BlockId,
		row: ValueId,
		name: &str,
	) -> ValueId {
		let field = expr(
			rw,
			block,
			ExprOp::Field {
				name: Strand::new(name),
			},
			&[row],
		);
		let query = expr(rw, block, ExprOp::Literal(Box::new(Value::from("x"))), &[]);
		expr(
			rw,
			block,
			ExprOp::Binary {
				op: BinaryOperator::Matches(crate::expr::operator::MatchesOperator {
					rf: None,
					operator: crate::expr::operator::BooleanOperator::And,
				}),
			},
			&[field, query],
		)
	}

	/// A statement in value position of `block`, whose region holds a `MATCHES` operator when
	/// `matches`.
	fn subquery(rw: &mut Rewriter<'_>, block: BlockId, matches: bool) -> ValueId {
		let nested = rw
			.create(
				InsertPoint::End(block),
				spec(OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)), &[], 1),
			)
			.expect("the nested statement is created");
		let (_, inner) = rw
			.create_region(nested, RegionKind::Subquery, &[])
			.expect("the nested region is created");
		let value = if matches {
			let left = expr(rw, inner, ExprOp::Literal(Box::new(Value::from("a"))), &[]);
			let right = expr(rw, inner, ExprOp::Literal(Box::new(Value::from("b"))), &[]);
			expr(
				rw,
				inner,
				ExprOp::Binary {
					op: BinaryOperator::Matches(crate::expr::operator::MatchesOperator {
						rf: None,
						operator: crate::expr::operator::BooleanOperator::And,
					}),
				},
				&[left, right],
			)
		} else {
			expr(rw, inner, ExprOp::Literal(Box::new(Value::from(1_i64))), &[])
		};
		emit(rw, inner, OpKind::Expr(ExprOp::Yield), &[value]);
		rw.ir().op(nested).expect("the nested statement exists").results[0]
	}

	/// A request holding one statement: the source `source` (over `operand` when it names a
	/// value), a `Select` loop over its rows, and in the loop's body a filter over the conjuncts
	/// `conjuncts` joined by `AND`, when there are any. Answers the container and the source.
	fn statement(
		source: SourceAttrs,
		operand: Option<ExprOp>,
		conjuncts: &[Conjunct],
	) -> (Ir, OpId) {
		guarded_statement(source, operand, conjuncts, false)
	}

	/// As [`statement`], with a permission filter over the rows of the source when `guard`.
	fn guarded_statement(
		source: SourceAttrs,
		operand: Option<ExprOp>,
		conjuncts: &[Conjunct],
		guard: bool,
	) -> (Ir, OpId) {
		filtered_statement(source, operand, &[conjuncts], guard)
	}

	/// As [`guarded_statement`], with one filter in the loop's body for each of `filters`, in
	/// order, each over the rows the one before it passes on. A filter of no conjunct is left out.
	fn filtered_statement(
		source: SourceAttrs,
		operand: Option<ExprOp>,
		filters: &[&[Conjunct]],
		guard: bool,
	) -> (Ir, OpId) {
		looped_statement(source, operand, filters, guard, LoopKind::Select, LoopSink::Output)
	}

	/// As [`filtered_statement`], over a loop of `kind` with `sink`.
	fn looped_statement(
		source: SourceAttrs,
		operand: Option<ExprOp>,
		filters: &[&[Conjunct]],
		guard: bool,
		kind: LoopKind,
		sink: LoopSink,
	) -> (Ir, OpId) {
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let top = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let stmt = rw
			.create(
				InsertPoint::End(top),
				spec(
					OpKind::Logical(LogOp::Stmt {
						commit: StatementCommit::Auto,
						control: None,
					}),
					&[],
					1,
				),
			)
			.expect("the statement is created");
		let (_, block) = rw
			.create_region(
				stmt,
				RegionKind::Statement(StatementCommit::Auto),
				&[TypeKnowledge::Unknown],
			)
			.expect("the statement region is created");
		let operands: Vec<ValueId> =
			operand.into_iter().map(|kind| expr(&mut rw, block, kind, &[])).collect();
		let source = rw
			.create(
				InsertPoint::End(block),
				spec(OpKind::Logical(LogOp::Source(Box::new(source))), &operands, 1),
			)
			.expect("the source is created");
		let rows = rw.ir().op(source).expect("the source exists").results[0];
		let looped = rw
			.create(
				InsertPoint::End(block),
				spec(
					OpKind::Logical(LogOp::Loop {
						kind,
						sink,
						order: SourceOrder::Delivery,
						step: None,
					}),
					&[rows],
					1,
				),
			)
			.expect("the loop is created");
		let (_, body) = rw
			.create_region(
				looped,
				RegionKind::Loop,
				&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			)
			.expect("the loop body is created");
		let element = rw.ir().block(body).expect("the body exists").args[0];
		let mut row = element;
		if guard {
			let permission = rw
				.create(
					InsertPoint::End(body),
					spec(
						OpKind::Logical(LogOp::PermissionFilter {
							guard: GuardKind::Read,
							computed: None,
						}),
						&[element],
						1,
					),
				)
				.expect("the permission filter is created");
			let (_, clause) = rw
				.create_region(permission, RegionKind::PermissionFrame, &[TypeKnowledge::Unknown])
				.expect("the permission frame is created");
			let allowed = expr(&mut rw, clause, ExprOp::Literal(Box::new(Value::Bool(true))), &[]);
			emit(&mut rw, clause, OpKind::Expr(ExprOp::Yield), &[allowed]);
			row = rw.ir().op(permission).expect("the permission filter exists").results[0];
		}
		for conjuncts in filters.iter().filter(|conjuncts| !conjuncts.is_empty()) {
			let filter = rw
				.create(InsertPoint::End(body), spec(OpKind::Logical(LogOp::Filter), &[row], 1))
				.expect("the filter is created");
			let (_, clause) = rw
				.create_region(
					filter,
					RegionKind::Clause(ClauseRole::Where),
					&[TypeKnowledge::Unknown],
				)
				.expect("the where clause is created");
			let judged = rw.ir().block(clause).expect("the clause exists").args[0];
			let mut acc = conjunct(&mut rw, clause, judged, &conjuncts[0]);
			for next in &conjuncts[1..] {
				let joined = rw
					.create(
						InsertPoint::End(clause),
						spec(
							OpKind::Expr(ExprOp::ShortCircuit {
								op: ShortCircuitOp::And,
							}),
							&[acc],
							1,
						),
					)
					.expect("the conjunction is created");
				let (_, lazy) = rw
					.create_region(joined, RegionKind::LazyOperand, &[])
					.expect("the right operand is created");
				let right = conjunct(&mut rw, lazy, judged, next);
				emit(&mut rw, lazy, OpKind::Expr(ExprOp::Yield), &[right]);
				acc = rw.ir().op(joined).expect("the conjunction exists").results[0];
			}
			emit(&mut rw, clause, OpKind::Expr(ExprOp::Yield), &[acc]);
			row = rw.ir().op(filter).expect("the filter exists").results[0];
		}
		emit(&mut rw, body, OpKind::Logical(LogOp::Yield), &[row]);
		let output = rw.ir().op(looped).expect("the loop exists").results[0];
		emit(&mut rw, block, OpKind::Logical(LogOp::Yield), &[output]);
		(ir, source)
	}

	fn table_source() -> SourceAttrs {
		SourceAttrs {
			source_ref: SourceRefShape::Table("t".into()),
			with: None,
			for_update: false,
			may_be_defined: false,
			may_meet_change: false,
			resolved: false,
		}
	}

	/// Converts every source of `ir` with the source pattern alone, over what `physical` holds.
	async fn converted(mut ir: Ir, physical: &Held) -> Result<Ir, IrError> {
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let cx = PhysCx {
			effects: EffectCx {
				registry: &registry,
				catalog: &NoCatalog,
				frames: &frames,
			},
			physical,
			config: crate::exec::ir::iface::catalog::default_config(),
		};
		let mut set = ConversionSet::new();
		set.add(Box::new(SourcePattern));
		let mut facts = FactCache::new();
		convert(&mut ir, &mut facts, &set, &cx, &NotYetFolder, &PHYSICAL_PHASE).await?;
		declared_shapes_hold(&ir);
		Ok(ir)
	}

	/// Every physical op the conversion made has the operand count, result count and region kinds
	/// its tag declares.
	fn declared_shapes_hold(ir: &Ir) {
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			let Some(data) = ir.op(op) else {
				return Walk::Continue;
			};
			let OpKind::Phys(kind) = &data.kind else {
				return Walk::Continue;
			};
			let operands = data.operands.len();
			match kind.arity().operands {
				OperandRule::Exactly(n) => assert_eq!(operands, usize::from(n), "{:?}", kind.tag()),
				OperandRule::AtLeast(n) => assert!(operands >= usize::from(n), "{:?}", kind.tag()),
				OperandRule::Grouped(_) => {}
			}
			assert_eq!(data.results.len(), usize::from(kind.arity().results), "{:?}", kind.tag());
			let held: Vec<RegionKind> = data
				.regions
				.iter()
				.filter_map(|region| ir.region(*region))
				.map(|r| r.kind)
				.collect();
			assert_eq!(held, kind.region_kinds().to_vec(), "the regions of {:?}", kind.tag());
			Walk::Continue
		});
	}

	/// The ops of `ir` the walk visits, in pre-order.
	fn ops(ir: &Ir) -> Vec<OpKind> {
		let mut found = Vec::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(data) = ir.op(op) {
				found.push(data.kind.clone());
			}
			Walk::Continue
		});
		found
	}

	fn count(ir: &Ir, wanted: impl Fn(&OpKind) -> bool) -> usize {
		ops(ir).iter().filter(|kind| wanted(kind)).count()
	}

	fn phys(ir: &Ir, tag: PhysTag) -> usize {
		count(ir, |kind| matches!(kind, OpKind::Phys(phys) if phys.tag() == tag))
	}

	fn logical(ir: &Ir, tag: LogTag) -> usize {
		count(ir, |kind| matches!(kind, OpKind::Logical(log) if log.tag() == tag))
	}

	fn binary(ir: &Ir, wanted: impl Fn(&BinaryOperator) -> bool) -> usize {
		count(ir, |kind| matches!(kind, OpKind::Expr(ExprOp::Binary { op }) if wanted(op)))
	}

	fn plain(name: &str) -> TableAnswers {
		TableAnswers::plain(name.into(), true)
	}

	fn indexed(indexes: Vec<IndexDefinition>) -> Held {
		Held::of(vec![plain("t").with_indexes(indexes)])
	}

	#[tokio::test]
	async fn a_table_read_converts_to_a_table_scan() {
		let (ir, _) = statement(table_source(), None, &[]);
		let ir = converted(ir, &Held::of(vec![plain("t")])).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::TableScan), 1);
		assert_eq!(logical(&ir, LogTag::Source), 0);
		assert_eq!(phys(&ir, PhysTag::Enforced), 0);
	}

	#[tokio::test]
	async fn a_record_id_read_converts_to_a_record_scan_over_its_record_id() {
		let source = SourceAttrs {
			source_ref: SourceRefShape::RecordId,
			with: None,
			for_update: false,
			may_be_defined: false,
			may_meet_change: false,
			resolved: false,
		};
		let record = ExprOp::RecordId(Box::new(RecordIdAttrs {
			table: "t".into(),
			key_shape: KeyShape::Number(1),
		}));
		let (ir, _) = statement(source, Some(record), &[]);
		let ir = converted(ir, &Held::of(vec![plain("t")])).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::RecordScan), 1);
		assert_eq!(logical(&ir, LogTag::Source), 0);
	}

	#[tokio::test]
	async fn an_indexed_equality_converts_to_an_index_scan_and_leaves_its_filter() {
		let (ir, _) = statement(table_source(), None, &[Conjunct::Equals("x", 1)]);
		let held = indexed(vec![index(1, "ix", "x", Index::Idx)]);
		let ir = converted(ir, &held).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::IndexScan), 1);
		assert_eq!(logical(&ir, LogTag::Filter), 1, "the conjunct stays in the filter");
	}

	#[tokio::test]
	async fn a_table_that_is_not_defined_reads_nothing() {
		for conjunct in [Conjunct::Falsity, Conjunct::Constants(1, 2)] {
			let (ir, _) = statement(table_source(), None, &[conjunct]);
			let held = Held::of(vec![TableAnswers::plain("t".into(), false)]);
			let ir = converted(ir, &held).await.expect("the source converts");
			assert_eq!(phys(&ir, PhysTag::Vacant), 1);
			assert_eq!(phys(&ir, PhysTag::TableScan), 0);
		}
	}

	#[tokio::test]
	async fn a_read_that_enforces_is_wrapped_in_phys_enforced() {
		let (ir, _) = statement(table_source(), None, &[]);
		let mut answers = plain("t");
		answers.enforces = true;
		let ir = converted(ir, &Held::of(vec![answers])).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::Enforced), 1);
		assert_eq!(phys(&ir, PhysTag::TableScan), 1);
	}

	#[tokio::test]
	async fn a_source_whose_table_is_enforced_above_it_is_not_enforced_again() {
		let mut answers = plain("t");
		answers.enforces = true;
		let held = Held::of(vec![answers]);
		let (ir, _) = guarded_statement(table_source(), None, &[], true);
		let ir = converted(ir, &held).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::TableScan), 1);
		assert_eq!(phys(&ir, PhysTag::Enforced), 0, "the nodes above enforce");
		// With no node above, the read enforces.
		let (ir, _) = guarded_statement(table_source(), None, &[], false);
		let ir = converted(ir, &held).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::Enforced), 1);
	}

	#[test]
	fn the_subjects_of_a_write_are_read_unenforced() {
		// A write's stages judge each stored record themselves: the source is never cut to what
		// the caller may select, and is never refused for it.
		let stages = WriteStages(Box::new([WriteStageKind::CheckUpdatePermissions]));
		let (ir, source) = looped_statement(
			table_source(),
			None,
			&[],
			false,
			LoopKind::Write,
			LoopSink::Write(Box::new(stages)),
		);
		assert!(offer_of(&ir, source).expect("an offer").enforced_above);
		let (ir, source) = statement(table_source(), None, &[]);
		assert!(!offer_of(&ir, source).expect("an offer").enforced_above);
	}

	#[tokio::test]
	async fn answers_the_memo_does_not_hold_are_not_loaded() {
		let (ir, _) = statement(table_source(), None, &[]);
		let error = converted(ir, &Held::of(Vec::new())).await.expect_err("nothing is held");
		assert!(matches!(error, IrError::NotLoaded { .. }), "{error:?}");
	}

	#[tokio::test]
	async fn answers_after_a_catalog_change_plan_the_statement_late_bound() {
		let (ir, _) = statement(table_source(), None, &[]);
		let mut held = Held::of(vec![plain("t")]);
		held.unknown = true;
		let error = converted(ir, &held).await.expect_err("unknown answers");
		assert!(matches!(error, IrError::LateBind { .. }), "{error:?}");
		let internal: crate::exec::vm::NotCovered = error.into();
		assert_eq!(internal.kind, crate::exec::vm::NotCoveredKind::LateBind);
	}

	#[tokio::test]
	async fn a_key_range_scan_takes_its_deferred_bounds_as_operands() {
		let (ir, _) = statement(table_source(), None, &[Conjunct::IsParam("a", "p")]);
		let mut table = plain("t");
		table.key = Some(crate::exec::ir::phys::ops::key_range_fixtures::pair());
		let ir = converted(ir, &Held::of(vec![table])).await.expect("the source converts");
		let mut scans = Vec::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Phys(PhysOp::KeyRangeScan(attrs))) =
				ir.op(op).map(|data| &data.kind)
			{
				scans.push((op, attrs.access.holes()));
			}
			Walk::Continue
		});
		let [(scan, holes)] = scans.as_slice() else {
			panic!("one key-range scan, got {scans:?}");
		};
		assert_eq!(*holes, 1);
		let operands = &ir.op(*scan).expect("the scan exists").operands;
		assert_eq!(operands.len(), 1, "the deferred bound is the scan's operand");
		let bound = ir.value(operands[0]).map(|value| value.def);
		let Some(ValueDef::OpResult {
			op,
			..
		}) = bound
		else {
			panic!("the bound is an op's result");
		};
		assert!(matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Expr(ExprOp::FreeParam { name })) if name.as_str() == "p"
		));
		assert_eq!(logical(&ir, LogTag::Filter), 1, "the conjunct stays in the filter");
	}

	#[tokio::test]
	async fn a_source_naming_a_value_reads_values_without_the_catalog() {
		let source = SourceAttrs {
			source_ref: SourceRefShape::Param,
			with: None,
			for_update: false,
			may_be_defined: false,
			may_meet_change: false,
			resolved: false,
		};
		let operand = ExprOp::FreeParam {
			name: Strand::new("p"),
		};
		let (ir, _) = statement(source, Some(operand), &[]);
		let ir = converted(ir, &Held::of(Vec::new())).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::Values), 1);
		assert_eq!(logical(&ir, LogTag::Source), 0);
	}

	#[tokio::test]
	async fn a_locked_read_of_a_whole_table_replaces_the_statement_by_its_refusal() {
		let mut source = table_source();
		source.for_update = true;
		let (ir, _) = statement(source, None, &[]);
		let ir = converted(ir, &Held::of(vec![plain("t")])).await.expect("a refusal is an answer");
		assert_eq!(logical(&ir, LogTag::Source), 0);
		assert_eq!(logical(&ir, LogTag::Loop), 0);
		assert_eq!(phys(&ir, PhysTag::TableScan), 0);
		assert_eq!(
			count(&ir, |kind| matches!(
				kind,
				OpKind::Expr(ExprOp::Raise(raise)) if matches!(**raise, RaiseKind::Refused(..))
			)),
			1
		);
	}

	#[tokio::test]
	async fn a_record_id_conjunct_is_a_point_lookup_over_a_fresh_record_id() {
		let (ir, _) = statement(table_source(), None, &[Conjunct::RecordId("t", 5)]);
		let ir = converted(ir, &Held::of(vec![plain("t")])).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::RecordScan), 1);
		assert_eq!(phys(&ir, PhysTag::TableScan), 0);
		assert_eq!(
			count(&ir, |kind| matches!(kind, OpKind::Expr(ExprOp::RecordId(..)))),
			2,
			"the operand of the lookup and the one the conjunct compares"
		);
		assert_eq!(logical(&ir, LogTag::Filter), 1, "the conjunct stays in the filter");
	}

	#[tokio::test]
	async fn an_unresolved_permission_context_scans_enforced_with_no_index() {
		let (ir, _) = statement(table_source(), None, &[Conjunct::Equals("x", 1)]);
		let mut answers = plain("t").with_indexes(vec![index(1, "ix", "x", Index::Idx)]);
		answers.restricted = RestrictedSelect::Unresolved;
		answers.enforces = true;
		let ir = converted(ir, &Held::of(vec![answers])).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::IndexScan), 0);
		assert_eq!(phys(&ir, PhysTag::TableScan), 1);
		assert_eq!(phys(&ir, PhysTag::Enforced), 1);
	}

	/// A search over the vector index `hn` of `t`.
	fn knn(extra: Option<Conjunct>) -> Vec<Conjunct> {
		let mut conjuncts = vec![Conjunct::Knn("v", vec![1.0, 2.0], 2, 40)];
		conjuncts.extend(extra);
		conjuncts
	}

	#[tokio::test]
	async fn a_knn_scan_judges_its_operator_and_the_filter_above_keeps_the_rest() {
		let (ir, _) = statement(table_source(), None, &knn(Some(Conjunct::Equals("x", 1))));
		let held = indexed(vec![index(1, "hn", "v", hnsw())]);
		let ir = converted(ir, &held).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::KnnScan), 1, "a KNN scan");
		assert_eq!(
			binary(&ir, |op| matches!(op, BinaryOperator::NearestNeighbor(..))),
			0,
			"no op evaluates the operator the search answers"
		);
		// The conjunct the search does not answer is judged in the scan and above it.
		assert_eq!(binary(&ir, |op| matches!(op, BinaryOperator::Equal)), 2);
		assert_eq!(logical(&ir, LogTag::Filter), 0, "the filter is replaced");
		assert_eq!(phys(&ir, PhysTag::Filter), 1);
	}

	/// The bitmap of `ir`: its attributes, the kinds of its regions and the tags of the ops of each
	/// region, in order.
	fn bitmap_of(ir: &Ir) -> (BitmapAttrs, Vec<RegionKind>, Vec<Vec<PhysTag>>) {
		let mut found = None;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(data) = ir.op(op)
				&& let OpKind::Phys(PhysOp::Bitmap(attrs)) = &data.kind
			{
				let kinds =
					data.regions.iter().map(|region| ir.region(*region).expect("a region").kind);
				let bodies = data.regions.iter().map(|region| {
					ops_in(ir, *region)
						.filter_map(|member| match &ir.op(member)?.kind {
							OpKind::Phys(kind) => Some(kind.tag()),
							OpKind::Expr(..) | OpKind::Logical(..) => None,
						})
						.collect()
				});
				found = Some(((**attrs).clone(), kinds.collect(), bodies.collect()));
			}
			Walk::Continue
		});
		found.expect("a bitmap")
	}

	/// A bitmap fused from an `OR` states the index union it replaced as its one `Pipeline`
	/// region, the read that answers when a branch of the bitmap overflows. Only an index whose
	/// entries carry document ids is a branch of a bitmap.
	#[tokio::test]
	async fn a_bitmap_of_an_or_holds_the_union_it_replaced_as_a_region() {
		let (ir, _) = statement(table_source(), None, &[Conjunct::Either("x", 1, "y", 2)]);
		let current = |id, name, col| IndexDefinition {
			format_version: crate::catalog::INDEX_FORMAT_VERSION,
			..index(id, name, col, Index::Idx)
		};
		let held = indexed(vec![current(1, "ix", "x"), current(2, "iy", "y")]);
		let ir = converted(ir, &held).await.expect("the source converts");
		let (attrs, kinds, bodies) = bitmap_of(&ir);
		assert!(attrs.fallback);
		assert_eq!(kinds, vec![RegionKind::Pipeline]);
		assert_eq!(bodies, vec![vec![PhysTag::IndexUnion, PhysTag::Yield]]);
	}

	#[tokio::test]
	async fn a_knn_condition_read_from_two_filters_is_one_region_of_the_scan() {
		let first = [Conjunct::Knn("v", vec![1.0, 2.0], 2, 40), Conjunct::Equals("x", 1)];
		let second = [Conjunct::Equals("y", 2)];
		let (ir, _) = filtered_statement(table_source(), None, &[&first, &second], false);
		let held = indexed(vec![index(1, "hn", "v", hnsw())]);
		let ir = converted(ir, &held).await.expect("the source converts");
		let (attrs, kinds) = knn_scan_of(&ir);
		assert!(attrs.filter.expect("the search judges the rest").residual);
		assert_eq!(kinds, vec![RegionKind::Clause(ClauseRole::Where)]);
		assert_eq!(
			count(&ir, |kind| matches!(
				kind,
				OpKind::Expr(ExprOp::ShortCircuit {
					op: ShortCircuitOp::And
				})
			)),
			1,
			"the scan's region joins the conjunct of each filter"
		);
		assert_eq!(binary(&ir, |op| matches!(op, BinaryOperator::NearestNeighbor(..))), 0);
		assert_eq!(phys(&ir, PhysTag::Filter), 1, "the first filter keeps what it has left");
		assert_eq!(logical(&ir, LogTag::Filter), 1, "the second filter holds only what it held");
	}

	#[tokio::test]
	async fn a_filter_a_knn_scan_answers_whole_is_removed_among_several() {
		let first = [Conjunct::Equals("x", 1)];
		let second = [Conjunct::Knn("v", vec![1.0, 2.0], 2, 40)];
		let (ir, _) = filtered_statement(table_source(), None, &[&first, &second], false);
		let held = indexed(vec![index(1, "hn", "v", hnsw())]);
		let ir = converted(ir, &held).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::KnnScan), 1);
		assert_eq!(logical(&ir, LogTag::Filter), 1, "the first filter is untouched");
		assert_eq!(phys(&ir, PhysTag::Filter), 0);
		assert_eq!(binary(&ir, |op| matches!(op, BinaryOperator::NearestNeighbor(..))), 0);
	}

	/// The KNN scan of `ir`, with the kinds of its regions.
	fn knn_scan_of(ir: &Ir) -> (KnnScanAttrs, Vec<RegionKind>) {
		let mut found = None;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(data) = ir.op(op)
				&& let OpKind::Phys(PhysOp::KnnScan(attrs)) = &data.kind
			{
				let kinds = data
					.regions
					.iter()
					.map(|region| ir.region(*region).expect("the region exists").kind)
					.collect();
				found = Some(((**attrs).clone(), kinds));
			}
			Walk::Continue
		});
		found.expect("a KNN scan")
	}

	#[tokio::test]
	async fn a_knn_scan_gates_its_candidates_on_the_permission_above_its_source() {
		let (ir, _) =
			guarded_statement(table_source(), None, &knn(Some(Conjunct::Equals("x", 1))), true);
		let held = indexed(vec![index(1, "hn", "v", hnsw())]);
		let ir = converted(ir, &held).await.expect("the source converts");
		let (attrs, kinds) = knn_scan_of(&ir);
		let filter = attrs.filter.expect("the search is gated");
		assert_eq!(filter.permissions.len(), 1);
		assert_eq!(filter.permissions[0].guard, GuardKind::Read);
		assert!(filter.residual);
		// The permission comes first, then the condition.
		assert_eq!(kinds, vec![RegionKind::PermissionFrame, RegionKind::Clause(ClauseRole::Where)]);
		assert_eq!(logical(&ir, LogTag::PermissionFilter), 1, "the permission stays above");
	}

	#[tokio::test]
	async fn a_knn_search_with_no_condition_is_gated_on_the_permission_alone() {
		let (ir, _) = guarded_statement(table_source(), None, &knn(None), true);
		let held = indexed(vec![index(1, "hn", "v", hnsw())]);
		let ir = converted(ir, &held).await.expect("the source converts");
		let (attrs, kinds) = knn_scan_of(&ir);
		let filter = attrs.filter.expect("the search is gated");
		assert_eq!(filter.permissions.len(), 1);
		assert!(!filter.residual && !filter.matches_condition && filter.prefilter.is_none());
		assert_eq!(kinds, vec![RegionKind::PermissionFrame]);
	}

	#[tokio::test]
	async fn a_filter_whose_every_conjunct_a_knn_scan_answers_is_removed() {
		let (ir, _) = statement(table_source(), None, &knn(None));
		let held = indexed(vec![index(1, "hn", "v", hnsw())]);
		let ir = converted(ir, &held).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::KnnScan), 1);
		assert_eq!(logical(&ir, LogTag::Filter), 0);
		assert_eq!(phys(&ir, PhysTag::Filter), 0);
		assert_eq!(binary(&ir, |op| matches!(op, BinaryOperator::NearestNeighbor(..))), 0);
	}
	/// A statement whose source the conversion replaced by its refusal: a raise of the refusal and
	/// nothing that reads.
	fn refused(ir: &Ir) -> bool {
		phys(ir, PhysTag::TableScan) == 0
			&& phys(ir, PhysTag::KnnScan) == 0
			&& count(ir, |kind| {
				matches!(
					kind,
					OpKind::Expr(ExprOp::Raise(raise)) if matches!(**raise, RaiseKind::Refused(..))
				)
			}) == 1
	}

	#[tokio::test]
	async fn a_knn_operator_under_an_or_refuses_the_statement_whatever_the_index() {
		for held in [Held::of(vec![plain("t")]), indexed(vec![index(1, "hn", "v", hnsw())])] {
			let (ir, _) = statement(table_source(), None, &[Conjunct::KnnUnderOr("x", 1)]);
			let ir = converted(ir, &held).await.expect("a refusal is an answer");
			assert!(refused(&ir), "main refuses a KNN operator it cannot place");
		}
	}

	#[tokio::test]
	async fn a_brute_force_operator_hides_every_knn_operator_from_the_access_path() {
		let (ir, _) =
			statement(table_source(), None, &knn(Some(Conjunct::Brute("v", vec![0.0, 0.0], 1))));
		let held = indexed(vec![index(1, "hn", "v", hnsw())]);
		let ir = converted(ir, &held).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::KnnScan), 0, "no index search beside a brute-force rank");
		assert_eq!(phys(&ir, PhysTag::TableScan), 1);
		assert_eq!(
			binary(&ir, |op| matches!(op, BinaryOperator::NearestNeighbor(..))),
			2,
			"the filter keeps both operators until the search strips them"
		);
	}

	#[tokio::test]
	async fn a_record_id_conjunct_beside_a_brute_force_operator_is_a_point_lookup() {
		let (ir, _) = statement(
			table_source(),
			None,
			&[Conjunct::RecordId("t", 5), Conjunct::Brute("v", vec![0.0, 0.0], 1)],
		);
		let ir = converted(ir, &Held::of(vec![plain("t")])).await.expect("the source converts");
		assert_eq!(phys(&ir, PhysTag::RecordScan), 1, "main strips the operator before the lookup");
		assert_eq!(phys(&ir, PhysTag::TableScan), 0);
	}
	/// The predicate of the one filter of `ir`, read as the decision reads it.
	fn filter_predicate(ir: &Ir) -> Result<Reconstructed, IrError> {
		let mut region = None;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(data) = ir.op(op)
				&& matches!(data.kind, OpKind::Logical(LogOp::Filter))
			{
				region = data.regions.first().copied();
			}
			Walk::Continue
		});
		let mut predicate = Reconstructed::default();
		let region = region.expect("the statement holds a filter");
		assert!(predicate_of_region(ir, region, &mut predicate)?);
		Ok(predicate)
	}

	/// A `MATCHES` operator beside a call is read whole, as the planner holds it: its walk for a
	/// `MATCHES` operator finds the one in the chain, and the call keeps its name and arguments.
	#[test]
	fn a_matches_operator_beside_a_call_is_read_whole() {
		let (ir, _) = statement(table_source(), None, &[Conjunct::MatchesOrCall("a")]);
		let predicate = filter_predicate(&ir).expect("the predicate is read");
		assert!(predicate.withheld.is_empty());
		let [(0, read)] = predicate.conjuncts.as_slice() else {
			panic!("one conjunct is read, got {:?}", predicate.conjuncts);
		};
		assert!(crate::exec::planner::util::expr_contains_matches(read));
		let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Or,
			..
		} = read
		else {
			panic!("a disjunction, got {read:?}");
		};
		let LogicalExpr::Binary {
			left: measured,
			op: BinaryOperator::MoreThan,
			..
		} = &**left
		else {
			panic!("a comparison, got {left:?}");
		};
		assert!(matches!(
			&**measured,
			LogicalExpr::Call(call)
				if matches!(&call.receiver, Function::Normal(name) if name == "string::len")
					&& call.args == [LogicalExpr::Field(Strand::from("a"))]
		));
	}

	/// A `MATCHES` operator inside a nested statement belongs to the nested plan: the conjunct that
	/// holds it is a residual of the filter, as it is for the planner, whose walks stop there.
	#[test]
	fn a_matches_operator_in_a_nested_statement_leaves_the_conjunct_a_residual() {
		let (ir, _) = statement(table_source(), None, &[Conjunct::EqualsSubqueryOfMatches("a")]);
		let predicate = filter_predicate(&ir).expect("the predicate is read");
		assert_eq!(predicate.withheld, vec![0]);
		assert!(predicate.conjuncts.is_empty());
	}

	/// A nested statement beside a `MATCHES` operator reads as an opaque node: no path, literal or
	/// parameter, so no index is keyed on it, and two of them differ so that two conjuncts that
	/// differ only there keep their positions.
	#[test]
	fn a_nested_statement_beside_a_matches_operator_is_an_opaque_node() {
		let (ir, _) = statement(
			table_source(),
			None,
			&[Conjunct::MatchesOrSubquery("a"), Conjunct::MatchesOrSubquery("a")],
		);
		let predicate = filter_predicate(&ir).expect("the predicate is read");
		assert!(predicate.withheld.is_empty());
		let [(0, first), (1, second)] = predicate.conjuncts.as_slice() else {
			panic!("two conjuncts are read, got {:?}", predicate.conjuncts);
		};
		assert_ne!(first, second);
		for read in [first, second] {
			let LogicalExpr::Binary {
				right,
				..
			} = read
			else {
				panic!("a disjunction, got {read:?}");
			};
			let LogicalExpr::Binary {
				right: nested,
				..
			} = &**right
			else {
				panic!("a comparison, got {right:?}");
			};
			assert!(!nested.is_path());
			assert!(crate::exec::planner::util::try_literal_to_value(nested).is_none());
			assert!(matches!(
				&**nested,
				LogicalExpr::Call(call)
					if matches!(&call.receiver, Function::Normal(name) if name.starts_with("__opaque"))
						&& call.args.is_empty()
			));
		}
	}

	/// A `KNN` operator in the body of a loop is found where the planner's walk finds it, and a
	/// field the body reads is read where the planner's check for a restricted field finds it: the
	/// opaque node that stands for the loop carries both as arguments.
	#[test]
	fn a_knn_operator_and_a_field_in_a_loop_body_are_carried_by_the_opaque_node() {
		let (ir, _) = statement(table_source(), None, &[Conjunct::LoopOfKnn]);
		let predicate = filter_predicate(&ir).expect("the predicate is read");
		assert!(predicate.withheld.is_empty());
		let [(0, read)] = predicate.conjuncts.as_slice() else {
			panic!("one conjunct is read, got {:?}", predicate.conjuncts);
		};
		assert!(has_knn_operator(read));
		assert!(!read.is_path());
		let LogicalExpr::Binary {
			left,
			..
		} = read
		else {
			panic!("a comparison, got {read:?}");
		};
		let LogicalExpr::Call(call) = &**left else {
			panic!("an opaque node, got {left:?}");
		};
		assert!(call.args.contains(&LogicalExpr::Field(Strand::from("secret"))));
	}

	/// A form no binder emits inside an expression is an invariant break, not a conjunct the
	/// analysis silently misses.
	#[test]
	fn a_form_no_binder_emits_in_a_conjunct_is_an_invariant_error() {
		let (ir, _) = statement(table_source(), None, &[Conjunct::MatchesOrDeref("a")]);
		assert!(matches!(filter_predicate(&ir), Err(IrError::Verify(_))));
	}

	/// A `MATCHES` operator beside a call reaches the decision, which answers for it: the source
	/// converts, to a read or to the refusal the planner raises.
	#[tokio::test]
	async fn a_matches_operator_beside_a_call_reaches_the_decision() {
		let (ir, _) = statement(table_source(), None, &[Conjunct::MatchesOrCall("a")]);
		converted(ir, &Held::of(vec![plain("t")])).await.expect("the decision answers");
	}
}
