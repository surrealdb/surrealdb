//! One step of a write's per-record order, and the identity an order names it
//! by.
//!
//! A [`WriteStage`] is shared across every record a statement touches, so it
//! takes `&self`: all per-record mutable state lives on [`WriteRow`]. A stage
//! never decides *where* it runs — the nine per-record orders assembled in
//! [`super::plan`] do — and never branches on the statement form, because the
//! form's differences are carried as data: which stages an order contains,
//! which image or action a stage acts on, and at what position it sits.
//!
//! [`StageId`] is what an order is made of. Naming a stage rather than holding
//! its implementation lets an order be written as plain data, and keeps the
//! whole order in one file rather than spread across the stages themselves.

// Dead-code lint is allowed for the items in this module.
#![allow(dead_code)]
// A stage list is only a contract while every stage is named. A wildcard arm
// over `StageId` — in the registry, or in an order's assembly — absorbs a new
// stage silently, which is the one failure this module is shaped to prevent.
#![deny(clippy::wildcard_enum_match_arm)]

use crate::doc::Action;

/// The outcome of a stage that did not fail.
///
/// There is no error variant by construction. A stage that fails returns
/// `Err(ControlFlow)`, which aborts the statement rather than the record: the
/// transaction is cancelled and every record the statement already wrote is
/// discarded. Folding failure into this enum would make "this record is over"
/// and "this statement is over" the same value, and every caller would then
/// have to re-derive which of the two it was holding.
///
/// [`StageOutcome::SkipRow`] is the distinct, non-failing outcome: the record
/// is excluded by a condition, a permission predicate or a missing-record
/// check, the statement succeeds, and nothing is emitted for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StageOutcome {
	/// Continue to the next stage.
	Continue,
	/// Abandon this record, emit nothing for it, statement succeeds.
	SkipRow,
}

/// Which record image a stage acts on.
///
/// The one stage whose implementation is shared but whose subject differs:
/// `check_select_permissions` gates the post-image on eight of the nine orders
/// and the pre-image on `DELETE`, which has no post-image to gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum RecordImage {
	/// The before-image, as the record stood when the statement read it.
	Initial,
	/// The after-image the statement's stages built.
	Current,
}

/// Which form's record-type rule the table-type slot enforces.
///
/// Four distinct checks occupy one slot, and the slot's *position* is what
/// varies between orders: `UPSERT` and `INSERT` check first, ahead of the
/// permission gate; `CREATE` and `RELATE` check after the record id exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum TableTypeFor {
	Create,
	Upsert,
	Relate,
	Insert,
}

/// Why a lightweight edge may not be written, which is the phrase the guard
/// names in its failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum LightweightReason {
	/// `UPDATE`: a lightweight edge is immutable.
	Updated,
	/// `RELATE` carrying a data clause: a lightweight edge holds no data
	/// beyond its endpoints.
	GivenADataClause,
}

impl LightweightReason {
	/// The phrase the guard's failure names.
	pub(crate) fn as_str(self) -> &'static str {
		match self {
			LightweightReason::Updated => "updated",
			LightweightReason::GivenADataClause => "given a data clause",
		}
	}
}

/// The identity of a stage within an order.
///
/// An order is a list of these. A variant carries a payload exactly where two
/// orders hold the same stage and differ in what it acts on — the image a
/// permission gate reads, the action an observer is notified with, the record
/// type a guard enforces — so that the difference is data in the order rather
/// than a branch inside the stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum StageId {
	/// Tenant confinement. Heads all nine orders, ahead of every permission
	/// gate and of the create/update selection on the three forms that have
	/// one. A record user writing outside its own tenant is stopped here and
	/// nowhere else: the read path enforces this at the scan operators, and a
	/// write performs no scan.
	CheckRecordUserAccess,
	/// The record must already exist. Present in the four updating orders and
	/// `DELETE`, absent from the four creating orders.
	CheckRecordExists,
	/// A view table is read-only. Follows the permission gate on every order
	/// that has one, so an actor without permission gets the permission
	/// outcome rather than an error naming the table's storage mode.
	CheckTableNotView,
	/// A lightweight edge carries no data beyond its endpoints.
	CheckTableNotLightweight(LightweightReason),
	/// The table's declared record type admits this write.
	CheckTableType(TableTypeFor),
	/// The pre-write table permission of a creating order. Short-circuits only
	/// on a `NONE` create permission; a conditional one is left to
	/// [`StageId::CheckCreatePermissions`], because its subject is the record
	/// the data clause has not yet built.
	CheckPermissionsQuickCreate,
	/// The pre-write table permission of an updating order, evaluated in full
	/// before any user expression in the condition or the data clause.
	CheckUpdatePermissions,
	/// `DELETE`'s pre-write table permission, evaluated in full before the
	/// condition.
	CheckDeletePermissions,
	/// The post-write table permission of a creating order, evaluated against
	/// the record the data clause built.
	CheckCreatePermissions,
	/// The post-write table permission of an updating order. Not a second
	/// evaluation: it returns immediately unless the table's update permission
	/// is a predicate.
	RecheckUpdatePermissions,
	/// The statement's `WHERE`, evaluated once per candidate record before the
	/// data clause so a data clause with side effects runs only for records the
	/// condition accepts.
	CheckWhereCondition,
	/// Evaluate the data clause into [`super::row::ComputedInput`].
	ComputeInputData,
	/// The record's special fields (`id`, `in`, `out`) are consistent with the
	/// statement's own target.
	CheckDataFields,
	/// Apply the evaluated data clause to the post-image.
	ProcessRecordData,
	/// `INSERT` create's data application, which merges the row's own value
	/// rather than applying a statement-level clause.
	ProcessMergeData,
	/// Stamp the record's defaults, and the endpoints and record-type marker of
	/// an edge.
	DefaultRecordData,
	/// Derive a record id for a target that named a table rather than a record.
	/// `INSERT` runs this before its create attempt rather than inside it, so
	/// a rolled-back attempt does not derive a second id.
	GenerateRecordId,
	/// Apply the table's field schema — types, defaults, values, assertions,
	/// references — in dependency order.
	ProcessTableFields,
	/// Drop the fields the schema leaves empty.
	CleanupTableFields,
	/// Release the outgoing references the record holds. `DELETE` only, and
	/// before the clear: it walks the post-image, which the clear blanks.
	CleanupTableReferences,
	/// Empty the post-image. `DELETE` only.
	ClearRecordData,
	/// Write the record.
	StoreRecordData,
	/// Write the edge keys of a relation.
	StoreEdgesData,
	/// Keep the inline edge-property payloads in step with the record.
	StoreInlineAdjacencyData,
	/// Maintain the table's indexes, and report whether an in-flight index
	/// build deferred this record's doc-id removal.
	StoreIndexData,
	/// Act on that report: release the record's doc-id, or write the durable
	/// marker that hands the release to the build's replay. `DELETE` only, and
	/// between the index maintenance that reports it and the purge.
	ReclaimDocId,
	/// Remove the record and everything keyed on it. `DELETE` only.
	PurgeRecordData,
	/// Derive the computed fields the record's observers read. Computed fields
	/// are stripped before storage, so every stage after this one — references,
	/// views, events, live queries, changefeeds, and the output projection —
	/// would otherwise see the record without them.
	MaterialiseObservedFields,
	/// Maintain the reference keys the record's fields declare.
	ProcessTableReferences,
	/// Recompute the foreign materialised views this write affects.
	ProcessTableViews(Action),
	/// Run the table's event bodies.
	ProcessTableEvents(Action),
	/// Notify the table's live-query subscriptions.
	ProcessTableLives(Action),
	/// Append to the table's change feed.
	ProcessChangefeeds,
	/// Gate the image the statement is about to project.
	CheckSelectPermissions(RecordImage),
	/// Project the record the statement returns for this record, or drop it.
	OutputWrite,
}

impl StageId {
	/// The stage's name, as `EXPLAIN` renders it.
	///
	/// A payload that selects between several variants of a stage is part of
	/// the name; a payload that only selects a subject or an action is not, and is
	/// rendered through [`WriteStage::attrs`] instead.
	pub(crate) fn name(self) -> &'static str {
		match self {
			StageId::CheckRecordUserAccess => "check_record_user_access",
			StageId::CheckRecordExists => "check_record_exists",
			StageId::CheckTableNotView => "check_table_not_view",
			StageId::CheckTableNotLightweight(_) => "check_table_not_lightweight",
			StageId::CheckTableType(TableTypeFor::Create) => "check_table_type_create",
			StageId::CheckTableType(TableTypeFor::Upsert) => "check_table_type_upsert",
			StageId::CheckTableType(TableTypeFor::Relate) => "check_table_type_relate",
			StageId::CheckTableType(TableTypeFor::Insert) => "check_table_type_insert",
			StageId::CheckPermissionsQuickCreate => "check_permissions_quick_create",
			StageId::CheckUpdatePermissions => "check_update_permissions",
			StageId::CheckDeletePermissions => "check_delete_permissions",
			StageId::CheckCreatePermissions => "check_create_permissions",
			StageId::RecheckUpdatePermissions => "recheck_update_permissions",
			StageId::CheckWhereCondition => "check_where_condition",
			StageId::ComputeInputData => "compute_input_data",
			StageId::CheckDataFields => "check_data_fields",
			StageId::ProcessRecordData => "process_record_data",
			StageId::ProcessMergeData => "process_merge_data",
			StageId::DefaultRecordData => "default_record_data",
			StageId::GenerateRecordId => "generate_record_id",
			StageId::ProcessTableFields => "process_table_fields",
			StageId::CleanupTableFields => "cleanup_table_fields",
			StageId::CleanupTableReferences => "cleanup_table_references",
			StageId::ClearRecordData => "clear_record_data",
			StageId::StoreRecordData => "store_record_data",
			StageId::StoreEdgesData => "store_edges_data",
			StageId::StoreInlineAdjacencyData => "store_inline_adjacency_data",
			StageId::StoreIndexData => "store_index_data",
			StageId::ReclaimDocId => "reclaim_doc_id",
			StageId::PurgeRecordData => "purge_record_data",
			StageId::MaterialiseObservedFields => "materialise_observed_fields",
			StageId::ProcessTableReferences => "process_table_references",
			StageId::ProcessTableViews(_) => "process_table_views",
			StageId::ProcessTableEvents(_) => "process_table_events",
			StageId::ProcessTableLives(_) => "process_table_lives",
			StageId::ProcessChangefeeds => "process_changefeeds",
			StageId::CheckSelectPermissions(_) => "check_select_permissions",
			StageId::OutputWrite => "output_write",
		}
	}
}
