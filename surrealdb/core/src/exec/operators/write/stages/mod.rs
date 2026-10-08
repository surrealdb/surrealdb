//! The bodies of the stages the orders name.
//!
//! One module per concern, so that a module answers for one concern.
//!
//! [`lookup`] is the only route from a [`StageId`] to a body. It is total over
//! the enum and sends each id to exactly one module, so a stage has one owner
//! and a new variant does not compile until it is given one.

// Routing a stage to its owner is the one decision this file makes, and a
// wildcard arm would make it silently: a new stage would route nowhere and
// read as unimplemented rather than as unowned.
#![warn(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use super::stage::{StageId, WriteStage};

pub(crate) mod data;
pub(crate) mod gate;
pub(crate) mod guard;
pub(crate) mod index;
pub(crate) mod lives;
pub(crate) mod observe;
pub(crate) mod output;
pub(crate) mod purge;
pub(crate) mod refs;
pub(crate) mod schema;
pub(crate) mod store;
pub(crate) mod view;

/// The body of a stage, when one has landed.
///
/// A `None` means the stage's body has not been written yet, not that the
/// stage does nothing: [`WritePlan::resolve`](super::plan::WritePlan) refuses
/// to bind an order naming one, which is what keeps a half-built order from
/// writing anything.
pub(crate) fn lookup(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::CheckWhereCondition
		| StageId::ComputeInputData
		| StageId::CheckDataFields
		| StageId::ProcessRecordData
		| StageId::ProcessMergeData
		| StageId::DefaultRecordData
		| StageId::GenerateRecordId => data::stage(id),

		StageId::CheckRecordUserAccess
		| StageId::CheckPermissionsQuickCreate
		| StageId::CheckUpdatePermissions
		| StageId::CheckDeletePermissions
		| StageId::CheckCreatePermissions
		| StageId::RecheckUpdatePermissions
		| StageId::CheckSelectPermissions(_) => gate::stage(id),

		StageId::CheckRecordExists
		| StageId::CheckTableNotView
		| StageId::CheckTableNotLightweight(_)
		| StageId::CheckTableType(_) => guard::stage(id),

		StageId::ProcessTableFields
		| StageId::CleanupTableFields
		| StageId::MaterialiseObservedFields => schema::stage(id),

		StageId::StoreIndexData | StageId::ReclaimDocId => index::stage(id),

		StageId::ClearRecordData
		| StageId::StoreRecordData
		| StageId::StoreEdgesData
		| StageId::StoreInlineAdjacencyData => store::stage(id),

		StageId::PurgeRecordData => purge::stage(id),

		StageId::CleanupTableReferences | StageId::ProcessTableReferences => refs::stage(id),

		StageId::ProcessTableViews(_) => view::stage(id),

		StageId::ProcessTableLives(_) => lives::stage(id),

		StageId::ProcessTableEvents(_) | StageId::ProcessChangefeeds => observe::stage(id),

		StageId::OutputWrite => output::stage(id),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A module answers only for the stages [`lookup`] routes to it.
	///
	/// Each module's `stage` is also reachable directly, so a body registered in
	/// the wrong module would be dead rather than wrong — unreachable through
	/// `lookup`, and invisible everywhere except here.
	/// One module's registry, in the shape [`lookup`] calls it by.
	type Registry = fn(StageId) -> Option<Arc<dyn WriteStage>>;

	#[test]
	fn a_module_answers_only_for_its_own_stages() {
		let modules: [(&str, Registry); 12] = [
			("data", data::stage),
			("gate", gate::stage),
			("guard", guard::stage),
			("index", index::stage),
			("lives", lives::stage),
			("observe", observe::stage),
			("output", output::stage),
			("purge", purge::stage),
			("refs", refs::stage),
			("schema", schema::stage),
			("store", store::stage),
			("view", view::stage),
		];
		for id in StageId::ALL {
			let owners: Vec<&str> =
				modules.iter().filter(|(_, f)| f(*id).is_some()).map(|(n, _)| *n).collect();
			assert!(owners.len() <= 1, "{id:?} is registered by more than one module: {owners:?}");
			assert_eq!(
				owners.is_empty(),
				lookup(*id).is_none(),
				"{id:?} is registered by {owners:?}, which is not where lookup routes it"
			);
		}
	}
}
