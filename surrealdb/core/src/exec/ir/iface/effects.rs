//! What an op does: its effects, failure classes, footprint, rollback behaviour and determinism.
//!
//! [`OpEffects`] is the one answer every consumer that moves, merges, folds or drops an op reads.
//! Its facts are conservative: a field a producer cannot name is widened to the strongest value,
//! never omitted.

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::{Strand, TableName};

use super::fallibility::{ContextDep, Fallibility};
use super::ids::SnapshotId;
use crate::exec::logical::footprint::CatalogKind;
use crate::expr::logical::EffectSet;

/// The effects of one op.
///
/// A local answer does not fold the ops in the op's regions; the transitive answer is an
/// analysis over the container.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OpEffects {
	pub(crate) set: EffectSet,
	pub(crate) fallibility: Fallibility,
	/// Each dependency a `CONTEXT` failure rests on.
	pub(crate) context_deps: SmallVec<[ContextDep; 1]>,
	pub(crate) footprint: IrFootprint,
	pub(crate) rollback: Rollback,
	pub(crate) determinism: Determinism,
}

impl OpEffects {
	/// The answer for an op nothing is known about: every effect ([`EffectSet::TOP`], so a test of
	/// any flag or predicate finds it, a write included), failure for every dependency, reads and
	/// writes of [`IrResource::Unknown`], effects a rollback does not undo, and no equality with
	/// another evaluation of itself.
	pub(crate) fn top() -> Self {
		Self {
			set: EffectSet::TOP,
			fallibility: Fallibility::TOP,
			context_deps: SmallVec::new(),
			footprint: IrFootprint {
				reads: smallvec![IrResource::Unknown],
				writes: smallvec![IrResource::Unknown],
			},
			rollback: Rollback::Survives,
			determinism: Determinism::NonDeterministic,
		}
	}
}

/// Whether the effects of an op are undone when the enclosing transaction or block rolls back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Rollback {
	NoEffects,
	/// Every effect is undone: KV writes, catalog writes, transactional async-event queue entries.
	Undone,
	/// Some effect survives: external calls, opaque bodies, notifications, sequence allocation.
	Survives,
}

/// How far two evaluations of an op may be treated as one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Determinism {
	Deterministic,
	/// Reads stored data: equal answers within one statement only where no write of the
	/// statement reaches the footprint.
	WithinStatementUnlessWritten,
	/// A clock read: one value per top-level statement.
	PerStatement,
	NonDeterministic,
}

/// The resources an op reads and writes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct IrFootprint {
	pub(crate) reads: SmallVec<[IrResource; 2]>,
	pub(crate) writes: SmallVec<[IrResource; 2]>,
}

/// Something an op may read or write.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum IrResource {
	/// Records of `table` (every table when `None`): the `fields` of the records `key` names.
	Data {
		table: Option<TableName>,
		fields: FieldSet,
		key: KeyOf,
	},
	/// A catalog definition of `kind`: on `table` where it belongs to one (every table when
	/// `None`), named `name` (every name when `None`).
	Catalog {
		kind: CatalogKind,
		table: Option<TableName>,
		name: Option<Strand>,
	},
	/// The snapshot a versioned read observes.
	Snapshot(SnapshotId),
	/// The writes a table's references cascade to.
	Cascade {
		table: TableName,
	},
	/// The rejection a table's constraints may raise.
	Reject {
		table: TableName,
	},
	/// The queue an event's async work is appended to.
	Queue {
		event: Strand,
	},
	/// A session variable.
	Session {
		name: Strand,
	},
	/// Anything at all.
	Unknown,
}

/// The fields of a record an access touches.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FieldSet {
	/// Every field. Used when a producer cannot name the fields it touches.
	All,
	Some(SmallVec<[Strand; 4]>),
}

/// The record key an iteration of a loop touches, as a path from the loop's element.
///
/// `Any` when no path is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum KeyOf {
	Any,
	Elem(ElemPath),
}

/// The path from a loop's element to its identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ElemPath {
	/// The `id` of a record stream: the rows of a table, index or range source.
	Id,
	/// The element itself, for a value stream: a set, an integer range, a deduplicated array,
	/// `SELECT VALUE id`.
	Elem,
}

/// Whether a delivered stream can hold one identity twice along its [`ElemPath`].
///
/// A fact about the delivered stream of a loop source, after every operator of the source. It is
/// derived by analysis and never stored on an op.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Multiplicity {
	Distinct,
	MayRepeat,
}
