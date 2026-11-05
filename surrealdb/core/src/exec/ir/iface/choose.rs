//! `Choose`: a guarded choice between prepared alternatives.
//!
//! A `Choose` holds one region per arm. Guards are evaluated by the VM over the discriminant
//! operand, catalog descriptors and run-time signals; the choice is made once at the safe point
//! of its granularity and never compiles anything the program does not already hold, except for
//! the sanctioned unit loads of [`ArmBody::UnitLoad`].
//!
//! Contract of every `Choose`:
//!
//! 1. It has between one and [`MAX_CHOOSE_ARMS`] arms. The last arm is guarded by
//!    [`Guard::Otherwise`], the generic arm that is correct for every input, unless
//!    [`GenericArm::ExhaustiveBy`] names a fact proving the arms exhaustive.
//! 2. A guard is total: it raises nothing, reads no record value, and compiles nothing. Where its
//!    input is unavailable or its source fails, the guard is false and a later arm runs.
//! 3. Every arm raises the source's errors and returns the same rows in the same order, or the
//!    statement's sort keys are total so every arm's sorted output is one sequence. A statement
//!    whose sort keys are not total is planned with one access path and has no `Choose`.
//! 4. The switch happens only at a safe point of the granularity.
//! 5. Only an arm of a [`ChooseOrigin::LogicalDynamicTarget`] choice may be a per-table unit load;
//!    guarded physical arms and kind-dispatch arms are lowered into the program.

use smallvec::SmallVec;
use surrealdb_strand::{Strand, TableName};

use super::estimate::SizeBucket;
use super::unit::UnitKind;
use crate::expr::Kind;

/// The most arms one `Choose` holds.
pub(crate) const MAX_CHOOSE_ARMS: usize = 8;

/// The arms of one `Choose` and what they promise.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ChooseSpec {
	pub(crate) granularity: ChooseGranularity,
	/// Arm `i` is region `i` of the op.
	pub(crate) arms: SmallVec<[ArmSpec; 4]>,
	pub(crate) generic: GenericArm,
	pub(crate) equivalence: ArmEquivalence,
	pub(crate) origin: ChooseOrigin,
}

/// The safe point at which a `Choose` switches.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) enum ChooseGranularity {
	/// Once, at the start of the statement.
	#[default]
	Statement,
	/// At a morsel boundary.
	Morsel,
	/// At a batch boundary.
	Batch,
}

/// One arm: when it is taken, what it runs, and how `EXPLAIN` names it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ArmSpec {
	pub(crate) guard: Guard,
	pub(crate) body: ArmBody,
	pub(crate) label: ArmLabel,
}

/// What an arm runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ArmBody {
	/// Lowered into the program with the `Choose`.
	PreLowered,
	/// Loaded as a unit when the arm is taken.
	UnitLoad(UnitKind),
}

/// The condition under which an arm is taken. Total: see the module contract.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Guard {
	/// A dynamic target names this table.
	TableIs(TableName),
	/// The index is defined and ready.
	IndexReady(IndexRef),
	/// The discriminant value is of this class.
	ValueKindIs(ValueKindClass),
	/// The value is of this type, as a type hint states.
	TypeHolds(Kind),
	/// The run-time size is at most the bucket, read from run-time signals or an estimate.
	SizeAtMost {
		bucket: SizeBucket,
	},
	/// The generic arm's guard: true for every input.
	Otherwise,
}

/// What proves the arms of a `Choose` cover every input.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum GenericArm {
	/// The last arm is guarded by [`Guard::Otherwise`].
	Present,
	/// No generic arm; the fact proves the arms exhaustive.
	ExhaustiveBy(ExhaustiveFact),
}

/// What an exhaustive `Choose` rests on.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ExhaustiveFact {
	/// The discriminant is known to hold this kind.
	TypeFact(Kind),
}

/// Why every arm yields the same output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ArmEquivalence {
	SameRowsSameOrder,
	/// The statement's sort keys end in a unique key or the record id.
	OrderedByTotalKey,
}

/// Who wrote the `Choose`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ChooseOrigin {
	/// A dynamic target, one arm per table the run may name.
	LogicalDynamicTarget,
	/// A source whose value kind is only known at run time.
	LogicalKindDispatch,
	/// The physical layer over prepared arms.
	PhysicalGuarded,
}

/// An index of a table.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct IndexRef {
	pub(crate) table: TableName,
	pub(crate) index: Strand,
	/// The position of the index among the online indexes of its table, which orders the access
	/// path candidates.
	pub(crate) position: u32,
	/// Per column of the index, true when its declared kind cannot hold an array, so the column
	/// keeps one entry per record. Empty when no column is marked; a column past the end of the
	/// slice is not marked.
	pub(crate) scalar: Box<[bool]>,
}

impl IndexRef {
	/// The reference to the index `definition`, at `position` among the online indexes of its
	/// table. `scalar` marks the columns that cannot hold an array. A `scalar` slice with no true
	/// flag is stored empty, so two references to one index compare equal however their flags
	/// were read.
	pub(crate) fn of(
		definition: &crate::catalog::IndexDefinition,
		position: usize,
		scalar: &[bool],
	) -> Self {
		Self {
			table: definition.table_name.clone(),
			index: definition.name.clone(),
			position: u32::try_from(position).unwrap_or(u32::MAX),
			scalar: if scalar.contains(&true) {
				scalar.into()
			} else {
				Box::default()
			},
		}
	}
}

/// The classes of value a `FROM` source or a dynamic target may hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum ValueKindClass {
	RecordId,
	Table,
	Array,
	Other,
}

/// How `EXPLAIN` names an arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ArmLabel(pub(crate) &'static str);
