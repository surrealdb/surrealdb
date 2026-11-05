//! Unit loading: the only compile that happens while a program runs.
//!
//! A running program never re-plans or recompiles its own code. A version-keyed unit may compile
//! on a compile-cache miss, and every such load is a unit load: the kind of unit it loads is a
//! [`UnitKind`], and the static part of the key it loads by is a [`UnitStaticKey`].

use surrealdb_strand::{Strand, TableName};

/// The kind of unit a load fetches or compiles.
///
/// One variant for each kind of unit the compile cache holds, plus the per-table arm of a
/// dynamic target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum UnitKind {
	Function,
	TableRead,
	FieldState,
	SelectPermission,
	Closure,
	Expr,
	Statement,
	SessionStatement,
	FrontEnd,
	StageBundle,
	FunctionCall,
	ParamGuard,
	SpecialisedBody,
	GenericBody,
	/// The statement planned for one table a dynamic target names at run time.
	DynamicTargetArm,
}

/// A unit load as the IR plans it.
///
/// The run-time parts of the key are operands of the loading op. The statement timestamp is
/// always one of them: it is the loaded unit's entry argument.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LoadUnitSpec {
	pub(crate) kind: UnitKind,
	/// The parts of the key known when the program is planned.
	pub(crate) static_key: UnitStaticKey,
}

/// The part of a unit's key that is known at plan time.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum UnitStaticKey {
	/// A unit named by a catalog object, such as a function.
	Named(Strand),
	/// A unit of one table.
	Table(TableName),
	/// A unit of one statement shape, by its digest.
	Shape(u64),
	/// A unit whose whole key is run-time operands.
	None,
}
