//! The answers the per-planning memo gives, and the plan-time value classes they are phrased in.
//!
//! The memo answers only from what it holds. A name it does not hold is answered `NotLoaded` and
//! becomes pending: the caller awaits a load and builds again. Once the compile has passed a
//! catalog change, a definition lookup answers `Unknown` whatever the memo holds.

use crate::exec::ir::iface::choose::ValueKindClass;
use crate::val::Value;

/// What the memo answers for a catalog lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CatalogAnswer<T> {
	/// The definition, as the requesting transaction sees it.
	Found(T),
	/// The transaction's catalog holds no such definition.
	Absent,
	/// The memo has not loaded the name. The name is now pending: the caller awaits
	/// `PlanningCatalog::load_pending` and builds again.
	NotLoaded,
	/// A catalog change precedes this point of the compile, so no definition the memo holds
	/// describes the catalog the code here runs against: neither a held definition nor the
	/// absence of one. The caller plans for run time. Nothing is read and nothing is pending.
	Unknown,
}

impl<T> CatalogAnswer<T> {
	/// The same answer with `f` applied to a found definition.
	pub(crate) fn map<U>(self, f: impl FnOnce(T) -> U) -> CatalogAnswer<U> {
		match self {
			Self::Found(def) => CatalogAnswer::Found(f(def)),
			Self::Absent => CatalogAnswer::Absent,
			Self::NotLoaded => CatalogAnswer::NotLoaded,
			Self::Unknown => CatalogAnswer::Unknown,
		}
	}

	/// The found definition, or `None` for `Absent`, `NotLoaded` and `Unknown`.
	pub(crate) fn found(&self) -> Option<&T> {
		match self {
			Self::Found(def) => Some(def),
			Self::Absent | Self::NotLoaded | Self::Unknown => None,
		}
	}
}

/// How a free name read by an op is bound for this execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FreeName {
	/// Bound nowhere: the name evaluates to `NONE`.
	Unbound,
	/// Bound at plan time to a value of this class.
	Bound(ValueKindClass),
	/// `$auth`, `$session`, `$token` or `$access`.
	Principal,
	/// Resolved only when the statement runs.
	RunTime,
	/// A stored parameter the memo has not loaded; pending, as for [`CatalogAnswer::NotLoaded`].
	NotLoaded,
}

/// The exact value of a top-frame free name, for a decision that plans from it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ExactAnswer {
	/// The value. The memo has recorded the dependence.
	Value(Value),
	/// No value at plan time: the consumer takes its run-time form.
	RunTime,
	/// A stored parameter the memo has not loaded; pending.
	NotLoaded,
}

/// Which plan-time values a compile may plan from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionValues {
	/// A request's arrival compile, or its own top-level statement planned when reached
	/// (`UnitObject::SessionStatement`): bindings, stored params and principal values.
	Fold,
	/// A statement or expression reached at run time and served to every principal of a class
	/// (`UnitObject::{Statement, Expr, FrontEnd}`): bindings and stored params, never a principal
	/// value. The names it is told are shadowed are read at run time.
	RunTime,
	/// Definition code compiled as a unit (`UnitObject::{Function, FunctionCall, SpecialisedBody,
	/// GenericBody, ParamGuard, TableRead, FieldState, SelectPermission, StageBundle, Closure,
	/// DynamicTargetArm}`): no free name is answered at plan time. Definer code holds none of the
	/// caller's bindings, and a body resolves its names when it runs.
	Isolated,
}

/// The layout facts of a table: layout generations and migration state. No table layout carries
/// generations, so the memo answers `None` for every table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LayoutFacts {
	_reserved: (),
}

/// The class of `value` among the classes a `FROM` source or a dynamic target may hold: a record
/// id, a table, an array, or any other value.
pub(crate) fn kind_class(value: &Value) -> ValueKindClass {
	match value {
		Value::RecordId(_) => ValueKindClass::RecordId,
		Value::Table(_) => ValueKindClass::Table,
		Value::Array(_) => ValueKindClass::Array,
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => ValueKindClass::Other,
	}
}

/// The principal names. A free name equal to one of these is a principal read.
pub(crate) const PRINCIPAL_NAMES: [&str; 4] = ["auth", "session", "token", "access"];
