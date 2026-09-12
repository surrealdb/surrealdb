//! What a builtin does besides compute its result, declared once per
//! function and read by every pass that rewrites a plan.
//!
//! A rewrite that duplicates, drops, folds or moves a call needs to know what
//! the call does beyond its arguments: whether it draws entropy, reads the
//! clock, reaches past the transaction's snapshot, runs a body it cannot see,
//! or reads the row it is evaluated against. [`EffectSet`] is that answer as
//! data on the function's registry row, so the passes read a declaration
//! rather than each deriving its own from the function's name or shape.
//!
//! The default-open hazard this closes: a builtin with no declared effects is
//! pure, and a forgotten declaration silently licenses a rewrite of real
//! behaviour. Every builtin therefore declares its set at its definition — the
//! `define_*_function!` macros require it — and the registry's guard test pins
//! every set that is not [`EffectSet::PURE`] by name.
//!
//! Answering with more effects than a function has only forbids rewrites, so
//! an unclear classification belongs on the effectful side.

use std::fmt::{self, Debug, Formatter};
use std::ops::{BitOr, BitOrAssign, Bound};

use super::expr::{LogicalCall, LogicalExpr, LogicalRecordIdKey};
use crate::expr::{BinaryOperator, Function, PrefixOperator};

/// The effects of one builtin, as a set of flags.
///
/// The flags describe what evaluating the function does or depends on beyond
/// its arguments. Which rewrites a set permits is answered by the predicates
/// on it — [`is_deterministic`](Self::is_deterministic),
/// [`writes_data`](Self::writes_data) — never by a pass testing a flag itself,
/// so a flag's meaning for rewrites is stated in one place.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct EffectSet(u16);

impl EffectSet {
	/// No effect: the result is a function of the arguments alone.
	pub const PURE: Self = Self(0);

	/// Stages a write through the transaction, or runs a body that may.
	pub const WRITES_DATA: Self = Self(1 << 0);
	/// Changes something outside the transaction: a remote endpoint, a
	/// bucket, the response of the API request being served.
	pub const WRITES_EXTERNAL: Self = Self(1 << 1);
	/// Reads something outside the transaction's snapshot: a remote endpoint,
	/// a bucket, the API request being served.
	pub const READS_EXTERNAL: Self = Self(1 << 2);
	/// Advances a counter the transaction does not snapshot, so each call
	/// answers differently and leaves the counter moved.
	pub const ADVANCES_STATE: Self = Self(1 << 3);
	/// Draws randomness.
	pub const ENTROPY: Self = Self(1 << 4);
	/// Reads the clock or the environment's time zone.
	pub const CLOCK: Self = Self(1 << 5);
	/// Waits. No world state changes, but the wait is the function's whole
	/// point, so dropping or repeating the call changes what the statement
	/// does.
	pub const BLOCKING: Self = Self(1 << 6);
	/// Runs a body the call site cannot see: a stored endpoint, a statement
	/// given as text, a continuation handed in as a value.
	pub const OPAQUE_BODY: Self = Self(1 << 7);
	/// Reads records or indexes through the transaction. Snapshot-stable
	/// within a statement.
	pub const READS_DATA: Self = Self(1 << 8);
	/// Reads a definition from the catalog.
	pub const READS_CATALOG: Self = Self(1 << 9);
	/// Reads the session: `$session`, `$auth`, `$token`.
	pub const READS_SESSION: Self = Self(1 << 10);
	/// Evaluates against the row it is called on rather than against its
	/// arguments alone.
	pub const READS_ROW: Self = Self(1 << 11);

	/// The effects that make a second call answer differently from the first,
	/// or leave something behind; see [`is_deterministic`](Self::is_deterministic).
	const NON_DETERMINISTIC: Self = Self(
		Self::WRITES_DATA.0
			| Self::WRITES_EXTERNAL.0
			| Self::READS_EXTERNAL.0
			| Self::ADVANCES_STATE.0
			| Self::ENTROPY.0
			| Self::CLOCK.0
			| Self::BLOCKING.0
			| Self::OPAQUE_BODY.0,
	);

	/// The effects that leave something behind once the statement is done; see
	/// [`mutates_state`](Self::mutates_state).
	const MUTATING: Self =
		Self(Self::WRITES_DATA.0 | Self::WRITES_EXTERNAL.0 | Self::ADVANCES_STATE.0);

	/// Every flag, with its name, in bit order.
	const ALL: &'static [(EffectSet, &'static str)] = &[
		(Self::WRITES_DATA, "WRITES_DATA"),
		(Self::WRITES_EXTERNAL, "WRITES_EXTERNAL"),
		(Self::READS_EXTERNAL, "READS_EXTERNAL"),
		(Self::ADVANCES_STATE, "ADVANCES_STATE"),
		(Self::ENTROPY, "ENTROPY"),
		(Self::CLOCK, "CLOCK"),
		(Self::BLOCKING, "BLOCKING"),
		(Self::OPAQUE_BODY, "OPAQUE_BODY"),
		(Self::READS_DATA, "READS_DATA"),
		(Self::READS_CATALOG, "READS_CATALOG"),
		(Self::READS_SESSION, "READS_SESSION"),
		(Self::READS_ROW, "READS_ROW"),
	];

	/// The set holding every flag of either operand.
	pub const fn union(self, other: Self) -> Self {
		Self(self.0 | other.0)
	}

	/// Whether every flag of `other` is in this set.
	pub const fn contains(self, other: Self) -> bool {
		self.0 & other.0 == other.0
	}

	/// Whether any flag of `other` is in this set.
	pub const fn intersects(self, other: Self) -> bool {
		self.0 & other.0 != 0
	}

	/// Whether the set is [`Self::PURE`].
	pub const fn is_pure(self) -> bool {
		self.0 == 0
	}

	/// Whether a second call with the same arguments answers the same and
	/// leaves nothing behind: the two properties a rewrite needs before it may
	/// duplicate, drop or fold a call.
	///
	/// A read of the transaction's own snapshot, the catalog, the session or
	/// the row does not disturb this: within one statement each answers the
	/// same every time it is asked, so a call that only reads them may be
	/// evaluated once for many rows or once per mention.
	pub const fn is_deterministic(self) -> bool {
		!self.intersects(Self::NON_DETERMINISTIC)
	}

	/// Whether a call may stage a write, which is what decides the transaction
	/// a statement holding the call needs.
	pub const fn writes_data(self) -> bool {
		self.contains(Self::WRITES_DATA)
	}

	/// Whether a call changes state that outlives the statement — a data
	/// write, a write outside the transaction, a counter advanced — so that
	/// how many times it runs is itself part of the answer. A grouped
	/// projection holding such a call is evaluated per row rather than once
	/// per group, and not at all for a group that saw no rows.
	pub const fn mutates_state(self) -> bool {
		self.intersects(Self::MUTATING)
	}
}

impl BitOr for EffectSet {
	type Output = Self;

	fn bitor(self, rhs: Self) -> Self {
		self.union(rhs)
	}
}

impl BitOrAssign for EffectSet {
	fn bitor_assign(&mut self, rhs: Self) {
		*self = self.union(rhs);
	}
}

impl Debug for EffectSet {
	/// `PURE`, or the flags present joined by ` | `, in bit order.
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		if self.is_pure() {
			return f.write_str("PURE");
		}
		let mut first = true;
		for (flag, name) in Self::ALL {
			if self.contains(*flag) {
				if !first {
					f.write_str(" | ")?;
				}
				f.write_str(name)?;
				first = false;
			}
		}
		Ok(())
	}
}

// ---------------------------------------------------------------------------
// What an expression does: one fold over the declarations
// ---------------------------------------------------------------------------

/// The parameters bound to the row or the current value at evaluation:
/// `$this` and `$self` are the current value, `$parent` the row of the
/// enclosing statement (this row, at the top level), and `$value` the raw
/// value of a guard column's field. An expression naming one reads the row.
pub const ROW_PARAMS: &[&str] = &["this", "self", "parent", "value"];

/// The parameters that name the iteration row itself — [`ROW_PARAMS`] less
/// `$value`, which a layer binds for its own columns. A read through one of
/// these reaches the row other than by a field's name, so what it reaches
/// cannot be told from the expression.
pub const ITERATION_PARAMS: &[&str] = &["this", "self", "parent"];

/// What the fold consults besides the expression: the declared effects of
/// the builtins it calls, and the classification of the names it reads.
///
/// The logical layer holds no registry, so a caller supplies these answers;
/// the fold itself is the same for every caller.
pub trait EffectOracle {
	/// The declared [`EffectSet`] of the named builtin, or `None` for a name
	/// the registry does not know. An unknown name is taken to be as
	/// effectful as a body the call site cannot see.
	fn builtin(&self, name: &str) -> Option<EffectSet>;

	/// Whether `.name(args)` is guaranteed to reach a builtin, whatever the
	/// receiver holds.
	///
	/// SECURITY: being registered is not enough. A registered method with no
	/// implementation for the receiver's type degrades to invoking a closure
	/// stored in a field of that name — so a name registered for one type
	/// alone still runs an unseen body when the receiver is an object
	/// carrying that field. Only a name an object receiver dispatches for is
	/// guaranteed to reach a builtin.
	fn method_always_dispatches(&self, name: &str) -> bool;

	/// Whether the named builtin is evaluated against the row it is called on
	/// rather than against its arguments — a projection function.
	fn evaluates_against_row(&self, name: &str) -> bool;

	/// Whether reading `$name` fetches a `DEFINE PARAM` whose `PERMISSIONS`
	/// clause is judged at the read, so that the read may be refused.
	fn permission_checked_param(&self, name: &str) -> bool;
}

/// What evaluating an expression does, derived by [`effects_of`] from the
/// declarations of the builtins it calls and the shape of its nodes.
///
/// Every field answers for the whole expression, a nested statement
/// ([`LogicalExpr::Subquery`]) counted as one opaque node and not entered.
/// Each field is stated as the property a rewrite needs of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Effects {
	/// The union of the declared effects of every builtin the expression
	/// calls, with [`EffectSet::OPAQUE_BODY`] for every call whose body the
	/// call site cannot see — a user-defined, scripted, module or silo
	/// function, an unknown name, an applied value, an unregistered method —
	/// [`EffectSet::READS_ROW`] for a call evaluated against the row, and
	/// [`EffectSet::READS_EXTERNAL`] for a model.
	pub set: EffectSet,
	/// Whether some node forbids evaluating the expression twice in place of
	/// once, or not at all in place of once: a call that is not
	/// deterministic, a method, a closure, an applied value, control flow, a
	/// nested statement, a generated record-id key, or a step that fetches
	/// the record its base may name — a field read, `.*`, `.{ … }` or a
	/// `[WHERE …]` filter over a base that may hold a record id. Together with
	/// the expression's read-only classification this is what
	/// [`Self::effect_free`] answers, and [`Self::pure`] is that plus
	/// [`Self::index_operator`].
	pub impure: bool,
	/// Whether a `MATCHES` or `KNN` operator appears, which an access path
	/// answers rather than evaluation, so the expression is not pure.
	pub index_operator: bool,
	/// Whether evaluating the expression may raise or signal control flow,
	/// so that a rule dropping it unevaluated may change what a statement
	/// reports. Which operators are total is decided in [`node_may_raise`].
	pub may_raise: bool,
	/// Whether the expression reads anything of the value it is evaluated
	/// against — a field, the document, the current value, a row parameter,
	/// a lookup, a nested statement, a call evaluated against the row — so
	/// that it does not mean the same at every position.
	pub reads_row: bool,
	/// Whether the expression may read the row other than by a field's name:
	/// through an iteration parameter, the document, a nested statement, a
	/// call evaluated against the row, or the invocation of a value whose
	/// body is not in the expression. What such a read reaches cannot be
	/// told from the expression, so a node that sets columns on the row
	/// withholds it.
	pub reads_row_opaquely: bool,
}

impl Effects {
	/// Whether evaluating the expression a second time, or before rather than
	/// after another expression of the same row, leaves the world as
	/// evaluating it once in written order would.
	///
	/// [`Self::impure`] is the whole of the answer today: every [`node_set`]
	/// arm that can contribute a [`EffectSet::NON_DETERMINISTIC`] flag has a
	/// matching arm in [`node_is_impure`], so `!impure` already implies
	/// `set.is_deterministic()`. The second term is kept so that an arm gaining
	/// a flag without a matching `impure` arm is turned down rather than
	/// silently admitted.
	pub const fn effect_free(self) -> bool {
		!self.impure && self.set.is_deterministic()
	}

	/// Whether evaluating the expression twice yields the same value as
	/// evaluating it once, with no effect either way — what a rule needs before
	/// it may keep one of two evaluations in place of both.
	///
	/// This is [`Self::effect_free`] plus [`Self::index_operator`]: a `MATCHES`
	/// or `KNN` is answered by an access path rather than by evaluation, so a
	/// rule may not move it, while an operator choosing the shape of its own
	/// batch is free to. Stating it this way makes `pure ⟹ effect_free` hold by
	/// construction rather than by an invariant nothing checks.
	pub const fn pure(self) -> bool {
		self.effect_free() && !self.index_operator
	}

	/// The effects of an expression that does nothing: the identity of
	/// [`Self::join`].
	pub const INERT: Self = Self {
		set: EffectSet::PURE,
		impure: false,
		index_operator: false,
		may_raise: false,
		reads_row: false,
		reads_row_opaquely: false,
	};

	/// The effects of evaluating both.
	fn join(self, other: Self) -> Self {
		Self {
			set: self.set.union(other.set),
			impure: self.impure || other.impure,
			index_operator: self.index_operator || other.index_operator,
			may_raise: self.may_raise || other.may_raise,
			reads_row: self.reads_row || other.reads_row,
			reads_row_opaquely: self.reads_row_opaquely || other.reads_row_opaquely,
		}
	}
}

/// The effects of `expr`, from the declarations `oracle` answers and the
/// shape of every node, a nested statement not entered.
pub fn effects_of(expr: &LogicalExpr, oracle: &dyn EffectOracle) -> Effects {
	let mut children = Effects::INERT;
	expr.for_each_child(&mut |child| children = children.join(effects_of(child, oracle)));
	Effects {
		set: children.set.union(node_set(expr, oracle)),
		impure: children.impure || node_is_impure(expr, oracle),
		index_operator: children.index_operator
			|| matches!(
				expr,
				LogicalExpr::Binary {
					op: BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_),
					..
				}
			),
		may_raise: node_may_raise(expr, children.may_raise, oracle),
		reads_row: children.reads_row || node_reads_row(expr, oracle),
		reads_row_opaquely: children.reads_row_opaquely || node_reads_row_opaquely(expr, oracle),
	}
}

/// Whether `expr` itself invokes a value whose body is not in the
/// expression: an applied value, or a method whose name the registry does
/// not define, which calls a closure stored under that name on the receiver.
/// A registered method is not this: it dispatches on the receiver's type and
/// evaluates against the receiver, and its arguments are read as written.
pub fn invokes_unseen_body(expr: &LogicalExpr, oracle: &dyn EffectOracle) -> bool {
	match expr {
		LogicalExpr::Invoke {
			..
		} => true,
		LogicalExpr::Method {
			name,
			..
		} => !oracle.method_always_dispatches(name.as_str()),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => false,
	}
}

/// Whether the call is evaluated against the row rather than its arguments:
/// a projection builtin, or any call whose body is not visible — a
/// user-defined, scripted, module or silo function. A model is applied to
/// its arguments alone.
fn call_evaluates_against_row(call: &LogicalCall, oracle: &dyn EffectOracle) -> bool {
	match &call.receiver {
		Function::Normal(name) => oracle.evaluates_against_row(name),
		Function::Model(_) => false,
		Function::Custom(_)
		| Function::Script(_)
		| Function::Module(..)
		| Function::Silo {
			..
		} => true,
	}
}

/// The declared effects this node adds; see [`Effects::set`].
fn node_set(expr: &LogicalExpr, oracle: &dyn EffectOracle) -> EffectSet {
	match expr {
		LogicalExpr::Call(call) => match &call.receiver {
			Function::Normal(name) => {
				let declared = oracle.builtin(name).unwrap_or(EffectSet::OPAQUE_BODY);
				if oracle.evaluates_against_row(name) {
					declared.union(EffectSet::READS_ROW)
				} else {
					declared
				}
			}
			Function::Model(_) => EffectSet::READS_EXTERNAL,
			Function::Custom(_)
			| Function::Script(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => EffectSet::OPAQUE_BODY.union(EffectSet::READS_ROW),
		},
		LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Method {
			..
		} => {
			if invokes_unseen_body(expr, oracle) {
				EffectSet::OPAQUE_BODY
			} else {
				EffectSet::PURE
			}
		}
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => EffectSet::PURE,
	}
}

/// Whether this node forbids evaluating the expression twice in place of
/// once, or not at all; see [`Effects::impure`].
fn node_is_impure(expr: &LogicalExpr, oracle: &dyn EffectOracle) -> bool {
	match expr {
		LogicalExpr::Call(call) => match &call.receiver {
			Function::Normal(name) => {
				oracle.builtin(name).is_none_or(|effects| !effects.is_deterministic())
			}
			Function::Custom(_)
			| Function::Script(_)
			| Function::Model(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => true,
		},
		LogicalExpr::Method {
			..
		}
		| LogicalExpr::Closure(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_)
		| LogicalExpr::Invoke {
			..
		} => true,
		LogicalExpr::RecordId(rid) => key_is_generated(&rid.key),
		// Reading a field off a record id fetches that record, which derives
		// its `COMPUTED` fields and judges its `PERMISSIONS`. What the read
		// answers is therefore decided by bodies this expression does not
		// hold, and a `COMPUTED` body that draws entropy or reads the clock
		// answers afresh every time the record is fetched. `.*` and `.{ … }`
		// fetch a record id the same way.
		//
		// An iteration parameter is no exemption here. It names the row in
		// hand, but a row is not always a record: `SELECT VALUE link FROM o`
		// yields bare record ids, and `$this.f` over one fetches like any
		// other base. [`node_may_raise`] takes that risk deliberately; a
		// caller of this predicate is about to keep one evaluation in place of
		// two, where the difference is the answer itself.
		//
		// A `[WHERE …]` filter fetches the same way and is not saved by its
		// predicate's shape: the predicate is evaluated with each element of
		// the base as the current value, so a leading field in it reads off
		// that element, and an element that is a record id is fetched. The
		// predicate alone cannot show this, because it names the field
		// directly rather than through a step whose base is the element.
		//
		// A graph traversal reads records the same way and for the same
		// reason. It reaches the edges of its base and the records those
		// edges name, and what it yields of them is the materialised record —
		// `COMPUTED` fields derived, `PERMISSIONS` judged — so two traversals
		// of one base answer from two separate materialisations. The base
		// still decides whether anything is read at all: a base that cannot
		// be or hold a record id has no edges to reach.
		LogicalExpr::Access {
			base,
			..
		}
		| LogicalExpr::Each(base)
		| LogicalExpr::Destructure {
			base,
			..
		}
		| LogicalExpr::Filter {
			base,
			..
		}
		| LogicalExpr::Lookup {
			base,
			..
		} => may_dereference(base),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::If(_) => false,
	}
}

/// Whether evaluating this node may raise or signal control flow, given
/// whether its children may (`children`).
///
/// `false` for what the evaluators answer for every input: a literal, a
/// field, the document, the current value, a table, a constant and a record
/// id with a literal key; `NOT`, the comparison, containment, geometric and
/// short-circuit operators (`=`, `!=`, `==`, `?=`, `*=`, `<`, `<=`, `>`,
/// `>=`, the `CONTAINS` and `INSIDE` families, `OUTSIDE`, `INTERSECTS`,
/// `AND`, `OR`, `??`, `?:`), whose `fnc::operate` implementations return a
/// value for any operands; unary `+` and a prefix range, which wrap their
/// operand; and a container, an `IF`, a field access, `.*`, `[0]`, `[$]`, a
/// flatten, a destructure or `?.` over such. `true` for a cast, arithmetic
/// (`+ - * / % **`, unary `-`), a binary range, an index step, every call,
/// method, closure and applied value, `THROW`, `BREAK`, `CONTINUE`, a nested
/// plan, a lookup, a recursion, a broadcast, a `[WHERE]` filter, a set-field,
/// a `MATCHES` or `KNN`, a record id with a generated or range key — and
/// anything this list does not name. A step that fetches the record its base
/// may name raises where nothing under the step can, since the fetch derives
/// `COMPUTED` fields and judges `PERMISSIONS`.
///
/// A parameter is inert where its value is in hand before the plan runs, and
/// raises where reading it fetches a `DEFINE PARAM` whose `PERMISSIONS`
/// clause denies the caller ([`EffectOracle::permission_checked_param`]).
fn node_may_raise(expr: &LogicalExpr, children: bool, oracle: &dyn EffectOracle) -> bool {
	match expr {
		LogicalExpr::Param(param) => oracle.permission_checked_param(param.as_str()),
		LogicalExpr::Literal(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Table(_)
		| LogicalExpr::Constant(_) => false,
		LogicalExpr::RecordId(rid) => match &rid.key {
			LogicalRecordIdKey::Number(_)
			| LogicalRecordIdKey::String(_)
			| LogicalRecordIdKey::Uuid(_) => false,
			LogicalRecordIdKey::Array(_) | LogicalRecordIdKey::Object(_) => children,
			LogicalRecordIdKey::Generate(_) | LogicalRecordIdKey::Range(_) => true,
		},
		LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::If(_) => children,
		LogicalExpr::Each(base)
		| LogicalExpr::Destructure {
			base,
			..
		}
		| LogicalExpr::Access {
			base,
			..
		} => base_dereference_may_raise(base) || children,
		LogicalExpr::Prefix {
			op,
			..
		} => match op {
			PrefixOperator::Not
			| PrefixOperator::Positive
			| PrefixOperator::Range
			| PrefixOperator::RangeInclusive => children,
			PrefixOperator::Negate | PrefixOperator::Cast(_) => true,
		},
		LogicalExpr::Binary {
			op,
			..
		} => match op {
			BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::Inside
			| BinaryOperator::NotInside
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::NoneInside
			| BinaryOperator::Outside
			| BinaryOperator::Intersects
			| BinaryOperator::And
			| BinaryOperator::Or
			| BinaryOperator::NullCoalescing
			| BinaryOperator::TenaryCondition => children,
			BinaryOperator::Add
			| BinaryOperator::Subtract
			| BinaryOperator::Multiply
			| BinaryOperator::Divide
			| BinaryOperator::Remainder
			| BinaryOperator::Power
			| BinaryOperator::Range
			| BinaryOperator::RangeInclusive
			| BinaryOperator::RangeSkip
			| BinaryOperator::RangeSkipInclusive
			| BinaryOperator::Matches(_)
			| BinaryOperator::NearestNeighbor(_) => true,
		},
		LogicalExpr::Index {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_)
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::SetField {
			..
		} => true,
	}
}

/// Whether this node reads something of the value the expression is
/// evaluated against; see [`Effects::reads_row`].
fn node_reads_row(expr: &LogicalExpr, oracle: &dyn EffectOracle) -> bool {
	match expr {
		LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Subquery(_)
		| LogicalExpr::Lookup {
			..
		} => true,
		LogicalExpr::Param(param) => ROW_PARAMS.contains(&param.as_str()),
		LogicalExpr::Call(call) => call_evaluates_against_row(call, oracle),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => false,
	}
}

/// Whether this node reads the row other than by a field's name; see
/// [`Effects::reads_row_opaquely`].
fn node_reads_row_opaquely(expr: &LogicalExpr, oracle: &dyn EffectOracle) -> bool {
	match expr {
		LogicalExpr::Doc | LogicalExpr::Subquery(_) => true,
		LogicalExpr::Param(param) => ITERATION_PARAMS.contains(&param.as_str()),
		LogicalExpr::Call(call) => call_evaluates_against_row(call, oracle),
		LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Method {
			..
		} => invokes_unseen_body(expr, oracle),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => false,
	}
}

/// Whether a record-id key is generated where the id is built, so that
/// building the id twice names two different records: `t:rand()`,
/// `t:ulid()` and `t:uuid()`, and a range either of whose bounds is one.
///
/// The literal `t:uuid` forms — a `Uuid` key — are written down and are not
/// generated. An `Array` or `Object` key holds expressions, which the fold
/// visits as children; a `Generate` inside a range bound is the one position
/// no expression walk reaches, since a key that generates carries no
/// sub-expression to walk.
pub fn key_is_generated(key: &LogicalRecordIdKey) -> bool {
	match key {
		LogicalRecordIdKey::Generate(_) => true,
		LogicalRecordIdKey::Range(range) => [&range.start, &range.end].into_iter().any(
			|bound| matches!(bound, Bound::Included(key) | Bound::Excluded(key) if key_is_generated(key)),
		),
		LogicalRecordIdKey::Number(_)
		| LogicalRecordIdKey::String(_)
		| LogicalRecordIdKey::Uuid(_)
		| LogicalRecordIdKey::Array(_)
		| LogicalRecordIdKey::Object(_) => false,
	}
}

/// Whether reading a field off `base` may raise: the fetch a record id
/// triggers judges the record's `PERMISSIONS`, which may refuse.
///
/// This is [`may_dereference`] with one exemption. An iteration parameter
/// (`$this`, `$self`, `$parent`) is taken to name a row in hand, whose fetch
/// the statement has already made and whose permissions it has already passed,
/// so a read off it raises nothing new. A correlated subquery reading
/// `$parent.f` is the shape decorrelation turns into a join, and the exemption
/// is what lets it.
///
/// The exemption is an approximation, and deliberately so: it is not sound for
/// every row, since `SELECT VALUE id FROM o` yields bare record ids and
/// `$parent.f` over one fetches. Narrowing it to rows that are records needs
/// the row's kind, which a rule does not carry. **It belongs to this question
/// alone.** A caller asking whether an evaluation may be duplicated —
/// [`Effects::impure`], the substitution a projection pushdown leaves behind —
/// must ask [`may_dereference`], which grants no exemption: there the fetch is
/// made a second time, and a `COMPUTED` body that draws entropy answers afresh.
///
/// A name of the minted join-alias form is *not* such a shape, though it
/// names the matched right row wherever it really is one. The name is
/// reserved the other way round: a plan that carries `_join<n>` anywhere
/// makes a join mint a different index, precisely so a user's field may keep
/// the name — and a user's field may hold a link. Separating the two needs
/// the indices a rewrite minted, which a rule does not carry either.
fn base_dereference_may_raise(base: &LogicalExpr) -> bool {
	if let LogicalExpr::Param(name) = base {
		return !ITERATION_PARAMS.contains(&name.as_str());
	}
	may_dereference(base)
}

/// Whether a value `expr` produces may be, or hold, a record id: a literal,
/// or a collection of literals, cannot; a record id, and everything whose
/// value only the run decides, may.
pub fn may_dereference(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Literal(_) => false,
		LogicalExpr::Array(items) | LogicalExpr::Set(items) => items.iter().any(may_dereference),
		LogicalExpr::Object(entries) => entries.iter().any(|(_, entry)| may_dereference(entry)),
		LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => true,
	}
}

#[cfg(test)]
mod tests {
	use super::EffectSet;

	#[test]
	fn a_read_of_stable_state_is_deterministic_and_an_effect_is_not() {
		assert!(EffectSet::PURE.is_deterministic());
		assert!(
			(EffectSet::READS_DATA
				| EffectSet::READS_CATALOG
				| EffectSet::READS_SESSION
				| EffectSet::READS_ROW)
				.is_deterministic()
		);
		for (flag, _) in EffectSet::ALL {
			let stable = matches!(
				*flag,
				EffectSet::READS_DATA
					| EffectSet::READS_CATALOG
					| EffectSet::READS_SESSION
					| EffectSet::READS_ROW
			);
			assert_eq!(flag.is_deterministic(), stable, "{flag:?}");
		}
	}

	#[test]
	fn what_outlives_the_statement_mutates_state() {
		for (flag, _) in EffectSet::ALL {
			let outlives = matches!(
				*flag,
				EffectSet::WRITES_DATA | EffectSet::WRITES_EXTERNAL | EffectSet::ADVANCES_STATE
			);
			assert_eq!(flag.mutates_state(), outlives, "{flag:?}");
		}
		assert!(!EffectSet::PURE.mutates_state());
	}

	#[test]
	fn only_a_data_write_decides_the_transaction() {
		assert!(EffectSet::WRITES_DATA.writes_data());
		assert!((EffectSet::OPAQUE_BODY | EffectSet::WRITES_DATA).writes_data());
		assert!(!EffectSet::WRITES_EXTERNAL.writes_data());
		assert!(!EffectSet::OPAQUE_BODY.writes_data());
	}

	#[test]
	fn the_debug_form_names_every_flag_present() {
		assert_eq!(format!("{:?}", EffectSet::PURE), "PURE");
		assert_eq!(
			format!("{:?}", EffectSet::WRITES_EXTERNAL | EffectSet::READS_EXTERNAL),
			"WRITES_EXTERNAL | READS_EXTERNAL"
		);
		assert_eq!(EffectSet::ALL.len(), 12, "every flag is named");
	}
}
