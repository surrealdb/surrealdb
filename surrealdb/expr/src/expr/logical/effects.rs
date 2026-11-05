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
//! A builtin with no declared effects is treated as pure, so a forgotten
//! declaration would silently license a rewrite of real behaviour. Every
//! builtin therefore declares its set at its definition: the
//! `define_*_function!` macros require it, and the registry's guard test pins
//! every set that is not [`EffectSet::PURE`] by name.
//!
//! Answering with more effects than a function has only forbids rewrites, so
//! an unclear classification belongs on the effectful side.

use std::fmt::{self, Debug, Formatter};
use std::ops::{BitOr, BitOrAssign, Bound};

use super::expr::{LogicalCall, LogicalDestructureField, LogicalExpr, LogicalRecordIdKey};
use super::schema::{Schema, coercion_is_identity, kind_may_be_record};
use crate::expr::{BinaryOperator, Function, Kind, PrefixOperator};

/// The effects of one builtin, as a set of flags.
///
/// The flags describe what evaluating the function does or depends on beyond
/// its arguments. Which rewrites a set permits is answered by the predicates
/// on it — [`is_deterministic`](Self::is_deterministic),
/// [`writes_data`](Self::writes_data) — never by a pass testing a flag itself,
/// so a flag's meaning for rewrites is stated in one place.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct EffectSet(u32);

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
	// Bit 10 is reserved and assigned to no flag.
	/// Evaluates against the row it is called on rather than against its
	/// arguments alone.
	pub const READS_ROW: Self = Self(1 << 11);
	/// Reads the principal and connection data that are constant for the
	/// request: `$auth`, `$token`, `$access`, `$session`, and the
	/// `session::{ac, rd, token, id, ip, origin}` functions.
	pub const READS_PRINCIPAL: Self = Self(1 << 12);
	/// Reads the namespace or database in use, which changes at `USE`:
	/// `session::{ns, db}`.
	pub const READS_SESSION_DB: Self = Self(1 << 13);
	/// Writes the session: a session `LET`, which a cancelled statement undoes,
	/// and `USE`, which it does not.
	pub const WRITES_SESSION: Self = Self(1 << 14);
	/// Sends live-query notifications.
	pub const NOTIFY: Self = Self(1 << 15);
	/// Queues asynchronous work that runs after the call returns.
	pub const DEFERS: Self = Self(1 << 16);
	/// Raises by a catalog rule rather than by its arguments: an `ASSERT`, a
	/// `READONLY` field, a `REJECT` clause.
	pub const MAY_REJECT: Self = Self(1 << 17);
	/// Changes the catalog, so that every catalog read ordered after it may
	/// answer differently.
	pub const CHANGES_CATALOG: Self = Self(1 << 18);

	/// Every read of the session: [`Self::READS_PRINCIPAL`] and
	/// [`Self::READS_SESSION_DB`]. A union of two flags rather than a flag, so
	/// it is absent from [`Self::ALL`].
	pub const READS_SESSION: Self = Self(Self::READS_PRINCIPAL.0 | Self::READS_SESSION_DB.0);

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
			| Self::OPAQUE_BODY.0
			| Self::WRITES_SESSION.0
			| Self::NOTIFY.0
			| Self::DEFERS.0
			| Self::CHANGES_CATALOG.0,
	);

	/// [`Self::NON_DETERMINISTIC`] but for [`Self::CLOCK`]: the effects that keep a call from
	/// running for several rows at once; see
	/// [`is_deterministic_but_for_clock`](Self::is_deterministic_but_for_clock).
	pub const NON_DETERMINISTIC_BUT_FOR_CLOCK: Self =
		Self(Self::NON_DETERMINISTIC.0 & !Self::CLOCK.0);

	/// The effects that leave something behind once the statement is done; see
	/// [`mutates_state`](Self::mutates_state).
	const MUTATING: Self = Self(
		Self::WRITES_DATA.0
			| Self::WRITES_EXTERNAL.0
			| Self::ADVANCES_STATE.0
			| Self::WRITES_SESSION.0
			| Self::NOTIFY.0
			| Self::DEFERS.0
			| Self::CHANGES_CATALOG.0,
	);

	/// Every single flag, once, with its name, in bit order. A union constant
	/// such as [`Self::READS_SESSION`] is not listed, so [`Debug`] names each
	/// flag once.
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
		(Self::READS_ROW, "READS_ROW"),
		(Self::READS_PRINCIPAL, "READS_PRINCIPAL"),
		(Self::READS_SESSION_DB, "READS_SESSION_DB"),
		(Self::WRITES_SESSION, "WRITES_SESSION"),
		(Self::NOTIFY, "NOTIFY"),
		(Self::DEFERS, "DEFERS"),
		(Self::MAY_REJECT, "MAY_REJECT"),
		(Self::CHANGES_CATALOG, "CHANGES_CATALOG"),
	];

	/// Every effect a set can name: the union of every flag in [`Self::ALL`]. The answer for a
	/// body nothing is known about, so a test of any flag by [`intersects`](Self::intersects)
	/// finds it.
	pub const TOP: Self = {
		let mut bits = 0;
		let mut i = 0;
		while i < Self::ALL.len() {
			bits |= Self::ALL[i].0.0;
			i += 1;
		}
		Self(bits)
	};

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
	/// A read of the transaction's own snapshot, the catalog, the principal,
	/// the namespace and database in use ([`Self::READS_PRINCIPAL`],
	/// [`Self::READS_SESSION_DB`]) or the row does not disturb this: within one
	/// statement each answers the same every time it is asked, so a call that
	/// only reads them may be evaluated once for many rows or once per mention.
	/// A `READS_DATA` expression is deterministic only within a statement in
	/// which no write reaches the read; a caller that cannot show that must not
	/// treat the answer as stable. [`Self::MAY_REJECT`] is neutral: it neither
	/// makes a call non-deterministic nor leaves anything behind.
	pub const fn is_deterministic(self) -> bool {
		!self.intersects(Self::NON_DETERMINISTIC)
	}

	/// [`is_deterministic`](Self::is_deterministic) but for reads of the clock: whether a second
	/// call answers the same, as the clock allows, and leaves nothing behind. Calls such a set
	/// describes may run for several rows at once, each reading the clock when it runs.
	pub const fn is_deterministic_but_for_clock(self) -> bool {
		!self.intersects(Self(Self::NON_DETERMINISTIC.0 & !Self::CLOCK.0))
	}

	/// Whether a call may stage a write, which is what decides the transaction
	/// a statement holding the call needs.
	pub const fn writes_data(self) -> bool {
		self.contains(Self::WRITES_DATA)
	}

	/// Whether a call changes state that outlives the statement — a data
	/// write, a write outside the transaction, a counter advanced, a session
	/// write, a notification, deferred work, a catalog change — so that
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

	/// The argument positions in which the named builtin accepts a closure it
	/// then runs against the row — `check` of `array::any`, `mapper` of
	/// `array::fold` — or `None` for a name the registry does not know.
	/// Empty for a builtin that runs nothing it is handed.
	///
	/// SECURITY: a closure arriving in one of these positions as a value
	/// runs a body the call site cannot see; see [`invokes_unseen_body`].
	fn closure_args(&self, name: &str) -> Option<&'static [usize]>;

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

	/// The effects a call of the user-defined function `fn::name` adds at the
	/// call site, or `None` for a body the call site cannot see. A summary
	/// that names the functions it calls rather than folding their bodies in
	/// answers for the call alone ([`EffectSet::READS_ROW`]); every other
	/// oracle leaves the body unseen.
	fn custom_function(&self, _name: &str) -> Option<EffectSet> {
		None
	}

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
	/// function, an unknown name, an applied value, an unregistered method, a
	/// builtin declaring it, a builtin handed a closure it runs as a value
	/// rather than written in the call —
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
	/// reports. Which operators are total is decided in [`node_may_raise`]. A
	/// cast of an operand proven to hold a kind the target coerces by identity,
	/// and a field read or destructure of a base proven not to be a record id,
	/// raise only where their children do.
	pub may_raise: bool,
	/// Whether the expression reads anything of the value it is evaluated
	/// against — a field, the document, the current value, a row parameter,
	/// a lookup, a nested statement, a call evaluated against the row — so
	/// that it does not mean the same at every position.
	pub reads_row: bool,
	/// Whether the expression may read the row other than by a field's name:
	/// through an iteration parameter, the document, a nested statement, a
	/// call evaluated against the row, or the invocation of a value whose
	/// body is not in the expression ([`invokes_unseen_body`]). What such a
	/// read reaches cannot be told from the expression, so a node that sets
	/// columns on the row withholds it.
	pub reads_row_opaquely: bool,
}

impl Effects {
	/// Whether evaluating the expression a second time, or before rather than
	/// after another expression of the same row, leaves the world as
	/// evaluating it once in written order would.
	///
	/// [`Self::impure`] decides the answer. Every [`node_set`] arm that can
	/// contribute a [`EffectSet::NON_DETERMINISTIC`] flag has a matching arm in
	/// [`node_is_impure`], so `!impure` implies `set.is_deterministic()`. The
	/// second term catches an arm that adds such a flag without a matching
	/// `impure` arm, and reports the expression as not effect free.
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

/// What a node's parent reads of one child: the child's effects, the kind it
/// is proven to evaluate to against the open schema
/// ([`LogicalExpr::checked_kind`]) and whether it may be or hold a record id
/// ([`may_dereference`]).
///
/// Each fact of a node is a function of the node's own shape and of the same
/// facts of its children, so a caller walking a tree bottom-up folds each once
/// per node ([`node_effects`], [`LogicalExpr::node_checked_kind`],
/// [`node_may_dereference`]) rather than recomputing it over every subtree.
#[derive(Clone, Debug)]
pub struct ChildFacts {
	pub effects: Effects,
	pub checked_kind: Option<Kind>,
	pub may_dereference: bool,
}

/// The facts of `expr`, folded from the facts of its children.
pub fn facts_of(expr: &LogicalExpr, oracle: &dyn EffectOracle) -> ChildFacts {
	let mut children = Vec::new();
	expr.for_each_child(&mut |child| children.push(facts_of(child, oracle)));
	let kinds = children.iter().map(|child| child.checked_kind.clone()).collect::<Vec<_>>();
	ChildFacts {
		effects: node_effects(expr, &children, oracle),
		checked_kind: expr.node_checked_kind(&kinds, &Schema::open()),
		may_dereference: node_may_dereference(expr, &children),
	}
}

/// The effects of `expr`, from the declarations `oracle` answers and the
/// shape of every node, a nested statement not entered.
pub fn effects_of(expr: &LogicalExpr, oracle: &dyn EffectOracle) -> Effects {
	facts_of(expr, oracle).effects
}

/// The effects of `expr` given the facts of its children, one per child in the
/// order of [`LogicalExpr::for_each_child`]. This is the one definition of the
/// rules [`effects_of`] applies at each node.
pub fn node_effects(
	expr: &LogicalExpr,
	children: &[ChildFacts],
	oracle: &dyn EffectOracle,
) -> Effects {
	let joined = children.iter().fold(Effects::INERT, |all, child| all.join(child.effects));
	Effects {
		set: joined.set.union(node_set(expr, oracle)),
		impure: joined.impure || node_is_impure(expr, children, oracle),
		index_operator: joined.index_operator
			|| matches!(
				expr,
				LogicalExpr::Binary {
					op: BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_),
					..
				}
			),
		may_raise: node_may_raise(expr, children, joined.may_raise, oracle),
		reads_row: joined.reads_row || node_reads_row(expr, oracle),
		reads_row_opaquely: joined.reads_row_opaquely || node_reads_row_opaquely(expr, oracle),
	}
}

/// Whether `expr` itself invokes a value whose body is not in the
/// expression: an applied value; a method whose name the registry does not
/// define, which calls a closure stored under that name on the receiver; or
/// a call to a builtin that runs a closure it is handed — `array::any`,
/// `array::map`, `value::chain` and the like — with a value that may be a
/// closure not written in the call in a position the builtin runs one from.
/// A registered method that always dispatches is not this: it evaluates
/// against the receiver, and its arguments are read as written.
///
/// A body a builtin runs evaluates against the row the call is evaluated
/// against, exactly as `$pred()` does, so the same judgement applies: when
/// the closure is written in the call its body is in the expression and is
/// read like any other child; when it arrives any other way — a parameter, a
/// field of an object, the result of another call — what it reads cannot be
/// told from the call. Only the positions the builtin declares
/// ([`EffectOracle::closure_args`]) are judged, so the array `array::any`
/// searches or the seed `array::fold` starts from may be a field or a
/// parameter without making the call opaque. A builtin the registry does
/// not know, or one declaring [`EffectSet::OPAQUE_BODY`] outright, is judged
/// in every position: nothing says where its body comes from.
pub fn invokes_unseen_body(expr: &LogicalExpr, oracle: &dyn EffectOracle) -> bool {
	match expr {
		LogicalExpr::Invoke {
			..
		} => true,
		LogicalExpr::Method {
			name,
			..
		} => !oracle.method_always_dispatches(name.as_str()),
		LogicalExpr::Call(call) => match &call.receiver {
			Function::Normal(name) => {
				let every_position = oracle
					.builtin(name)
					.is_none_or(|effects| effects.contains(EffectSet::OPAQUE_BODY));
				match oracle.closure_args(name) {
					Some(slots) if !every_position => slots
						.iter()
						.filter_map(|&slot| call.args.get(slot))
						.any(may_carry_unseen_closure),
					_ => call.args.iter().any(may_carry_unseen_closure),
				}
			}
			// A body of these is not one of the arguments: a user-defined,
			// scripted, module or silo function runs its own, which
			// [`call_evaluates_against_row`] accounts for, and a model has
			// none.
			Function::Model(_)
			| Function::Custom(_)
			| Function::Script(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => false,
		},
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
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
		| LogicalExpr::Subquery(_) => false,
	}
}

/// Whether the value of `arg` may be a closure whose body is not in the
/// expression: what the argument in a builtin's closure position has to be
/// for the call to invoke an unseen body.
///
/// `false` for a closure written in place — its body is in the expression and
/// is read as a child — and for what can never evaluate to a closure: a
/// literal, a record id, a table name, a constant, a container holding only
/// such, a comparison, an arithmetic, containment, range or index operator,
/// and `!`. Everything else may: a parameter, a field, a path, a call, a
/// method, an `IF`, a nested statement, and the operators that yield one of
/// their operands (`??`, `?:`, `AND`, `OR`), which may if an operand may. A
/// field is included because a row is not always a stored record — a
/// projection can put a closure in a column — and being wrong here is a
/// permission leak, so the unclear cases fall on the opaque side.
pub fn may_carry_unseen_closure(arg: &LogicalExpr) -> bool {
	match arg {
		LogicalExpr::Closure(_)
		| LogicalExpr::Literal(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_) => false,
		LogicalExpr::Array(items) | LogicalExpr::Set(items) => {
			items.iter().any(may_carry_unseen_closure)
		}
		LogicalExpr::Object(fields) => {
			fields.iter().any(|(_, value)| may_carry_unseen_closure(value))
		}
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => match op {
			BinaryOperator::NullCoalescing
			| BinaryOperator::TenaryCondition
			| BinaryOperator::And
			| BinaryOperator::Or => may_carry_unseen_closure(left) || may_carry_unseen_closure(right),
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
			| BinaryOperator::Add
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
			| BinaryOperator::NearestNeighbor(_) => false,
		},
		LogicalExpr::Prefix {
			op,
			expr,
		} => match op {
			PrefixOperator::Not
			| PrefixOperator::Positive
			| PrefixOperator::Negate
			| PrefixOperator::Range
			| PrefixOperator::RangeInclusive => false,
			PrefixOperator::Cast(_) => may_carry_unseen_closure(expr),
		},
		LogicalExpr::Param(_)
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
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => true,
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
				let mut set = oracle.builtin(name).unwrap_or(EffectSet::OPAQUE_BODY);
				if oracle.evaluates_against_row(name) {
					set = set.union(EffectSet::READS_ROW);
				}
				// A closure the builtin runs from one of its declared
				// positions is a body the call site cannot see whenever it is
				// not written in the call; the builtin itself declares only
				// what it does besides.
				if invokes_unseen_body(expr, oracle) {
					set = set.union(EffectSet::OPAQUE_BODY);
				}
				set
			}
			Function::Model(_) => EffectSet::READS_EXTERNAL,
			Function::Custom(name) => oracle
				.custom_function(name)
				.unwrap_or(EffectSet::OPAQUE_BODY.union(EffectSet::READS_ROW)),
			Function::Script(_)
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
		| LogicalExpr::Mock(_)
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
fn node_is_impure(expr: &LogicalExpr, children: &[ChildFacts], oracle: &dyn EffectOracle) -> bool {
	match expr {
		LogicalExpr::Call(call) => match &call.receiver {
			Function::Normal(name) => {
				oracle.builtin(name).is_none_or(|effects| !effects.is_deterministic())
					|| invokes_unseen_body(expr, oracle)
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
		// A `[WHERE …]` filter fetches the same way, whatever the shape of its
		// predicate: the predicate is evaluated with each element of
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
		//
		// A field read whose base is proven not to be a record id
		// ([`proven_not_record`]) reads a value in hand and fetches nothing. So
		// does a destructure of such a base whose pattern names plain fields
		// and aliased bodies only: a `f.*` entry or a nested `f.{ … }` fetches
		// the record id its field holds, whatever the base.
		LogicalExpr::Access {
			..
		} => {
			let base = base_facts(children);
			base.may_dereference && !proven_not_record(base.checked_kind.as_ref())
		}
		LogicalExpr::Destructure {
			fields,
			..
		} => {
			let base = base_facts(children);
			base.may_dereference && destructure_fetches(base.checked_kind.as_ref(), fields)
		}
		LogicalExpr::Each(_)
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Lookup {
			..
		} => base_facts(children).may_dereference,
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
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
/// flatten, a destructure or `?.` over such. A cast whose operand is proven
/// ([`LogicalExpr::checked_kind`], against the open schema) to hold a kind the
/// target coerces by identity ([`coercion_is_identity`]) yields its operand
/// unchanged and raises only where the operand does. A field read or a
/// destructure whose base is proven not to be a record id
/// ([`kind_may_be_record`]) fetches nothing and raises only where its children
/// do. `true` for any other cast, arithmetic
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
fn node_may_raise(
	expr: &LogicalExpr,
	child_facts: &[ChildFacts],
	children: bool,
	oracle: &dyn EffectOracle,
) -> bool {
	match expr {
		LogicalExpr::Param(param) => oracle.permission_checked_param(param.as_str()),
		LogicalExpr::Literal(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
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
		LogicalExpr::Each(base) => {
			base_dereference_may_raise(base, base_facts(child_facts).may_dereference) || children
		}
		LogicalExpr::Access {
			base,
			..
		} => {
			let facts = base_facts(child_facts);
			(base_dereference_may_raise(base, facts.may_dereference)
				&& !proven_not_record(facts.checked_kind.as_ref()))
				|| children
		}
		LogicalExpr::Destructure {
			base,
			fields,
		} => {
			let facts = base_facts(child_facts);
			(base_dereference_may_raise(base, facts.may_dereference)
				&& destructure_fetches(facts.checked_kind.as_ref(), fields))
				|| children
		}
		LogicalExpr::Prefix {
			op,
			..
		} => match op {
			PrefixOperator::Not
			| PrefixOperator::Positive
			| PrefixOperator::Range
			| PrefixOperator::RangeInclusive => children,
			PrefixOperator::Cast(kind) => {
				!cast_is_identity(kind, base_facts(child_facts).checked_kind.as_ref()) || children
			}
			PrefixOperator::Negate => true,
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
pub(super) fn node_reads_row(expr: &LogicalExpr, oracle: &dyn EffectOracle) -> bool {
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
		| LogicalExpr::Mock(_)
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
		LogicalExpr::Call(call) => {
			call_evaluates_against_row(call, oracle) || invokes_unseen_body(expr, oracle)
		}
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
		| LogicalExpr::Mock(_)
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
///
/// A caller reading a field of `base`, or destructuring it with a pattern for
/// which [`fields_fetch`] is false, further declines this when `base` is proven
/// not to be a record id ([`proven_not_record`]), since the read then fetches
/// nothing.
fn base_dereference_may_raise(base: &LogicalExpr, base_may_dereference: bool) -> bool {
	if let LogicalExpr::Param(name) = base {
		return !ITERATION_PARAMS.contains(&name.as_str());
	}
	base_may_dereference
}

/// The facts of the first child, which is the base of every step form and the
/// operand of a prefix operator.
fn base_facts(children: &[ChildFacts]) -> &ChildFacts {
	&children[0]
}

/// Whether a base whose checked kind is `kind` is proven, against the open
/// schema, to hold a value that is not a record id, so a field read or
/// destructure of it fetches nothing. The proof is
/// [`LogicalExpr::checked_kind`], never a declared kind: a stored field's
/// declaration is enforced on write alone.
fn proven_not_record(kind: Option<&Kind>) -> bool {
	kind.is_some_and(|kind| !kind_may_be_record(kind))
}

/// Whether destructuring a base whose checked kind is `kind` with `fields` may
/// fetch: the base is not proven to be a non-record ([`proven_not_record`]), or
/// the pattern fetches a held record id whatever the base ([`fields_fetch`]).
fn destructure_fetches(kind: Option<&Kind>, fields: &[LogicalDestructureField]) -> bool {
	!proven_not_record(kind) || fields_fetch(fields)
}

/// Whether a destructure pattern fetches a record id held by a field of its
/// base, whatever the base: an `f.*` entry, or a nested `f.{ … }` whose
/// destructure fetches in turn. An aliased body is a child expression and is
/// judged as one.
fn fields_fetch(fields: &[LogicalDestructureField]) -> bool {
	fields.iter().any(|field| {
		matches!(field, LogicalDestructureField::All(_) | LogicalDestructureField::Destructure(..))
	})
}

/// Whether a cast to `kind` of an operand whose checked kind, against the open
/// schema, is `operand` yields the operand unchanged: `operand` is one `kind`
/// coerces by identity.
fn cast_is_identity(kind: &Kind, operand: Option<&Kind>) -> bool {
	operand.is_some_and(|checked| coercion_is_identity(kind, checked))
}

/// Whether a value `expr` produces may be, or hold, a record id: a literal,
/// or a collection of literals, cannot; a record id, and everything whose
/// value only the run decides, may.
pub fn may_dereference(expr: &LogicalExpr) -> bool {
	dereference_by(expr, || {
		let mut any = false;
		expr.for_each_child(&mut |child| any = any || may_dereference(child));
		any
	})
}

/// [`may_dereference`] of a node given the same fact of each child, one per
/// child in the order of [`LogicalExpr::for_each_child`].
pub fn node_may_dereference(expr: &LogicalExpr, children: &[ChildFacts]) -> bool {
	dereference_by(expr, || children.iter().any(|child| child.may_dereference))
}

/// The one definition of [`may_dereference`]: `any_child` answers whether any
/// child of a collection may, and is asked of a collection alone.
fn dereference_by(expr: &LogicalExpr, any_child: impl FnOnce() -> bool) -> bool {
	match expr {
		LogicalExpr::Literal(_) => false,
		LogicalExpr::Array(_) | LogicalExpr::Set(_) | LogicalExpr::Object(_) => any_child(),
		LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
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
	use super::{EffectOracle, EffectSet, Effects, effects_of, invokes_unseen_body};
	use crate::expr::logical::{
		LogicalCall, LogicalClosure, LogicalExpr, LogicalLiteral, lower_expr,
	};
	use crate::expr::{BinaryOperator, Function, Param};

	/// An oracle declaring `array::any` and `array::fold` as pure builtins
	/// running a closure from their last position, `api::invoke` as running
	/// a body it cannot see outright, and `string::len` as pure, with no
	/// methods and no projection functions.
	struct Declared;

	impl EffectOracle for Declared {
		fn builtin(&self, name: &str) -> Option<EffectSet> {
			match name {
				"array::any" | "array::fold" | "string::len" => Some(EffectSet::PURE),
				"api::invoke" => Some(EffectSet::OPAQUE_BODY),
				_ => None,
			}
		}

		fn closure_args(&self, name: &str) -> Option<&'static [usize]> {
			match name {
				"array::any" => Some(&[1]),
				"array::fold" => Some(&[2]),
				"string::len" | "api::invoke" => Some(&[]),
				_ => None,
			}
		}

		fn method_always_dispatches(&self, _name: &str) -> bool {
			false
		}

		fn evaluates_against_row(&self, _name: &str) -> bool {
			false
		}

		fn permission_checked_param(&self, _name: &str) -> bool {
			false
		}
	}

	fn call(name: &str, args: Vec<LogicalExpr>) -> LogicalExpr {
		LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal(name.to_string()),
			args,
		}))
	}

	fn param(name: &str) -> LogicalExpr {
		LogicalExpr::Param(Param::from(name.to_string()))
	}

	fn int(n: i64) -> LogicalExpr {
		LogicalExpr::Literal(LogicalLiteral::Integer(n))
	}

	fn closure(body: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Closure(Box::new(LogicalClosure {
			args: Vec::new(),
			returns: None,
			body_sql: std::sync::Arc::from(surrealdb_types::ToSql::to_sql(&body)),
			body,
		}))
	}

	#[test]
	fn a_callback_builtin_handed_a_value_invokes_an_unseen_body() {
		let one = LogicalExpr::Array(vec![int(1)]);
		// The closure as a parameter, and read off an object in a parameter.
		assert!(invokes_unseen_body(
			&call("array::any", vec![one.clone(), param("pred")]),
			&Declared
		));
		let field = LogicalExpr::Access {
			base: Box::new(param("o")),
			field: "check".into(),
		};
		assert!(invokes_unseen_body(&call("array::any", vec![one.clone(), field]), &Declared));
		// The closure as the result of another call.
		let inner = call("array::any", vec![one.clone(), param("pred")]);
		assert!(invokes_unseen_body(&call("array::any", vec![one, inner]), &Declared));
		// A builtin the oracle does not know is as opaque as one declaring it.
		assert!(invokes_unseen_body(&call("fn::unknown", vec![param("pred")]), &Declared));
	}

	#[test]
	fn a_closure_written_in_the_call_is_not_unseen() {
		let one = LogicalExpr::Array(vec![int(1)]);
		let body = LogicalExpr::Binary {
			left: Box::new(LogicalExpr::Field("secret".into())),
			op: BinaryOperator::Equal,
			right: Box::new(LogicalExpr::Literal(LogicalLiteral::String("yes".into()))),
		};
		assert!(!invokes_unseen_body(
			&call("array::any", vec![one.clone(), closure(body)]),
			&Declared
		));
		// Nor is a value that can never be a closure: a literal check, a
		// comparison, a container of literals.
		assert!(!invokes_unseen_body(&call("array::any", vec![one.clone(), int(1)]), &Declared));
		let cmp = LogicalExpr::Binary {
			left: Box::new(param("a")),
			op: BinaryOperator::MoreThan,
			right: Box::new(int(1)),
		};
		assert!(!invokes_unseen_body(&call("array::any", vec![one.clone(), cmp]), &Declared));
		// `??` yields an operand, so it carries what its operands may.
		let coalesce = LogicalExpr::Binary {
			left: Box::new(param("pred")),
			op: BinaryOperator::NullCoalescing,
			right: Box::new(int(1)),
		};
		assert!(invokes_unseen_body(&call("array::any", vec![one, coalesce]), &Declared));
	}

	#[test]
	fn a_pure_builtin_runs_no_body_whatever_it_is_handed() {
		assert!(!invokes_unseen_body(&call("string::len", vec![param("pred")]), &Declared));
		assert!(!invokes_unseen_body(
			&call("string::len", vec![LogicalExpr::Field("name".into())]),
			&Declared
		));
	}

	#[test]
	fn only_the_declared_closure_position_is_judged() {
		let tags = LogicalExpr::Field("tags".into());
		// The array searched is a field and the check a literal: nothing in
		// the closure position may be a body, so the call is transparent.
		assert!(!invokes_unseen_body(&call("array::any", vec![tags.clone(), int(1)]), &Declared));
		assert!(!invokes_unseen_body(&call("array::any", vec![param("arr"), int(1)]), &Declared));
		// The same array with a parameter in the closure position is not.
		assert!(invokes_unseen_body(&call("array::any", vec![tags, param("pred")]), &Declared));
		// `array::fold` runs its third argument: a parameter seed is not a
		// body, a parameter mapper is.
		let body = closure(param("acc"));
		assert!(!invokes_unseen_body(
			&call("array::fold", vec![param("arr"), param("init"), body]),
			&Declared
		));
		assert!(invokes_unseen_body(
			&call("array::fold", vec![param("arr"), int(0), param("f")]),
			&Declared
		));
		// A closure position the call leaves empty carries nothing.
		assert!(!invokes_unseen_body(&call("array::any", vec![param("arr")]), &Declared));
		// A builtin declaring an opaque body outright is judged in every
		// position, having declared none.
		assert!(invokes_unseen_body(&call("api::invoke", vec![param("req")]), &Declared));
		assert!(!invokes_unseen_body(&call("api::invoke", vec![int(1)]), &Declared));
	}

	#[test]
	fn a_closure_a_builtin_runs_is_an_effect_of_the_call() {
		use super::effects_of;
		let tags = LogicalExpr::Field("tags".into());
		let transparent = effects_of(&call("array::any", vec![tags.clone(), int(1)]), &Declared);
		assert!(transparent.pure(), "{transparent:?}");
		assert!(!transparent.reads_row_opaquely);
		let opaque = effects_of(&call("array::any", vec![tags, param("pred")]), &Declared);
		assert!(opaque.set.contains(EffectSet::OPAQUE_BODY), "{opaque:?}");
		assert!(opaque.impure);
		assert!(opaque.reads_row_opaquely);
	}

	#[test]
	fn a_read_of_stable_state_is_deterministic_and_an_effect_is_not() {
		assert!(EffectSet::PURE.is_deterministic());
		assert!(
			(EffectSet::READS_DATA
				| EffectSet::READS_CATALOG
				| EffectSet::READS_PRINCIPAL
				| EffectSet::READS_SESSION_DB
				| EffectSet::READS_ROW)
				.is_deterministic()
		);
		assert!(EffectSet::READS_SESSION.is_deterministic());
		for (flag, _) in EffectSet::ALL {
			let stable = matches!(
				*flag,
				EffectSet::READS_DATA
					| EffectSet::READS_CATALOG
					| EffectSet::READS_PRINCIPAL
					| EffectSet::READS_SESSION_DB
					| EffectSet::READS_ROW
					| EffectSet::MAY_REJECT
			);
			assert_eq!(flag.is_deterministic(), stable, "{flag:?}");
		}
	}

	#[test]
	fn what_outlives_the_statement_mutates_state() {
		for (flag, _) in EffectSet::ALL {
			let outlives = matches!(
				*flag,
				EffectSet::WRITES_DATA
					| EffectSet::WRITES_EXTERNAL
					| EffectSet::ADVANCES_STATE
					| EffectSet::WRITES_SESSION
					| EffectSet::NOTIFY
					| EffectSet::DEFERS
					| EffectSet::CHANGES_CATALOG
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
		assert_eq!(EffectSet::ALL.len(), 18, "every flag is named");
		assert_eq!(
			format!("{:?}", EffectSet::READS_PRINCIPAL | EffectSet::READS_SESSION_DB),
			"READS_PRINCIPAL | READS_SESSION_DB"
		);
		assert_eq!(
			format!("{:?}", EffectSet::READS_SESSION),
			format!("{:?}", EffectSet::READS_PRINCIPAL | EffectSet::READS_SESSION_DB)
		);
	}

	#[test]
	fn the_session_union_is_the_two_split_reads_and_no_listed_flag() {
		assert_eq!(
			EffectSet::READS_SESSION,
			EffectSet::READS_PRINCIPAL | EffectSet::READS_SESSION_DB
		);
		assert!(EffectSet::ALL.iter().all(|(flag, _)| *flag != EffectSet::READS_SESSION));
		let mut seen = EffectSet::PURE;
		for (flag, _) in EffectSet::ALL {
			assert!(!seen.intersects(*flag), "{flag:?} overlaps an earlier flag");
			seen |= *flag;
		}
		// Bit 10 is reserved and no flag holds it.
		assert!(!seen.intersects(EffectSet(1 << 10)));
	}

	#[test]
	fn top_holds_every_flag_and_is_neither_deterministic_nor_inert() {
		for (flag, _) in EffectSet::ALL {
			assert!(EffectSet::TOP.contains(*flag), "TOP lacks {flag:?}");
		}
		assert!(EffectSet::TOP.contains(EffectSet::READS_SESSION));
		assert!(!EffectSet::TOP.is_deterministic());
		assert!(EffectSet::TOP.mutates_state());
	}

	fn effects(text: &str) -> Effects {
		let expr = lower_expr(crate::syn::expr(text).expect("expression parses"))
			.expect("expression lowers");
		effects_of(&expr, &Declared)
	}

	#[test]
	fn a_cast_proven_identity_raises_only_where_its_operand_does() {
		assert!(!effects("<option<bool>> (a > 1)").may_raise);
		// The inner cast raises.
		assert!(effects("<number> (<int> a)").may_raise);
		// A `bool` is not a `number`.
		assert!(effects("<number> (a > 1)").may_raise);
		// The operand raises.
		assert!(effects("<any> (a + 1)").may_raise);
	}

	#[test]
	fn a_read_of_a_base_proven_not_a_record_fetches_nothing() {
		let object = effects("{ x: 1 }.x");
		assert!(!object.may_raise);
		assert!(object.pure());
		assert!(!effects("{ x: a }.x").may_raise);
		let cast = effects("(<object> $v).f");
		assert!(!cast.impure);
		// The cast raises: `$v` is not proven an object.
		assert!(cast.may_raise);
		assert!(effects("(<record> $v).f").impure);
		let field = effects("a.f");
		assert!(field.may_raise);
		assert!(field.impure);
		assert!(!effects("(<array<object>> $v).f").impure);
		assert!(effects("(<array<record>> $v).f").impure);
	}

	#[test]
	fn a_destructure_fetching_a_field_is_no_read_of_a_value_in_hand() {
		let plain = effects("(<object> $v).{ f }");
		assert!(!plain.impure);
		assert!(plain.may_raise);
		assert!(!effects("{ f: 1 }.{ f }").may_raise);
		// `f.*` and a nested destructure fetch a record id held by `f`.
		for text in [
			"(<object> $v).{ f.* }",
			"(<object> $v).{ f.{ g } }",
			"(<object> $v).{ f, h.* }",
			"{ f: a }.{ f.* }",
		] {
			let fetch = effects(text);
			assert!(fetch.impure, "{text} is impure");
			assert!(fetch.may_raise, "{text} may raise");
		}
	}

	#[test]
	fn arithmetic_raises_whatever_the_operand_kinds() {
		assert!(effects("(<int> a) + 1").may_raise);
		assert!(effects("-(<float> a)").may_raise);
		assert!(effects("(<int> a) / 0").may_raise);
	}
}
