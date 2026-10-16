//! What an expression op does and how it is shaped: its local effects and fallibility, and the
//! structural facts the verifier and lowering read.
//!
//! The effect and failure answers are local: they do not fold the ops in the op's regions, and a
//! call answers by the call rule of its callee kind, never by the callee body. An answer that
//! depends on an operand fact (a base that is not a record id, a conversion that is the identity,
//! a closure the call can see) takes its conservative arm, because the answer sees only the op's
//! attributes. The transitive answer is computed by the analysis pass.
//!
//! Both answers come from one per-variant [`Rule`], so they never disagree. Every effect set an
//! op declares itself passes through [`floor`], which adds the failure classes the effects imply.
//! A callee's declaration does not: the call rule passes the declared class through and adds only
//! what the call itself does, so a builtin whose effects contradict its class stays visible to the
//! release verifier.
//!
//! The structural answers (terminator, region kinds, arity and native lowering) are fixed
//! properties of an op's attributes, read by the generic verifier rules and by lowering. They
//! decide nothing. Native lowering answers the leaf class of what the VM lowering emits for the
//! op; a class that names a host interface or a bar is built from types the VM keeps private, so
//! such an op answers a call of a unit.

#![deny(clippy::wildcard_enum_match_arm)]

use std::ops::Bound;

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::Strand;

use super::ops::{DestructureEntry, ExprOp, KeyBound, KeyShape, RaiseKind};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{
	Determinism, FieldSet, IrFootprint, IrResource, KeyOf, OpEffects, Rollback,
};
use crate::exec::ir::iface::fallibility::{ContextDep, Fallibility};
use crate::exec::ir::iface::region_kind::{ClauseRole, ErrorScope, RegionKind, RegionKinds};
use crate::exec::ir::iface::statements::{Place, PlacePart};
use crate::exec::ir::op_kind::{
	Arity, IsaCaps, LeafClass, NativeLowering, OperandRule, Terminator,
};
use crate::expr::logical::{EffectSet, MisplacedControl};
use crate::expr::{BinaryOperator, Function, PrefixOperator};

/// The answer of a pure op: no effect, no footprint, nothing to roll back, deterministic. `class`
/// is the only fact the caller states.
fn pure(class: Fallibility) -> OpEffects {
	OpEffects {
		set: EffectSet::PURE,
		fallibility: class,
		context_deps: SmallVec::new(),
		footprint: IrFootprint {
			reads: SmallVec::new(),
			writes: SmallVec::new(),
		},
		rollback: Rollback::NoEffects,
		determinism: Determinism::Deterministic,
	}
}

/// The answer of an op declared by its effect `set` and failure `class`. Every other fact derives
/// from `set` by the rules below, so each is stated once.
///
/// - Determinism: a flag that makes a second evaluation differ is `NonDeterministic`; otherwise a
///   clock read is `PerStatement` and a data read `WithinStatementUnlessWritten`.
/// - Rollback: an effect outside the transaction is `Survives`; a transactional write or catalog
///   change is `Undone`.
/// - Footprint: an opaque body reads and writes `Unknown`. Otherwise a data read or write names
///   every record, and a catalog read or change names `Unknown`, which conflicts with every other
///   access.
fn from_set(set: EffectSet, class: Fallibility, deps: SmallVec<[ContextDep; 1]>) -> OpEffects {
	let determinism = if !set.is_deterministic_but_for_clock() {
		Determinism::NonDeterministic
	} else if set.contains(EffectSet::CLOCK) {
		Determinism::PerStatement
	} else if set.contains(EffectSet::READS_DATA) {
		Determinism::WithinStatementUnlessWritten
	} else {
		Determinism::Deterministic
	};
	let rollback = if set.intersects(
		EffectSet::WRITES_EXTERNAL
			| EffectSet::ADVANCES_STATE
			| EffectSet::OPAQUE_BODY
			| EffectSet::NOTIFY
			| EffectSet::WRITES_SESSION,
	) {
		Rollback::Survives
	} else if set
		.intersects(EffectSet::WRITES_DATA | EffectSet::CHANGES_CATALOG | EffectSet::DEFERS)
	{
		Rollback::Undone
	} else {
		Rollback::NoEffects
	};
	let mut footprint = IrFootprint {
		reads: SmallVec::new(),
		writes: SmallVec::new(),
	};
	if set.contains(EffectSet::OPAQUE_BODY) {
		footprint.reads.push(IrResource::Unknown);
		footprint.writes.push(IrResource::Unknown);
	} else {
		if set.contains(EffectSet::READS_DATA) {
			footprint.reads.push(all_data());
		}
		if set.contains(EffectSet::READS_CATALOG) {
			footprint.reads.push(IrResource::Unknown);
		}
		if set.contains(EffectSet::WRITES_DATA) {
			footprint.writes.push(all_data());
		}
		if set.contains(EffectSet::CHANGES_CATALOG) {
			footprint.writes.push(IrResource::Unknown);
		}
	}
	OpEffects {
		set,
		fallibility: class,
		context_deps: deps,
		footprint,
		rollback,
		determinism,
	}
}

/// Every field of every record.
fn all_data() -> IrResource {
	IrResource::Data {
		table: None,
		fields: FieldSet::All,
		key: KeyOf::Any,
	}
}

/// `class` with the failure classes `set` implies: a data read or a rejection fails on stored
/// data, and an opaque body or an external call fails on the context and on data. This is what
/// the release verifier checks of a transitive answer.
fn floor(set: EffectSet, class: Fallibility) -> Fallibility {
	let mut class = class;
	if set.intersects(EffectSet::READS_DATA | EffectSet::MAY_REJECT) {
		class = class | Fallibility::DATA;
	}
	if set
		.intersects(EffectSet::OPAQUE_BODY | EffectSet::READS_EXTERNAL | EffectSet::WRITES_EXTERNAL)
	{
		class = class | Fallibility::CONTEXT | Fallibility::DATA;
	}
	class
}

/// How the answer's footprint, rollback and determinism are produced.
#[derive(Clone, Copy, Debug)]
enum Shape {
	/// [`pure`].
	Pure,
	/// [`from_set`] over the rule's set.
	Derived,
	/// The reads of a fetch: every record, and nothing written. Fetched records derive `COMPUTED`
	/// fields whose bodies the op does not hold, so two evaluations may differ and the effects of
	/// those bodies survive a rollback.
	Fetch,
	/// [`OpEffects::top`].
	Top,
}

/// One op's declaration: what both the effect and the failure answer are read from.
struct Rule {
	set: EffectSet,
	class: Fallibility,
	deps: SmallVec<[ContextDep; 1]>,
	shape: Shape,
}

impl Rule {
	/// A pure op that fails as `class` says.
	fn pure(class: Fallibility) -> Self {
		Self {
			set: EffectSet::PURE,
			class,
			deps: SmallVec::new(),
			shape: Shape::Pure,
		}
	}

	/// An op the dialect declares itself: its class is floored by what `set` implies.
	fn declared(set: EffectSet, class: Fallibility) -> Self {
		Self::declared_with(set, class, SmallVec::new())
	}

	/// [`Self::declared`] with the context dependencies a `CONTEXT` failure rests on.
	fn declared_with(set: EffectSet, class: Fallibility, deps: SmallVec<[ContextDep; 1]>) -> Self {
		Self {
			set,
			class: floor(set, class),
			deps,
			shape: Shape::Derived,
		}
	}

	/// A call of something that declares its own effects and class. The declared class passes
	/// through unfloored. The call adds `VALUE` for argument coercion and `CONTEXT` for the
	/// capability check every callee passes, and `CONTEXT | DATA` for a body it cannot see or a
	/// call that leaves the process.
	fn call(set: EffectSet, declared: Fallibility) -> Self {
		let mut class = declared | Fallibility::VALUE | Fallibility::CONTEXT;
		if set.intersects(
			EffectSet::OPAQUE_BODY | EffectSet::READS_EXTERNAL | EffectSet::WRITES_EXTERNAL,
		) {
			class = class | Fallibility::DATA;
		}
		Self {
			set,
			class,
			deps: smallvec![ContextDep::Capabilities],
			shape: Shape::Derived,
		}
	}

	/// The read of a fetching op.
	fn fetch() -> Self {
		Self::declared(
			EffectSet::READS_DATA | EffectSet::READS_CATALOG | EffectSet::OPAQUE_BODY,
			Fallibility::TOP,
		)
		.with_shape(Shape::Fetch)
	}

	/// An op nothing is known about.
	fn top() -> Self {
		Self::top_with(SmallVec::new())
	}

	/// [`Self::top`] that still names the dependencies its failure rests on.
	fn top_with(deps: SmallVec<[ContextDep; 1]>) -> Self {
		Self {
			set: EffectSet::TOP,
			class: Fallibility::TOP,
			deps,
			shape: Shape::Top,
		}
	}

	fn with_shape(mut self, shape: Shape) -> Self {
		self.shape = shape;
		self
	}

	fn into_effects(self) -> OpEffects {
		match self.shape {
			Shape::Pure => {
				let mut effects = pure(self.class);
				effects.context_deps = self.deps;
				effects
			}
			Shape::Derived => from_set(self.set, self.class, self.deps),
			Shape::Fetch => OpEffects {
				set: self.set,
				fallibility: self.class,
				context_deps: self.deps,
				footprint: IrFootprint {
					reads: smallvec![all_data()],
					writes: SmallVec::new(),
				},
				rollback: Rollback::Survives,
				determinism: Determinism::NonDeterministic,
			},
			Shape::Top => OpEffects {
				context_deps: self.deps,
				..OpEffects::top()
			},
		}
	}
}

impl ExprOp {
	/// The op's own effects, excluding the ops of its regions. [`Self::fallibility`] answers the
	/// `fallibility` field of this value.
	pub(crate) fn local_effects(&self, cx: &EffectCx<'_>) -> OpEffects {
		self.answer(cx)
	}

	/// The op's own failure classes. Total by type: no op is without an answer. It reads the same
	/// declaration as [`Self::local_effects`] and builds no footprint.
	pub(crate) fn fallibility(&self, cx: &EffectCx<'_>) -> Fallibility {
		self.class(cx)
	}

	/// The full answer, derived from the op's [`Rule`].
	fn answer(&self, cx: &EffectCx<'_>) -> OpEffects {
		self.rule(cx).into_effects()
	}

	/// The failure classes of the op's [`Rule`].
	fn class(&self, cx: &EffectCx<'_>) -> Fallibility {
		self.rule(cx).class
	}

	/// The declaration of every op. The match names every variant, so a variant added to the
	/// dialect does not compile until it is declared here.
	fn rule(&self, cx: &EffectCx<'_>) -> Rule {
		let infallible = Fallibility::INFALLIBLE;
		let value = Fallibility::VALUE;
		match self {
			ExprOp::Literal(..)
			| ExprOp::Array
			| ExprOp::Set
			| ExprOp::Object {
				..
			}
			| ExprOp::Table {
				..
			}
			| ExprOp::Mock(..)
			| ExprOp::Constant {
				..
			}
			| ExprOp::Doc
			| ExprOp::Yield
			| ExprOp::Truthy
			| ExprOp::First
			| ExprOp::Last
			| ExprOp::Flatten {
				..
			}
			| ExprOp::Optional
			| ExprOp::ShortCircuit {
				..
			}
			| ExprOp::If
			| ExprOp::Subquery(_)
			| ExprOp::ClosureCreate(..)
			| ExprOp::DepthCharge {
				..
			} => Rule::pure(infallible),
			ExprOp::RecordId(attrs) => match &attrs.key_shape {
				KeyShape::Number(..)
				| KeyShape::String(..)
				| KeyShape::Uuid(..)
				| KeyShape::Array {
					..
				}
				| KeyShape::Object {
					..
				} => Rule::pure(infallible),
				KeyShape::Generate(..) => Rule::declared(EffectSet::ENTROPY, value),
				KeyShape::Range(range) => {
					let generated = [&range.start, &range.end].into_iter().any(|bound| {
						matches!(
							bound,
							std::ops::Bound::Included(KeyBound::Generate(..))
								| std::ops::Bound::Excluded(KeyBound::Generate(..))
						)
					});
					if generated {
						Rule::declared(EffectSet::ENTROPY, value)
					} else {
						Rule::pure(value)
					}
				}
			},
			ExprOp::SessionParam {
				..
			} => Rule::declared(EffectSet::READS_PRINCIPAL, infallible),
			ExprOp::CatalogParam {
				name,
			} => Rule::declared_with(
				EffectSet::READS_CATALOG,
				Fallibility::CONTEXT | Fallibility::DATA,
				smallvec![ContextDep::ParamPermission {
					name: name.clone()
				}],
			),
			ExprOp::FreeParam {
				name,
			} => Rule::declared_with(
				EffectSet::READS_CATALOG | EffectSet::READS_PRINCIPAL,
				Fallibility::CONTEXT | Fallibility::DATA,
				smallvec![ContextDep::ParamPermission {
					name: name.clone()
				}],
			),
			ExprOp::Field {
				..
			} => Rule::pure(value | Fallibility::CONTEXT | Fallibility::DATA),
			ExprOp::ComputedRead(..) => Rule::top(),
			ExprOp::Access {
				..
			}
			| ExprOp::Each
			| ExprOp::Destructure {
				..
			} => Rule::fetch(),
			ExprOp::Deref {
				..
			} => Rule::declared(EffectSet::READS_DATA | EffectSet::READS_CATALOG, Fallibility::DATA),
			ExprOp::Index
			| ExprOp::SetField {
				..
			}
			| ExprOp::TupleRow {
				..
			}
			| ExprOp::RepeatRecurse
			| ExprOp::Postfix {
				..
			}
			| ExprOp::Dispatch(..)
			| ExprOp::Convert(..)
			| ExprOp::Throw => Rule::pure(value),
			ExprOp::Prefix(op) => match &**op {
				PrefixOperator::Not
				| PrefixOperator::Positive
				| PrefixOperator::Range
				| PrefixOperator::RangeInclusive => Rule::pure(infallible),
				PrefixOperator::Negate | PrefixOperator::Cast(..) => Rule::pure(value),
			},
			ExprOp::Binary {
				op,
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
				| BinaryOperator::Or
				| BinaryOperator::And
				| BinaryOperator::NullCoalescing
				| BinaryOperator::TenaryCondition => Rule::pure(infallible),
				BinaryOperator::Add
				| BinaryOperator::Subtract
				| BinaryOperator::Multiply
				| BinaryOperator::Divide
				| BinaryOperator::Remainder
				| BinaryOperator::Power
				| BinaryOperator::Range
				| BinaryOperator::RangeInclusive
				| BinaryOperator::RangeSkip
				| BinaryOperator::RangeSkipInclusive => Rule::pure(value),
				BinaryOperator::Matches(..) | BinaryOperator::NearestNeighbor(..) => {
					Rule::declared(EffectSet::READS_DATA, Fallibility::DATA)
				}
			},
			ExprOp::Raise(kind) => match &**kind {
				RaiseKind::Folded(..) | RaiseKind::InvalidControlFlow | RaiseKind::Refused(..) => {
					Rule::pure(value)
				}
				RaiseKind::Misplaced(control) => match control {
					MisplacedControl::BeginInSession => Rule::pure(Fallibility::CONTEXT),
					MisplacedControl::NestedBegin
					| MisplacedControl::CommitOutsideBlock
					| MisplacedControl::CancelOutsideBlock => Rule::pure(value),
				},
			},
			ExprOp::Now {
				..
			} => Rule::declared(EffectSet::CLOCK, infallible),
			ExprOp::Call(call) => call_rule(&call.function, cx),
			ExprOp::Method {
				name,
			} => method_rule(name.as_str(), cx),
			ExprOp::Invoke => Rule::call(EffectSet::OPAQUE_BODY, Fallibility::TOP),
			ExprOp::Agg {
				kind,
				..
			} => match cx.registry.get_aggregate(kind) {
				Some(aggregate) => Rule::call(EffectSet::PURE, aggregate.fallibility()),
				None => Rule::top(),
			},
		}
	}

	/// Whether the op ends the block it stands last in.
	pub(crate) fn is_terminator(&self) -> bool {
		self.terminator().is_some()
	}

	/// How the op ends its block, or `None` for an op that is not a terminator.
	pub(crate) fn terminator(&self) -> Option<Terminator> {
		match self {
			ExprOp::Throw | ExprOp::Raise(..) => Some(Terminator::Raise),
			ExprOp::Yield => Some(Terminator::Yield),
			ExprOp::Literal(..)
			| ExprOp::Array
			| ExprOp::Set
			| ExprOp::Object {
				..
			}
			| ExprOp::RecordId(..)
			| ExprOp::FreeParam {
				..
			}
			| ExprOp::SessionParam {
				..
			}
			| ExprOp::CatalogParam {
				..
			}
			| ExprOp::Table {
				..
			}
			| ExprOp::Mock(..)
			| ExprOp::Constant {
				..
			}
			| ExprOp::Field {
				..
			}
			| ExprOp::ComputedRead(..)
			| ExprOp::Doc
			| ExprOp::Access {
				..
			}
			| ExprOp::Deref {
				..
			}
			| ExprOp::Index
			| ExprOp::First
			| ExprOp::Last
			| ExprOp::Each
			| ExprOp::Flatten {
				..
			}
			| ExprOp::Dispatch(..)
			| ExprOp::Method {
				..
			}
			| ExprOp::Destructure {
				..
			}
			| ExprOp::Optional
			| ExprOp::RepeatRecurse
			| ExprOp::SetField {
				..
			}
			| ExprOp::TupleRow {
				..
			}
			| ExprOp::Prefix(..)
			| ExprOp::Convert(..)
			| ExprOp::Postfix {
				..
			}
			| ExprOp::Invoke
			| ExprOp::Binary {
				..
			}
			| ExprOp::ShortCircuit {
				..
			}
			| ExprOp::Call(..)
			| ExprOp::ClosureCreate(..)
			| ExprOp::If
			| ExprOp::Subquery(_)
			| ExprOp::Truthy
			| ExprOp::Agg {
				..
			}
			| ExprOp::Now {
				..
			}
			| ExprOp::DepthCharge {
				..
			} => None,
		}
	}

	/// The kind of each of the op's regions, in position order.
	pub(crate) fn region_kinds(&self) -> RegionKinds {
		match self {
			ExprOp::Destructure {
				shape,
			} => SmallVec::from_elem(RegionKind::LazyOperand, aliased_entries(shape)),
			ExprOp::Optional
			| ExprOp::ShortCircuit {
				..
			} => smallvec![RegionKind::LazyOperand],
			ExprOp::ClosureCreate(..) => smallvec![RegionKind::ClosureBody],
			ExprOp::TupleRow {
				places,
			} => SmallVec::from_elem(RegionKind::Clause(ClauseRole::Where), place_filters(places)),
			ExprOp::If => smallvec![RegionKind::ValueIf, RegionKind::ValueIf],
			ExprOp::Subquery(strategy) => {
				SmallVec::from_elem(RegionKind::Subquery, strategy.region_count())
			}
			ExprOp::Agg {
				..
			} => smallvec![RegionKind::ErrorScope(ErrorScope::ShapeToNone)],
			ExprOp::Literal(..)
			| ExprOp::Array
			| ExprOp::Set
			| ExprOp::Object {
				..
			}
			| ExprOp::RecordId(..)
			| ExprOp::FreeParam {
				..
			}
			| ExprOp::SessionParam {
				..
			}
			| ExprOp::CatalogParam {
				..
			}
			| ExprOp::Table {
				..
			}
			| ExprOp::Mock(..)
			| ExprOp::Constant {
				..
			}
			| ExprOp::Field {
				..
			}
			| ExprOp::ComputedRead(..)
			| ExprOp::Doc
			| ExprOp::Access {
				..
			}
			| ExprOp::Deref {
				..
			}
			| ExprOp::Index
			| ExprOp::First
			| ExprOp::Last
			| ExprOp::Each
			| ExprOp::Flatten {
				..
			}
			| ExprOp::Dispatch(..)
			| ExprOp::Method {
				..
			}
			| ExprOp::RepeatRecurse
			| ExprOp::SetField {
				..
			}
			| ExprOp::Prefix(..)
			| ExprOp::Convert(..)
			| ExprOp::Postfix {
				..
			}
			| ExprOp::Invoke
			| ExprOp::Binary {
				..
			}
			| ExprOp::Call(..)
			| ExprOp::Throw
			| ExprOp::Yield
			| ExprOp::Truthy
			| ExprOp::Now {
				..
			}
			| ExprOp::Raise(..)
			| ExprOp::DepthCharge {
				..
			} => SmallVec::new(),
		}
	}

	/// The operand and result counts the op accepts. A count past `u16::MAX` answers
	/// `Exactly(u16::MAX)`, which the generic arity rule rejects: no count is ever truncated into
	/// one the op could satisfy.
	pub(crate) fn arity(&self) -> Arity {
		match self {
			ExprOp::Literal(..)
			| ExprOp::FreeParam {
				..
			}
			| ExprOp::SessionParam {
				..
			}
			| ExprOp::CatalogParam {
				..
			}
			| ExprOp::Table {
				..
			}
			| ExprOp::Mock(..)
			| ExprOp::Constant {
				..
			}
			| ExprOp::Subquery(_)
			| ExprOp::Agg {
				..
			} => exactly(0, 1),
			ExprOp::Array | ExprOp::Set => at_least(0, 1),
			ExprOp::Object {
				keys,
			} => exactly(keys.len(), 1),
			ExprOp::TupleRow {
				places,
			} => exactly(places.len() + place_subscripts(places), 1),
			ExprOp::RecordId(attrs) => exactly(key_operands(&attrs.key_shape), 1),
			ExprOp::Field {
				..
			}
			| ExprOp::ComputedRead(..)
			| ExprOp::Doc
			| ExprOp::Access {
				..
			}
			| ExprOp::Deref {
				..
			}
			| ExprOp::First
			| ExprOp::Last
			| ExprOp::Each
			| ExprOp::Flatten {
				..
			}
			| ExprOp::RepeatRecurse
			| ExprOp::Prefix(..)
			| ExprOp::Convert(..)
			| ExprOp::Postfix {
				..
			}
			| ExprOp::Truthy
			| ExprOp::Dispatch(..)
			| ExprOp::Destructure {
				..
			}
			| ExprOp::Optional
			| ExprOp::ShortCircuit {
				..
			}
			| ExprOp::If
			| ExprOp::Now {
				..
			} => exactly(1, 1),
			ExprOp::Index
			| ExprOp::SetField {
				..
			}
			| ExprOp::Binary {
				..
			} => exactly(2, 1),
			ExprOp::Method {
				..
			} => at_least(2, 1),
			ExprOp::Invoke => at_least(1, 1),
			ExprOp::Call(..) | ExprOp::ClosureCreate(..) => at_least(0, 1),
			ExprOp::DepthCharge {
				..
			}
			| ExprOp::Raise(..) => exactly(0, 0),
			ExprOp::Throw => exactly(1, 0),
			ExprOp::Yield => at_least(0, 0),
		}
	}

	/// How the op reaches the interpreter on the target `isa` describes: the class of the
	/// instruction or host step the VM lowering emits for the corresponding expression. Every op
	/// answers the same on every target.
	///
	/// A class that names a host interface (`Kernel`) or a bar (`NonLeaf`) is built from types the
	/// VM keeps private, so an op of that class answers [`NativeLowering::UnitCall`]; the doc line
	/// of each such arm names the class it takes. `registry` names the index functions a call of
	/// [`Function::Normal`] may hold.
	pub(crate) fn lowers_natively(
		&self,
		_isa: IsaCaps,
		registry: &FunctionRegistry,
	) -> NativeLowering {
		let leaf = NativeLowering::Native {
			leaf_class: LeafClass::Leaf,
		};
		match self {
			// The interpreter answers each of these from the program and the row alone.
			ExprOp::Literal(..)
			| ExprOp::Array
			| ExprOp::Set
			| ExprOp::Object {
				..
			}
			| ExprOp::RecordId(..)
			| ExprOp::Table {
				..
			}
			| ExprOp::Mock(..)
			| ExprOp::Constant {
				..
			}
			| ExprOp::Doc
			| ExprOp::Index
			| ExprOp::First
			| ExprOp::Last
			| ExprOp::Flatten {
				..
			}
			| ExprOp::Dispatch(..)
			| ExprOp::Optional
			| ExprOp::SetField {
				..
			}
			| ExprOp::Prefix(..)
			| ExprOp::Convert(..)
			| ExprOp::Postfix {
				..
			}
			| ExprOp::ShortCircuit {
				..
			}
			| ExprOp::If
			| ExprOp::Throw
			| ExprOp::Yield
			| ExprOp::Truthy
			| ExprOp::Agg {
				..
			}
			| ExprOp::Raise(..)
			| ExprOp::DepthCharge {
				..
			} => leaf,
			ExprOp::Binary {
				op,
			} => match op {
				// An answer from an index: `Kernel(&[Storage])`.
				BinaryOperator::Matches(..) | BinaryOperator::NearestNeighbor(..) => {
					NativeLowering::UnitCall
				}
				BinaryOperator::Subtract
				| BinaryOperator::Add
				| BinaryOperator::Multiply
				| BinaryOperator::Divide
				| BinaryOperator::Remainder
				| BinaryOperator::Power
				| BinaryOperator::Equal
				| BinaryOperator::ExactEqual
				| BinaryOperator::NotEqual
				| BinaryOperator::AllEqual
				| BinaryOperator::AnyEqual
				| BinaryOperator::Or
				| BinaryOperator::And
				| BinaryOperator::NullCoalescing
				| BinaryOperator::TenaryCondition
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
				| BinaryOperator::Range
				| BinaryOperator::RangeInclusive
				| BinaryOperator::RangeSkip
				| BinaryOperator::RangeSkipInclusive => leaf,
			},
			// The class is that of the builtin the step services, except for the callees whose
			// class the callee kind fixes: `NonLeaf(Bar::Script)` for a custom function, a script,
			// a module and a silo function, `NonLeaf(Bar::Model)` for a model, and
			// `Kernel(&[Storage])` for an index function, which `Instr::CallIndexFn` lowers.
			ExprOp::Call(function) => match &function.function {
				Function::Normal(name) if !registry.is_index_function(name) => {
					NativeLowering::Native {
						leaf_class: LeafClass::ByOperand,
					}
				}
				Function::Normal(..)
				| Function::Custom(..)
				| Function::Script(..)
				| Function::Model(..)
				| Function::Module(..)
				| Function::Silo {
					..
				} => NativeLowering::UnitCall,
			},
			// The class is that of the method the step services.
			ExprOp::Method {
				..
			} => NativeLowering::Native {
				leaf_class: LeafClass::ByOperand,
			},
			// A reading of the statement clock is the call of the clock builtin it was bound
			// from, so its class is that of the builtin.
			ExprOp::Now {
				..
			} => NativeLowering::Native {
				leaf_class: LeafClass::ByOperand,
			},
			// `Kernel(&[Storage, Permissions])`: a fetch of the record a value names, or of a
			// recursion's phase. A `field` is an operand of the row when the rows are objects and
			// no instruction at all; over a row of any value it is a path step, which takes the
			// host, and which row it reads is a fact of the pipeline, not of the op.
			ExprOp::Field {
				..
			}
			| ExprOp::Access {
				..
			}
			| ExprOp::Each
			| ExprOp::Deref {
				..
			}
			| ExprOp::Destructure {
				..
			}
			| ExprOp::ComputedRead(..)
			| ExprOp::RepeatRecurse => NativeLowering::UnitCall,
			// `Kernel(&[Params, Catalog])`: a parameter read, which may reach a stored definition.
			ExprOp::FreeParam {
				..
			}
			| ExprOp::CatalogParam {
				..
			}
			| ExprOp::SessionParam {
				..
			} => NativeLowering::UnitCall,
			// `Kernel(&[Link])`: linking the unit of a closure, or applying one.
			ExprOp::ClosureCreate(..) | ExprOp::Invoke => NativeLowering::UnitCall,
			// `Kernel(&[Params])`: a statement run as a plan of its own.
			ExprOp::Subquery(_) => NativeLowering::UnitCall,
			// The rows of an `INSERT`: each value, subscript and filter is a host pipeline of the
			// write's source, which applies the places as it reads the row.
			ExprOp::TupleRow {
				..
			} => NativeLowering::UnitCall,
		}
	}
}

/// The subscript operands the places hold: one per [`PlacePart::Dynamic`] and [`PlacePart::Index`].
fn place_subscripts(places: &[Place]) -> usize {
	places
		.iter()
		.flat_map(|place| place.parts.iter())
		.filter(|part| matches!(part, PlacePart::Dynamic(_) | PlacePart::Index(_)))
		.count()
}

/// The filter regions the places hold: one per [`PlacePart::Filter`].
fn place_filters(places: &[Place]) -> usize {
	places
		.iter()
		.flat_map(|place| place.parts.iter())
		.filter(|part| matches!(part, PlacePart::Filter(_)))
		.count()
}

/// The operand count `n` as the arity rule states it: a count past `u16::MAX` is `u16::MAX`.
fn operand_count(n: usize) -> u16 {
	u16::try_from(n).unwrap_or(u16::MAX)
}

/// An op of exactly `operands` operands and `results` results.
fn exactly(operands: usize, results: u16) -> Arity {
	Arity {
		operands: OperandRule::Exactly(operand_count(operands)),
		results,
	}
}

/// An op of at least `operands` operands and `results` results.
fn at_least(operands: usize, results: u16) -> Arity {
	Arity {
		operands: OperandRule::AtLeast(operand_count(operands)),
		results,
	}
}

/// The operands a record id's key takes: one per element or value, and for a range those of each
/// included or excluded bound.
fn key_operands(shape: &KeyShape) -> usize {
	match shape {
		KeyShape::Number(..)
		| KeyShape::String(..)
		| KeyShape::Uuid(..)
		| KeyShape::Generate(..) => 0,
		KeyShape::Array {
			len,
		} => *len as usize,
		KeyShape::Object {
			keys,
		} => keys.len(),
		KeyShape::Range(range) => {
			bound_operands(&range.start).saturating_add(bound_operands(&range.end))
		}
	}
}

/// The operands of one end of a key range.
fn bound_operands(bound: &Bound<KeyBound>) -> usize {
	match bound {
		Bound::Unbounded => 0,
		Bound::Included(key) | Bound::Excluded(key) => match key {
			KeyBound::Number(..)
			| KeyBound::String(..)
			| KeyBound::Uuid(..)
			| KeyBound::Generate(..) => 0,
			KeyBound::Array {
				len,
			} => *len as usize,
			KeyBound::Object {
				keys,
			} => keys.len(),
		},
	}
}

/// The aliased entries of a destructure, nested destructures included. Each owns one region.
fn aliased_entries(shape: &[DestructureEntry]) -> usize {
	shape
		.iter()
		.map(|entry| match entry {
			DestructureEntry::Aliased(..) => 1,
			DestructureEntry::Destructure(_, nested) => aliased_entries(nested),
			DestructureEntry::All(..) | DestructureEntry::Field(..) | DestructureEntry::Rest => 0,
		})
		.sum()
}

/// The call rule of a function call, by the kind of its callee.
fn call_rule(function: &Function, cx: &EffectCx<'_>) -> Rule {
	match function {
		Function::Normal(name) => {
			let registry = cx.registry;
			if let Some(builtin) = registry.get(name) {
				let mut set = builtin.effects();
				if registry.is_projection(name) {
					set |= EffectSet::READS_ROW;
				}
				if !builtin.closure_args().is_empty() {
					set |= EffectSet::OPAQUE_BODY;
				}
				Rule::call(set, builtin.fallibility())
			} else if let Some(aggregate) = registry.get_aggregate(name) {
				Rule::call(EffectSet::PURE, aggregate.fallibility())
			} else if let Some(index) = registry.get_index_function(name) {
				Rule::call(EffectSet::PURE, index.fallibility())
			} else if let Some(projection) = registry.get_projection(name) {
				Rule::call(EffectSet::READS_ROW, projection.fallibility())
			} else {
				Rule::top()
			}
		}
		Function::Custom(name) => Rule::top_with(smallvec![
			ContextDep::Capabilities,
			ContextDep::FunctionPermission {
				name: Strand::new(name)
			}
		]),
		Function::Script(..)
		| Function::Module(..)
		| Function::Silo {
			..
		} => Rule::top(),
		Function::Model(..) => Rule::call(EffectSet::READS_EXTERNAL, Fallibility::TOP),
	}
}

/// The call rule of a method call: the union over every builtin the name may resolve to. A name
/// that cannot be shown to reach a builtin for an object receiver may run a closure the receiver
/// carries, whose body the call cannot see. A name any of whose builtins runs a closure argument
/// runs a body the call cannot see either, as the call of such a builtin does; a method call that
/// remains in the IR is one whose closure the binder did not hand to a loop.
fn method_rule(name: &str, cx: &EffectCx<'_>) -> Rule {
	let registry = cx.registry;
	match registry.get_method(name) {
		Some(descriptor) => {
			let implementations = || {
				descriptor
					.type_impls
					.iter()
					.map(|(_, function)| function)
					.chain(descriptor.fallback.iter())
			};
			let mut set = registry.method_effects(name);
			if !descriptor.dispatches_for_an_object()
				|| implementations().any(|function| !function.closure_args().is_empty())
			{
				set |= EffectSet::OPAQUE_BODY;
			}
			let declared = implementations()
				.fold(Fallibility::INFALLIBLE, |class, function| class | function.fallibility());
			Rule::call(set, declared)
		}
		None => Rule::call(EffectSet::OPAQUE_BODY, Fallibility::TOP),
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use surrealdb_strand::TableName;

	use super::*;
	use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
	use crate::exec::ir::expr::ops::{
		ClosureAttrs, ComputedReadAttrs, ConvertAttrs, ConvertMode, ConvertSite, DestructureEntry,
		DispatchTarget, ExprTag, FoldedError, KeyRange, NestedStrategy, NowWhich, OnFail,
		RecordIdAttrs, ShortCircuitOp,
	};
	use crate::exec::ir::iface::CatalogObjectKey;
	use crate::exec::ir::iface::catalog::SemanticCatalog;
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::logical::footprint::CatalogKind;
	use crate::exec::logical::pipelines::AggregateCombine;
	use crate::exec::vm::compile_cache::ObjectVersion;
	use crate::exec::vm::payload::Refusal;
	use crate::expr::logical::LogicalPostfixOperator;
	use crate::expr::operator::{BooleanOperator, MatchesOperator, NearestNeighbor};
	use crate::expr::{Constant, Kind, Mock, RecordIdKeyGen};
	use crate::val::{Uuid, Value};

	/// A semantic catalog that holds no definition.
	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	/// What an effect context borrows: a builtin registry and a frame table.
	struct Env {
		registry: FunctionRegistry,
		frames: FrameTable,
	}

	impl Env {
		fn new() -> Self {
			Self::with(FunctionRegistry::with_builtins())
		}

		fn with(registry: FunctionRegistry) -> Self {
			Self {
				registry,
				frames: FrameTable::default(),
			}
		}

		fn cx(&self) -> EffectCx<'_> {
			EffectCx {
				registry: &self.registry,
				catalog: &NoCatalog,
				frames: &self.frames,
			}
		}
	}

	/// A builtin that declares it reads data and cannot fail: the declaration the release
	/// verifier's consistency rule exists to reject.
	#[derive(Debug)]
	struct Liar;

	impl ScalarFunction for Liar {
		fn name(&self) -> &'static str {
			"test::liar"
		}

		fn signature(&self) -> Signature {
			Signature::new()
		}

		fn effects(&self) -> EffectSet {
			EffectSet::READS_DATA
		}

		fn fallibility(&self) -> Fallibility {
			Fallibility::INFALLIBLE
		}
	}

	const NONE: Fallibility = Fallibility::INFALLIBLE;
	const VALUE: Fallibility = Fallibility::VALUE;
	const CONTEXT: Fallibility = Fallibility::CONTEXT;
	const DATA: Fallibility = Fallibility::DATA;

	fn strand(text: &str) -> Strand {
		Strand::new(text)
	}

	fn table(text: &str) -> TableName {
		TableName::new(strand(text))
	}

	fn call(name: &str) -> ExprOp {
		ExprOp::call(Function::Normal(name.to_owned()))
	}

	fn binary(op: BinaryOperator) -> ExprOp {
		ExprOp::Binary {
			op,
		}
	}

	fn prefix(op: PrefixOperator) -> ExprOp {
		ExprOp::Prefix(Box::new(op))
	}

	fn record_id(key_shape: KeyShape) -> ExprOp {
		ExprOp::RecordId(Box::new(RecordIdAttrs {
			table: table("t"),
			key_shape,
		}))
	}

	fn raise(kind: RaiseKind) -> ExprOp {
		ExprOp::Raise(Box::new(kind))
	}

	fn range(start: Bound<KeyBound>, end: Bound<KeyBound>) -> KeyShape {
		KeyShape::Range(Box::new(KeyRange {
			start,
			end,
		}))
	}

	/// The simplest op of each tag.
	fn fixture(tag: ExprTag) -> ExprOp {
		match tag {
			ExprTag::Literal => ExprOp::Literal(Box::new(Value::from(1i64))),
			ExprTag::Array => ExprOp::Array,
			ExprTag::Set => ExprOp::Set,
			ExprTag::Object => ExprOp::Object {
				keys: Box::new([]),
			},
			ExprTag::RecordId => record_id(KeyShape::Number(1)),
			ExprTag::FreeParam => ExprOp::FreeParam {
				name: strand("q"),
			},
			ExprTag::SessionParam => ExprOp::SessionParam {
				name: strand("auth"),
			},
			ExprTag::CatalogParam => ExprOp::CatalogParam {
				name: strand("p"),
			},
			ExprTag::Table => ExprOp::Table {
				name: table("t"),
			},
			ExprTag::Mock => ExprOp::Mock(Box::new(Mock::Count(table("t"), 1))),
			ExprTag::Constant => ExprOp::Constant {
				c: Constant::MathE,
			},
			ExprTag::Field => ExprOp::Field {
				name: strand("a"),
			},
			ExprTag::ComputedRead => ExprOp::ComputedRead(Box::new(ComputedReadAttrs {
				field: strand("a"),
				def: (
					CatalogObjectKey {
						kind: CatalogKind::Field,
						ns: None,
						db: None,
						table: Some(table("t")),
						name: Some(strand("a")),
					},
					ObjectVersion::Inline,
				),
			})),
			ExprTag::Doc => ExprOp::Doc,
			ExprTag::Access => ExprOp::Access {
				field: strand("a"),
			},
			ExprTag::Deref => ExprOp::Deref {
				table_hint: None,
			},
			ExprTag::Index => ExprOp::Index,
			ExprTag::First => ExprOp::First,
			ExprTag::Last => ExprOp::Last,
			ExprTag::Each => ExprOp::Each,
			ExprTag::Flatten => ExprOp::Flatten {
				explicit: true,
			},
			ExprTag::Dispatch => ExprOp::Dispatch(Box::new(DispatchTarget::Method(strand("len")))),
			ExprTag::Method => ExprOp::Method {
				name: strand("len"),
			},
			ExprTag::Destructure => ExprOp::Destructure {
				shape: Box::new([DestructureEntry::Field(strand("a"))]),
			},
			ExprTag::Optional => ExprOp::Optional,
			ExprTag::RepeatRecurse => ExprOp::RepeatRecurse,
			ExprTag::SetField => ExprOp::SetField {
				field: strand("a"),
			},
			ExprTag::TupleRow => ExprOp::TupleRow {
				places: Box::new([Place {
					parts: Box::new([PlacePart::Field(strand("a"))]),
				}]),
			},
			ExprTag::Prefix => prefix(PrefixOperator::Not),
			ExprTag::Convert => ExprOp::Convert(Box::new(ConvertAttrs {
				mode: ConvertMode::Cast,
				kind: Kind::Int,
				site: ConvertSite(0),
				on_fail: OnFail::Error,
			})),
			ExprTag::Postfix => ExprOp::Postfix {
				op: LogicalPostfixOperator::Range,
			},
			ExprTag::Invoke => ExprOp::Invoke,
			ExprTag::Binary => binary(BinaryOperator::Add),
			ExprTag::ShortCircuit => ExprOp::ShortCircuit {
				op: ShortCircuitOp::And,
			},
			ExprTag::Call => call("math::abs"),
			ExprTag::ClosureCreate => ExprOp::ClosureCreate(Box::new(ClosureAttrs {
				params: Box::new([]),
				returns: None,
				body_sql: "1".into(),
				captures: Box::new([]),
				reads_row: false,
				reads_root: false,
				literal: Default::default(),
			})),
			ExprTag::If => ExprOp::If,
			ExprTag::Throw => ExprOp::Throw,
			ExprTag::Subquery => ExprOp::Subquery(NestedStrategy::Apply),
			ExprTag::Yield => ExprOp::Yield,
			ExprTag::Truthy => ExprOp::Truthy,
			ExprTag::Agg => ExprOp::Agg {
				kind: "count",
				combine: AggregateCombine::Exact,
			},
			ExprTag::Now => ExprOp::Now {
				which: NowWhich::Datetime,
			},
			ExprTag::Raise => raise(RaiseKind::InvalidControlFlow),
			ExprTag::DepthCharge => ExprOp::DepthCharge {
				levels: 1,
			},
		}
	}

	fn binary_operators() -> Vec<BinaryOperator> {
		vec![
			BinaryOperator::Subtract,
			BinaryOperator::Add,
			BinaryOperator::Multiply,
			BinaryOperator::Divide,
			BinaryOperator::Remainder,
			BinaryOperator::Power,
			BinaryOperator::Equal,
			BinaryOperator::ExactEqual,
			BinaryOperator::NotEqual,
			BinaryOperator::AllEqual,
			BinaryOperator::AnyEqual,
			BinaryOperator::Or,
			BinaryOperator::And,
			BinaryOperator::NullCoalescing,
			BinaryOperator::TenaryCondition,
			BinaryOperator::LessThan,
			BinaryOperator::LessThanEqual,
			BinaryOperator::MoreThan,
			BinaryOperator::MoreThanEqual,
			BinaryOperator::Contain,
			BinaryOperator::NotContain,
			BinaryOperator::ContainAll,
			BinaryOperator::ContainAny,
			BinaryOperator::ContainNone,
			BinaryOperator::Inside,
			BinaryOperator::NotInside,
			BinaryOperator::AllInside,
			BinaryOperator::AnyInside,
			BinaryOperator::NoneInside,
			BinaryOperator::Outside,
			BinaryOperator::Intersects,
			BinaryOperator::Range,
			BinaryOperator::RangeInclusive,
			BinaryOperator::RangeSkip,
			BinaryOperator::RangeSkipInclusive,
			BinaryOperator::Matches(MatchesOperator {
				rf: None,
				operator: BooleanOperator::And,
			}),
			BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::KTree(1))),
		]
	}

	/// The class the design gives a binary operator.
	fn binary_class(op: &BinaryOperator) -> Fallibility {
		match op {
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
			| BinaryOperator::Or
			| BinaryOperator::And
			| BinaryOperator::NullCoalescing
			| BinaryOperator::TenaryCondition => NONE,
			BinaryOperator::Add
			| BinaryOperator::Subtract
			| BinaryOperator::Multiply
			| BinaryOperator::Divide
			| BinaryOperator::Remainder
			| BinaryOperator::Power
			| BinaryOperator::Range
			| BinaryOperator::RangeInclusive
			| BinaryOperator::RangeSkip
			| BinaryOperator::RangeSkipInclusive => VALUE,
			BinaryOperator::Matches(..) | BinaryOperator::NearestNeighbor(..) => DATA,
		}
	}

	fn prefix_operators() -> Vec<PrefixOperator> {
		vec![
			PrefixOperator::Not,
			PrefixOperator::Positive,
			PrefixOperator::Negate,
			PrefixOperator::Range,
			PrefixOperator::RangeInclusive,
			PrefixOperator::Cast(Kind::Int),
		]
	}

	fn key_shapes() -> Vec<KeyShape> {
		vec![
			KeyShape::Number(1),
			KeyShape::String(strand("k")),
			KeyShape::Uuid(Uuid::nil()),
			KeyShape::Generate(RecordIdKeyGen::Rand),
			KeyShape::Generate(RecordIdKeyGen::Ulid),
			KeyShape::Generate(RecordIdKeyGen::Uuid),
			KeyShape::Array {
				len: 2,
			},
			KeyShape::Object {
				keys: Box::new(["a".to_owned()]),
			},
			range(Bound::Included(KeyBound::Number(1)), Bound::Unbounded),
			range(Bound::Unbounded, Bound::Excluded(KeyBound::String(strand("z")))),
			range(
				Bound::Included(KeyBound::Number(1)),
				Bound::Included(KeyBound::Generate(RecordIdKeyGen::Rand)),
			),
		]
	}

	fn raise_kinds() -> Vec<RaiseKind> {
		vec![
			RaiseKind::Folded(FoldedError {
				text: "x".to_owned(),
			}),
			RaiseKind::InvalidControlFlow,
			RaiseKind::Misplaced(MisplacedControl::NestedBegin),
			RaiseKind::Misplaced(MisplacedControl::BeginInSession),
			RaiseKind::Misplaced(MisplacedControl::CommitOutsideBlock),
			RaiseKind::Misplaced(MisplacedControl::CancelOutsideBlock),
			RaiseKind::Refused(Box::new(Refusal::Query {
				message: "m".to_owned(),
			})),
		]
	}

	/// Whether the op is answered by the call rule.
	fn is_call_rule(op: &ExprOp) -> bool {
		match op.tag() {
			ExprTag::Call | ExprTag::Method | ExprTag::Agg | ExprTag::Invoke => true,
			ExprTag::Literal
			| ExprTag::Array
			| ExprTag::Set
			| ExprTag::Object
			| ExprTag::RecordId
			| ExprTag::FreeParam
			| ExprTag::SessionParam
			| ExprTag::CatalogParam
			| ExprTag::Table
			| ExprTag::Mock
			| ExprTag::Constant
			| ExprTag::Field
			| ExprTag::ComputedRead
			| ExprTag::Doc
			| ExprTag::Access
			| ExprTag::Deref
			| ExprTag::Index
			| ExprTag::First
			| ExprTag::Last
			| ExprTag::Each
			| ExprTag::Flatten
			| ExprTag::Dispatch
			| ExprTag::Destructure
			| ExprTag::Optional
			| ExprTag::RepeatRecurse
			| ExprTag::SetField
			| ExprTag::TupleRow
			| ExprTag::Prefix
			| ExprTag::Convert
			| ExprTag::Postfix
			| ExprTag::Binary
			| ExprTag::ShortCircuit
			| ExprTag::ClosureCreate
			| ExprTag::If
			| ExprTag::Throw
			| ExprTag::Subquery
			| ExprTag::Yield
			| ExprTag::Truthy
			| ExprTag::Now
			| ExprTag::Raise
			| ExprTag::DepthCharge => false,
		}
	}

	/// An op of every tag, and of every variant of the payloads the answer reads.
	fn dialect_ops() -> Vec<ExprOp> {
		let mut ops: Vec<ExprOp> = ExprTag::ALL.iter().map(|tag| fixture(*tag)).collect();
		ops.extend(binary_operators().into_iter().map(binary));
		ops.extend(prefix_operators().into_iter().map(prefix));
		ops.extend(key_shapes().into_iter().map(record_id));
		ops.extend(raise_kinds().into_iter().map(raise));
		for op in [
			ShortCircuitOp::And,
			ShortCircuitOp::Or,
			ShortCircuitOp::NullCoalescing,
			ShortCircuitOp::TernaryCondition,
		] {
			ops.push(ExprOp::ShortCircuit {
				op,
			});
		}
		for which in [
			NowWhich::Datetime,
			NowWhich::UnixSeconds,
			NowWhich::UnixMillis,
			NowWhich::UnixMicros,
			NowWhich::UnixNanos,
		] {
			ops.push(ExprOp::Now {
				which,
			});
		}
		ops
	}

	/// A call, method or aggregate op of every name the registry holds, and one of each callee
	/// kind it does not.
	fn callee_ops(registry: &FunctionRegistry) -> Vec<ExprOp> {
		let mut ops = Vec::new();
		let mut names: Vec<&'static str> = Vec::new();
		names.extend(registry.scalar_functions().map(|f| f.name()));
		names.extend(registry.aggregate_functions().map(|f| f.name()));
		names.extend(registry.index_functions().map(|f| f.name()));
		names.extend(registry.projection_functions().map(|f| f.name()));
		ops.extend(names.into_iter().map(call));
		ops.extend(registry.aggregate_functions().map(|f| ExprOp::Agg {
			kind: f.name(),
			combine: AggregateCombine::Exact,
		}));
		ops.extend(registry.methods().names().map(|name| ExprOp::Method {
			name: strand(name),
		}));
		ops.push(call("no::such::function"));
		ops.push(ExprOp::call(Function::Custom("x".to_owned())));
		ops.push(ExprOp::call(Function::Module("m".to_owned(), None, crate::expr::ExtPrefix::Ext)));
		ops.push(ExprOp::call(Function::Silo {
			org: "o".to_owned(),
			pkg: "p".to_owned(),
			major: 1,
			minor: 0,
			patch: 0,
			sub: None,
		}));
		ops.push(ExprOp::Method {
			name: strand("no_such_method"),
		});
		ops.push(ExprOp::Agg {
			kind: "no::such::aggregate",
			combine: AggregateCombine::Exact,
		});
		ops
	}

	/// The implications the release verifier checks of an answer.
	fn assert_floored(effects: &OpEffects, what: &str) {
		let class = effects.fallibility;
		let set = effects.set;
		if set.contains(EffectSet::READS_DATA) {
			assert!(class.contains(DATA), "{what}: READS_DATA without DATA");
		}
		if set.contains(EffectSet::MAY_REJECT) {
			assert!(class.contains(DATA), "{what}: MAY_REJECT without DATA");
		}
		for flag in [EffectSet::OPAQUE_BODY, EffectSet::READS_EXTERNAL, EffectSet::WRITES_EXTERNAL]
		{
			if set.contains(flag) {
				assert!(class.contains(CONTEXT | DATA), "{what}: {flag:?} without CONTEXT | DATA");
			}
		}
	}

	/// The declared class of the callee a call names, by the call rule's order of lookup.
	fn declared_class(registry: &FunctionRegistry, name: &str) -> Option<Fallibility> {
		registry
			.get(name)
			.map(|f| f.fallibility())
			.or_else(|| registry.get_aggregate(name).map(|f| f.fallibility()))
			.or_else(|| registry.get_index_function(name).map(|f| f.fallibility()))
			.or_else(|| registry.get_projection(name).map(|f| f.fallibility()))
	}

	/// The class a call answers for a declaration and the set it answers.
	fn call_class(declared: Fallibility, set: EffectSet) -> Fallibility {
		let class = declared | VALUE | CONTEXT;
		if set.intersects(
			EffectSet::OPAQUE_BODY | EffectSet::READS_EXTERNAL | EffectSet::WRITES_EXTERNAL,
		) {
			class | DATA
		} else {
			class
		}
	}

	#[test]
	fn local_answers_are_consistent() {
		let env = Env::new();
		let cx = env.cx();
		for op in dialect_ops() {
			let effects = op.local_effects(&cx);
			let what = format!("{:?}", op);
			assert_eq!(op.fallibility(&cx), effects.fallibility, "{what}");
			assert_floored(&effects, &what);
		}
		for op in callee_ops(&env.registry) {
			let effects = op.local_effects(&cx);
			let what = format!("{:?}", op);
			assert_eq!(op.fallibility(&cx), effects.fallibility, "{what}");
			let known = if let ExprOp::Call(function) = &op {
				match &function.function {
					Function::Normal(name) => {
						declared_class(&env.registry, name).map(|declared| (declared, None))
					}
					Function::Custom(..)
					| Function::Script(..)
					| Function::Model(..)
					| Function::Module(..)
					| Function::Silo {
						..
					} => None,
				}
			} else if let ExprOp::Agg {
				kind,
				..
			} = &op
			{
				env.registry.get_aggregate(kind).map(|a| (a.fallibility(), None))
			} else if let ExprOp::Method {
				name,
			} = &op
			{
				env.registry.get_method(name.as_str()).map(|descriptor| {
					let declared = descriptor
						.type_impls
						.iter()
						.map(|(_, function)| function)
						.chain(descriptor.fallback.iter())
						.fold(NONE, |class, function| class | function.fallibility());
					(declared, Some(descriptor.dispatches_for_an_object()))
				})
			} else {
				None
			};
			let Some((declared, dispatches)) = known else {
				continue;
			};
			assert_eq!(effects.fallibility, call_class(declared, effects.set), "{what}");
			assert!(
				effects.context_deps.contains(&ContextDep::Capabilities),
				"{what}: no capability dependency"
			);
			if dispatches == Some(false) {
				assert!(effects.set.contains(EffectSet::OPAQUE_BODY), "{what}");
			}
		}
	}

	#[test]
	fn calls_pass_the_declaration_through() {
		let env = Env::new();
		let cx = env.cx();
		for function in env.registry.scalar_functions() {
			let name = function.name();
			let effects = call(name).local_effects(&cx);
			assert!(effects.set.contains(function.effects()), "{name}");
			assert_eq!(
				effects.fallibility,
				call_class(function.fallibility(), effects.set),
				"{name}"
			);
		}
	}

	#[test]
	fn a_lying_builtin_is_not_floored() {
		let mut registry = FunctionRegistry::with_builtins();
		registry.register(&Liar);
		let env = Env::with(registry);
		let cx = env.cx();
		let op = call("test::liar");
		let effects = op.local_effects(&cx);
		assert!(effects.set.contains(EffectSet::READS_DATA));
		assert_eq!(effects.fallibility, VALUE | CONTEXT);
		assert!(!effects.fallibility.contains(DATA));
		assert_eq!(op.fallibility(&cx), VALUE | CONTEXT);
	}

	#[test]
	fn unconditional_rows_are_exact() {
		let env = Env::new();
		let cx = env.cx();
		let exact = |op: ExprOp, set: EffectSet, class: Fallibility| {
			let effects = op.local_effects(&cx);
			let what = format!("{:?}", op);
			assert_eq!(effects.set, set, "{what}");
			assert_eq!(effects.fallibility, class, "{what}");
			assert_eq!(op.fallibility(&cx), class, "{what}");
		};
		let pure = EffectSet::PURE;

		exact(
			ExprOp::SessionParam {
				name: strand("auth"),
			},
			EffectSet::READS_PRINCIPAL,
			NONE,
		);
		exact(fixture(ExprTag::CatalogParam), EffectSet::READS_CATALOG, CONTEXT | DATA);
		exact(
			fixture(ExprTag::FreeParam),
			EffectSet::READS_CATALOG | EffectSet::READS_PRINCIPAL,
			CONTEXT | DATA,
		);
		exact(fixture(ExprTag::Field), pure, VALUE | CONTEXT | DATA);
		exact(fixture(ExprTag::Deref), EffectSet::READS_DATA | EffectSet::READS_CATALOG, DATA);
		for tag in [
			ExprTag::Index,
			ExprTag::SetField,
			ExprTag::TupleRow,
			ExprTag::RepeatRecurse,
			ExprTag::Postfix,
			ExprTag::Dispatch,
			ExprTag::Convert,
			ExprTag::Throw,
		] {
			exact(fixture(tag), pure, VALUE);
		}
		for op in prefix_operators() {
			let class = match op {
				PrefixOperator::Negate | PrefixOperator::Cast(..) => VALUE,
				PrefixOperator::Not
				| PrefixOperator::Positive
				| PrefixOperator::Range
				| PrefixOperator::RangeInclusive => NONE,
			};
			exact(prefix(op), pure, class);
		}
		for op in binary_operators() {
			let class = binary_class(&op);
			let set = if class == DATA {
				EffectSet::READS_DATA
			} else {
				pure
			};
			exact(binary(op), set, class);
		}
		exact(fixture(ExprTag::Now), EffectSet::CLOCK, NONE);
		exact(raise(RaiseKind::InvalidControlFlow), pure, VALUE);
		exact(
			raise(RaiseKind::Folded(FoldedError {
				text: "x".to_owned(),
			})),
			pure,
			VALUE,
		);
		exact(
			raise(RaiseKind::Refused(Box::new(Refusal::Query {
				message: "m".to_owned(),
			}))),
			pure,
			VALUE,
		);
		for control in [
			MisplacedControl::NestedBegin,
			MisplacedControl::CommitOutsideBlock,
			MisplacedControl::CancelOutsideBlock,
		] {
			exact(raise(RaiseKind::Misplaced(control)), pure, VALUE);
		}
		exact(raise(RaiseKind::Misplaced(MisplacedControl::BeginInSession)), pure, CONTEXT);
		for shape in [
			KeyShape::Number(1),
			KeyShape::String(strand("k")),
			KeyShape::Uuid(Uuid::nil()),
			KeyShape::Array {
				len: 1,
			},
			KeyShape::Object {
				keys: Box::new(["a".to_owned()]),
			},
		] {
			exact(record_id(shape), pure, NONE);
		}
		exact(record_id(KeyShape::Generate(RecordIdKeyGen::Rand)), EffectSet::ENTROPY, VALUE);
		exact(
			record_id(range(Bound::Included(KeyBound::Number(1)), Bound::Unbounded)),
			pure,
			VALUE,
		);
		exact(
			record_id(range(
				Bound::Unbounded,
				Bound::Excluded(KeyBound::Generate(RecordIdKeyGen::Ulid)),
			)),
			EffectSet::ENTROPY,
			VALUE,
		);
		exact(
			fixture(ExprTag::Access),
			EffectSet::READS_DATA | EffectSet::READS_CATALOG | EffectSet::OPAQUE_BODY,
			Fallibility::TOP,
		);
		assert_eq!(fixture(ExprTag::ComputedRead).local_effects(&cx), OpEffects::top());
	}

	#[test]
	fn fetching_ops_read_every_record_and_derive_opaquely() {
		let env = Env::new();
		let cx = env.cx();
		for tag in [ExprTag::Access, ExprTag::Each, ExprTag::Destructure] {
			let effects = fixture(tag).local_effects(&cx);
			assert_eq!(effects.footprint.reads.as_slice(), &[all_data()], "{tag:?}");
			assert!(effects.footprint.writes.is_empty(), "{tag:?}");
			assert_eq!(effects.determinism, Determinism::NonDeterministic, "{tag:?}");
			assert_eq!(effects.rollback, Rollback::Survives, "{tag:?}");
			assert!(effects.set.contains(EffectSet::OPAQUE_BODY), "{tag:?}");
		}
	}

	#[test]
	fn shapes_derive_from_the_set() {
		let effects = from_set(EffectSet::PURE, NONE, SmallVec::new());
		assert_eq!(effects, pure(NONE));

		let effects = from_set(EffectSet::CLOCK, NONE, SmallVec::new());
		assert_eq!(effects.determinism, Determinism::PerStatement);
		assert_eq!(effects.rollback, Rollback::NoEffects);

		let effects = from_set(EffectSet::READS_DATA, DATA, SmallVec::new());
		assert_eq!(effects.determinism, Determinism::WithinStatementUnlessWritten);
		assert_eq!(effects.footprint.reads.as_slice(), &[all_data()]);

		let effects =
			from_set(EffectSet::READS_DATA | EffectSet::READS_CATALOG, DATA, SmallVec::new());
		assert_eq!(effects.footprint.reads.as_slice(), &[all_data(), IrResource::Unknown]);

		let effects = from_set(EffectSet::ENTROPY, VALUE, SmallVec::new());
		assert_eq!(effects.determinism, Determinism::NonDeterministic);

		let effects = from_set(EffectSet::WRITES_DATA, DATA, SmallVec::new());
		assert_eq!(effects.rollback, Rollback::Undone);
		assert_eq!(effects.footprint.writes.as_slice(), &[all_data()]);

		let effects = from_set(EffectSet::CHANGES_CATALOG, DATA, SmallVec::new());
		assert_eq!(effects.rollback, Rollback::Undone);
		assert_eq!(effects.footprint.writes.as_slice(), &[IrResource::Unknown]);

		let effects = from_set(EffectSet::WRITES_DATA | EffectSet::NOTIFY, DATA, SmallVec::new());
		assert_eq!(effects.rollback, Rollback::Survives);

		let effects = from_set(EffectSet::OPAQUE_BODY, Fallibility::TOP, SmallVec::new());
		assert_eq!(effects.rollback, Rollback::Survives);
		assert_eq!(effects.footprint.reads.as_slice(), &[IrResource::Unknown]);
		assert_eq!(effects.footprint.writes.as_slice(), &[IrResource::Unknown]);
	}

	#[test]
	fn floor_adds_the_implied_classes() {
		assert_eq!(floor(EffectSet::PURE, NONE), NONE);
		assert_eq!(floor(EffectSet::READS_DATA, NONE), DATA);
		assert_eq!(floor(EffectSet::MAY_REJECT, VALUE), VALUE | DATA);
		assert_eq!(floor(EffectSet::OPAQUE_BODY, NONE), CONTEXT | DATA);
		assert_eq!(floor(EffectSet::READS_EXTERNAL, NONE), CONTEXT | DATA);
		assert_eq!(floor(EffectSet::WRITES_EXTERNAL, VALUE), VALUE | CONTEXT | DATA);
		assert_eq!(floor(EffectSet::CLOCK, NONE), NONE);
	}

	#[test]
	fn call_rule() {
		let env = Env::new();
		let cx = env.cx();

		let abs = env.registry.get("math::abs").expect("math::abs is a builtin");
		let effects = call("math::abs").local_effects(&cx);
		assert!(effects.fallibility.contains(abs.fallibility() | VALUE | CONTEXT));
		assert!(effects.context_deps.contains(&ContextDep::Capabilities));

		let effects = call("no::such::function").local_effects(&cx);
		assert_eq!(effects, OpEffects::top());

		let effects = ExprOp::call(Function::Custom("x".to_owned())).local_effects(&cx);
		assert_eq!(effects.fallibility, Fallibility::TOP);
		assert_eq!(effects.set, EffectSet::TOP);
		assert!(effects.context_deps.contains(&ContextDep::FunctionPermission {
			name: strand("x")
		}));
		assert!(effects.context_deps.contains(&ContextDep::Capabilities));

		let effects = call("http::get").local_effects(&cx);
		assert!(effects.set.contains(EffectSet::READS_EXTERNAL));
		assert!(effects.fallibility.contains(CONTEXT | DATA));

		let effects = call("array::map").local_effects(&cx);
		assert!(effects.set.contains(EffectSet::OPAQUE_BODY));
		assert!(effects.fallibility.contains(CONTEXT | DATA));

		let effects = ExprOp::Method {
			name: strand("no_such_method"),
		}
		.local_effects(&cx);
		assert!(effects.set.contains(EffectSet::OPAQUE_BODY));
		assert_eq!(effects.fallibility, Fallibility::TOP);

		for name in env.registry.methods().names() {
			let descriptor = env.registry.get_method(name).expect("the name is registered");
			let effects = ExprOp::Method {
				name: strand(name),
			}
			.local_effects(&cx);
			assert!(
				descriptor.dispatches_for_an_object()
					|| effects.set.contains(EffectSet::OPAQUE_BODY),
				"{name}: a method that does not dispatch for an object must hold OPAQUE_BODY"
			);
			let runs_a_closure = descriptor
				.type_impls
				.iter()
				.map(|(_, function)| function)
				.chain(descriptor.fallback.iter())
				.any(|function| !function.closure_args().is_empty());
			assert!(
				!runs_a_closure || effects.set.contains(EffectSet::OPAQUE_BODY),
				"{name}: a method that runs a closure argument must hold OPAQUE_BODY"
			);
		}

		// The generic methods run a closure argument whose body may write, read data or draw
		// entropy, though the builtin declares itself pure.
		for name in ["chain", "expect", "map", "filter"] {
			let effects = ExprOp::Method {
				name: strand(name),
			}
			.local_effects(&cx);
			assert!(effects.set.contains(EffectSet::OPAQUE_BODY), "{name}");
			assert_eq!(effects.determinism, Determinism::NonDeterministic, "{name}");
			assert_eq!(effects.rollback, Rollback::Survives, "{name}");
			assert!(effects.fallibility.contains(CONTEXT | DATA), "{name}");
		}

		let effects = ExprOp::Invoke.local_effects(&cx);
		assert!(effects.set.contains(EffectSet::OPAQUE_BODY));
		assert!(effects.fallibility.contains(CONTEXT | DATA));

		let effects = ExprOp::Agg {
			kind: "no::such::aggregate",
			combine: AggregateCombine::Exact,
		}
		.local_effects(&cx);
		assert_eq!(effects, OpEffects::top());
	}

	#[test]
	fn pure_ops_answer_pure_shapes() {
		let env = Env::new();
		let cx = env.cx();
		for tag in [
			ExprTag::Literal,
			ExprTag::Array,
			ExprTag::Set,
			ExprTag::Object,
			ExprTag::Table,
			ExprTag::Mock,
			ExprTag::Constant,
			ExprTag::Field,
			ExprTag::Doc,
			ExprTag::Index,
			ExprTag::First,
			ExprTag::Last,
			ExprTag::Flatten,
			ExprTag::Dispatch,
			ExprTag::Optional,
			ExprTag::RepeatRecurse,
			ExprTag::SetField,
			ExprTag::TupleRow,
			ExprTag::Prefix,
			ExprTag::Convert,
			ExprTag::Postfix,
			ExprTag::Binary,
			ExprTag::ShortCircuit,
			ExprTag::ClosureCreate,
			ExprTag::If,
			ExprTag::Throw,
			ExprTag::Subquery,
			ExprTag::Yield,
			ExprTag::Truthy,
			ExprTag::Raise,
			ExprTag::DepthCharge,
			ExprTag::RecordId,
		] {
			let effects = fixture(tag).local_effects(&cx);
			assert_eq!(effects, pure(effects.fallibility), "{tag:?}");
			assert!(effects.set.is_pure(), "{tag:?}");
			assert!(effects.footprint.reads.is_empty(), "{tag:?}");
			assert!(effects.footprint.writes.is_empty(), "{tag:?}");
			assert_eq!(effects.rollback, Rollback::NoEffects, "{tag:?}");
			assert_eq!(effects.determinism, Determinism::Deterministic, "{tag:?}");
		}
		assert!(!is_call_rule(&fixture(ExprTag::Field)));
	}

	#[test]
	fn now_is_per_statement() {
		let env = Env::new();
		let cx = env.cx();
		let effects = fixture(ExprTag::Now).local_effects(&cx);
		assert_eq!(effects.set, EffectSet::CLOCK);
		assert_eq!(effects.determinism, Determinism::PerStatement);
		assert_eq!(effects.fallibility, Fallibility::INFALLIBLE);
		assert_eq!(effects.rollback, Rollback::NoEffects);
	}

	fn aliased(name: &str) -> DestructureEntry {
		DestructureEntry::Aliased(strand(name))
	}

	#[test]
	fn structure_table() {
		let regions = |tag: ExprTag| fixture(tag).region_kinds().into_vec();
		for tag in [
			ExprTag::Literal,
			ExprTag::FreeParam,
			ExprTag::SessionParam,
			ExprTag::CatalogParam,
			ExprTag::Table,
			ExprTag::Mock,
			ExprTag::Constant,
		] {
			assert_eq!(fixture(tag).arity(), exactly(0, 1), "{tag:?}");
			assert!(regions(tag).is_empty(), "{tag:?}");
		}
		for tag in [ExprTag::Array, ExprTag::Set] {
			assert_eq!(fixture(tag).arity(), at_least(0, 1), "{tag:?}");
			assert!(regions(tag).is_empty(), "{tag:?}");
		}
		for tag in [
			ExprTag::Field,
			ExprTag::ComputedRead,
			ExprTag::Doc,
			ExprTag::Access,
			ExprTag::Deref,
			ExprTag::First,
			ExprTag::Last,
			ExprTag::Each,
			ExprTag::Flatten,
			ExprTag::RepeatRecurse,
			ExprTag::Prefix,
			ExprTag::Convert,
			ExprTag::Postfix,
			ExprTag::Truthy,
			ExprTag::Dispatch,
			ExprTag::Now,
		] {
			assert_eq!(fixture(tag).arity(), exactly(1, 1), "{tag:?}");
			assert!(regions(tag).is_empty(), "{tag:?}");
		}
		for tag in [ExprTag::Index, ExprTag::SetField, ExprTag::Binary] {
			assert_eq!(fixture(tag).arity(), exactly(2, 1), "{tag:?}");
			assert!(regions(tag).is_empty(), "{tag:?}");
		}
		assert_eq!(fixture(ExprTag::Method).arity(), at_least(2, 1));
		assert_eq!(fixture(ExprTag::Invoke).arity(), at_least(1, 1));
		assert_eq!(fixture(ExprTag::Call).arity(), at_least(0, 1));
		assert!(regions(ExprTag::Method).is_empty());
		assert!(regions(ExprTag::Invoke).is_empty());
		assert!(regions(ExprTag::Call).is_empty());

		assert_eq!(fixture(ExprTag::Optional).arity(), exactly(1, 1));
		assert_eq!(regions(ExprTag::Optional), [RegionKind::LazyOperand]);
		assert_eq!(fixture(ExprTag::ShortCircuit).arity(), exactly(1, 1));
		assert_eq!(regions(ExprTag::ShortCircuit), [RegionKind::LazyOperand]);
		assert_eq!(fixture(ExprTag::ClosureCreate).arity(), at_least(0, 1));
		assert_eq!(regions(ExprTag::ClosureCreate), [RegionKind::ClosureBody]);
		assert_eq!(fixture(ExprTag::If).arity(), exactly(1, 1));
		assert_eq!(regions(ExprTag::If), [RegionKind::ValueIf, RegionKind::ValueIf]);
		assert_eq!(fixture(ExprTag::Subquery).arity(), exactly(0, 1));
		assert_eq!(regions(ExprTag::Subquery), [RegionKind::Subquery]);
		assert_eq!(fixture(ExprTag::Agg).arity(), exactly(0, 1));
		assert_eq!(regions(ExprTag::Agg), [RegionKind::ErrorScope(ErrorScope::ShapeToNone)]);

		assert_eq!(fixture(ExprTag::DepthCharge).arity(), exactly(0, 0));
		assert_eq!(fixture(ExprTag::Throw).arity(), exactly(1, 0));
		assert_eq!(fixture(ExprTag::Raise).arity(), exactly(0, 0));
		assert_eq!(fixture(ExprTag::Yield).arity(), at_least(0, 0));
		for tag in [ExprTag::DepthCharge, ExprTag::Throw, ExprTag::Raise, ExprTag::Yield] {
			assert!(regions(tag).is_empty(), "{tag:?}");
		}

		assert_eq!(fixture(ExprTag::Throw).terminator(), Some(Terminator::Raise));
		assert_eq!(fixture(ExprTag::Raise).terminator(), Some(Terminator::Raise));
		assert_eq!(fixture(ExprTag::Yield).terminator(), Some(Terminator::Yield));
		for op in dialect_ops().into_iter().chain(callee_ops(&FunctionRegistry::with_builtins())) {
			assert_eq!(op.is_terminator(), op.terminator().is_some(), "{:?}", op.tag());
			let terminates = matches!(op.tag(), ExprTag::Throw | ExprTag::Raise | ExprTag::Yield);
			assert_eq!(op.is_terminator(), terminates, "{:?}", op.tag());
		}
	}

	#[test]
	fn operand_counts_follow_the_payload() {
		let keys = |names: &[&str]| -> Box<[String]> {
			names.iter().map(|name| (*name).to_owned()).collect()
		};
		let object = ExprOp::Object {
			keys: keys(&["a", "b", "c"]),
		};
		assert_eq!(object.arity(), exactly(3, 1));

		assert_eq!(record_id(KeyShape::Number(1)).arity(), exactly(0, 1));
		assert_eq!(record_id(KeyShape::String(strand("k"))).arity(), exactly(0, 1));
		assert_eq!(record_id(KeyShape::Uuid(Uuid::nil())).arity(), exactly(0, 1));
		assert_eq!(record_id(KeyShape::Generate(RecordIdKeyGen::Rand)).arity(), exactly(0, 1));
		assert_eq!(
			record_id(KeyShape::Array {
				len: 4
			})
			.arity(),
			exactly(4, 1)
		);
		assert_eq!(
			record_id(KeyShape::Object {
				keys: keys(&["a", "b"])
			})
			.arity(),
			exactly(2, 1)
		);
		let bounded = |start, end| record_id(range(start, end)).arity();
		assert_eq!(bounded(Bound::Unbounded, Bound::Unbounded), exactly(0, 1));
		assert_eq!(
			bounded(
				Bound::Included(KeyBound::Array {
					len: 2
				}),
				Bound::Excluded(KeyBound::Object {
					keys: keys(&["a", "b", "c"])
				}),
			),
			exactly(5, 1)
		);
		assert_eq!(
			bounded(
				Bound::Included(KeyBound::Number(1)),
				Bound::Excluded(KeyBound::Array {
					len: 3
				}),
			),
			exactly(3, 1)
		);
		assert_eq!(
			bounded(
				Bound::Unbounded,
				Bound::Included(KeyBound::Array {
					len: 1
				}),
			),
			exactly(1, 1)
		);

		let shape = |entries: Vec<DestructureEntry>| ExprOp::Destructure {
			shape: entries.into_boxed_slice(),
		};
		let regions = |op: ExprOp| op.region_kinds().into_vec();
		assert!(
			regions(shape(vec![
				DestructureEntry::All(strand("a")),
				DestructureEntry::Field(strand("b")),
				DestructureEntry::Rest,
			]))
			.is_empty()
		);
		assert_eq!(
			regions(shape(vec![aliased("a"), DestructureEntry::Field(strand("b")), aliased("c")])),
			[RegionKind::LazyOperand, RegionKind::LazyOperand]
		);
		let nested = shape(vec![
			aliased("a"),
			DestructureEntry::Destructure(
				strand("o"),
				Box::new([
					aliased("b"),
					DestructureEntry::Destructure(strand("p"), Box::new([aliased("c")])),
					DestructureEntry::Rest,
				]),
			),
			aliased("d"),
		]);
		assert_eq!(regions(nested.clone()), [RegionKind::LazyOperand; 4]);
		assert_eq!(nested.arity(), exactly(1, 1));
	}

	#[test]
	fn an_operand_count_past_u16_never_truncates() {
		let past = usize::from(u16::MAX) + 1;
		assert_eq!(exactly(past, 1).operands, OperandRule::Exactly(u16::MAX));
		assert_eq!(at_least(past, 1).operands, OperandRule::AtLeast(u16::MAX));
		let count = u32::try_from(past).expect("u16::MAX + 1 fits in u32");
		let wide = record_id(KeyShape::Array {
			len: count,
		});
		assert_eq!(wide.arity(), exactly(usize::from(u16::MAX), 1));
		let two_wide = record_id(range(
			Bound::Included(KeyBound::Array {
				len: count,
			}),
			Bound::Included(KeyBound::Array {
				len: count,
			}),
		));
		assert_eq!(two_wide.arity().operands, OperandRule::Exactly(u16::MAX));
		let keys: Box<[String]> = (0..past).map(|n| n.to_string()).collect();
		let object = ExprOp::Object {
			keys,
		};
		assert_eq!(object.arity().operands, OperandRule::Exactly(u16::MAX));
	}

	#[test]
	fn tuple_row_structure_follows_its_places() {
		let place = |parts: Vec<PlacePart>| Place {
			parts: parts.into_boxed_slice(),
		};
		let row = ExprOp::TupleRow {
			places: Box::new([
				place(vec![PlacePart::Field(strand("a")), PlacePart::Dynamic(3)]),
				place(vec![
					PlacePart::Filter(0),
					PlacePart::Field(strand("b")),
					PlacePart::Index(4),
				]),
				place(vec![PlacePart::Field(strand("c")), PlacePart::All, PlacePart::Filter(1)]),
			]),
		};
		// Three values, then the two subscripts, and one filter region per filter part.
		assert_eq!(row.arity(), exactly(5, 1));
		assert_eq!(row.region_kinds().into_vec(), vec![RegionKind::Clause(ClauseRole::Where); 2]);
		assert_eq!(fixture(ExprTag::TupleRow).arity(), exactly(1, 1));
		assert!(fixture(ExprTag::TupleRow).region_kinds().is_empty());
	}

	#[test]
	fn native_lowering_rows() {
		let leaf = NativeLowering::Native {
			leaf_class: LeafClass::Leaf,
		};
		let by_operand = NativeLowering::Native {
			leaf_class: LeafClass::ByOperand,
		};
		let isa = IsaCaps::default();
		let registry = &FunctionRegistry::with_builtins();
		for tag in [
			ExprTag::Literal,
			ExprTag::Array,
			ExprTag::Set,
			ExprTag::Object,
			ExprTag::RecordId,
			ExprTag::Table,
			ExprTag::Mock,
			ExprTag::Constant,
			ExprTag::Doc,
			ExprTag::Index,
			ExprTag::First,
			ExprTag::Last,
			ExprTag::Flatten,
			ExprTag::SetField,
			ExprTag::Prefix,
			ExprTag::Convert,
			ExprTag::Postfix,
			ExprTag::Binary,
			ExprTag::ShortCircuit,
			ExprTag::If,
			ExprTag::Truthy,
			ExprTag::Yield,
			ExprTag::Throw,
			ExprTag::Raise,
			ExprTag::DepthCharge,
			ExprTag::Optional,
			ExprTag::Dispatch,
			ExprTag::Agg,
		] {
			assert_eq!(fixture(tag).lowers_natively(isa, registry), leaf, "{tag:?}");
		}
		for tag in [ExprTag::Call, ExprTag::Method, ExprTag::Now] {
			assert_eq!(fixture(tag).lowers_natively(isa, registry), by_operand, "{tag:?}");
		}
		// A callee kind that fixes the class answers the unit call: an index function, and every
		// callee that is not a builtin.
		for name in ["search::score", "search::highlight", "search::offsets"] {
			assert_eq!(
				call(name).lowers_natively(isa, registry),
				NativeLowering::UnitCall,
				"{name}"
			);
		}
		for function in [
			Function::Custom("f".to_owned()),
			Function::Module("m".to_owned(), None, crate::expr::ExtPrefix::Ext),
		] {
			let what = format!("{function:?}");
			let op = ExprOp::call(function);
			assert_eq!(op.lowers_natively(isa, registry), NativeLowering::UnitCall, "{what}");
		}
		for tag in [
			ExprTag::FreeParam,
			ExprTag::SessionParam,
			ExprTag::CatalogParam,
			ExprTag::ComputedRead,
			ExprTag::Field,
			ExprTag::Access,
			ExprTag::Deref,
			ExprTag::Each,
			ExprTag::Destructure,
			ExprTag::RepeatRecurse,
			ExprTag::ClosureCreate,
			ExprTag::Invoke,
			ExprTag::Subquery,
			ExprTag::TupleRow,
		] {
			assert_eq!(
				fixture(tag).lowers_natively(isa, registry),
				NativeLowering::UnitCall,
				"{tag:?}"
			);
		}
		for op in binary_operators() {
			let expected = if matches!(
				op,
				BinaryOperator::Matches(..) | BinaryOperator::NearestNeighbor(..)
			) {
				NativeLowering::UnitCall
			} else {
				leaf
			};
			assert_eq!(binary(op.clone()).lowers_natively(isa, registry), expected, "{op:?}");
		}
		for shape in key_shapes() {
			assert_eq!(record_id(shape.clone()).lowers_natively(isa, registry), leaf, "{shape:?}");
		}
		for kind in raise_kinds() {
			assert_eq!(raise(kind.clone()).lowers_natively(isa, registry), leaf, "{kind:?}");
		}
		// The answer does not depend on the target.
		for op in dialect_ops() {
			assert_eq!(
				op.lowers_natively(isa, registry),
				op.lowers_natively(IsaCaps(u32::MAX), registry)
			);
		}
	}
}
