//! The guards of the e-graph's rules: the side conditions a rule's application is conditional on.
//!
//! A guard reads the analyses (effects, fallibility, multiplicity, type knowledge) and never the
//! data. A guard that cannot decide refuses the rule.
//!
//! The plain predicates ([`infallible`], [`pure`], [`deterministic`], [`footprint_disjoint`])
//! take the analyses' own types as input, so code outside the e-graph uses the same definition
//! a rule uses. [`holds`] evaluates a rule's [`SideCondition`] over the facts of its match, and
//! [`guarded_union`] is the only way a rule or a seed joins two classes.

#[cfg(feature = "egraph")]
use egg::{EGraph, Id};
#[cfg(feature = "egraph")]
use surrealdb_strand::TableName;

#[cfg(feature = "egraph")]
use super::analysis::{IrAnalysis, reads_written};
#[cfg(feature = "egraph")]
use super::decl::{ClassFacts, EgraphView, Match, NodeShape};
#[cfg(feature = "egraph")]
use super::lang::ENode;
use crate::exec::ir::iface::effects::Determinism;
#[cfg(feature = "egraph")]
use crate::exec::ir::iface::effects::{FieldSet, IrFootprint};
#[cfg(feature = "egraph")]
use crate::exec::ir::iface::egraph::{KindClass, SideCondition};
use crate::exec::ir::iface::fallibility::Fallibility;
#[cfg(feature = "egraph")]
use crate::exec::ir::iface::types::TypeKnowledge;
#[cfg(feature = "egraph")]
use crate::expr::Kind;
use crate::expr::logical::EffectSet;

/// Whether `f` holds no failure dependency.
pub(crate) fn infallible(f: Fallibility) -> bool {
	f.is_infallible()
}

/// Whether `e` is a function of the row's own fields and literals only: a subset of
/// [`EffectSet::READS_ROW`]. A clock read is not pure.
pub(crate) fn pure(e: EffectSet) -> bool {
	EffectSet::READS_ROW.contains(e)
}

/// Whether two evaluations of an op of class `d` are one value: always for a deterministic op and
/// a statement-timestamp read, and for a read of stored data only where no write of the statement
/// reaches it (`reads_written` is false).
pub(crate) fn deterministic(d: Determinism, reads_written: bool) -> bool {
	match d {
		Determinism::Deterministic | Determinism::PerStatement => true,
		Determinism::WithinStatementUnlessWritten => !reads_written,
		Determinism::NonDeterministic => false,
	}
}

/// Whether neither `fp`, nor the fields `row_fields` names of the row source it reads, meets a
/// write of `writes`.
#[cfg(feature = "egraph")]
pub(crate) fn footprint_disjoint(
	fp: &IrFootprint,
	row_fields: Option<(&TableName, &FieldSet)>,
	writes: &IrFootprint,
) -> bool {
	!reads_written(fp, row_fields, writes)
}

/// The facts of the class the match binds `var` to, or `None` for a variable it does not bind.
#[cfg(feature = "egraph")]
fn facts_of<'v>(m: &Match, view: &'v dyn EgraphView, var: &str) -> Option<&'v ClassFacts> {
	m.vars.iter().find(|(name, _)| *name == var).map(|(_, class)| view.facts(*class))
}

/// Whether `cond` holds over the facts of the classes `m` binds.
///
/// A variable the match does not bind makes its condition false, and so does a fact the view
/// cannot answer: a guard that cannot decide refuses.
#[cfg(feature = "egraph")]
pub(crate) fn holds(cond: &SideCondition, m: &Match, view: &dyn EgraphView) -> bool {
	let facts = |var: &str| facts_of(m, view, var);
	match *cond {
		SideCondition::Infallible(v) => facts(v).is_some_and(|f| infallible(f.fallibility)),
		SideCondition::FallibilityAtMost(v, class) => {
			facts(v).is_some_and(|f| f.fallibility.class() <= class)
		}
		SideCondition::Pure(v) => facts(v).is_some_and(|f| pure(f.effects)),
		SideCondition::Deterministic(v) => {
			facts(v).is_some_and(|f| deterministic(f.determinism, f.reads_written))
		}
		SideCondition::SameFrame(a, b) => match (facts(a), facts(b)) {
			(Some(fa), Some(fb)) => fa.frame.is_some() && fa.frame == fb.frame,
			_ => false,
		},
		SideCondition::NotPermissionConjunct(v) => facts(v).is_some_and(|f| !f.in_permission_frame),
		SideCondition::FootprintDisjointFromStatementWrites(v) => {
			facts(v).is_some_and(|f| !f.reads_written)
		}
		SideCondition::NoWriteBetween(a, b) => match (facts(a), facts(b)) {
			(Some(fa), Some(fb)) => !fa.reads_written && !fb.reads_written,
			_ => false,
		},
		// Registration requires the variable to be the direct kid of an `expr.truthy` node, which
		// proves the condition by the pattern's shape.
		SideCondition::ConsumedAsTruthy(_) => true,
		SideCondition::TypeFact(v, class) => facts(v).is_some_and(
			|f| matches!(&f.ty, TypeKnowledge::Fact { kind, .. } if kind_admits(class, kind)),
		),
		// A requirement names a variable, never a table, so no per-table fact of the view can be
		// looked up for it.
		SideCondition::PhysicalFact(_) => false,
	}
}

/// Whether a value of `kind` is in `class`. `Any` admits no kind: a `TypeFact(?x, Any)` proves
/// nothing a rule could use.
#[cfg(feature = "egraph")]
pub(crate) fn kind_admits(class: KindClass, kind: &Kind) -> bool {
	match kind {
		Kind::Bool => class == KindClass::Bool,
		Kind::Int => matches!(class, KindClass::Int | KindClass::Number),
		Kind::Float
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Number
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32 => class == KindClass::Number,
		Kind::String => class == KindClass::String,
		Kind::Object => class == KindClass::Object,
		Kind::Array(..) => class == KindClass::Array,
		Kind::Record(_) => class == KindClass::Record,
		Kind::Any
		| Kind::Named(_)
		| Kind::None
		| Kind::Null
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Duration
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Either(_)
		| Kind::Set(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_) => false,
		Kind::Enum(_) => class == KindClass::String,
	}
}

/// Why two classes are being joined, and what the guard that checks the join needs to know.
#[cfg(feature = "egraph")]
#[derive(Clone, Copy, Debug)]
pub(crate) enum UnionWhy {
	/// A rule joins the class of its right side to the root of its match.
	Rule {
		root: Id,
	},
	/// A seed joins a logged replacement. The caller states whether the replaced value belongs to
	/// the incumbent or to a candidate copy (`from_known`) and whether the new value is a
	/// physical value of that same copy (`to_phys_of_same_copy`).
	Seed {
		from_known: bool,
		to_phys_of_same_copy: bool,
	},
}

/// The guard that refuses the join of the distinct canonical classes `a` and `b`, if one does.
#[cfg(feature = "egraph")]
fn refusal(eg: &EGraph<ENode, IrAnalysis>, a: Id, b: Id, why: UnionWhy) -> Option<&'static str> {
	// U1: one frame and one snapshot, both known.
	let (fa, fb) = (&eg[a].data, &eg[b].data);
	let same = |x: &ClassFacts, y: &ClassFacts| {
		x.frame.is_some() && x.frame == y.frame && x.snapshot.is_some() && x.snapshot == y.snapshot
	};
	if !same(fa, fb) {
		return Some("U1");
	}

	// U2: two opaque leaves are equal to nothing but themselves.
	let leaves = |class: Id| {
		eg[class]
			.nodes
			.iter()
			.filter_map(|node| match node.id.shape {
				NodeShape::Opaque {
					leaf,
				} => Some(leaf),
				NodeShape::Op {
					..
				}
				| NodeShape::Region {
					..
				}
				| NodeShape::Arg {
					..
				} => None,
			})
			.collect::<Vec<_>>()
	};
	let (la, lb) = (leaves(a), leaves(b));
	if la.iter().any(|x| lb.iter().any(|y| x != y)) {
		return Some("U2");
	}

	match why {
		// U3: the join never makes a class depend on a binder the users of the matched root
		// cannot see.
		UnionWhy::Rule {
			root,
		} => {
			let visible = &eg[root].data.free_binders;
			let sees = |facts: &ClassFacts| {
				facts.free_binders.iter().all(|binder| visible.binary_search(binder).is_ok())
			};
			if !sees(fa) || !sees(fb) {
				return Some("U3");
			}
		}
		// U4: a seed replaces a value of the incumbent or of a candidate copy by a physical
		// value of the same copy.
		UnionWhy::Seed {
			from_known,
			to_phys_of_same_copy,
		} => {
			if !from_known || !to_phys_of_same_copy {
				return Some("U4");
			}
		}
	}
	None
}

/// Joins the classes of `a` and `b` unless a guard (U1 to U4) refuses, and answers whether two
/// distinct classes were joined.
///
/// A pair already in one class answers `false` and is not a refusal. A refusal counts into
/// [`IrAnalysis::union_refused`] and answers `false`; the rule that asked keeps its match
/// unapplied. A refused seed is a defect of the seeding, so it also fails a debug assertion.
#[cfg(feature = "egraph")]
pub(crate) fn guarded_union(
	eg: &mut EGraph<ENode, IrAnalysis>,
	a: Id,
	b: Id,
	why: UnionWhy,
) -> bool {
	let (a, b) = (eg.find(a), eg.find(b));
	if a == b {
		return false;
	}
	match refusal(eg, a, b, why) {
		Some(guard) => {
			eg.analysis.union_refused = eg.analysis.union_refused.saturating_add(1);
			debug_assert!(
				matches!(why, UnionWhy::Rule { .. }),
				"a seed's union was refused by guard {guard}"
			);
			false
		}
		None => eg.union(a, b),
	}
}
