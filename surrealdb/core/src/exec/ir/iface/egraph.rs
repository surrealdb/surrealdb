//! The e-graph boundary: what a caller needs to register a rule and to bound saturation.
//!
//! The boundary is this crate's own and names no `egg` type; the node language, the identity
//! encoding of a node, the rule-pattern syntax and the cost hook stay behind it. Saturation stops
//! at the first limit, the best extraction so far is used and is always valid, and the stop is
//! reported, never an error.

use web_time::Duration;

use super::fallibility::FallibilityClass;
use crate::exec::ir::error::IrError;
use crate::exec::ir::op_kind::OpTag;

/// The dialect a rule rewrites in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RuleDialect {
	/// Adds logical e-nodes; never matches or produces a physical node.
	LogicalRewrite,
	/// Adds physical e-nodes to an e-class holding logical ones; never produces a logical node.
	Implementation,
}

/// A condition on a rule's pattern variable that must hold for the rule to apply.
///
/// Each names its variables as they appear in the rule, `?var`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SideCondition {
	Infallible(&'static str),
	FallibilityAtMost(&'static str, FallibilityClass),
	Pure(&'static str),
	Deterministic(&'static str),
	SameFrame(&'static str, &'static str),
	NotPermissionConjunct(&'static str),
	FootprintDisjointFromStatementWrites(&'static str),
	NoWriteBetween(&'static str, &'static str),
	ConsumedAsTruthy(&'static str),
	TypeFact(&'static str, KindClass),
	/// A fact about the physical catalog; admitted by implementation rules only.
	PhysicalFact(PhysFactReq),
}

/// The class of kind a type-fact condition requires.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum KindClass {
	Bool,
	Int,
	Number,
	String,
	Object,
	Array,
	Record,
	Any,
}

/// A fact about the physical catalog an implementation rule requires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PhysFactReq {
	IndexOn(&'static str),
	UniqueKey(&'static str),
	OrderedBy(&'static str),
}

/// The bounds of one saturation. It stops at the first limit reached.
///
/// `nodes` and `iterations` count work, so they stop a run at the same point under any load, and
/// they are the bounds that decide a plan. `wall` is a safety stop set well above the time a
/// saturation within them takes, so a run that reaches it is one the work bounds failed to stop,
/// and the statement keeps its unrewritten plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EgraphLimits {
	pub(crate) nodes: usize,
	pub(crate) iterations: usize,
	/// Measured with `web_time::Instant` by the runner's hook.
	pub(crate) wall: Duration,
}

/// The default bounds of one saturation.
pub(crate) const EGRAPH_DEFAULT: EgraphLimits = EgraphLimits {
	nodes: 10_000,
	iterations: 8,
	wall: Duration::from_millis(10),
};

/// A declarative rewrite rule: printable data, never code that inspects a container.
pub(crate) trait EgraphRule: Send + Sync {
	fn name(&self) -> &'static str;
	fn dialect(&self) -> RuleDialect;
	/// The op tags the rule's patterns mention.
	fn tags(&self) -> &'static [OpTag];
	fn side_conditions(&self) -> &[SideCondition];
	/// Why the rule is sound; required when it has no side condition.
	fn soundness(&self) -> Option<&'static str>;
}

/// The registered rules.
///
/// Registration rejects a [`RuleDialect::LogicalRewrite`] rule that mentions a physical tag or
/// carries a physical fact, an [`RuleDialect::Implementation`] rule that mentions no physical tag
/// (its right side is rooted at one), a rule with neither a side condition nor a stated soundness
/// argument, and a name that is already registered. A rejected rule leaves the set as it was.
#[derive(Default)]
pub(crate) struct EgraphRuleSet(Vec<Box<dyn EgraphRule>>);

impl EgraphRuleSet {
	/// An empty set.
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// The registered rules, in registration order.
	pub(crate) fn iter(&self) -> impl Iterator<Item = &dyn EgraphRule> {
		self.0.iter().map(|rule| &**rule)
	}

	/// Registers `rule` after checking the registration rules, or answers why it is rejected with
	/// [`IrError::Invariant`].
	pub(crate) fn register(&mut self, rule: Box<dyn EgraphRule>) -> Result<(), IrError> {
		let physical = rule.tags().iter().any(|tag| matches!(tag, OpTag::Phys(_)));
		let physical_fact = rule
			.side_conditions()
			.iter()
			.any(|condition| matches!(condition, SideCondition::PhysicalFact(_)));
		let reason = match rule.dialect() {
			RuleDialect::LogicalRewrite if physical => {
				Some("a logical rewrite rule mentions a physical tag")
			}
			RuleDialect::LogicalRewrite if physical_fact => {
				Some("a logical rewrite rule requires a physical fact")
			}
			RuleDialect::Implementation if !physical => {
				Some("an implementation rule mentions no physical tag")
			}
			RuleDialect::LogicalRewrite | RuleDialect::Implementation => None,
		};
		if let Some(what) = reason {
			return Err(IrError::Invariant {
				what,
			});
		}
		if rule.side_conditions().is_empty() && rule.soundness().is_none() {
			return Err(IrError::Invariant {
				what: "a rule has neither a side condition nor a soundness argument",
			});
		}
		if self.0.iter().any(|held| held.name() == rule.name()) {
			return Err(IrError::Invariant {
				what: "a rule name is registered twice",
			});
		}
		self.0.push(rule);
		Ok(())
	}
}
