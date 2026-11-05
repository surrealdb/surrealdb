//! The registry of per-module registrations: dialect rules, analyses, patterns, passes and
//! e-graph rules.
//!
//! Each module contributes through one `register` function and never mutates another module's
//! entries. The registry is built once per process and is immutable afterwards.

use std::sync::OnceLock;

use super::core::convert::ConversionSet;
use super::core::pattern::PatternSet;
use super::core::print::AttrSyntaxes;
use super::iface::conversion::{ExprDialect, LogicalDialect, Phase, PhysDialect};
use super::iface::egraph::EgraphRuleSet;

/// A pass over a container: a named step that runs in one phase.
pub(crate) trait IrPass: Send + Sync {
	fn name(&self) -> &'static str;
	/// The phase the pass runs in.
	fn phase(&self) -> Phase;
}

/// The registered passes, in the order they run.
#[derive(Default)]
pub(crate) struct PassList(pub(crate) Vec<Box<dyn IrPass>>);

/// The names of the registered analyses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AnalysisTable(pub(crate) Vec<&'static str>);

/// Everything the modules registered.
pub(crate) struct Registry {
	pub(crate) expr_patterns: PatternSet<ExprDialect>,
	pub(crate) logical_patterns: PatternSet<LogicalDialect>,
	pub(crate) phys_patterns: PatternSet<PhysDialect>,
	pub(crate) conversion: ConversionSet,
	pub(crate) egraph_rules: EgraphRuleSet,
	pub(crate) analyses: AnalysisTable,
	pub(crate) passes: PassList,
}

impl Registry {
	/// A registry holding nothing.
	pub(crate) fn empty() -> Self {
		Self {
			expr_patterns: PatternSet::new(),
			logical_patterns: PatternSet::new(),
			phys_patterns: PatternSet::new(),
			conversion: ConversionSet::new(),
			egraph_rules: EgraphRuleSet::new(),
			analyses: AnalysisTable::default(),
			passes: PassList::default(),
		}
	}
}

/// The process-wide registry: every module's registrations, built on first use and shared by every
/// request.
///
/// The modules register in a fixed order, so a rule or pass registered by a later module follows
/// the ones registered before it: the physical dialect's conversion and rewrite patterns, the
/// physical implementation rules, the analyses, the e-graph's logical rules, the passes, the loop
/// passes, the lowering and the caches.
pub(crate) fn build() -> &'static Registry {
	static REGISTRY: OnceLock<Registry> = OnceLock::new();
	REGISTRY.get_or_init(|| {
		let mut r = Registry::empty();
		super::phys::reg::register(&mut r);
		#[cfg(feature = "egraph")]
		super::egraph::rules_phys::register(&mut r);
		super::analysis::reg::register(&mut r);
		#[cfg(feature = "egraph")]
		super::egraph::reg::register(&mut r);
		super::passes::reg::register(&mut r);
		super::passes::loop_reg::register(&mut r);
		crate::exec::vm::ir_lower::reg::register(&mut r);
		super::cache::reg::register(&mut r);
		r
	})
}

/// The attribute syntaxes of the expression, logical and physical dialects.
///
/// Only the IR's text form reads them, through [`super::core::print::print`] and
/// [`super::core::parse::parse`]; planning never does, so the registry does not hold them.
pub(crate) fn attr_syntaxes() -> &'static AttrSyntaxes {
	static SYNTAXES: AttrSyntaxes = AttrSyntaxes([
		Some(&super::expr::print::ExprSyntax),
		Some(&super::logical::print::LogicalSyntax),
		Some(&super::phys::print::PhysSyntax),
	]);
	&SYNTAXES
}

// The registry is built once and shared by every request, so it is `Send` and `Sync`.
const _: () = {
	const fn assert_send_sync<T: Send + Sync>() {}
	assert_send_sync::<Registry>();
};

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn build_answers_one_registry_for_the_process() {
		assert!(std::ptr::eq(build(), build()));
	}

	#[test]
	fn attr_syntaxes_hold_each_dialect_at_its_index() {
		use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
		use crate::exec::ir::op_kind::OpKind;

		let syntaxes = attr_syntaxes();
		assert!(syntaxes.0.iter().all(Option::is_some));
		let mut text = String::new();
		syntaxes.0[0]
			.expect("the expression syntax")
			.print_attrs(&OpKind::Expr(ExprOp::Doc), &mut text);
		assert_eq!(text, ExprTag::Doc.mnemonic());
	}

	#[test]
	fn an_empty_registry_holds_nothing() {
		let registry = Registry::empty();
		assert!(registry.expr_patterns.is_empty());
		assert!(registry.logical_patterns.is_empty());
		assert!(registry.phys_patterns.is_empty());
		assert!(registry.conversion.is_empty());
		assert_eq!(registry.egraph_rules.iter().count(), 0);
		assert!(registry.analyses.0.is_empty());
		assert!(registry.passes.0.is_empty());
	}
}
