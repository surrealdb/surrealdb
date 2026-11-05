//! The e-graph lane's registration: its logical rewrite rules.

use super::decl::RuleDecl;
use super::rule::{DeclRule, registered};
use super::rules_logical::{REGISTERED, tags_of};
use crate::exec::ir::iface::egraph::EgraphRuleSet;
use crate::exec::ir::registry::Registry;

/// Checks the rules the planner runs, then offers each rule of [`REGISTERED`] to the registry's
/// e-graph rule set.
///
/// The planner's rule set is [`registered`], which runs the registration checks over every
/// declaration and panics on a rejection. It runs first, so a bad rule fails at process start
/// before the registry's set sees it.
///
/// Panics when a rule has no tag list or the registry's set rejects a rule.
pub(crate) fn register(r: &mut Registry) {
	registered();
	offer(&mut r.egraph_rules, REGISTERED);
}

/// Offers each of `decls` to `set`.
///
/// Panics when a declaration has no tag list or `set` rejects it. A declaration the planner's
/// rule set accepts passes every registration check that `set` makes.
pub(crate) fn offer(set: &mut EgraphRuleSet, decls: &[&'static RuleDecl]) {
	for decl in decls {
		let Some(tags) = tags_of(decl) else {
			panic!("e-graph rule `{}` has no tag list", decl.name);
		};
		if let Err(err) = set.register(Box::new(DeclRule::new(decl, tags))) {
			panic!("e-graph rule `{}` was rejected: {err}", decl.name);
		}
	}
}
