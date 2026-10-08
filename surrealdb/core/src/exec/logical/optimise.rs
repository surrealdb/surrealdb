//! The driver over the rewrite rules.
//!
//! [`optimise`] applies every rule at every node of the plan, nested plans
//! included, bottom-up, and repeats until a full pass over the plan changes
//! nothing. A rule that rewrites a node hands back a replacement whose inputs
//! may be new nodes — a predicate now sits beneath a projection it was above —
//! so the driver visits the replacement bottom-up before any rule sees it
//! again; a rule that fires at a node can therefore assume every rule has
//! reached its fixpoint beneath it.
//!
//! There are two kinds of rule, and the driver owns the traversal for both.
//! An [`rules::ExprRule`] rewrites one scalar in place and says whether it
//! changed it; the driver applies it to every scalar of the node before the
//! node rules see it, so such a rule never states which node kinds it
//! applies to and never walks the plan. A [`rules::Rule`] rewrites the node.
//! Because the driver folds a replacement's scalars when it visits it, an
//! expression rule reaches what a node rule produced — the constants an
//! inlined function body brings with it fold like the ones that were
//! written.
//!
//! A rule that rewrites the expression of a `FROM` source — a call inlined to
//! the record id it returns — rewrites the expression alone; the driver then
//! classifies the leaf as lowering would classify the rewritten expression
//! ([`Driver::reclassified`]), so no rule needs to know the source kinds.
//!
//! The rules are strict improvements (see the [`rules`] module), so the
//! iteration terminates. The driver bounds it all the same — a fixed number of
//! passes and a fixed number of rewrites per plan — and asserts in debug
//! builds that neither bound is reached, which the language suite exercises;
//! a release build past a bound keeps the plan as it stands, which is a
//! correct plan.

use super::fold::{aggregates_survive, is_condition, is_from_leaf};
use super::rules::{EXPR_RULES, ExprContext, RULES, RuleContext};
use super::{classify_source, next_alias_index, rewrite_deep};
use crate::expr::logical::{LogicalOp, LogicalSource, SourceRef};

/// The most full passes over the plan a fixpoint may take. A pass that changes
/// nothing ends the iteration; the rules reach their fixpoint in one changing
/// pass on every plan the language suite binds, and the second pass confirms
/// it.
const MAX_PASSES: usize = 8;

/// The most rewrites one plan may take across every pass.
const MAX_REWRITES: usize = 10_000;

/// The fixpoint of the rule set over `op`.
pub(super) fn optimise(op: LogicalOp, cx: &mut RuleContext<'_>) -> LogicalOp {
	cx.next_alias = cx.next_alias.max(next_alias_index(&op));
	let mut driver = Driver {
		cx,
		rewrites: 0,
		exhausted: false,
	};
	let mut op = op;
	let mut passes = 0;
	loop {
		let before = driver.rewrites;
		op = driver.visit(op);
		passes += 1;
		if driver.rewrites == before || driver.exhausted {
			break;
		}
		if passes >= MAX_PASSES {
			debug_assert!(
				false,
				"the rule set did not reach a fixpoint in {MAX_PASSES} passes:\n{op}"
			);
			break;
		}
	}
	op
}

struct Driver<'a, 'cx> {
	cx: &'a mut RuleContext<'cx>,
	rewrites: usize,
	/// Set once [`MAX_REWRITES`] is spent: every node is then returned as it
	/// stands.
	exhausted: bool,
}

impl Driver<'_, '_> {
	/// One bottom-up pass over `op` and every plan nested in it.
	fn visit(&mut self, op: LogicalOp) -> LogicalOp {
		rewrite_deep(op, &mut |node| self.at_node(node))
	}

	/// The rules at one node whose inputs are at their fixpoint. The
	/// expression rules run over the node's own scalars first, then the node
	/// rules the node's kind selects; the first to fire replaces the node,
	/// and the replacement is visited whole before the rules see it again.
	///
	/// Selecting by kind is not only how a rule is spared writing an arm for
	/// every node it ignores — it is where most of the rule set's work goes
	/// unspent, since most rules apply to one or two kinds.
	fn at_node(&mut self, mut node: LogicalOp) -> LogicalOp {
		if self.exhausted {
			return node;
		}
		if self.rewrite_scalars(&mut node) {
			self.rewrites += 1;
		}
		let kind = node.kind();
		for rule in RULES {
			if !rule.kinds.contains(&kind) {
				continue;
			}
			if (rule.apply)(&mut node, self.cx) {
				self.rewrites += 1;
				if self.rewrites > MAX_REWRITES {
					debug_assert!(
						false,
						"the rule set rewrote one plan more than {MAX_REWRITES} times, \
						 last by `{}`:\n{node}",
						rule.name
					);
					self.exhausted = true;
					return node;
				}
				return self.visit(Self::reclassified(node));
			}
		}
		node
	}

	/// The expression rules over every scalar this node evaluates, rewritten
	/// in place. Answers whether any rule changed anything.
	///
	/// These are the rewrites that hold wherever the expression sits and
	/// whatever the plan around it has become — a value replaced by the value
	/// it computes to. A rewrite that turns on the shape beneath the node
	/// does not belong here, because that shape is what the rules change.
	///
	/// A `FROM` leaf is skipped whole: its expression is the clause itself,
	/// and what that names — a table to scan, or the rows themselves — is the
	/// physical planner's to decide from the expression as written.
	fn rewrite_scalars(&mut self, node: &mut LogicalOp) -> bool {
		if is_from_leaf(node) {
			return false;
		}
		let ecx = ExprContext {
			registry: self.cx.registry,
			capabilities: self.cx.capabilities,
			// No field read is inert here, which is what withholds the two
			// folds that discard an unevaluated operand. Whether discarding
			// one is sound is read off the node beneath, and the rules move
			// and remove those nodes — `identity` drops a
			// `PermissionFilter[true]`, whose presence is the only record
			// that a table's permission was conditional. Those folds run
			// once before the rules, where that reading still holds; see
			// [`super::fold::fold_constants`].
			restricted: None,
			aggregates_survive: aggregates_survive(node),
			condition: self.cx.folds_clock && is_condition(node),
		};
		let mut changed = false;
		let Ok(()) = node.visit_scalars_mut(&mut |scalar| {
			for (_, rule) in EXPR_RULES {
				if rule(scalar, &ecx) {
					changed = true;
				}
			}
			Ok::<_, std::convert::Infallible>(())
		});
		changed
	}

	/// A rewritten node, with a `FROM` source whose expression a rule rewrote
	/// classified as lowering would classify that expression: a record id is
	/// a record read, a table name a table read, a value literal a `Values`
	/// leaf, and anything else stays a source evaluated at run time. The
	/// resolve pass has already run, so a leaf it did not see has no resolved
	/// schema and its scan enforces at run time, as the source it replaces
	/// did. Every other node is returned as it came.
	#[expect(clippy::wildcard_enum_match_arm, reason = "only a `FROM` leaf is reclassified")]
	fn reclassified(node: LogicalOp) -> LogicalOp {
		match node {
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Dynamic(expr),
				schema: _,
				with,
				for_update,
			}) => classify_source(expr, None, with, for_update),
			other => other,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::super::functions::{Functions, InlineBody};
	use super::super::lower_test as lower;
	use super::super::rules::test_support::{rewritten, rewritten_with};
	use crate::expr::logical::{LogicalOp, LogicalSource, SourceRef, lower_expr};

	/// The leaf beneath every single-input node of `op`.
	fn leaf(op: &LogicalOp) -> &LogicalOp {
		let mut current = op;
		while let [child] = current.children() {
			current = child;
		}
		current
	}

	#[test]
	fn a_rewritten_source_expression_is_classified_as_lowering_would() {
		// A call in `FROM` inlined to a record id is a record read, keeping
		// its lock; one inlined to a value literal is a `Values` leaf.
		let body = |text: &str| InlineBody {
			args: Vec::new(),
			returns: None,
			expr: lower_expr(crate::syn::expr(text).expect("expression parses"))
				.expect("expression lowers"),
		};
		let functions = Functions::from_bodies([
			("rid".to_owned(), body("item:one")),
			("rows".to_owned(), body("[{ a: 1 }]")),
		]);
		let plan = rewritten_with(lower("SELECT * FROM fn::rid() LIMIT 0 FOR UPDATE"), &functions);
		assert!(
			matches!(
				leaf(&plan),
				LogicalOp::Source(LogicalSource {
					source: SourceRef::RecordId(_),
					schema: None,
					for_update: true,
					..
				})
			),
			"{plan}"
		);
		let plan = rewritten_with(lower("SELECT * FROM fn::rows()"), &functions);
		assert!(matches!(leaf(&plan), LogicalOp::Values(_)), "{plan}");
	}

	#[test]
	fn a_plain_statement_is_unchanged() {
		for sql in [
			"SELECT * FROM t",
			"SELECT * FROM t WHERE a > 1",
			"SELECT a, count() FROM t WHERE b GROUP BY a ORDER BY a LIMIT 1",
		] {
			let before = lower(sql);
			let after = rewritten(before.clone());
			assert_eq!(after, before, "`{sql}` should be left alone");
		}
	}

	#[test]
	fn the_fixpoint_is_reached_across_nested_plans() {
		// Three predicates in three nested plans merge into one: each push
		// creates the adjacency the next merge needs, so the replacement has
		// to be visited again before the pass ends.
		let plan = rewritten(lower(
			"SELECT * FROM (SELECT * FROM (SELECT * FROM t WHERE a) WHERE b) WHERE c",
		));
		assert!(plan.to_string().contains("Filter[a AND b AND c]"), "{plan}");
	}

	#[test]
	fn a_second_pass_changes_nothing() {
		let once = rewritten(lower(
			"SELECT a FROM (SELECT * FROM (SELECT * FROM t WHERE a) WHERE b) WHERE c AND a IN (SELECT VALUE x FROM u WHERE y)",
		));
		let twice = rewritten(once.clone());
		assert_eq!(once, twice, "{once}");
	}
}
