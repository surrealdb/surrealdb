//! The e-graph's implementation rules: equalities that choose between physical methods.
//!
//! No implementation rule is registered. [`REGISTERED`] is empty, so [`rules`] returns nothing and
//! [`register`] adds nothing to the rule set. Physical seeds come from the conversion, whose sort
//! and limit patterns already produce the top-k sort. A registered implementation rule would
//! pre-match the statements it names and make them seeded, so a rule joins the planner only when
//! its name is listed in [`REGISTERED`].
//!
//! [`catalogue`] lists every designed rule. A catalogue rule is declarative data: the op tags its
//! patterns mention, the side conditions that make it sound and, when it has none, the statement
//! of why it is sound. Every right side is rooted at a physical tag and no rule produces a logical
//! node. Each entry carries a [`Withheld`] giving the reason it is not registered and the requests
//! or guards that admit it. A rule whose swap changes the order of tied rows, reads an index in
//! place of a table, or produces `phys.prefetch` stays withheld until what admits it holds.

use crate::exec::ir::iface::egraph::{EgraphRule, PhysFactReq, RuleDialect, SideCondition};
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::OpTag;
use crate::exec::ir::phys::ops::PhysTag;
use crate::exec::ir::registry::Registry;

/// Why a catalogue rule does not register, and what would admit it.
#[derive(Clone, Copy, Debug)]
struct Withheld {
	/// The statement of why the rule is not registered.
	reason: &'static str,
	/// The requests and guards that must hold before the rule may register.
	admitted_by: &'static [&'static str],
}

/// An implementation rule: the tags its patterns mention, with the physical root of its right
/// side last, and the justification of its soundness.
#[derive(Clone, Copy, Debug)]
struct ImplRule {
	name: &'static str,
	tags: &'static [OpTag],
	conditions: &'static [SideCondition],
	soundness: Option<&'static str>,
	/// `Some` while the rule is not offered to the rule set.
	withheld: Option<Withheld>,
}

impl EgraphRule for ImplRule {
	fn name(&self) -> &'static str {
		self.name
	}

	fn dialect(&self) -> RuleDialect {
		RuleDialect::Implementation
	}

	fn tags(&self) -> &'static [OpTag] {
		self.tags
	}

	fn side_conditions(&self) -> &[SideCondition] {
		self.conditions
	}

	fn soundness(&self) -> Option<&'static str> {
		self.soundness
	}
}

/// Every designed implementation rule, in registration order.
const CATALOGUE: [ImplRule; 7] = [
	// A filter over a source read through an index on the filtered field. The conjuncts the index
	// does not serve stay in a residual physical filter above the scan; evaluating them in a
	// different order than written is sound only when none of them can fail. The index scan
	// returns rows in index order, so the swap is sound only where the statement's keys are total.
	ImplRule {
		name: "filter_scan_to_index_scan",
		tags: &[
			OpTag::Logical(LogTag::Filter),
			OpTag::Logical(LogTag::Source),
			OpTag::Phys(PhysTag::IndexScan),
			OpTag::Phys(PhysTag::Filter),
		],
		conditions: &[
			SideCondition::PhysicalFact(PhysFactReq::IndexOn("?field")),
			SideCondition::Infallible("?pred"),
		],
		soundness: None,
		withheld: Some(Withheld {
			reason: "the swap changes the order of tied rows and the condition set has no total-keys condition",
			admitted_by: &["R-L4-20"],
		}),
	},
	// An equality on a field that carries a unique key reads at most one record by key, so no two
	// rows tie. The lookup yields the records whose field may equal the key; every conjunct of the
	// predicate stays in a physical filter above it. An index over a column the actor may not read
	// would answer for that column, and an index entry carries no version stamp, so the swap needs
	// a side condition stating "no restricted column for this principal" and "the read is not
	// versioned"; the access-path patterns state both through `access::decide`.
	ImplRule {
		name: "filter_eq_to_index_lookup",
		tags: &[
			OpTag::Logical(LogTag::Filter),
			OpTag::Logical(LogTag::Source),
			OpTag::Phys(PhysTag::IndexLookup),
			OpTag::Phys(PhysTag::Filter),
		],
		conditions: &[SideCondition::PhysicalFact(PhysFactReq::UniqueKey("?field"))],
		soundness: None,
		withheld: Some(Withheld {
			reason: "the condition set cannot state a restricted column or a versioned read",
			admitted_by: &["R-L4-22"],
		}),
	},
	// A limit over a sort keeps a bounded prefix of the sorted rows. The serial sort keeps ties
	// in arrival order and the top-k keeps the same prefix, so the rows kept are the same.
	ImplRule {
		name: "limit_sort_to_topk",
		tags: &[
			OpTag::Logical(LogTag::Limit),
			OpTag::Logical(LogTag::Sort),
			OpTag::Phys(PhysTag::Sort),
		],
		conditions: &[],
		soundness: Some(
			"the serial sort keeps ties in arrival order and the top-k keeps the same prefix",
		),
		withheld: Some(Withheld {
			reason: "the conversion's sort and limit patterns already produce the top-k sort, so the seed holds it; no implementation rule registers in 4.0 (D4 R19)",
			admitted_by: &["D4 R19"],
		}),
	},
	// A sort whose keys the access path already delivers in order needs no sorting breaker; the
	// breaker is removed through `Legality::fuse`, never by editing legality. The swap has the
	// guards of `filter_eq_to_index_lookup`, and a multi-valued (array-element) column also orders
	// and breaks ties differently from the serial sort, so it needs a side condition that no key
	// is such a column.
	ImplRule {
		name: "sort_by_index_order",
		tags: &[
			OpTag::Logical(LogTag::Sort),
			OpTag::Logical(LogTag::Source),
			OpTag::Phys(PhysTag::IndexScan),
		],
		conditions: &[SideCondition::PhysicalFact(PhysFactReq::OrderedBy("?keys"))],
		soundness: None,
		withheld: Some(Withheld {
			reason: "the swap needs the guards of the index lookup and a no-multi-valued-key condition",
			admitted_by: &["R-L4-22"],
		}),
	},
	// A join whose right key is indexed reads the right side through that index per left row.
	// The right reads move after the left rows' effects, so the right footprint must be
	// disjoint from the statement's writes. The lookup join pairs rows in another order than the
	// hash join, so the swap is sound only where the statement's keys are total.
	ImplRule {
		name: "join_to_lookup_join",
		tags: &[OpTag::Logical(LogTag::Join), OpTag::Phys(PhysTag::Join)],
		conditions: &[
			SideCondition::PhysicalFact(PhysFactReq::IndexOn("?key")),
			SideCondition::FootprintDisjointFromStatementWrites("?right"),
		],
		soundness: None,
		withheld: Some(Withheld {
			reason: "the lookup join pairs rows in another order than the hash join and the condition set has no total-keys condition",
			admitted_by: &["R-L4-20", "L4-FD-13"],
		}),
	},
	// Reads in a loop body are issued ahead of the iteration that consumes them. The verifier
	// rejects `phys.prefetch` while the host service it stands for does not exist.
	ImplRule {
		name: "loop_reads_to_prefetch",
		tags: &[
			OpTag::Logical(LogTag::Loop),
			OpTag::Logical(LogTag::Source),
			OpTag::Phys(PhysTag::Prefetch),
		],
		conditions: &[SideCondition::FootprintDisjointFromStatementWrites("?read")],
		soundness: None,
		withheld: Some(Withheld {
			reason: "the verifier rejects phys.prefetch while the host service it stands for does not exist",
			admitted_by: &["schedule::PREFETCH_EXECUTABLE"],
		}),
	},
	// A loop over a filtered table scan whose filtered field carries an index reads the matching
	// entries instead of every row. The conjuncts the index does not serve stay in a residual
	// physical filter, and the rule matches physical nodes only, so no logical node is read or
	// produced. An index over a column the principal may not read would answer for that column,
	// and a conjunct that can fail must not be reordered, so the swap needs the index fact and an
	// infallible conjunct.
	ImplRule {
		name: "alt_btree_access",
		tags: &[
			OpTag::Phys(PhysTag::Loop),
			OpTag::Phys(PhysTag::Filter),
			OpTag::Phys(PhysTag::TableScan),
			OpTag::Phys(PhysTag::IndexScan),
		],
		conditions: &[
			SideCondition::PhysicalFact(PhysFactReq::IndexOn("?field")),
			SideCondition::Infallible("?conjunct"),
		],
		soundness: None,
		withheld: Some(Withheld {
			reason: "the swap reads an index in place of a table, and the value-oracle review of D4 section 7.5 has not cleared it",
			admitted_by: &[
				"R-L6-1 PhysFactReq::BTreeOnField",
				"R-L4-20",
				"D4 §7.5 G1–G7",
				"SECURITY_GUIDE value-oracle review",
			],
		}),
	},
];

/// The names of the catalogue rules that register. Empty: no implementation rule registers.
const REGISTERED: &[&str] = &[];

/// The registered implementation rules, in registration order. Answers none.
pub(crate) fn rules() -> Vec<Box<dyn EgraphRule>> {
	CATALOGUE
		.iter()
		.filter(|rule| REGISTERED.contains(&rule.name))
		.map(|rule| Box::new(*rule) as Box<dyn EgraphRule>)
		.collect()
}

/// Every catalogue rule, registered or withheld, in catalogue order.
pub(crate) fn catalogue() -> impl Iterator<Item = &'static dyn EgraphRule> {
	let all: &'static [ImplRule] = &CATALOGUE;
	all.iter().map(|rule| rule as &'static dyn EgraphRule)
}

/// Registers the implementation rules with `r`.
///
/// Each registered rule is offered to the rule set; a rule the set refuses is left out. With no
/// registered rule, nothing is offered.
pub(crate) fn register(r: &mut Registry) {
	for rule in rules() {
		let _refused = r.egraph_rules.register(rule);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// No catalogue rule registers: the list and the answer are both empty.
	#[test]
	fn no_implementation_rule_is_registered() {
		assert!(REGISTERED.is_empty());
		assert!(rules().is_empty());
		assert_eq!(catalogue().count(), CATALOGUE.len());
	}

	/// Registration offers the rule set nothing. The registry is built empty rather than through
	/// `registry::build()`, which also registers the logical rules.
	#[test]
	fn register_offers_nothing() {
		let mut r = Registry::empty();
		register(&mut r);
		assert_eq!(r.egraph_rules.iter().count(), 0);
	}

	/// The planner's compiled rule set holds no implementation rule.
	#[test]
	fn the_planner_rule_set_holds_no_implementation_rule() {
		assert_eq!(
			crate::exec::ir::egraph::rule::registered()
				.of_dialect(RuleDialect::Implementation)
				.count(),
			0
		);
	}

	/// Every catalogue rule is an implementation rule whose right side is rooted at a physical tag,
	/// and every rule but the physical-only IR1 slot matches a logical node.
	#[test]
	fn every_catalogue_rule_is_an_implementation_rule_rooted_at_phys() {
		for rule in catalogue() {
			assert_eq!(rule.dialect(), RuleDialect::Implementation, "{}", rule.name());
			assert!(
				matches!(rule.tags().last(), Some(OpTag::Phys(_))),
				"{} is not rooted at a physical tag",
				rule.name()
			);
			if rule.name() != "alt_btree_access" {
				assert!(
					rule.tags().iter().any(|tag| matches!(tag, OpTag::Logical(_))),
					"{} matches no logical node",
					rule.name()
				);
			}
		}
	}

	/// Every catalogue rule names a side condition or states why it is sound.
	#[test]
	fn every_catalogue_rule_has_a_condition_or_soundness() {
		for rule in catalogue() {
			assert!(
				!rule.side_conditions().is_empty() || rule.soundness().is_some(),
				"{} has neither a side condition nor a soundness statement",
				rule.name()
			);
		}
	}

	/// Every withheld rule states why and names what admits it, and the names are distinct.
	#[test]
	fn every_withheld_rule_names_what_admits_it() {
		let mut seen = std::collections::HashSet::new();
		for rule in &CATALOGUE {
			assert!(seen.insert(rule.name), "{} is listed twice", rule.name);
			let withheld = rule
				.withheld
				.as_ref()
				.unwrap_or_else(|| panic!("{} is not withheld although none registers", rule.name));
			assert!(!withheld.reason.is_empty(), "{} has no reason", rule.name);
			assert!(!withheld.admitted_by.is_empty(), "{} names nothing that admits it", rule.name);
		}
	}

	/// A rule that replaces a filter with an index lookup keeps a physical filter on its right.
	#[test]
	fn an_index_lookup_keeps_its_residual_filter() {
		let rule = CATALOGUE
			.iter()
			.find(|rule| rule.name == "filter_eq_to_index_lookup")
			.expect("the rule exists");
		assert!(rule.tags.contains(&OpTag::Phys(PhysTag::Filter)));
	}

	/// The rule definitions construct no logical node: outside this test module, `LogOp::` does
	/// not occur.
	#[test]
	fn rules_phys_builds_no_logical_node() {
		let source = include_str!("rules_phys.rs");
		let production =
			source.split("#[cfg(test)]").next().expect("the module has a production part");
		for (number, line) in production.lines().enumerate() {
			assert!(!line.contains("LogOp::"), "line {} names a logical constructor", number + 1);
		}
	}
}
