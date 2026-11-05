//! The depth budgets each settings profile charges, as queries meet them.

use surrealdb_expr::parser::{ParserSettings, query_with_settings};

/// `depth` nested object literals, wrapped in the smallest statement that
/// can hold one.
fn nested_object_query(depth: usize) -> String {
	format!("RETURN {}1{};", "{ l: ".repeat(depth), " }".repeat(depth))
}

fn parses(source: &str, settings: ParserSettings) -> bool {
	query_with_settings(source, settings).is_ok()
}

/// A bracketed level is charged to the object budget and to the expression
/// budget, and not to the query budget.
///
/// The query budget bounds statement nesting — a subquery, a block, an `IF`
/// — and a collection literal is none of those. Charging it here as well
/// would leave the smaller of the two bounding collection nesting, so the
/// documented object limit would not be the one a query meets. Each budget is
/// squeezed in turn with the other two generous, so a rejection can only come
/// from the budget under test.
#[test]
fn a_bracketed_level_is_charged_to_the_collection_budgets() {
	const GENEROUS: usize = 1000;
	const DEPTH: usize = 20;

	let source = nested_object_query(DEPTH);
	let generous = ParserSettings {
		object_recursion_limit: GENEROUS,
		query_recursion_limit: GENEROUS,
		expr_recursion_limit: GENEROUS,
		..Default::default()
	};
	assert!(parses(&source, generous), "all three budgets clear the nesting");

	for (budget, settings) in [
		(
			"object",
			ParserSettings {
				object_recursion_limit: DEPTH - 1,
				..generous
			},
		),
		(
			"expression",
			ParserSettings {
				expr_recursion_limit: DEPTH - 1,
				..generous
			},
		),
	] {
		assert!(
			!parses(&source, settings),
			"the {budget} budget is charged once per bracketed level"
		);
	}

	// Squeezed far below the nesting, not merely to `DEPTH - 1`, so this
	// cannot pass on an off-by-one in what the statement frame costs.
	assert!(
		parses(
			&source,
			ParserSettings {
				query_recursion_limit: 4,
				..generous
			}
		),
		"a collection literal must not be charged to the statement-nesting budget"
	);

	// The budget still bounds what it is for: a block is a statement holding
	// statements, and nesting one is charged here.
	let blocks = format!("RETURN {}1{};", "{ ".repeat(DEPTH), " }".repeat(DEPTH));
	assert!(parses(&blocks, generous), "the fixture parses when nothing is squeezed");
	assert!(
		!parses(
			&blocks,
			ParserSettings {
				query_recursion_limit: DEPTH - 1,
				..generous
			}
		),
		"nested blocks are charged to the statement-nesting budget"
	);
}

/// The default profile's collection nesting is the documented object limit,
/// not some smaller budget that binds first.
///
/// The ceiling a query meets is the one an operator reads in the settings. A
/// second budget charged for the same level would silently move it, which is
/// what this fixes in place.
#[test]
fn the_default_profile_nests_collections_to_its_object_limit() {
	let settings = ParserSettings::default();
	let limit = settings.object_recursion_limit;
	// The statement frame costs a level or two, so the assertion is that the
	// ceiling tracks the object limit rather than sitting at some unrelated
	// number far below it.
	assert!(
		parses(&nested_object_query(limit - 4), settings),
		"the default profile rejects collection nesting well inside its object limit of {limit}"
	);
	assert!(
		!parses(&nested_object_query(limit + 1), settings),
		"the default profile accepts collection nesting past its object limit of {limit}"
	);
}

/// A set's first element is bounded by the query budget and its later
/// elements by the object budget, so the ceiling depends on which position
/// the nesting sits in.
///
/// `{ … , 0 }` and `{ 0, … }` hold the same value, and this pins that they do
/// not parse to the same depth. A set is only told apart from a block by the
/// token after its first expression, so that expression is charged before
/// which budget applies is known, and it is charged as a block's statement —
/// the reading that keeps block nesting bounded by the smaller budget, since
/// a block's depth is paid for on the call stack when the tree is lowered and
/// dropped.
///
/// This is a pin on current behaviour, not an endorsement: resolving it means
/// settling the set/block ambiguity before the first element is parsed.
#[test]
fn a_sets_first_element_is_bounded_by_the_statement_budget() {
	/// Builds `{ <nested>, 0 }` when `first`, else `{ 0, <nested> }`.
	fn nested_set_query(depth: usize, first: bool) -> String {
		let mut set = "1".to_string();
		for _ in 0..depth {
			set = if first {
				format!("{{ {set}, 0 }}")
			} else {
				format!("{{ 0, {set} }}")
			};
		}
		format!("RETURN {set};")
	}

	let settings = ParserSettings::default();
	let objects = settings.object_recursion_limit;
	let queries = settings.query_recursion_limit;
	assert!(queries < objects, "the pin only says something while the budgets differ");

	// Nesting in a later position reaches the object budget.
	assert!(
		parses(&nested_set_query(objects - 4, false), settings),
		"a set nesting in a later position is bounded below the object limit of {objects}"
	);
	// Nesting in the first position is bounded by the query budget instead.
	assert!(
		!parses(&nested_set_query(objects - 4, true), settings),
		"a set nesting in the first position reached the object limit of {objects}, so the \
		 asymmetry this pins is gone"
	);
	assert!(
		parses(&nested_set_query(queries - 4, true), settings),
		"a set nesting in the first position is bounded below the query limit of {queries}"
	);
}

/// A value nested to the depth a caller declares storable parses back
/// under the profile derived from that same depth, in the shape an export
/// renders it: `INSERT [ { … } ];`.
#[test]
fn value_depth_profile_reads_back_the_deepest_value() {
	const DEPTH: usize = 256;

	// A value at `DEPTH` is a record object with `DEPTH - 1` levels below
	// it, and the `INSERT` array the exporter wraps it in is one bracket
	// more than that.
	let record = format!("{}'leaf'{}", "{ l: ".repeat(DEPTH - 1), " }".repeat(DEPTH - 1));
	let source = format!("INSERT [ {record} ];");
	assert!(parses(&source, ParserSettings::for_value_depth(DEPTH)));
}

/// The margin the profile adds is real headroom on every budget, so a
/// value at the declared depth cannot sit exactly on a ceiling and be
/// pushed past it by the framing around it.
#[test]
fn value_depth_profile_exceeds_the_declared_depth() {
	const DEPTH: usize = 256;

	let settings = ParserSettings::for_value_depth(DEPTH);
	assert!(settings.object_recursion_limit > DEPTH);
	assert!(settings.query_recursion_limit > DEPTH);
	assert!(settings.expr_recursion_limit > DEPTH);
}
