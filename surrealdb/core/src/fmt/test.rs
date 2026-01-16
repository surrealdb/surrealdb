use surrealdb_types::ToSql;

pub fn ensure_formats(s: &str) {
	let parsed = crate::syn::parse(s).unwrap();
	let parsed_formated = crate::syn::parse(&parsed.to_sql()).unwrap();

	let plan: crate::expr::LogicalPlan = parsed.clone().into();
	let parsed_formated_plan = crate::syn::parse(&plan.to_sql()).unwrap();

	assert_eq!(parsed, parsed_formated, "formatting the sql type changed the query");
	assert_eq!(parsed, parsed_formated_plan, "formatting the expr type changed the query");
}

macro_rules! test_case {
	($name:ident => $source:literal) => {
		#[test]
		fn $name() {
			ensure_formats($source)
		}
	};
}

test_case!(idiom_after_select => "(SELECT foo FROM bar ORDER BY foo)[0]");
test_case!(idiom_after_create => "(CREATE foo:1 SET V = $a)[0]");
test_case!(idiom_after_closure => "(|$a: number| { $a })[0]");

test_case!(covered_expr => "(1 + 1) * 3");
