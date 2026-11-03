//! Lowering of the PostgreSQL JSON operators.

use surrealdb_expr::expr::logical::{LogicalExpr, LogicalOp};

use super::expr::MAX_TWO_SIDED_STEPS;
use crate::array_literal::parse_text_array_literal;
use crate::catalog::MemoryCatalog;
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::{binder, lower_bound_select};

fn lower_select(sql: &str) -> Result<LogicalOp, PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "doc", &["data"]);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None)?;
	lower_bound_select(&bound, &policy)
}

fn first_target(op: &LogicalOp) -> &LogicalExpr {
	let LogicalOp::Project(project) = op else {
		panic!("expected a projection, got {op:?}");
	};
	let surrealdb_expr::expr::logical::LogicalFields::Select(fields) = &project.fields else {
		panic!("expected selected fields");
	};
	let surrealdb_expr::expr::logical::LogicalField::Single {
		expr,
		..
	} = &fields[0]
	else {
		panic!("expected a single field");
	};
	expr
}

fn rendered(expr: &LogicalExpr) -> String {
	format!("{expr:?}")
}

/// The text of `sql`'s first output column once lowered.
fn lowered(sql: &str) -> String {
	let op = lower_select(sql).unwrap_or_else(|err| panic!("{sql} must lower: {err:?}"));
	rendered(first_target(&op))
}

/// How many times the document column is read by the lowered expression.
fn column_reads(text: &str) -> usize {
	text.matches("Field(\"data\")").count()
}

#[test]
fn text_array_literals_split_into_elements() {
	let parse = |text: &str| parse_text_array_literal(text);
	let text = |items: &[&str]| Some(items.iter().map(|item| Some(item.to_string())).collect());
	assert_eq!(parse("{a,b}"), text(&["a", "b"]));
	assert_eq!(parse("{ a , b }"), text(&["a", "b"]));
	assert_eq!(parse("{a b,c}"), text(&["a b", "c"]));
	assert_eq!(parse("{\"a b\",0}"), text(&["a b", "0"]));
	assert_eq!(parse("{\"say \\\"hi\\\"\"}"), text(&["say \"hi\""]));
	assert_eq!(parse("{}"), Some(Vec::new()));
	assert_eq!(parse("a,b"), None);
	assert_eq!(parse("{{a,b},{c}}"), None, "nested braces are not a path");
	assert_eq!(parse("{\"unterminated}"), None);
}

#[test]
fn a_quote_toggles_quoting_wherever_it_stands_in_an_element() {
	let parse = |text: &str| parse_text_array_literal(text);
	let one = |item: &str| Some(vec![Some(item.to_string())]);
	assert_eq!(parse("{\"a\"b}"), one("ab"), "text after a closing quote joins the element");
	assert_eq!(parse("{a\"b c\"d}"), one("ab cd"), "a quote in the middle opens a quoted part");
	assert_eq!(parse("{x\"\"}"), one("x"), "an empty quoted part adds nothing");
	assert_eq!(parse("{\"a b\"\\ }"), one("a b "), "an escaped space after a quote is kept");
	assert_eq!(parse("{\"a, b\"c}"), one("a, bc"), "a comma in a quoted part is text");
	assert_eq!(parse("{a\"}"), None, "a quote that is never closed");
	assert_eq!(parse("{a\"b}"), None, "the closing brace is inside the quote");
	assert_eq!(parse("{\"a}"), None);
	assert_eq!(parse("{\"a\"\"b\"}"), one("ab"));
}

#[test]
fn space_after_the_last_significant_character_is_not_part_of_an_element() {
	let parse = |text: &str| parse_text_array_literal(text);
	let one = |item: &str| Some(vec![Some(item.to_string())]);
	assert_eq!(parse("{a\\,b }"), one("a,b"), "space after an escape and text is dropped");
	assert_eq!(parse("{a\\,b\t}"), one("a,b"));
	assert_eq!(parse("{a\\b   }"), one("ab"));
	assert_eq!(parse("{a\\ b   , c}").map(|items| items.len()), Some(2));
	assert_eq!(parse("{a\\ }"), one("a "), "an escaped space is text");
	assert_eq!(parse("{a\\  }"), one("a "), "only the space after the escaped one is dropped");
	assert_eq!(parse("{\"a \"  }"), one("a "), "space inside quotes is text");
	assert_eq!(parse("{ a b  }"), one("a b"), "space between words is text");
	assert_eq!(parse("{\\ a}"), one(" a"), "an escaped space leads the element");
}

#[test]
fn space_is_ascii_and_includes_the_vertical_tab() {
	let parse = |text: &str| parse_text_array_literal(text);
	let one = |item: &str| Some(vec![Some(item.to_string())]);
	assert_eq!(parse("{\u{b}x}"), one("x"));
	assert_eq!(parse("{x\u{b}}"), one("x"));
	assert_eq!(parse("\u{b}{x}\u{c}\r\n"), one("x"));
	assert_eq!(parse(" \t{ }\n"), Some(Vec::new()));
	assert_eq!(parse("\u{a0}{x}"), None, "a no-break space is not space");
	assert_eq!(parse("{x}\u{2003}"), None, "an em space is not space");
	assert_eq!(parse("{\u{a0}x}"), one("\u{a0}x"), "and inside the braces it is text");
	assert_eq!(parse("{x} y"), None, "text after the closing brace");
	assert_eq!(parse("{x}}"), None);
}

#[test]
fn the_unquoted_word_null_is_a_null_element_and_a_quoted_one_is_text() {
	let parse = |text: &str| parse_text_array_literal(text);
	assert_eq!(parse("{NULL}"), Some(vec![None]));
	assert_eq!(parse("{a,null,b}"), Some(vec![Some("a".to_string()), None, Some("b".to_string())]));
	assert_eq!(parse("{ Null }"), Some(vec![None]));
	assert_eq!(parse("{\"NULL\"}"), Some(vec![Some("NULL".to_string())]));
	assert_eq!(parse("{\\NULL}"), Some(vec![Some("NULL".to_string())]));
	assert_eq!(parse("{NULLS}"), Some(vec![Some("NULLS".to_string())]));
}

#[test]
fn an_empty_unquoted_element_is_malformed_and_an_empty_quoted_one_is_text() {
	let parse = |text: &str| parse_text_array_literal(text);
	for literal in ["{a,,b}", "{,}", "{a,}", "{,a}", "{ , }"] {
		assert_eq!(parse(literal), None, "{literal}");
	}
	assert_eq!(parse("{\"\"}"), Some(vec![Some(String::new())]));
	assert_eq!(parse("{a,\"\",b}").map(|items| items.len()), Some(3));
}

#[test]
fn a_backslash_escapes_the_next_character_of_an_unquoted_element() {
	let parse = |text: &str| parse_text_array_literal(text);
	assert_eq!(parse("{a\\\\b}"), Some(vec![Some("a\\b".to_string())]));
	assert_eq!(parse("{a\\,b}"), Some(vec![Some("a,b".to_string())]));
	assert_eq!(parse("{a\\\"b}"), Some(vec![Some("a\"b".to_string())]));
	assert_eq!(parse("{a\\}"), None, "an escape needs a character to escape");
}

#[test]
fn a_field_step_is_an_index_by_text_which_reads_an_object_only() {
	for sql in [
		"SELECT data -> 'k' FROM doc",
		"SELECT data ->> 'k' FROM doc",
		"SELECT data #>> '{k}' FROM doc",
	] {
		let text = lowered(sql);
		assert!(
			text.contains("Index { base: Field(\"data\"), index: Literal(String(\"k\")) }"),
			"{sql}: {text}"
		);
		assert!(!text.contains("Access"), "{sql}: an access maps over an array: {text}");
	}
}

#[test]
fn an_integer_operand_reaches_an_array_element_and_nothing_else() {
	// From the start, the element is the one-element slice of an array, which is NONE for a value
	// that is not an array: an index by integer would also read the member of an object.
	let text = lowered("SELECT data -> 2 FROM doc");
	assert!(
		text.contains(
			"Index { base: Index { base: Field(\"data\"), index: Binary { left: Literal(Integer(2)), \
			 op: Range, right: Literal(Integer(3)) } }, index: Literal(Integer(0)) }"
		),
		"{text}"
	);
	assert!(!text.contains("Call") && !text.contains("Optional"), "{text}");
	// The last element is the `[$]` step, which is NONE for any value but a non-empty array.
	let text = lowered("SELECT data ->> -1 FROM doc");
	assert!(text.contains("Last(Field(\"data\"))"), "{text}");
	assert!(!text.contains("array::at"), "{text}");
	let text = lowered("SELECT data -> -1 FROM doc");
	assert!(!text.contains("Call") && !text.contains("Optional"), "{text}");
}

#[test]
fn an_integer_operand_before_the_last_element_reads_the_array_once_whole() {
	let text = lowered("SELECT data -> -2 FROM doc");
	assert!(text.contains("array::at") && text.contains("Literal(Integer(-2))"), "{text}");
	assert!(text.contains("Optional { base: Field(\"data\")"), "{text}");
	assert!(!text.contains("type::is_array"), "the array test is a slice, not a call: {text}");
}

#[test]
fn the_forms_that_read_an_array_in_place_copy_neither_the_array_nor_call_a_function() {
	for sql in [
		"SELECT data -> 0 FROM doc",
		"SELECT data -> 7 FROM doc",
		"SELECT data -> -1 FROM doc",
		"SELECT data ->> 0 FROM doc",
		"SELECT data ->> -1 FROM doc",
		"SELECT data #>> '{0}' FROM doc",
		"SELECT data #>> '{-1}' FROM doc",
		"SELECT data #>> '{01}' FROM doc",
		"SELECT data #>> '{+1}' FROM doc",
		"SELECT data #>> '{a,-1,b}' FROM doc",
		"SELECT data #>> '{a,01,-1}' FROM doc",
	] {
		let text = lowered(sql);
		assert!(!text.contains("array::at"), "{sql}: {text}");
		assert!(!text.contains("type::is_array"), "{sql}: {text}");
		assert!(
			!text.contains("Optional { base: Field(\"data\")"),
			"{sql}: the array is the base of a path and is not bound as a value: {text}"
		);
	}
}

#[test]
fn a_path_step_that_spells_an_integer_reads_both_an_object_and_an_array() {
	let plain = lowered("SELECT data #>> '{a,1}' FROM doc");
	assert!(plain.contains("Literal(Integer(1))"), "{plain}");
	assert!(!plain.contains("Range"), "a plain non-negative step is one index: {plain}");
	for (step, element) in [("-1", "Last("), ("01", "op: Range"), ("+1", "op: Range")] {
		let text = lowered(&format!("SELECT data #>> '{{a,{step}}}' FROM doc"));
		assert!(
			text.contains(&format!(
				"Index {{ base: Index {{ base: Field(\"data\"), index: Literal(String(\"a\")) }}, \
				 index: Literal(String(\"{step}\")) }}"
			)),
			"{step}: an object is read by the step's own text: {text}"
		);
		assert!(text.contains(element), "{step}: an array is read by the index: {text}");
		assert!(text.contains("op: NullCoalescing"), "{step}: {text}");
	}
	let negative = lowered("SELECT data #>> '{a,-2}' FROM doc");
	assert!(negative.contains("Literal(Integer(-2))"), "{negative}");
	assert!(
		negative.contains("Index { base: Current, index: Literal(String(\"-2\")) }"),
		"an object is read by the step's own text: {negative}"
	);
}

#[test]
fn a_path_with_a_null_element_is_null() {
	for path in ["{NULL}", "{a,null,b}", "{ NuLL }"] {
		let text = lowered(&format!("SELECT data #>> '{path}' FROM doc"));
		assert_eq!(text, "Literal(Null)", "{path}");
	}
	let quoted = lowered("SELECT data #>> '{\"NULL\"}' FROM doc");
	assert!(
		quoted.contains("Literal(String(\"NULL\"))"),
		"a quoted NULL is a member name: {quoted}"
	);
}

#[test]
fn a_path_step_that_is_not_a_32_bit_integer_is_a_member_name() {
	for step in ["1x", "2147483648", "-2147483649", "1.5", ""] {
		let text = lowered(&format!("SELECT data #>> '{{\"{step}\"}}' FROM doc"));
		assert!(!text.contains("array::at"), "{step:?}: {text}");
		assert!(text.contains(&format!("String(\"{step}\")")), "{step:?}: {text}");
	}
}

#[test]
fn every_operator_reads_the_document_once_however_long_the_chain() {
	let mut chain = String::from("data");
	for step in 0..24 {
		chain = match step % 3 {
			0 => format!("({chain} -> 'k')"),
			1 => format!("({chain} -> {step})"),
			_ => format!("({chain} -> -{step})"),
		};
	}
	let long_chain = format!("SELECT {chain} ->> 'z' FROM doc");
	let steps = ["a", "b", "1", "-5", "c"].repeat(12).join(",");
	let long_path = format!("SELECT data #>> '{{{steps}}}' FROM doc");
	for sql in [
		"SELECT data -> 'k' FROM doc",
		"SELECT data -> 3 FROM doc",
		"SELECT data ->> 1 FROM doc",
		"SELECT data ->> -1 FROM doc",
		"SELECT data ->> -2 FROM doc",
		"SELECT data #>> '{a,b}' FROM doc",
		"SELECT data #>> '{}' FROM doc",
		"SELECT data ->> lower('K') FROM doc",
		&long_chain,
		&long_path,
	] {
		let text = lowered(sql);
		assert_eq!(column_reads(&text), 1, "{sql}: {text}");
	}
}

#[test]
fn the_steps_that_read_their_value_twice_are_bounded_per_path() {
	let reads = |steps: usize| {
		let path = vec!["-1"; steps].join(",");
		column_reads(&lowered(&format!("SELECT data #>> '{{{path}}}' FROM doc")))
	};
	assert_eq!(reads(1), 2);
	assert_eq!(reads(MAX_TWO_SIDED_STEPS), 1 << MAX_TWO_SIDED_STEPS);
	assert_eq!(reads(MAX_TWO_SIDED_STEPS + 1), 1 << MAX_TWO_SIDED_STEPS);
	assert_eq!(reads(64), 1 << MAX_TWO_SIDED_STEPS);
}

/// A base that is not a place: it calls a function whose value differs between evaluations.
const SHIFTING_BASE: &str = "(CASE WHEN random() < 0.5 THEN data ELSE data END)";

/// How many times the shifting base is evaluated by the lowered expression: the comparison that
/// chooses the branch is lowered once per evaluation.
fn shifting_reads(text: &str) -> usize {
	text.matches("op: LessThan, right: Literal(Decimal(0.5))").count()
}

#[test]
fn a_step_over_a_value_that_is_not_a_place_evaluates_it_once() {
	for path in ["-1", "-1,-1,-1", "-1,-1,-1,-1,-1", "a,01,+1,-1", "-2,-1,-1", "0,-1"] {
		let sql = format!("SELECT {SHIFTING_BASE} #>> '{{{path}}}' FROM doc");
		let text = lowered(&sql);
		assert_eq!(shifting_reads(&text), 1, "{sql}: {text}");
	}
	let steps = vec!["-1"; 64].join(",");
	let text = lowered(&format!("SELECT {SHIFTING_BASE} #>> '{{{steps}}}' FROM doc"));
	assert_eq!(shifting_reads(&text), 1, "{text}");
}

#[test]
fn a_step_over_a_subquery_evaluates_it_once() {
	let text = lowered("SELECT (SELECT d.data FROM doc d) #>> '{-1,-1,-1}' FROM doc");
	assert_eq!(text.matches("Subquery(").count(), 1, "{text}");
}

#[test]
fn a_step_over_a_place_may_read_it_twice_whichever_way_the_place_is_spelled() {
	let two_sided = |base: &str, reads: usize| {
		let text = lowered(&format!("SELECT {base} #>> '{{-1,-1}}' FROM doc"));
		assert_eq!(text.matches("Field(\"data\")").count(), reads, "{base}: {text}");
	};
	two_sided("data", 4);
	two_sided("(data -> 'a')", 4);
	two_sided("(data -> 'a' -> 1 -> -1)", 4);
	two_sided("(data #>> '{a}')", 1);
}

#[test]
fn a_path_whose_first_step_binds_its_value_reads_the_document_once() {
	let text = lowered("SELECT data #>> '{-2,-1,-1,-1}' FROM doc");
	assert_eq!(column_reads(&text), 1, "{text}");
}

fn null_coalescings(text: &str) -> usize {
	text.matches("op: NullCoalescing, right: Literal(Null) }").count()
}

#[test]
fn a_chain_reads_the_value_where_it_lies_and_reads_an_absent_value_as_null_once() {
	let text = lowered("SELECT data -> 'a' -> 'b' FROM doc");
	assert!(
		text.contains(
			"Index { base: Index { base: Field(\"data\"), index: Literal(String(\"a\")) }, \
			 index: Literal(String(\"b\")) }"
		),
		"{text}"
	);
	assert_eq!(null_coalescings(&text), 1, "{text}");
	for sql in [
		"SELECT data -> 'a' -> 0 -> -1 -> 'b' FROM doc",
		"SELECT data -> 'a' ->> 0 FROM doc",
		"SELECT data -> 'a' -> -2 ->> 'b' FROM doc",
		"SELECT data -> 'a' #>> '{b,-1}' FROM doc",
		"SELECT data -> 'a' -> 'b' #>> '{c}' FROM doc",
	] {
		let text = lowered(sql);
		assert_eq!(null_coalescings(&text), 1, "{sql}: {text}");
	}
	let mut chain = String::from("data");
	for step in 0..24 {
		chain = format!("({chain} -> {})", ["'k'", "1", "-1"][step % 3]);
	}
	let text = lowered(&format!("SELECT {chain} -> 'z' FROM doc"));
	assert_eq!(null_coalescings(&text), 1, "{text}");
}

#[test]
fn a_null_test_of_a_json_operator_reads_the_place_it_reaches() {
	for sql in [
		"SELECT data -> 'a' IS NULL FROM doc",
		"SELECT data -> 'a' IS NOT NULL FROM doc",
		"SELECT data -> 'a' -> 0 IS NULL FROM doc",
		"SELECT data ->> 'a' IS NULL FROM doc",
		"SELECT data ->> -1 IS NOT NULL FROM doc",
		"SELECT data -> 'a' ->> 1 IS NULL FROM doc",
		"SELECT data #>> '{a,b}' IS NULL FROM doc",
		"SELECT data #>> '{a,-1}' IS NOT NULL FROM doc",
	] {
		let text = lowered(sql);
		assert!(text.contains("Field(\"data\")"), "{sql}: {text}");
		for conversion in [
			"NullCoalescing, right: Literal(Null)",
			"Optional",
			"encoding::json::encode",
			"is_string",
		] {
			assert!(
				!text.contains(conversion),
				"{sql}: {conversion} is not part of a null test: {text}"
			);
		}
		assert!(text.contains("op: Equal, right: Literal(None)"), "{sql}: {text}");
	}
}

#[test]
fn a_null_test_of_a_json_operator_over_a_value_that_is_not_a_place_evaluates_it_once() {
	for sql in [
		format!("SELECT {SHIFTING_BASE} -> 'a' IS NULL FROM doc"),
		format!("SELECT {SHIFTING_BASE} ->> -1 IS NOT NULL FROM doc"),
		format!("SELECT {SHIFTING_BASE} #>> '{{a,-1,-1}}' IS NULL FROM doc"),
		"SELECT data -> -2 IS NULL FROM doc".to_string(),
		"SELECT data #>> '{a,-2}' IS NULL FROM doc".to_string(),
		"SELECT (SELECT d.data FROM doc d) -> 'a' IS NULL FROM doc".to_string(),
	] {
		let text = lowered(&sql);
		assert!(text.contains("then: Literal(Bool(false))"), "{sql}: {text}");
		assert!(text.contains("op: NullCoalescing, right: Literal(Bool(true))"), "{sql}: {text}");
		assert!(text.contains("Binary { left: Optional {"), "{sql}: {text}");
		assert_eq!(shifting_reads(&text), usize::from(sql.contains("random")), "{sql}: {text}");
		assert_eq!(
			text.matches("Subquery(").count(),
			usize::from(sql.contains("SELECT d.")),
			"{sql}"
		);
	}
}

#[test]
fn a_null_test_of_a_json_operator_that_is_null_whatever_the_value_is_holds() {
	for sql in [
		"SELECT data -> NULL IS NULL FROM doc",
		"SELECT data #>> '{a,NULL}' IS NULL FROM doc",
		"SELECT data -> NULL -> 'a' IS NULL FROM doc",
	] {
		let text = lowered(sql);
		assert_eq!(text, "Literal(Bool(true))", "{sql}");
	}
}

#[test]
fn the_size_of_a_lowered_path_grows_in_proportion_to_its_length() {
	let size = |steps: usize| {
		let path = vec!["-1"; steps].join(",");
		lowered(&format!("SELECT data #>> '{{{path}}}' FROM doc")).len()
	};
	assert_eq!(size(64) - size(48), size(48) - size(32), "each step past the bound adds one size");
	assert_eq!(size(48) - size(32), size(32) - size(16));
}

#[test]
fn an_absent_member_is_null_and_so_is_a_json_null() {
	for sql in [
		"SELECT data -> 'k' FROM doc",
		"SELECT data -> 1 FROM doc",
		"SELECT data -> -1 FROM doc",
		"SELECT data -> -2 FROM doc",
		"SELECT data ->> 'k' FROM doc",
		"SELECT data ->> -1 FROM doc",
		"SELECT data #>> '{k}' FROM doc",
		"SELECT data #>> '{}' FROM doc",
	] {
		let text = lowered(sql);
		assert!(text.ends_with("op: NullCoalescing, right: Literal(Null) }"), "{sql}: {text}");
	}
}

#[test]
fn an_operand_that_is_not_text_or_a_32_bit_integer_has_no_operator() {
	for (sql, operator) in [
		("SELECT data -> 3000000000 FROM doc", "jsonb -> bigint"),
		("SELECT data ->> 2147483648 FROM doc", "jsonb ->> bigint"),
		("SELECT data ->> -2147483648 FROM doc", "jsonb ->> bigint"),
		("SELECT data -> -3000000000 FROM doc", "jsonb -> bigint"),
		("SELECT data -> 1.5 FROM doc", "jsonb -> numeric"),
		("SELECT data ->> -1.5 FROM doc", "jsonb ->> numeric"),
		("SELECT data -> true FROM doc", "jsonb -> boolean"),
		("SELECT data -> 'a' -> 1.5 FROM doc", "jsonb -> numeric"),
		("SELECT data -> NULL -> 1.5 FROM doc", "jsonb -> numeric"),
		("SELECT data -> 1.5 IS NULL FROM doc", "jsonb -> numeric"),
	] {
		let err = lower_select(sql).expect_err(sql);
		assert_eq!(err, PlanError::UndefinedOperator(operator.to_string()), "{sql}");
		assert_eq!(err.pg_sqlstate(), "42883", "{sql}");
		assert_eq!(err.to_string(), format!("operator does not exist: {operator}"), "{sql}");
	}
	for sql in [
		"SELECT data -> 2147483647 FROM doc",
		"SELECT data ->> -2147483647 FROM doc",
		"SELECT data -> (1 - 3) FROM doc",
		"SELECT data -> (4 / 2) FROM doc",
		"SELECT data -> (3 / 2) FROM doc",
		"SELECT data -> (7 % 4) FROM doc",
		"SELECT data ->> (2 * 3) FROM doc",
		"SELECT data ->> (1 + 1) FROM doc",
		"SELECT data ->> -(4 / 2) FROM doc",
	] {
		lower_select(sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
	}
}

#[test]
fn an_integer_literal_that_does_not_fit_64_bits_is_not_read_as_an_index() {
	for (sql, operator) in [
		("SELECT data ->> 9223372036854775808 FROM doc", "jsonb ->> numeric"),
		("SELECT data -> -9223372036854775809 FROM doc", "jsonb -> numeric"),
		("SELECT data -> -9223372036854775808 FROM doc", "jsonb -> bigint"),
	] {
		let err = lower_select(sql).expect_err(sql);
		assert_eq!(err, PlanError::UndefinedOperator(operator.to_string()), "{sql}");
		assert_eq!(err.pg_sqlstate(), "42883", "{sql}");
	}
}

#[test]
fn a_null_operand_is_null() {
	for sql in ["SELECT data -> NULL FROM doc", "SELECT data ->> NULL FROM doc"] {
		assert_eq!(lowered(sql), "Literal(Null)", "{sql}");
	}
}

#[test]
fn an_operand_that_is_not_a_literal_indexes_by_its_runtime_value() {
	let text = lowered("SELECT data ->> lower('K') FROM doc");
	assert!(text.contains("Index { base: Field(\"data\"), index: "), "{text}");
	assert!(!text.contains("array::at"), "{text}");
}

#[test]
fn extracted_text_is_null_when_absent_and_json_when_not_a_string() {
	for sql in ["SELECT data ->> 'k' FROM doc", "SELECT data #>> '{k}' FROM doc"] {
		let text = lowered(sql);
		assert!(text.contains("type::is_string"), "{sql}: {text}");
		assert!(text.contains("encoding::json::encode"), "{sql}: {text}");
		assert!(!text.contains("type::string"), "{sql}: {text}");
		assert!(text.contains("Optional"), "{sql}: the value is evaluated once: {text}");
	}
}

#[test]
fn a_path_that_is_not_a_text_array_literal_is_refused_as_invalid_text() {
	for sql in ["SELECT data #>> 'geo' FROM doc", "SELECT data #>> '{a,' FROM doc"] {
		let err = lower_select(sql).expect_err("refused");
		assert!(matches!(err, PlanError::InvalidTextRepresentation(_)), "{sql}: {err:?}");
		assert_eq!(err.pg_sqlstate(), "22P02", "{sql}");
	}
}

#[test]
fn a_path_that_is_not_a_literal_is_refused_as_unsupported() {
	let err = lower_select("SELECT data #>> $1 FROM doc").expect_err("refused");
	assert!(matches!(err, PlanError::UnsupportedExpression(_)), "{err:?}");
	assert_eq!(err.pg_sqlstate(), "0A000");
}

#[test]
fn containment_is_refused_as_a_feature_that_is_not_supported() {
	let err = lower_select("SELECT data @> '{\"a\":1}' FROM doc").expect_err("refused");
	assert!(matches!(err, PlanError::UnsupportedExpression(_)), "{err:?}");
	assert_eq!(err.pg_sqlstate(), "0A000");
}

#[test]
fn a_path_longer_than_the_limit_is_refused() {
	let steps = vec!["a"; 65].join(",");
	let err =
		lower_select(&format!("SELECT data #>> '{{{steps}}}' FROM doc")).expect_err("refused");
	assert!(matches!(err, PlanError::UnsupportedExpression(_)), "{err:?}");
	let steps = vec!["a"; 64].join(",");
	lower_select(&format!("SELECT data #>> '{{{steps}}}' FROM doc"))
		.expect("the limit itself lowers");
}
