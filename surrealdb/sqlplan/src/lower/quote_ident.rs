//! Lowering of PostgreSQL `quote_ident(text)`.
//!
//! The result is the input unchanged when it is a valid unquoted identifier
//! that keeps its meaning, otherwise the input wrapped in double quotes with
//! every embedded `"` doubled. An identifier is left bare when its first
//! character is a lowercase ASCII letter or `_`, the rest are lowercase ASCII
//! letters, digits, `_` or `$`, and the word is an unreserved PostgreSQL
//! keyword or not a keyword at all. Reserved, type-or-function-name and
//! column-name keywords are quoted, as are all other inputs including the
//! empty string. `NULL` yields `NULL`.

use surrealdb_expr::expr::logical::{LogicalCall, LogicalExpr, LogicalIf, LogicalLiteral};
use surrealdb_expr::expr::{BinaryOperator, Function};
use surrealdb_strand::Strand;

/// Pattern of an identifier that needs no quoting apart from the keyword check.
const BARE_IDENT_PATTERN: &str = "^[a-z_][a-z0-9_$]*$";

/// Lowercase PostgreSQL keywords that are not in the unreserved category
/// (reserved, type-or-function-name and column-name keywords), as in
/// PostgreSQL's `kwlist.h`. The list is self-contained so that a keyword the
/// parser does not model, or models in another category, is still quoted.
const QUOTED_KEYWORDS: &[&str] = &[
	// Reserved
	"all",
	"analyse",
	"analyze",
	"and",
	"any",
	"array",
	"as",
	"asc",
	"asymmetric",
	"both",
	"case",
	"cast",
	"check",
	"collate",
	"column",
	"constraint",
	"create",
	"current_catalog",
	"current_date",
	"current_role",
	"current_time",
	"current_timestamp",
	"current_user",
	"default",
	"deferrable",
	"desc",
	"distinct",
	"do",
	"else",
	"end",
	"except",
	"false",
	"fetch",
	"for",
	"foreign",
	"from",
	"grant",
	"group",
	"having",
	"in",
	"initially",
	"intersect",
	"into",
	"lateral",
	"leading",
	"limit",
	"localtime",
	"localtimestamp",
	"not",
	"null",
	"offset",
	"on",
	"only",
	"or",
	"order",
	"placing",
	"primary",
	"references",
	"returning",
	"select",
	"session_user",
	"some",
	"symmetric",
	"system_user",
	"table",
	"then",
	"to",
	"trailing",
	"true",
	"union",
	"unique",
	"user",
	"using",
	"variadic",
	"when",
	"where",
	"window",
	"with",
	// Type or function name
	"authorization",
	"binary",
	"collation",
	"concurrently",
	"cross",
	"current_schema",
	"freeze",
	"full",
	"ilike",
	"inner",
	"is",
	"isnull",
	"join",
	"left",
	"like",
	"natural",
	"notnull",
	"outer",
	"overlaps",
	"right",
	"similar",
	"tablesample",
	"verbose",
	// Column name
	"between",
	"bigint",
	"bit",
	"boolean",
	"char",
	"character",
	"coalesce",
	"dec",
	"decimal",
	"exists",
	"extract",
	"float",
	"greatest",
	"grouping",
	"inout",
	"int",
	"integer",
	"interval",
	"json",
	"json_array",
	"json_arrayagg",
	"json_exists",
	"json_object",
	"json_objectagg",
	"json_query",
	"json_scalar",
	"json_serialize",
	"json_table",
	"json_value",
	"least",
	"merge_action",
	"national",
	"nchar",
	"none",
	"normalize",
	"nullif",
	"numeric",
	"out",
	"overlay",
	"position",
	"precision",
	"real",
	"row",
	"setof",
	"smallint",
	"substring",
	"time",
	"timestamp",
	"treat",
	"trim",
	"values",
	"varchar",
	"xmlattributes",
	"xmlconcat",
	"xmlelement",
	"xmlexists",
	"xmlforest",
	"xmlnamespaces",
	"xmlparse",
	"xmlpi",
	"xmlroot",
	"xmlserialize",
	"xmltable",
];

/// Whether `s` is quoted by PostgreSQL's `quote_ident`.
fn needs_quoting(s: &str) -> bool {
	let mut chars = s.chars();
	let first_ok = matches!(chars.next(), Some('a'..='z' | '_'));
	let rest_ok = chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_' | '$'));
	!(first_ok && rest_ok) || QUOTED_KEYWORDS.contains(&s)
}

/// Apply `quote_ident` to a constant string.
pub(super) fn quote_ident_str(s: &str) -> String {
	if needs_quoting(s) {
		format!("\"{}\"", s.replace('"', "\"\""))
	} else {
		s.to_owned()
	}
}

fn string_literal(s: &str) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::String(Strand::from(s)))
}

fn call(name: &str, args: Vec<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal(name.to_string()),
		args,
	}))
}

fn binary(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(left),
		op,
		right: Box::new(right),
	}
}

/// Lower `quote_ident(arg)` for a non-constant argument, which is evaluated once: an optional
/// step binds its value to the current value, gives NONE for NULL or NONE, and the result of that
/// is NULL.
pub(super) fn lower_quote_ident(arg: LogicalExpr) -> LogicalExpr {
	let current = || LogicalExpr::Current;
	let keywords = LogicalExpr::Array(QUOTED_KEYWORDS.iter().map(|k| string_literal(k)).collect());
	let is_bare = call("string::matches", vec![current(), string_literal(BARE_IDENT_PATTERN)])
		.and_not_contained(keywords, current());
	let quoted = binary(
		binary(
			string_literal("\""),
			BinaryOperator::Add,
			call("string::replace", vec![current(), string_literal("\""), string_literal("\"\"")]),
		),
		BinaryOperator::Add,
		string_literal("\""),
	);
	binary(
		LogicalExpr::Optional {
			base: Box::new(arg),
			then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(is_bare, current())],
				otherwise: Some(quoted),
			}))),
		},
		BinaryOperator::NullCoalescing,
		LogicalExpr::Literal(LogicalLiteral::Null),
	)
}

trait AndNotContained {
	fn and_not_contained(self, set: LogicalExpr, item: LogicalExpr) -> LogicalExpr;
}

impl AndNotContained for LogicalExpr {
	fn and_not_contained(self, set: LogicalExpr, item: LogicalExpr) -> LogicalExpr {
		binary(self, BinaryOperator::And, binary(set, BinaryOperator::NotContain, item))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn quoting_rule() {
		let cases = [
			("foo", "foo"),
			("Foo", "\"Foo\""),
			("a b", "\"a b\""),
			("a\"b", "\"a\"\"b\""),
			("select", "\"select\""),
			("table", "\"table\""),
			("user", "\"user\""),
			("", "\"\""),
			("_x1$", "_x1$"),
			("1abc", "\"1abc\""),
			("$a", "\"$a\""),
			("caf\u{e9}", "\"caf\u{e9}\""),
			("name", "name"),
			("public", "public"),
			("table_name", "table_name"),
			("table_schema", "table_schema"),
			("CompatUser", "\"CompatUser\""),
			("left", "\"left\""),
			("int", "\"int\""),
			("action", "action"),
		];
		for (input, expected) in cases {
			assert_eq!(quote_ident_str(input), expected, "quote_ident({input:?})");
		}
	}

	#[test]
	fn the_argument_is_evaluated_once() {
		let arg = call("rand::string", Vec::new());
		let lowered = format!("{:?}", lower_quote_ident(arg));
		assert_eq!(lowered.matches("rand::string").count(), 1, "{lowered}");
		assert!(lowered.contains("NullCoalescing"), "{lowered}");
	}
}
