//! Tests for the GQL parser, mirroring the layout of
//! [`crate::syn::parser`]'s test module: statement level tests in `stmt`,
//! pattern tests in `pattern`, expression tests in `expr` and recursion and
//! pathological input tests in `limits`. The tests below are the original
//! smoke tests asserting the core parse shapes and targeted errors.

use crate::gql::ast::{
	BinaryOp, EdgeDirection, ElementPredicate, GqlExpr, GqlLiteral, GqlStep, LabelExpr,
	MatchClause, MatchItem, QuantifierKind, ReturnClause, ReturnItem, ReturnItems, SetQuantifier,
	TruthValue, UnaryOp,
};
use crate::gql::{GqlParserSettings, parse_str, parse_with_settings};

mod expr;
mod limits;
mod pattern;
mod stmt;

/// A test-only view over a parsed read query, mirroring the pre-mutation
/// `MatchQuery` shape (`items` + a mandatory `ret`) so the parser
/// statement/pattern tests keep their `query.items` / `query.ret` ergonomics.
/// The linear query now also carries mutation statements and an optional
/// `RETURN`; those are exercised by dedicated tests via [`parse_str`].
struct ParsedQuery {
	items: Vec<MatchItem>,
	ret: ReturnClause,
}

fn parse(source: &str) -> ParsedQuery {
	let program = match parse_str(source) {
		Ok(x) => x.program,
		Err(e) => panic!("failed to parse {source:?}: {:?}", e.render_on(source)),
	};
	let items = program
		.steps
		.into_iter()
		.map(|step| match step {
			GqlStep::Read(item) => item,
			GqlStep::Mutate(_) => {
				panic!("this parser test helper only supports read-only queries; use `parse_str`")
			}
		})
		.collect();
	ParsedQuery {
		items,
		ret: program.ret.expect("these parser tests use queries ending in a RETURN clause"),
	}
}

/// Returns the plain `MATCH` clauses of a parsed query's read body, asserting
/// every item is a plain (non-`OPTIONAL`) clause. Most statement and pattern
/// tests build queries with no `OPTIONAL` items, so this keeps their `[i]`
/// indexing into the clause list ergonomic.
#[track_caller]
fn match_clauses(query: &ParsedQuery) -> Vec<&MatchClause> {
	query
		.items
		.iter()
		.map(|item| match item {
			MatchItem::Match(clause) => clause,
			MatchItem::Optional(_) => panic!("expected a plain MATCH clause, found OPTIONAL"),
		})
		.collect()
}

/// Parses the source, expecting an error, and returns the rendered error.
fn parse_err(source: &str) -> String {
	let error = parse_str(source).expect_err("parsing should have failed");
	format!("{:?}", error.render_on(source))
}

/// Parses the source and returns its `RETURN` item list.
fn parse_return_items(source: &str) -> Vec<ReturnItem> {
	let query = parse(source);
	let ReturnItems::Items(items) = query.ret.items else {
		panic!("expected return items in {source:?}");
	};
	items
}

/// Parses `RETURN <expr>` and returns the expression of the single item.
fn parse_return_expr(expr: &str) -> GqlExpr {
	let source = format!("RETURN {expr}");
	let mut items = parse_return_items(&source);
	assert_eq!(items.len(), 1, "expected a single return item in {source:?}");
	items.pop().expect("one return item").expr
}

/// Parses `RETURN <expr>` and renders the resulting AST as a fully
/// parenthesized string, for exact precedence and shape assertions.
fn parse_expr_str(expr: &str) -> String {
	expr_str(&parse_return_expr(expr))
}

/// Renders an expression AST with explicit parentheses around every operator
/// node, making associativity and precedence visible.
fn expr_str(expr: &GqlExpr) -> String {
	match expr {
		GqlExpr::Literal(literal, _) => match literal {
			GqlLiteral::Null => "null".to_owned(),
			GqlLiteral::Bool(x) => x.to_string(),
			GqlLiteral::Integer(x) => x.to_string(),
			GqlLiteral::Float(x) => format!("{x:?}"),
			GqlLiteral::String(x) => format!("'{x}'"),
		},
		GqlExpr::Param {
			name,
			..
		} => format!("${name}"),
		GqlExpr::Variable(ident) => ident.name.clone(),
		GqlExpr::Property(base, name, _) => format!("{}.{}", expr_str(base), name.name),
		GqlExpr::Unary {
			op,
			expr,
			..
		} => {
			let op = match op {
				UnaryOp::Not => "NOT ",
				UnaryOp::Neg => "-",
				UnaryOp::Plus => "+",
			};
			format!("({op}{})", expr_str(expr))
		}
		GqlExpr::Binary {
			left,
			op,
			right,
			..
		} => {
			let op = match op {
				BinaryOp::Or => "OR",
				BinaryOp::Xor => "XOR",
				BinaryOp::And => "AND",
				BinaryOp::Eq => "=",
				BinaryOp::Neq => "<>",
				BinaryOp::Lt => "<",
				BinaryOp::Lte => "<=",
				BinaryOp::Gt => ">",
				BinaryOp::Gte => ">=",
				BinaryOp::Concat => "||",
				BinaryOp::Add => "+",
				BinaryOp::Sub => "-",
				BinaryOp::Mul => "*",
				BinaryOp::Div => "/",
			};
			format!("({} {op} {})", expr_str(left), expr_str(right))
		}
		GqlExpr::IsBool {
			expr,
			value,
			negated,
			..
		} => {
			let value = match value {
				TruthValue::True => "TRUE",
				TruthValue::False => "FALSE",
				TruthValue::Unknown => "UNKNOWN",
			};
			format!(
				"({} IS{} {value})",
				expr_str(expr),
				if *negated {
					" NOT"
				} else {
					""
				}
			)
		}
		GqlExpr::IsNull {
			expr,
			negated,
			..
		} => {
			format!(
				"({} IS{} NULL)",
				expr_str(expr),
				if *negated {
					" NOT"
				} else {
					""
				}
			)
		}
		GqlExpr::FunctionCall {
			name,
			quantifier,
			star,
			args,
			..
		} => {
			let quantifier = match quantifier {
				Some(SetQuantifier::Distinct) => "DISTINCT ",
				Some(SetQuantifier::All) => "ALL ",
				None => "",
			};
			let args: Vec<String> =
				star.iter().map(|_| "*".to_owned()).chain(args.iter().map(expr_str)).collect();
			format!("{}({quantifier}{})", name.name, args.join(", "))
		}
		GqlExpr::List(items, _) => {
			let items: Vec<String> = items.iter().map(expr_str).collect();
			format!("[{}]", items.join(", "))
		}
		GqlExpr::Map(fields, _) => {
			let fields: Vec<String> = fields
				.iter()
				.map(|(key, value)| format!("{}: {}", key.name, expr_str(value)))
				.collect();
			format!("{{{}}}", fields.join(", "))
		}
	}
}

/// Renders a label expression with explicit parentheses around every operator
/// node, for exact precedence assertions.
fn label_str(label: &LabelExpr) -> String {
	match label {
		LabelExpr::Name(ident) => ident.name.clone(),
		LabelExpr::Wildcard(_) => "%".to_owned(),
		LabelExpr::Negation(inner, _) => format!("!({})", label_str(inner)),
		LabelExpr::Conjunction(left, right, _) => {
			format!("({}&{})", label_str(left), label_str(right))
		}
		LabelExpr::Disjunction(left, right, _) => {
			format!("({}|{})", label_str(left), label_str(right))
		}
	}
}

#[track_caller]
fn assert_variable(expr: &GqlExpr, name: &str) {
	let GqlExpr::Variable(ident) = expr else {
		panic!("expected variable `{name}`, got {expr:?}");
	};
	assert_eq!(ident.name, name);
}

#[track_caller]
fn assert_property(expr: &GqlExpr, var: &str, prop: &str) {
	let GqlExpr::Property(base, name, _) = expr else {
		panic!("expected property `{var}.{prop}`, got {expr:?}");
	};
	assert_variable(base, var);
	assert_eq!(name.name, prop);
}

#[test]
fn representative_query() {
	let query = parse(
		"MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > 2020 \
		 RETURN a.name AS n, b.name ORDER BY n SKIP 5 LIMIT 10",
	);

	let clauses = match_clauses(&query);
	assert_eq!(clauses.len(), 1);
	let clause = clauses[0];
	assert_eq!(clause.patterns.len(), 1);

	let pattern = &clause.patterns[0];
	assert!(pattern.path_var.is_none());
	assert_eq!(pattern.start.var.as_ref().map(|x| x.name.as_str()), Some("a"));
	assert!(matches!(&pattern.start.label, Some(LabelExpr::Name(x)) if x.name == "person"));
	assert_eq!(pattern.steps.len(), 1);

	let step = &pattern.steps[0];
	assert_eq!(step.edge.direction, EdgeDirection::Right);
	assert_eq!(step.edge.var.as_ref().map(|x| x.name.as_str()), Some("k"));
	assert!(matches!(&step.edge.label, Some(LabelExpr::Name(x)) if x.name == "knows"));
	assert!(step.edge.quantifier.is_none());
	assert_eq!(step.node.var.as_ref().map(|x| x.name.as_str()), Some("b"));

	let Some(GqlExpr::Binary {
		left,
		op: BinaryOp::Gt,
		right,
		..
	}) = &clause.where_clause
	else {
		panic!("unexpected where clause: {:?}", clause.where_clause);
	};
	assert_property(left, "k", "since");
	assert!(matches!(**right, GqlExpr::Literal(GqlLiteral::Integer(2020), _)));

	let ret = &query.ret;
	assert_eq!(ret.quantifier, None);
	let ReturnItems::Items(items) = &ret.items else {
		panic!("expected return items");
	};
	assert_eq!(items.len(), 2);
	assert_property(&items[0].expr, "a", "name");
	assert_eq!(items[0].alias.as_ref().map(|x| x.name.as_str()), Some("n"));
	assert_eq!(items[0].text, "a.name");
	assert!(items[1].alias.is_none());
	assert_eq!(items[1].text, "b.name");

	assert_eq!(ret.order_by.len(), 1);
	assert_variable(&ret.order_by[0].expr, "n");
	assert_eq!(ret.order_by[0].ascending, None);
	assert_eq!(ret.order_by[0].nulls_first, None);
	assert!(matches!(ret.skip, Some(GqlExpr::Literal(GqlLiteral::Integer(5), _))));
	assert!(matches!(ret.limit, Some(GqlExpr::Literal(GqlLiteral::Integer(10), _))));
}

#[test]
fn expression_precedence() {
	let query = parse("RETURN 1 + 2 * 3, NOT a OR b, x = y AND z IS NOT TRUE");
	assert!(query.items.is_empty());
	let ReturnItems::Items(items) = &query.ret.items else {
		panic!("expected return items");
	};

	// `1 + (2 * 3)`
	let GqlExpr::Binary {
		op: BinaryOp::Add,
		right,
		..
	} = &items[0].expr
	else {
		panic!("expected addition: {:?}", items[0].expr);
	};
	assert!(matches!(
		**right,
		GqlExpr::Binary {
			op: BinaryOp::Mul,
			..
		}
	));

	// `(NOT a) OR b`
	assert!(matches!(
		items[1].expr,
		GqlExpr::Binary {
			op: BinaryOp::Or,
			..
		}
	));

	// `(x = y) AND ((z) IS NOT TRUE)`
	let GqlExpr::Binary {
		op: BinaryOp::And,
		right,
		..
	} = &items[2].expr
	else {
		panic!("expected conjunction: {:?}", items[2].expr);
	};
	assert!(matches!(
		**right,
		GqlExpr::IsBool {
			value: TruthValue::True,
			negated: true,
			..
		}
	));
}

#[test]
fn null_test_and_literals() {
	let query = parse("RETURN a.b IS NOT NULL, -1.5e3, 'it''s', \"str\", `weird var`.x, UNKNOWN");
	let ReturnItems::Items(items) = &query.ret.items else {
		panic!("expected return items");
	};
	assert!(matches!(
		items[0].expr,
		GqlExpr::IsNull {
			negated: true,
			..
		}
	));
	let GqlExpr::Unary {
		expr,
		..
	} = &items[1].expr
	else {
		panic!("expected unary minus: {:?}", items[1].expr);
	};
	assert!(matches!(**expr, GqlExpr::Literal(GqlLiteral::Float(x), _) if x == 1.5e3));
	assert!(matches!(&items[2].expr, GqlExpr::Literal(GqlLiteral::String(x), _) if x == "it's"));
	// In expression position a double-quoted token is a string literal.
	assert!(matches!(&items[3].expr, GqlExpr::Literal(GqlLiteral::String(x), _) if x == "str"));
	// An accent-quoted token is a delimited identifier: a variable.
	assert_property(&items[4].expr, "weird var", "x");
	// The UNKNOWN boolean literal is the null truth value.
	assert!(matches!(items[5].expr, GqlExpr::Literal(GqlLiteral::Null, _)));
}

#[test]
fn quantified_edge_and_function_call() {
	let query = parse("MATCH (a)-[:knows]->{1,3}(b) RETURN upper(b.name), count(b)");
	let step = &match_clauses(&query)[0].patterns[0].steps[0];
	assert!(step.edge.var.is_none());
	assert_eq!(
		step.edge.quantifier.as_ref().map(|x| x.kind),
		Some(QuantifierKind::Range(Some(1), Some(3)))
	);
	let ReturnItems::Items(items) = &query.ret.items else {
		panic!("expected return items");
	};
	let GqlExpr::FunctionCall {
		name,
		args,
		..
	} = &items[0].expr
	else {
		panic!("expected function call: {:?}", items[0].expr);
	};
	assert_eq!(name.name, "upper");
	assert_eq!(args.len(), 1);
	// `COUNT` is a reserved word, but any keyword followed by `(` is a
	// function name.
	let GqlExpr::FunctionCall {
		name,
		..
	} = &items[1].expr
	else {
		panic!("expected function call: {:?}", items[1].expr);
	};
	assert_eq!(name.name, "count");
}

#[test]
fn optional_match_labels_and_props() {
	let query = parse(
		"OPTIONAL MATCH (a IS person|company WHERE a.active) \
		 MATCH (b {name: 'x', age: 30}), (c) \
		 RETURN DISTINCT *",
	);
	assert_eq!(query.items.len(), 2);
	// The leading item is the plain `OPTIONAL MATCH` form: a block of exactly
	// one inner MATCH clause.
	let MatchItem::Optional(block) = &query.items[0] else {
		panic!("expected an OPTIONAL item, got {:?}", query.items[0]);
	};
	assert_eq!(block.items.len(), 1);
	let MatchItem::Match(optional_clause) = &block.items[0] else {
		panic!("expected a MATCH clause inside the OPTIONAL block");
	};
	let node = &optional_clause.patterns[0].start;
	assert!(matches!(node.label, Some(LabelExpr::Disjunction(..))));
	assert!(matches!(node.predicate, Some(ElementPredicate::Where(_))));
	let MatchItem::Match(plain_clause) = &query.items[1] else {
		panic!("expected a plain MATCH item, got {:?}", query.items[1]);
	};
	assert_eq!(plain_clause.patterns.len(), 2);
	let Some(ElementPredicate::Props(props)) = &plain_clause.patterns[0].start.predicate else {
		panic!("expected a property map");
	};
	assert_eq!(props.len(), 2);
	assert_eq!(props[0].0.name, "name");
	assert_eq!(query.ret.quantifier, Some(SetQuantifier::Distinct));
	assert!(matches!(query.ret.items, ReturnItems::Star));
}

#[test]
fn parameters_and_path_variable() {
	let query = parse("MATCH p = (a:person) WHERE a.age > $min RETURN a.name SKIP $s LIMIT $l");
	let clause = match_clauses(&query)[0];
	let pattern = &clause.patterns[0];
	assert_eq!(pattern.path_var.as_ref().map(|x| x.name.as_str()), Some("p"));
	let Some(GqlExpr::Binary {
		right,
		..
	}) = &clause.where_clause
	else {
		panic!("expected a where clause");
	};
	assert!(matches!(&**right, GqlExpr::Param { name, .. } if name == "min"));
	assert!(matches!(&query.ret.skip, Some(GqlExpr::Param { name, .. }) if name == "s"));
	assert!(matches!(&query.ret.limit, Some(GqlExpr::Param { name, .. }) if name == "l"));
}

#[test]
fn non_reserved_keywords_as_identifiers() {
	// `node`, `type` and `first` are non-reserved words; `Type` keeps its
	// original casing.
	let query = parse("MATCH (node:Type) RETURN node.first AS last");
	let node = &match_clauses(&query)[0].patterns[0].start;
	assert_eq!(node.var.as_ref().map(|x| x.name.as_str()), Some("node"));
	assert!(matches!(&node.label, Some(LabelExpr::Name(x)) if x.name == "Type"));
	let ReturnItems::Items(items) = &query.ret.items else {
		panic!("expected return items");
	};
	assert_property(&items[0].expr, "node", "first");
	assert_eq!(items[0].alias.as_ref().map(|x| x.name.as_str()), Some("last"));
}

#[test]
fn edge_directions() {
	let query = parse("MATCH (a)<-[x]-(b)~[y]~(c)<~[z]~(d)<-(e)<->(f)-(g) RETURN 1");
	let steps = &match_clauses(&query)[0].patterns[0].steps;
	let directions: Vec<_> = steps.iter().map(|x| x.edge.direction).collect();
	assert_eq!(
		directions,
		vec![
			EdgeDirection::Left,
			EdgeDirection::Undirected,
			EdgeDirection::LeftOrUndirected,
			EdgeDirection::Left,
			EdgeDirection::LeftOrRight,
			EdgeDirection::Any,
		]
	);
}

#[test]
fn targeted_errors() {
	assert!(
		parse_err("MATCH (a) WHERE a.x != 1 RETURN a").contains("GQL uses `<>` for inequality")
	);
	assert!(
		parse_err("RETURN a = b = c").contains("Comparison operators cannot be chained; use AND")
	);
	assert!(
		parse_err("RETURN a.x + 1 IS NULL")
			.contains("`IS NULL` may only directly follow a simple expression")
	);
	assert!(parse_err("RETURN x IN [1, 2]").contains("GQL has no `IN` membership operator"));
	assert!(
		parse_err("MATCH (a)-[k]->(b)-[j]-> RETURN 1")
			.contains("expected a node pattern after this edge pattern")
	);
	// (INSERT/SET/REMOVE/DELETE now parse — see `mutation_statements_parse`.)
	assert!(parse_err("MATCH (a) RETURN a UNION MATCH (b) RETURN b").contains("Composite queries"));
	// GROUP BY now parses (the lowering enforces its shape); a misplaced GROUP
	// after a page clause is still rejected.
	parse("MATCH (a) RETURN a.x GROUP BY a.x");
	assert!(
		parse_err("MATCH (a) RETURN a.x LIMIT 1 GROUP BY a.x")
			.contains("Unexpected `GROUP` clause")
	);
	assert!(parse_err("RETURN $$x").contains("Substituted parameters"));
	assert!(parse_err("MATCH (a:MATCH) RETURN 1").contains("`MATCH` is a reserved word"));
	// Path-search prefixes now parse (the lowering enforces what it executes); a
	// bare `SHORTEST` without a count or `GROUP` is still a syntax error.
	assert!(parse_err("MATCH SHORTEST (a)->(b) RETURN 1").contains("requires a path count"));
	assert!(
		parse_err("MATCH ((a)-[k]->(b)) RETURN 1")
			.contains("Parenthesized path pattern expressions")
	);
	assert!(parse_err("MATCH (a)-/<x>/->(b) RETURN 1").contains("Simplified path pattern"));
	assert!(parse_err("MATCH (a) RETURN a LIMIT 1 ORDER BY a").contains("Unexpected `ORDER`"));
	assert!(parse_err("MATCH (a) FINISH").contains("FINISH statements are not supported"));
	assert!(parse_err("RETURN EXISTS { MATCH (a) }").contains("`EXISTS` predicates"));
	assert!(parse_err("RETURN CASE WHEN a THEN 1 END").contains("`CASE` expressions"));
}

#[test]
fn recursion_limit() {
	let settings = GqlParserSettings {
		object_recursion_limit: 3,
		..Default::default()
	};
	let error =
		parse_with_settings("RETURN ((((1))))", settings).expect_err("parsing should have failed");
	let rendered = format!("{:?}", error.render_on("RETURN ((((1))))"));
	assert!(rendered.contains("Exceeded query expression nesting depth limit"));
	// The same input parses fine with the default limits.
	parse("RETURN ((((1))))");
}
