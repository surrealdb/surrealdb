use sqlparser::ast::{self as pg, BinaryOperator as PgBinOp, UnaryOperator as PgUnOp};
use surrealdb_core::expr::cond::Cond;
use surrealdb_core::expr::expression::Expr;
use surrealdb_core::expr::field::{Field, Fields, Selector};
use surrealdb_core::expr::function::{Function, FunctionCall};
use surrealdb_core::expr::idiom::Idiom;
use surrealdb_core::expr::literal::Literal;
use surrealdb_core::expr::operator::{BinaryOperator, PrefixOperator};
use surrealdb_core::expr::order::{Order, OrderList, Ordering};
use surrealdb_core::expr::param::Param;
use surrealdb_core::expr::part::{DestructurePart, Part};
use surrealdb_core::val::Regex;

use crate::error::TranslateError;

pub fn translate_expr(expr: pg::Expr) -> Result<Expr, TranslateError> {
	match expr {
		pg::Expr::Identifier(ident) => Ok(ident_to_idiom_expr(&ident)),

		pg::Expr::CompoundIdentifier(parts) => Ok(compound_ident_to_idiom_expr(&parts)),

		pg::Expr::Value(val) => translate_pg_value(val.value),

		pg::Expr::BinaryOp {
			left,
			op,
			right,
		} => {
			let left = translate_expr(*left)?;
			let right = translate_expr(*right)?;
			let op = translate_binary_op(op)?;
			Ok(Expr::Binary {
				left: Box::new(left),
				op,
				right: Box::new(right),
			})
		}

		pg::Expr::UnaryOp {
			op,
			expr,
		} => {
			let inner = translate_expr(*expr)?;
			match op {
				PgUnOp::Not => Ok(Expr::Prefix {
					op: PrefixOperator::Not,
					expr: Box::new(inner),
				}),
				PgUnOp::Minus => Ok(Expr::Prefix {
					op: PrefixOperator::Negate,
					expr: Box::new(inner),
				}),
				PgUnOp::Plus => Ok(inner),
				other => Err(TranslateError::unsupported(format!("unary operator {other}"))),
			}
		}

		pg::Expr::Nested(inner) => translate_expr(*inner),

		pg::Expr::IsNull(inner) => {
			let inner = translate_expr(*inner)?;
			Ok(Expr::Binary {
				left: Box::new(inner),
				op: BinaryOperator::Equal,
				right: Box::new(Expr::Literal(Literal::Null)),
			})
		}

		pg::Expr::IsNotNull(inner) => {
			let inner = translate_expr(*inner)?;
			Ok(Expr::Binary {
				left: Box::new(inner),
				op: BinaryOperator::NotEqual,
				right: Box::new(Expr::Literal(Literal::Null)),
			})
		}

		pg::Expr::Between {
			expr,
			negated,
			low,
			high,
		} => {
			let val = translate_expr(*expr)?;
			let low = translate_expr(*low)?;
			let high = translate_expr(*high)?;
			let between = Expr::Binary {
				left: Box::new(Expr::Binary {
					left: Box::new(val.clone()),
					op: BinaryOperator::LessThanEqual,
					right: Box::new(high),
				}),
				op: BinaryOperator::And,
				right: Box::new(Expr::Binary {
					left: Box::new(low),
					op: BinaryOperator::LessThanEqual,
					right: Box::new(val),
				}),
			};
			if negated {
				Ok(Expr::Prefix {
					op: PrefixOperator::Not,
					expr: Box::new(between),
				})
			} else {
				Ok(between)
			}
		}

		pg::Expr::InList {
			expr,
			list,
			negated,
		} => {
			let val = translate_expr(*expr)?;
			let items: Result<Vec<Expr>, _> = list.into_iter().map(translate_expr).collect();
			let contains = Expr::Binary {
				left: Box::new(val),
				op: BinaryOperator::Inside,
				right: Box::new(Expr::Literal(Literal::Array(items?))),
			};
			if negated {
				Ok(Expr::Prefix {
					op: PrefixOperator::Not,
					expr: Box::new(contains),
				})
			} else {
				Ok(contains)
			}
		}

		pg::Expr::Function(func) => translate_function_expr(func),

		pg::Expr::Cast {
			expr,
			data_type,
			..
		} => {
			let inner = translate_expr(*expr)?;
			let kind = super::types::translate_data_type(&data_type)?;
			Ok(Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal("type::cast".to_string()),
				arguments: vec![inner, Expr::Literal(Literal::String(kind))],
			})))
		}

		pg::Expr::Like {
			negated,
			expr,
			pattern,
			..
		} => translate_like(*expr, *pattern, negated, false),

		pg::Expr::ILike {
			negated,
			expr,
			pattern,
			..
		} => translate_like(*expr, *pattern, negated, true),

		pg::Expr::Wildcard(_) => Ok(Expr::Literal(Literal::None)),

		other => Err(TranslateError::unsupported(format!("expression: {other}"))),
	}
}

fn translate_pg_value(val: pg::Value) -> Result<Expr, TranslateError> {
	match val {
		pg::Value::Number(n, _) => {
			if let Ok(i) = n.parse::<i64>() {
				Ok(Expr::Literal(Literal::Integer(i)))
			} else if let Ok(f) = n.parse::<f64>() {
				Ok(Expr::Literal(Literal::Float(f)))
			} else {
				Err(TranslateError::mapping(format!("cannot parse number: {n}")))
			}
		}
		pg::Value::SingleQuotedString(s) | pg::Value::DoubleQuotedString(s) => {
			Ok(Expr::Literal(Literal::String(s)))
		}
		pg::Value::Boolean(b) => Ok(Expr::Literal(Literal::Bool(b))),
		pg::Value::Null => Ok(Expr::Literal(Literal::Null)),
		pg::Value::Placeholder(name) => {
			Ok(Expr::Param(Param(name.trim_start_matches('$').to_string())))
		}
		other => Err(TranslateError::unsupported(format!("value literal: {other}"))),
	}
}

fn translate_binary_op(op: PgBinOp) -> Result<BinaryOperator, TranslateError> {
	match op {
		PgBinOp::Plus => Ok(BinaryOperator::Add),
		PgBinOp::Minus => Ok(BinaryOperator::Subtract),
		PgBinOp::Multiply => Ok(BinaryOperator::Multiply),
		PgBinOp::Divide => Ok(BinaryOperator::Divide),
		PgBinOp::Modulo => Ok(BinaryOperator::Remainder),
		PgBinOp::Eq => Ok(BinaryOperator::Equal),
		PgBinOp::NotEq => Ok(BinaryOperator::NotEqual),
		PgBinOp::Lt => Ok(BinaryOperator::LessThan),
		PgBinOp::LtEq => Ok(BinaryOperator::LessThanEqual),
		PgBinOp::Gt => Ok(BinaryOperator::MoreThan),
		PgBinOp::GtEq => Ok(BinaryOperator::MoreThanEqual),
		PgBinOp::And => Ok(BinaryOperator::And),
		PgBinOp::Or => Ok(BinaryOperator::Or),
		other => Err(TranslateError::unsupported(format!("binary operator: {other}"))),
	}
}

fn translate_function_expr(func: pg::Function) -> Result<Expr, TranslateError> {
	let name = func.name.to_string().to_lowercase();
	let mapped_name = super::functions::map_function_name(&name);

	let arg_list = match func.args {
		pg::FunctionArguments::List(list) => list.args,
		pg::FunctionArguments::Subquery(_) => {
			return Err(TranslateError::unsupported("subquery as function argument"));
		}
		pg::FunctionArguments::None => Vec::new(),
	};
	let args: Result<Vec<Expr>, _> = arg_list
		.into_iter()
		.filter_map(|arg| match arg {
			pg::FunctionArg::Unnamed(pg::FunctionArgExpr::Expr(e)) => Some(translate_expr(e)),
			pg::FunctionArg::Unnamed(pg::FunctionArgExpr::Wildcard) => None,
			pg::FunctionArg::Named {
				arg: pg::FunctionArgExpr::Expr(e),
				..
			} => Some(translate_expr(e)),
			_ => None,
		})
		.collect();

	Ok(Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal(mapped_name.to_string()),
		arguments: args?,
	})))
}

pub fn ident_to_idiom(ident: &pg::Ident) -> Idiom {
	Idiom(vec![Part::Field(ident.value.clone())])
}

fn ident_to_idiom_expr(ident: &pg::Ident) -> Expr {
	Expr::Idiom(ident_to_idiom(ident))
}

fn compound_ident_to_idiom_expr(parts: &[pg::Ident]) -> Expr {
	let idiom_parts: Vec<Part> = parts.iter().map(|i| Part::Field(i.value.clone())).collect();
	Expr::Idiom(Idiom(idiom_parts))
}

pub fn translate_select_items(items: Vec<pg::SelectItem>) -> Result<Fields, TranslateError> {
	let mut fields = Vec::new();
	for item in items {
		match item {
			pg::SelectItem::UnnamedExpr(ref e) => {
				let alias = if let pg::Expr::CompoundIdentifier(parts) = e {
					parts.last().map(|i| Idiom(vec![Part::Field(i.value.clone())]))
				} else {
					None
				};
				let expr = translate_expr(e.clone())?;
				fields.push(Field::Single(Selector {
					expr,
					alias,
				}));
			}
			pg::SelectItem::ExprWithAlias {
				expr,
				alias,
			} => {
				let expr = translate_expr(expr)?;
				fields.push(Field::Single(Selector {
					expr,
					alias: Some(ident_to_idiom(&alias)),
				}));
			}
			pg::SelectItem::Wildcard(_) => {
				fields.push(Field::All);
			}
			pg::SelectItem::QualifiedWildcard(ref kind, _) => {
				let qualifier = match kind {
					pg::SelectItemQualifiedWildcardKind::ObjectName(name) => {
						name.0.first().and_then(|p| p.as_ident()).map(|id| id.value.clone())
					}
					_ => None,
				};
				match qualifier {
					Some(q) => {
						let destructure = Part::Destructure(vec![DestructurePart::All(q)]);
						fields.push(Field::Single(Selector {
							expr: Expr::Idiom(Idiom(vec![destructure])),
							alias: None,
						}));
					}
					None => fields.push(Field::All),
				}
			}
		}
	}
	Ok(Fields::Select(fields))
}

pub fn translate_order_by(order_by: Vec<pg::OrderByExpr>) -> Result<Ordering, TranslateError> {
	let mut orders = Vec::new();
	for o in order_by {
		let expr = translate_expr(o.expr)?;
		let idiom = expr_to_idiom(expr)?;
		let descending = o.options.asc.map(|a| !a).unwrap_or(false);
		orders.push(Order {
			value: idiom,
			collate: false,
			numeric: false,
			direction: !descending,
		});
	}
	Ok(Ordering::Order(OrderList(orders)))
}

pub fn translate_where(selection: Option<pg::Expr>) -> Result<Option<Cond>, TranslateError> {
	match selection {
		Some(expr) => {
			let cond = translate_expr(expr)?;
			Ok(Some(Cond(cond)))
		}
		None => Ok(None),
	}
}

pub fn translate_limit(limit: Option<pg::Expr>) -> Result<Expr, TranslateError> {
	match limit {
		Some(expr) => translate_expr(expr),
		None => Ok(Expr::Literal(Literal::None)),
	}
}

pub fn translate_offset(offset: Option<pg::Offset>) -> Result<Expr, TranslateError> {
	match offset {
		Some(o) => translate_expr(o.value),
		None => Ok(Expr::Literal(Literal::None)),
	}
}

fn expr_to_idiom(expr: Expr) -> Result<Idiom, TranslateError> {
	match expr {
		Expr::Idiom(idiom) => Ok(idiom),
		Expr::Literal(Literal::String(s)) => Ok(Idiom(vec![Part::Field(s)])),
		Expr::Table(t) => Ok(Idiom(vec![Part::Field(t.0)])),
		_ => Err(TranslateError::mapping("expected a field reference in ORDER BY")),
	}
}

fn translate_like(
	expr: pg::Expr,
	pattern: pg::Expr,
	negated: bool,
	case_insensitive: bool,
) -> Result<Expr, TranslateError> {
	let left = translate_expr(expr)?;
	let pattern_expr = translate_expr(pattern)?;

	let pattern_str = match &pattern_expr {
		Expr::Literal(Literal::String(s)) => s.clone(),
		_ => return Err(TranslateError::unsupported("LIKE with non-literal pattern")),
	};

	let regex_str = like_pattern_to_regex(&pattern_str, case_insensitive)?;
	let regex: Regex = regex_str
		.parse()
		.map_err(|e| TranslateError::mapping(format!("invalid regex from LIKE pattern: {e}")))?;

	let call = Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal("string::matches".to_string()),
		arguments: vec![left, Expr::Literal(Literal::Regex(regex))],
	}));
	if negated {
		Ok(Expr::Prefix {
			op: PrefixOperator::Not,
			expr: Box::new(call),
		})
	} else {
		Ok(call)
	}
}

fn like_pattern_to_regex(pattern: &str, case_insensitive: bool) -> Result<String, TranslateError> {
	let mut regex = String::with_capacity(pattern.len() + 8);
	if case_insensitive {
		regex.push_str("(?i)");
	}
	regex.push('^');

	let mut chars = pattern.chars().peekable();
	while let Some(ch) = chars.next() {
		match ch {
			'%' => regex.push_str(".*"),
			'_' => regex.push('.'),
			'\\' => {
				if let Some(&next) = chars.peek() {
					regex_escape_char(next, &mut regex);
					chars.next();
				}
			}
			other => regex_escape_char(other, &mut regex),
		}
	}

	regex.push('$');
	Ok(regex)
}

fn regex_escape_char(ch: char, out: &mut String) {
	match ch {
		'.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
			out.push('\\');
			out.push(ch);
		}
		_ => out.push(ch),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn pat(pattern: &str) -> String {
		like_pattern_to_regex(pattern, false).unwrap()
	}

	fn ipat(pattern: &str) -> String {
		like_pattern_to_regex(pattern, true).unwrap()
	}

	fn matches(pattern: &str, input: &str) -> bool {
		let re_str = pat(pattern);
		regex::Regex::new(&re_str).unwrap().is_match(input)
	}

	fn imatches(pattern: &str, input: &str) -> bool {
		let re_str = ipat(pattern);
		regex::Regex::new(&re_str).unwrap().is_match(input)
	}

	// -- like_pattern_to_regex: anchoring and structure ----------------

	#[test]
	fn anchored_exact_match() {
		assert_eq!(pat("hello"), "^hello$");
	}

	#[test]
	fn case_insensitive_flag() {
		assert_eq!(ipat("hello"), "(?i)^hello$");
	}

	#[test]
	fn empty_pattern() {
		assert_eq!(pat(""), "^$");
	}

	// -- wildcard translation -----------------------------------------

	#[test]
	fn percent_becomes_dotstar() {
		assert_eq!(pat("%"), "^.*$");
		assert_eq!(pat("a%"), "^a.*$");
		assert_eq!(pat("%b"), "^.*b$");
		assert_eq!(pat("%a%"), "^.*a.*$");
	}

	#[test]
	fn underscore_becomes_dot() {
		assert_eq!(pat("_"), "^.$");
		assert_eq!(pat("a_b"), "^a.b$");
		assert_eq!(pat("__"), "^..$");
	}

	#[test]
	fn mixed_wildcards() {
		assert_eq!(pat("_%"), "^..*$");
		assert_eq!(pat("%_"), "^.*.$");
		assert_eq!(pat("a%b_c"), "^a.*b.c$");
	}

	// -- regex metacharacter escaping ---------------------------------

	#[test]
	fn escapes_dot() {
		assert_eq!(pat("a.b"), r"^a\.b$");
	}

	#[test]
	fn escapes_plus() {
		assert_eq!(pat("a+b"), r"^a\+b$");
	}

	#[test]
	fn escapes_star() {
		assert_eq!(pat("a*b"), r"^a\*b$");
	}

	#[test]
	fn escapes_question_mark() {
		assert_eq!(pat("a?b"), r"^a\?b$");
	}

	#[test]
	fn escapes_parens() {
		assert_eq!(pat("(a)"), r"^\(a\)$");
	}

	#[test]
	fn escapes_brackets() {
		assert_eq!(pat("[a]"), r"^\[a\]$");
	}

	#[test]
	fn escapes_braces() {
		assert_eq!(pat("{a}"), r"^\{a\}$");
	}

	#[test]
	fn escapes_caret_dollar_pipe() {
		assert_eq!(pat("^$|"), r"^\^\$\|$");
	}

	#[test]
	fn escapes_backslash_literal() {
		assert_eq!(pat(r"a\\b"), r"^a\\b$");
	}

	// -- SQL LIKE escape sequences (backslash escapes) ----------------

	#[test]
	fn escaped_percent_is_literal() {
		assert_eq!(pat(r"100\%"), r"^100%$");
		assert!(matches(r"100\%", "100%"));
		assert!(!matches(r"100\%", "100abc"));
	}

	#[test]
	fn escaped_underscore_is_literal() {
		assert_eq!(pat(r"a\_b"), "^a_b$");
		assert!(matches(r"a\_b", "a_b"));
		assert!(!matches(r"a\_b", "aXb"));
	}

	#[test]
	fn escaped_backslash_is_literal() {
		assert_eq!(pat(r"a\\b"), r"^a\\b$");
		assert!(matches(r"a\\b", r"a\b"));
	}

	#[test]
	fn trailing_backslash_ignored() {
		assert_eq!(like_pattern_to_regex("abc\\", false).unwrap(), "^abc$");
	}

	// -- end-to-end match correctness ---------------------------------

	#[test]
	fn prefix_match() {
		assert!(matches("Ali%", "Alice"));
		assert!(!matches("Ali%", "Bob"));
	}

	#[test]
	fn suffix_match() {
		assert!(matches("%ice", "Alice"));
		assert!(!matches("%ice", "Bob"));
	}

	#[test]
	fn contains_match() {
		assert!(matches("%li%", "Alice"));
		assert!(matches("%li%", "Charlie"));
		assert!(!matches("%li%", "Bob"));
	}

	#[test]
	fn single_char_wildcard() {
		assert!(matches("_ob", "Bob"));
		assert!(!matches("_ob", "ob"));
		assert!(!matches("_ob", "XXob"));
	}

	#[test]
	fn exact_match_no_wildcards() {
		assert!(matches("Bob", "Bob"));
		assert!(!matches("Bob", "bob"));
		assert!(!matches("Bob", "Bobby"));
	}

	#[test]
	fn case_insensitive_match() {
		assert!(imatches("ali%", "Alice"));
		assert!(imatches("ALI%", "Alice"));
		assert!(!imatches("ali%", "Bob"));
	}

	#[test]
	fn match_all() {
		assert!(matches("%", "anything"));
		assert!(matches("%", ""));
	}

	#[test]
	fn match_single_any() {
		assert!(matches("_", "a"));
		assert!(!matches("_", ""));
		assert!(!matches("_", "ab"));
	}

	// -- translate_like integration -----------------------------------

	fn pg_string(s: &str) -> pg::Expr {
		pg::Expr::Value(pg::Value::SingleQuotedString(s.into()).with_empty_span())
	}

	#[test]
	fn translate_like_produces_string_matches_call() {
		let expr = pg::Expr::Identifier(pg::Ident::new("name"));
		let pattern = pg_string("Ali%");
		let result = translate_like(expr, pattern, false, false).unwrap();
		match result {
			Expr::FunctionCall(fc) => {
				assert_eq!(fc.receiver, Function::Normal("string::matches".to_string()));
				assert_eq!(fc.arguments.len(), 2);
				match &fc.arguments[1] {
					Expr::Literal(Literal::Regex(r)) => {
						assert_eq!(r.inner().as_str(), "^Ali.*$");
					}
					other => panic!("expected Regex literal, got: {other:?}"),
				}
			}
			other => panic!("expected FunctionCall, got: {other:?}"),
		}
	}

	#[test]
	fn translate_like_negated_wraps_with_not() {
		let expr = pg::Expr::Identifier(pg::Ident::new("name"));
		let pattern = pg_string("Ali%");
		let result = translate_like(expr, pattern, true, false).unwrap();
		match result {
			Expr::Prefix {
				op: PrefixOperator::Not,
				expr: inner,
			} => {
				assert!(matches!(*inner, Expr::FunctionCall(_)));
			}
			other => panic!("expected Prefix(Not, ...), got: {other:?}"),
		}
	}

	#[test]
	fn translate_like_rejects_non_literal_pattern() {
		let expr = pg::Expr::Identifier(pg::Ident::new("name"));
		let pattern = pg::Expr::Identifier(pg::Ident::new("other_col"));
		let result = translate_like(expr, pattern, false, false);
		assert!(result.is_err());
	}
}
