use std::ops::Bound;

use ast::{
	BinaryExpr, BinaryOperator, Expr, IdiomExpr, IdiomOperator, LookupSubject, NodeId, NodeListId,
	PostfixExpr, PostfixOperator, PrefixExpr, PrefixOperator, RecordIdKeyRange, Spanned,
};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use token::{BaseTokenKind, Joined, T};

use crate::Parser;
use crate::parse::peek::peek_starts_prime;
use crate::parse::prime::parse_prime;
use crate::parse::range::{TryRange, parse_prefix_range_sync, try_parse_infix_range_sync};
use crate::parse::record_id::{
	parse_peeked_record_id_key, parse_record_id_headless_range, peek_record_id_token,
	try_parse_record_id_range,
};
use crate::parse::utils::parse_seperated_list;
use crate::parse::{Parse, ParseResult};

// Constants defining precedences and associativity of operators.
//
// The higher the number the higher the binding power, i.e. given `a op1 b op2 c`
// if `op2` has a higher binding power then `op1` the expression is parsed as `a op1 (b op2 c)`
//
// Binding powers for infex operators can have two numbers, those two numbers together indicate the
// associativity of the operator. So SUM_BP with binding power (7,8) will parse `(a + b) + c`,
// while POWER_BP with binding power (12,11) will parse `a ** (b ** c)`

const BASE_BP: u8 = 0;
const AND_BP: (u8, u8) = (1, 2);
const OR_BP: (u8, u8) = (3, 4);
// Equality is a infix operator but it has no defined associativity.
const EQUALITY_BP: u8 = 5;
const RELATION_BP: u8 = 6;

const SUM_BP: (u8, u8) = (7, 8);
const PRODUCT_BP: (u8, u8) = (9, 10);
// The power operator is right associative, 2 ** 3 ** 4 -> 2 ** (3 ** 4) instead of (2 ** 3) ** 4.
// So the binding power is reversed from the left associative operators.
const POWER_BP: (u8, u8) = (12, 11);

const NULLISH_BP: (u8, u8) = (0, 0);

const RANGE_BP: u8 = 7;

const PREFIX_BP: u8 = 15;
const IDIOM_BP: u8 = 16;

impl Parse for ast::Expr {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		parse_pratt(parser, BASE_BP).await
	}
}

/// Recurse into the pratt entering into a new stack frame.
async fn parse_pratt_enter(parser: &mut Parser<'_, '_>, bp: u8) -> ParseResult<NodeId<Expr>> {
	let expr = parser.enter(async |parser| parse_pratt(parser, bp).await).await?;
	let expr = parser.push(expr);
	Ok(expr)
}

async fn parse_prefix_or_prime(parser: &mut Parser<'_, '_>) -> ParseResult<Expr> {
	let token = parser.peek_expect("an expression")?;
	let op = match token.token {
		T![!] => {
			let _ = parser.next();
			PrefixOperator::Not(token.span)
		}
		T![-] => {
			let _ = parser.next();
			PrefixOperator::Negate(token.span)
		}
		T![+] => {
			let _ = parser.next();
			PrefixOperator::Positive(token.span)
		}
		T![<] => match parser.peek_joined1()?.map(|x| x.token) {
			Some(T![-] | T![->]) => return parse_prime(parser).await,
			_ => {
				let _ = parser.next();
				let ty = parser.parse().await?;
				let _ = parser.expect_closing_delimiter(T![>], token.span)?;

				PrefixOperator::Cast(ty)
			}
		},
		// Special case, because apparently you can omit the dot when recursing from the
		// document.
		T![@] => {
			if let Some(x) = parser.peek1()?
				&& let BaseTokenKind::OpenBrace = x.token
			{
				let _ = parser.next();
				let _ = parser.next();
				let recurse = parse_recurse(parser, x.span, x.span).await?;
				let op_span = parser.span_since(x.span);
				let op = ast::IdiomOperator::Recurse(parser.push(recurse));
				let doc_span = parser.push(token.span);
				let left = parser.push(Expr::Document(doc_span));
				let span = parser.span_since(token.span);
				let expr = parser.push(ast::IdiomExpr {
					left,
					op: Spanned {
						value: op,
						span: op_span,
					},

					span,
				});
				return Ok(Expr::Idiom(expr));
			} else {
				return parse_prime(parser).await;
			}
		}
		T![..] => {
			let _ = parser.next();
			if parser.eat_joined(T![=])?.is_some() {
				PrefixOperator::RangeInclusive(parser.span_since(token.span))
			} else if peek_starts_prime(parser)? {
				PrefixOperator::Range(token.span)
			} else {
				let span = parser.push(token.span);
				return Ok(Expr::UnboundedRange(span));
			}
		}
		_ => {
			return parse_prime(parser).await;
		}
	};

	let expr = parse_pratt_enter(parser, PREFIX_BP).await?;
	let span = parser.span_since(token.span);
	let expr = parser.push(PrefixExpr {
		op,
		right: expr,
		span,
	});
	Ok(Expr::Prefix(expr))
}

async fn parse_basic_infix_op(
	parser: &mut Parser<'_, '_>,
	min_bp: u8,
	bp: (u8, u8),
	operator: BinaryOperator,
	span: Span,
	lhs: Expr,
) -> ParseResult<Option<Expr>> {
	if bp.0 < min_bp {
		return Ok(None);
	}

	let _ = parser.next()?;

	let right = parse_pratt_enter(parser, bp.1).await?;
	let left = parser.push(lhs);
	let expr = parser.push(BinaryExpr {
		left,
		op: Spanned {
			value: operator,
			span,
		},
		right,
		span,
	});
	Ok(Some(Expr::Binary(expr)))
}

fn is_equality_op(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::ExactEqual
			| BinaryOperator::Equal
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
	)
}

fn is_relation_op(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::GreaterThan
			| BinaryOperator::GreaterThanEqual
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::NoneInside
			| BinaryOperator::Outside
			| BinaryOperator::Intersects
			| BinaryOperator::NotInside
			| BinaryOperator::Inside
	)
}

async fn parse_range_infix_op(
	parser: &mut Parser<'_, '_>,
	operator: BinaryOperator,
	lhs: Expr,
	lhs_span: Span,
	op_span: Span,
) -> ParseResult<Option<Expr>> {
	let left = parser.push(lhs);
	let rhs = parse_pratt_enter(parser, RANGE_BP).await?;

	if let Expr::Binary(rhs) = parser[rhs]
		&& let BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive = parser[rhs].op.value
	{
		return Err(parser.with_error(|parser| {
			Level::Error
				.title(format!(
					"Operator `{}` has no defined associativity",
					parser.slice(lhs_span)
				))
				.snippet(
					parser
						.snippet()
						.annotate(
							AnnotationKind::Primary
								.span(lhs_span)
								.label("Cover either this expression."),
						)
						.annotate(AnnotationKind::Context.span(parser[rhs].op.span).label(
							"Or this expression with `()` to define an order of operations",
						)),
				)
				.to_diagnostic()
		}));
	}

	if let Expr::Postfix(rhs) = parser[rhs]
		&& let PostfixOperator::Range | PostfixOperator::RangeSkip = parser[rhs].op.value
	{
		return Err(parser.with_error(|parser| {
			Level::Error
				.title(format!(
					"Operator `{}` has no defined associativity",
					parser.slice(lhs_span)
				))
				.snippet(
					parser
						.snippet()
						.annotate(
							AnnotationKind::Primary
								.span(lhs_span)
								.label("Cover either this expression."),
						)
						.annotate(AnnotationKind::Context.span(parser[rhs].op.span).label(
							"Or this expression with `()` to define an order of operations",
						)),
				)
				.to_diagnostic()
		}));
	}

	let span = lhs_span.extend(parser.last_span);
	let expr = parser.push(BinaryExpr {
		left,
		op: Spanned {
			value: operator,
			span: op_span,
		},
		right: rhs,
		span,
	});
	Ok(Some(Expr::Binary(expr)))
}

/// Parse a infix operator which has no associativity, i.e. it is undefined whether `a op b op c`
/// means `(a op b) op c` or `a op (b op c)`
///
/// Will first check if the operator has the right binding power to be parsed at this point, if it
/// does it will eat up to `EAT` tokens, expecting the code to have already checked if the operator
/// to be parsed is correct.
#[allow(clippy::too_many_arguments)]
async fn parse_non_associative_infix_op(
	parser: &mut Parser<'_, '_>,
	min_bp: u8,
	bp: u8,
	operator: BinaryOperator,
	is_same: impl FnOnce(&BinaryOperator) -> bool,
	span: Span,
	lhs: Expr,
	eat: usize,
) -> ParseResult<Option<Expr>> {
	if bp < min_bp {
		return Ok(None);
	}

	for _ in 0..eat {
		let Some(_) = parser.next()? else {
			unreachable!()
		};
	}

	let left = parser.push(lhs);
	let right = parse_pratt_enter(parser, bp).await?;

	if let Expr::Binary(x) = parser[right]
		&& is_same(&parser[x].op.value)
	{
		return Err(parser.with_error(|parser| {
			Level::Error
				.title(format!("Operator `{}` has no defined associativity", parser.slice(span)))
				.snippet(
					parser
						.snippet()
						.annotate(
							AnnotationKind::Primary
								.span(span)
								.label("Cover either this expression."),
						)
						.annotate(AnnotationKind::Context.span(parser[x].op.span).label(
							"Or this expression with `()` to define an order of operations",
						)),
				)
				.to_diagnostic()
		}));
	}

	let expr = parser.push(BinaryExpr {
		left,
		op: Spanned {
			value: operator,
			span,
		},
		right,
		span,
	});
	Ok(Some(Expr::Binary(expr)))
}

async fn parse_equality_op(
	parser: &mut Parser<'_, '_>,
	min_bp: u8,
	op: BinaryOperator,
	span: Span,
	lhs: Expr,
) -> ParseResult<Option<Expr>> {
	parse_non_associative_infix_op(parser, min_bp, EQUALITY_BP, op, is_equality_op, span, lhs, 1)
		.await
}

async fn parse_relation_op(
	parser: &mut Parser<'_, '_>,
	min_bp: u8,
	op: BinaryOperator,
	span: Span,
	lhs: Expr,
) -> ParseResult<Option<Expr>> {
	parse_non_associative_infix_op(parser, min_bp, RELATION_BP, op, is_relation_op, span, lhs, 1)
		.await
}

async fn parse_bracket_postfix(
	parser: &mut Parser<'_, '_>,
	min_bp: u8,
	lhs: Expr,
	lhs_span: Span,
) -> ParseResult<Option<Expr>> {
	if IDIOM_BP < min_bp {
		return Ok(None);
	}

	let Some(open_token) = parser.next()? else {
		unreachable!()
	};

	let peek = parser.peek_expect("*, $, ?, WHERE, or an expression")?;

	match peek.token {
		T![?] | T![WHERE] => {
			let _ = parser.next();

			let left = parser.push(lhs);
			let cond = parser.parse_enter().await?;
			let close =
				parser.expect_closing_delimiter(BaseTokenKind::CloseBracket, open_token.span)?;
			let op = IdiomOperator::Where(cond);
			let postfix = parser.push(IdiomExpr {
				left,
				op: Spanned {
					span: open_token.span.extend(close.span),
					value: op,
				},
				span: lhs_span.extend(close.span),
			});
			Ok(Some(Expr::Idiom(postfix)))
		}
		T![*] => {
			let _ = parser.next();

			let left = parser.push(lhs);
			let close =
				parser.expect_closing_delimiter(BaseTokenKind::CloseBracket, open_token.span)?;
			let postfix = parser.push(IdiomExpr {
				left,
				op: Spanned {
					span: open_token.span.extend(close.span),
					value: IdiomOperator::All,
				},
				span: lhs_span.extend(close.span),
			});
			Ok(Some(Expr::Idiom(postfix)))
		}
		T![$] => {
			let _ = parser.next();

			let left = parser.push(lhs);
			let close =
				parser.expect_closing_delimiter(BaseTokenKind::CloseBracket, open_token.span)?;
			let postfix = parser.push(IdiomExpr {
				left,
				op: Spanned {
					span: open_token.span.extend(close.span),
					value: IdiomOperator::Last,
				},
				span: lhs_span.extend(close.span),
			});
			Ok(Some(Expr::Idiom(postfix)))
		}
		_ => {
			let left = parser.push(lhs);
			let index = parser.parse_enter().await?;
			let _ = parser.peek();
			let close =
				parser.expect_closing_delimiter(BaseTokenKind::CloseBracket, open_token.span)?;
			let op = IdiomOperator::Index(index);
			let postfix = parser.push(IdiomExpr {
				left,
				op: Spanned {
					span: open_token.span.extend(close.span),
					value: op,
				},
				span: lhs_span.extend(close.span),
			});
			Ok(Some(Expr::Idiom(postfix)))
		}
	}
}

impl Parse for ast::Destructure {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.peek_span();
		let field = parser.parse_sync()?;

		let peek = parser.peek_expect("`}`")?;
		match peek.token {
			T![:] => {
				let _ = parser.next();
				let expr = parser.parse_enter().await?;

				Ok(ast::Destructure {
					field,
					op: Some(Spanned {
						value: ast::DestructureOperator::Expr(expr),
						span: parser.span_since(peek.span),
					}),
					span: parser.span_since(start),
				})
			}
			T![.] => {
				let Some(peek) = parser.peek_joined1()? else {
					return Err(parser.unexpected("`.*` or `.{`"));
				};

				match peek.token {
					T![*] => {
						let _ = parser.next();
						let _ = parser.next();

						Ok(ast::Destructure {
							field,
							op: Some(Spanned {
								value: ast::DestructureOperator::All,
								span: parser.span_since(peek.span),
							}),
							span: parser.span_since(start),
						})
					}
					BaseTokenKind::OpenBrace => {
						let _ = parser.next();
						let _ = parser.next();

						let mut head = None;
						let mut tail = None;
						loop {
							if parser.eat(BaseTokenKind::CloseBrace)?.is_some() {
								break;
							}

							let item = parser.parse_enter::<ast::Destructure>().await?;
							parser.push_list(item, &mut head, &mut tail);

							if parser.eat(T![,])?.is_none() {
								let _ = parser.expect_closing_delimiter(
									BaseTokenKind::CloseBrace,
									peek.span,
								)?;
								break;
							}
						}

						Ok(ast::Destructure {
							field,
							op: Some(Spanned {
								value: ast::DestructureOperator::Destructure(head),
								span: parser.span_since(peek.span),
							}),
							span: parser.span_since(start),
						})
					}
					_ => Err(parser.unexpected("`.*` or `.{`")),
				}
			}
			_ => Ok(ast::Destructure {
				field,
				op: None,
				span: start,
			}),
		}
	}
}

async fn parse_recurse(
	parser: &mut Parser<'_, '_>,
	dot_span: Span,
	brace_span: Span,
) -> ParseResult<ast::Recurse> {
	fn has_int(p: &mut Parser<'_, '_>) -> ParseResult<bool> {
		Ok(p.peek_joined()?.map(|x| x.token == BaseTokenKind::Int).unwrap_or(false))
	}

	let expected = "`*`, `..` or an integer";
	let peek = parser.peek_expect(expected)?;
	let range = match peek.token {
		T![*] => (Bound::Unbounded, Bound::Unbounded),
		T![..] => {
			let bound = parse_prefix_range_sync(parser, has_int)?;
			(Bound::Unbounded, bound)
		}
		BaseTokenKind::Int => {
			let start = parser.parse_sync()?;
			match try_parse_infix_range_sync(parser, start, has_int)? {
				TryRange::None(start) => (Bound::Included(start), Bound::Included(start)),
				TryRange::Some {
					start,
					end,
				} => (start, end),
			}
		}
		_ => return Err(parser.unexpected(expected)),
	};

	let kind = if parser.eat(T![+])?.is_some() {
		let expect = "`PATH`, `SHORTEST`, or `COLLECT`";
		let peek = parser.peek_expect(expect)?;
		match peek.token {
			T![PATH] => {
				let _ = parser.next();
				let inclusive = if parser.eat(T![+])?.is_some() {
					let _ = parser.expect(T![INCLUSIVE])?;
					true
				} else {
					false
				};
				Some(ast::RecurseKind::Path {
					inclusive,
				})
			}
			T![SHORTEST] => {
				let _ = parser.next();
				let _ = parser.expect(T![=])?;
				let expected = "a parameter or a record id";
				let expects = parser.peek_expect(expected)?;
				let expects = match expects.token {
					BaseTokenKind::Param => Expr::Param(parser.parse_sync()?),
					x if x.is_identifier() => Expr::RecordId(parser.parse().await?),
					_ => return Err(parser.unexpected(expected)),
				};
				let expects = parser.push(expects);

				let inclusive = if parser.eat(T![+])?.is_some() {
					let _ = parser.expect(T![INCLUSIVE])?;
					true
				} else {
					false
				};
				Some(ast::RecurseKind::Shortest {
					expects,
					inclusive,
				})
			}
			T![COLLECT] => {
				let _ = parser.next();
				let inclusive = if parser.eat(T![+])?.is_some() {
					let _ = parser.expect(T![INCLUSIVE])?;
					true
				} else {
					false
				};
				Some(ast::RecurseKind::Collect {
					inclusive,
				})
			}
			_ => return Err(parser.unexpected(expect)),
		}
	} else {
		None
	};

	let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseBrace, brace_span)?;

	let span = parser.span_since(dot_span);
	let push_span = parser.push(span);

	let paren = parser.eat(BaseTokenKind::OpenParen)?;

	let mut expr = Expr::Document(push_span);
	while let Some(new_expr) = parser
		.enter(async |parser| {
			try_parse_infix_postfix_op(parser, IDIOM_BP, Expr::Document(push_span), span).await
		})
		.await?
	{
		expr = new_expr
	}

	if let Some(paren) = paren {
		let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, paren.span)?;
	}

	let expr = parser.push(expr);

	let span = parser.span_since(dot_span);
	Ok(ast::Recurse {
		start: range.0,
		end: range.1,
		kind,
		expr,
		span,
	})
}

/// Parses all operators which start with `.{` .
async fn parse_dot_brace_postfix(
	parser: &mut Parser<'_, '_>,
	lhs: Expr,
	lhs_span: Span,
	dot_span: Span,
) -> ParseResult<Expr> {
	let brace_token = parser.expect(BaseTokenKind::OpenBrace)?;

	let peek = parser.peek_expect("`*`, `..`, an integer, or an identifier")?;
	match peek.token {
		T![*] | T![..] | BaseTokenKind::Int => {
			let left = parser.push(lhs);
			let recurse = parse_recurse(parser, dot_span, brace_token.span).await?;
			let recurse = parser.push(recurse);
			let op = IdiomOperator::Recurse(recurse);
			let expr = IdiomExpr {
				left,
				op: Spanned {
					value: op,
					span: parser.span_since(dot_span),
				},
				span: parser.span_since(lhs_span),
			};
			let expr = parser.push(expr);
			Ok(Expr::Idiom(expr))
		}
		BaseTokenKind::CloseBrace => {
			let _ = parser.next();
			let left = parser.push(lhs);
			let op = IdiomOperator::Destructure(None);
			let expr = IdiomExpr {
				left,
				op: Spanned {
					value: op,
					span: parser.span_since(dot_span),
				},
				span: parser.span_since(lhs_span),
			};
			let expr = parser.push(expr);
			Ok(Expr::Idiom(expr))
		}
		x if x.is_identifier() => {
			let left = parser.push(lhs);

			let mut head = None;
			let mut cur = None;
			loop {
				if parser.eat(BaseTokenKind::CloseBrace)?.is_some() {
					break;
				}

				let item = parser.parse::<ast::Destructure>().await?;
				parser.push_list(item, &mut head, &mut cur);

				if parser.eat(T![,])?.is_none() {
					let _ = parser
						.expect_closing_delimiter(BaseTokenKind::CloseBrace, brace_token.span)?;
					break;
				}
			}

			let op = IdiomOperator::Destructure(head);
			let expr = IdiomExpr {
				left,
				op: Spanned {
					value: op,
					span: parser.span_since(dot_span),
				},
				span: parser.span_since(lhs_span),
			};
			let expr = parser.push(expr);
			Ok(Expr::Idiom(expr))
		}
		_ => Err(parser.unexpected("`*`, `..` or an identifier")),
	}
}

/// Parses all operators which start with `.`
/// This function must be called after checking if this is correct for precedence order (which it
/// practically always is).
async fn parse_dot_postfix(
	parser: &mut Parser<'_, '_>,
	lhs: Expr,
	lhs_span: Span,
) -> ParseResult<Expr> {
	let dot_token = parser.expect(T![.])?;
	let peek = parser.peek_expect("*, ?, .@, {, or an identifier")?;

	fn reject_seperated(
		parser: &mut Parser<'_, '_>,
		dot_span: Span,
		joined: Joined,
	) -> ParseResult<()> {
		let Joined::Seperated = joined else {
			return Ok(());
		};

		Err(parser.with_error(|parser| {
			Level::Error
				.title("Unexpected token `.` expected `.*`, `..`, `.?`, `.{`, `.@`")
				.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(dot_span)))
				.to_diagnostic()
		}))
	}

	match peek.token {
		// TODO: Make these token kinds?
		T![*] => {
			reject_seperated(parser, dot_token.span, peek.joined)?;
			let _ = parser.next();

			let left = parser.push(lhs);
			let idiom = parser.push(IdiomExpr {
				left,
				op: Spanned {
					span: dot_token.span.extend(peek.span),
					value: IdiomOperator::All,
				},
				span: lhs_span.extend(peek.span),
			});
			Ok(Expr::Idiom(idiom))
		}
		T![?] => {
			reject_seperated(parser, dot_token.span, peek.joined)?;
			let _ = parser.next();

			let left = parser.push(lhs);
			let idiom = parser.push(IdiomExpr {
				left,
				op: Spanned {
					span: dot_token.span.extend(peek.span),
					value: IdiomOperator::Option,
				},
				span: lhs_span.extend(peek.span),
			});
			Ok(Expr::Idiom(idiom))
		}
		T![@] => {
			reject_seperated(parser, dot_token.span, peek.joined)?;
			let _ = parser.next();

			let left = parser.push(lhs);
			let idiom = parser.push(IdiomExpr {
				left,
				op: Spanned {
					span: dot_token.span.extend(peek.span),
					value: IdiomOperator::Repeat,
				},
				span: lhs_span.extend(peek.span),
			});
			Ok(Expr::Idiom(idiom))
		}
		BaseTokenKind::OpenBrace => {
			reject_seperated(parser, dot_token.span, peek.joined)?;

			parse_dot_brace_postfix(parser, lhs, lhs_span, dot_token.span).await
		}
		x if x.is_identifier() => {
			let left = parser.push(lhs);
			let field = parser.parse_sync()?;
			let idiom = parser.push(IdiomExpr {
				left,
				op: Spanned {
					span: dot_token.span.extend(peek.span),
					value: IdiomOperator::Field(field),
				},
				span: lhs_span.extend(peek.span),
			});
			Ok(Expr::Idiom(idiom))
		}
		_ => Err(parser.with_error(|parser| {
			Level::Error
				.title(format!(
					"Unexpected token `{}` expected *, ?, .@, {{, or an identifier",
					parser.slice(peek.span)
				))
				.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(peek.span)))
				.to_diagnostic()
		})),
	}
}

/// # Panic
///
/// This function might panic if there is a peeked token. Called must ensure that no token is
/// peeked when calling this function.
async fn parse_lookup_range(parser: &mut Parser<'_, '_>) -> ParseResult<RecordIdKeyRange> {
	assert!(
		!parser.lex.has_peek(),
		"lexing record-id-keys requires that parser has no peeked tokens"
	);

	let Some(peek) = peek_record_id_token(parser)? else {
		return Err(parser.unexpected("a record-id key"));
	};
	match peek.token {
		T![..] => {
			let _ = parser.next();
			let bound = parse_record_id_headless_range(parser).await?;
			let range_span = parser.span_since(peek.span);

			Ok(ast::RecordIdKeyRange {
				start: Bound::Unbounded,
				end: bound.map(|x| parser.push(x)),

				span: range_span,
			})
		}
		_ => {
			let key = parse_peeked_record_id_key(parser).await?;
			match try_parse_record_id_range(parser, key).await? {
				TryRange::None(_) => Err(parser.unexpected("a record id key range")),
				TryRange::Some {
					start,
					end,
				} => {
					let range_span = parser.span_since(peek.span);
					Ok(ast::RecordIdKeyRange {
						start: start.map(|x| parser.push(x)),
						end: end.map(|x| parser.push(x)),
						span: range_span,
					})
				}
			}
		}
	}
}

impl Parse for ast::LookupSubject {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start_span = parser.peek_span();
		let table = parser.parse_sync()?;
		if let Some(peek) = parser.peek()?
			&& let T![:] = peek.token
		{
			let _ = parser.next();

			let range = parse_lookup_range(parser).await?;
			let range = parser.push(range);

			let field = if let Some(peek) = parser.peek()?
				&& let T![FIELD] = peek.token
			{
				let _ = parser.next();
				Some(parser.parse_sync()?)
			} else {
				None
			};

			let span = parser.span_since(start_span);
			Ok(ast::LookupSubject::Range(ast::LookupSubjectRange {
				range,
				table,
				field,
				span,
			}))
		} else {
			let span = parser.span_since(start_span);

			let field = if let Some(peek) = parser.peek()?
				&& let T![FIELD] = peek.token
			{
				let _ = parser.next();
				Some(parser.parse_sync()?)
			} else {
				None
			};

			Ok(ast::LookupSubject::Table(ast::LookupSubjectTable {
				table,
				field,
				span,
			}))
		}
	}
}

async fn parse_lookup_from(
	parser: &mut Parser<'_, '_>,
) -> ParseResult<Option<NodeListId<LookupSubject>>> {
	let expect = "`?` or an identifier";
	let peek = parser.peek_expect(expect)?;
	match peek.token {
		T![?] => {
			let _ = parser.next();
			Ok(None)
		}
		x if x.is_identifier() => {
			Ok(Some(parse_seperated_list(parser, T![,], Parser::parse).await?.1))
		}
		_ => Err(parser.unexpected(expect)),
	}
}

async fn parse_lookup_limit_start(
	parser: &mut Parser<'_, '_>,
) -> ParseResult<(Option<NodeId<Expr>>, Option<NodeId<Expr>>)> {
	if parser.eat(T![START])?.is_some() {
		let start = parser.parse_enter().await?;
		let limit = if parser.eat(T![LIMIT])?.is_some() {
			Some(parser.parse_enter().await?)
		} else {
			None
		};
		Ok((limit, Some(start)))
	} else {
		let limit = if parser.eat(T![LIMIT])?.is_some() {
			Some(parser.parse_enter().await?)
		} else {
			None
		};
		let start = if parser.eat(T![START])?.is_some() {
			Some(parser.parse_enter().await?)
		} else {
			None
		};
		Ok((limit, start))
	}
}

impl Parse for ast::Lookup {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let expect = "`?`, `(` or an identifier";
		let peek = parser.peek_expect(expect)?;
		match peek.token {
			T![?] => {
				let _ = parser.next();
				Ok(ast::Lookup::Any(peek.span))
			}
			BaseTokenKind::OpenParen => {
				let _ = parser.next();
				if parser.eat(T![SELECT])?.is_some() {
					let fields = parser.parse().await?;

					let _ = parser.expect(T![FROM])?;

					let from = parse_lookup_from(parser).await?;
					let condition = if parser.eat(T![WHERE])?.is_some() {
						Some(parser.parse_enter().await?)
					} else {
						None
					};

					let split_span = parser.peek_span();
					let split = if parser.eat(T![SPLIT])?.is_some() {
						let _ = parser.eat(T![ON])?;
						let (_, splits) = parse_seperated_list(parser, T![,], async |parser| {
							parser.parse_enter().await
						})
						.await?;
						Some(splits)
					} else {
						None
					};
					let split_span = parser.span_since(split_span);

					let group = if let Some(x) = parser.eat(T![GROUP])? {
						if parser.eat(T![ALL])?.is_some() {
							Some(ast::Group::All)
						} else {
							if split.is_some() {
								return Err(parser.with_error(|parser|{
									let title =format!("Unexpected token `{}`, selects cannot both have a `GROUP BY` clause and a `SPLIT ON` clause",parser.slice(x.span));
									Level::Error.
										title(title)
											.snippet(parser.snippet()
												.annotate(AnnotationKind::Primary.span(x.span))
												.annotate(AnnotationKind::Context.span(split_span).label("Previous `SPLIT ON` clause"))
											).to_diagnostic()
								}));
							}

							let _ = parser.expect(T![BY])?;
							let (_, groups) = parse_seperated_list(parser, T![,], async |parser| {
								parser.parse_enter().await
							})
							.await?;
							Some(ast::Group::Fields(groups))
						}
					} else {
						None
					};

					let order = if let Some(x) = parser.peek()?
						&& let T![ORDER] = x.token
					{
						Some(parser.parse().await?)
					} else {
						None
					};

					let (limit, start) = parse_lookup_limit_start(parser).await?;

					let alias = if parser.eat(T![AS])?.is_some() {
						Some(parser.parse().await?)
					} else {
						None
					};

					let _ =
						parser.expect_closing_delimiter(BaseTokenKind::CloseParen, peek.span)?;

					let span = parser.span_since(peek.span);
					Ok(ast::Lookup::Select(ast::SelectLookup {
						fields,
						from,
						condition,
						split,
						group,
						order,
						limit,
						start,
						span,
						alias,
					}))
				} else {
					let from = parse_lookup_from(parser).await?;
					let condition = if parser.eat(T![WHERE])?.is_some() {
						Some(parser.parse_enter().await?)
					} else {
						None
					};

					let (limit, start) = parse_lookup_limit_start(parser).await?;

					let alias = if parser.eat(T![AS])?.is_some() {
						Some(parser.parse().await?)
					} else {
						None
					};

					let _ =
						parser.expect_closing_delimiter(BaseTokenKind::CloseParen, peek.span)?;

					let span = parser.span_since(peek.span);
					Ok(ast::Lookup::Basic(ast::BasicLookup {
						from,
						condition,
						limit,
						start,
						span,
						alias,
					}))
				}
			}
			x if x.is_identifier() => {
				let ident = parser.parse_sync()?;

				if let Some(peek) = parser.peek()?
					&& let T![:] = peek.token
				{
					let _ = parser.next();

					// parse_lookup_range requires an empty peek buffer, otherwise it cannot parse
					// the record id correctly.
					assert!(
						!parser.lex.has_peek(),
						"cannot peek past a `:` signifying a record id"
					);

					let range = parse_lookup_range(parser).await?;
					let range = parser.push(range);
					let span = parser.span_since(peek.span);
					let subject = parser.push(ast::LookupSubject::Range(ast::LookupSubjectRange {
						table: ident,
						range,
						field: None,
						span,
					}));
					Ok(ast::Lookup::Subject(subject))
				} else {
					let span = parser.span_since(peek.span);
					let subject = parser.push(ast::LookupSubject::Table(ast::LookupSubjectTable {
						table: ident,
						field: None,
						span,
					}));
					Ok(ast::Lookup::Subject(subject))
				}
			}
			_ => Err(parser.unexpected(expect)),
		}
	}
}

/// Function which implements the parsing for infix operators, postfix operates and idiom operators
/// (which are just postfix operators).
///
/// Returns None if parsing the next operator would violate precedence, or if the next token does
/// not start an operator.
async fn try_parse_infix_postfix_op(
	parser: &mut Parser<'_, '_>,
	min_bp: u8,
	lhs: Expr,
	lhs_span: Span,
) -> ParseResult<Option<Expr>> {
	let Some(peek) = parser.peek()? else {
		return Ok(None);
	};

	match peek.token {
		T![&&] | T![AND] => {
			parse_basic_infix_op(parser, min_bp, AND_BP, BinaryOperator::And, peek.span, lhs).await
		}
		T![||] | T![OR] => {
			parse_basic_infix_op(parser, min_bp, OR_BP, BinaryOperator::Or, peek.span, lhs).await
		}
		T![+] => {
			parse_basic_infix_op(parser, min_bp, SUM_BP, BinaryOperator::Add, peek.span, lhs).await
		}
		T![-] => {
			parse_basic_infix_op(parser, min_bp, SUM_BP, BinaryOperator::Subtract, peek.span, lhs)
				.await
		}
		BaseTokenKind::Times | T![*] => {
			parse_basic_infix_op(
				parser,
				min_bp,
				PRODUCT_BP,
				BinaryOperator::Multiply,
				peek.span,
				lhs,
			)
			.await
		}
		BaseTokenKind::Divide | T![/] => {
			parse_basic_infix_op(parser, min_bp, PRODUCT_BP, BinaryOperator::Divide, peek.span, lhs)
				.await
		}
		T![%] => {
			parse_basic_infix_op(
				parser,
				min_bp,
				PRODUCT_BP,
				BinaryOperator::Remainder,
				peek.span,
				lhs,
			)
			.await
		}
		T![**] => {
			parse_basic_infix_op(parser, min_bp, POWER_BP, BinaryOperator::Power, peek.span, lhs)
				.await
		}
		T![=] => parse_equality_op(parser, min_bp, BinaryOperator::Equal, peek.span, lhs).await,
		T![==] => {
			parse_equality_op(parser, min_bp, BinaryOperator::ExactEqual, peek.span, lhs).await
		}
		T![!=] => parse_equality_op(parser, min_bp, BinaryOperator::NotEqual, peek.span, lhs).await,
		T![*=] => parse_equality_op(parser, min_bp, BinaryOperator::AllEqual, peek.span, lhs).await,
		T![?=] => parse_equality_op(parser, min_bp, BinaryOperator::AnyEqual, peek.span, lhs).await,
		T![?:] => {
			parse_basic_infix_op(
				parser,
				min_bp,
				NULLISH_BP,
				BinaryOperator::TenaryCondition,
				peek.span,
				lhs,
			)
			.await
		}
		T![??] => {
			parse_basic_infix_op(
				parser,
				min_bp,
				NULLISH_BP,
				BinaryOperator::NullCoalescing,
				peek.span,
				lhs,
			)
			.await
		}
		T![<] => {
			if let Some(peek1) = parser.peek_joined1()?
				&& let T![-] | T![->] = peek1.token
			{
				if IDIOM_BP < min_bp {
					return Ok(None);
				}

				let dir = match peek1.token {
					T![-] => ast::Direction::In,
					T![->] => ast::Direction::Both,
					_ => unreachable!(),
				};

				let _ = parser.next();
				let _ = parser.next();

				let lhs = parser.push(lhs);

				let lookup = parser.parse().await?;
				let op_span = parser.span_since(peek.span);

				let op = IdiomOperator::Graph {
					direction: dir,
					lookup,
				};
				let expr = IdiomExpr {
					left: lhs,
					op: Spanned {
						value: op,
						span: op_span,
					},
					span: parser.span_since(lhs_span),
				};
				let expr = Expr::Idiom(parser.push(expr));
				return Ok(Some(expr));
			}

			parse_relation_op(parser, min_bp, BinaryOperator::LessThan, peek.span, lhs).await
		}
		T![->] => {
			if IDIOM_BP < min_bp {
				return Ok(None);
			}
			let _ = parser.next();
			let lhs = parser.push(lhs);

			let lookup = parser.parse().await?;
			let op_span = parser.span_since(peek.span);

			let op = IdiomOperator::Graph {
				direction: ast::Direction::Out,
				lookup,
			};
			let expr = IdiomExpr {
				left: lhs,
				op: Spanned {
					value: op,
					span: op_span,
				},
				span: parser.span_since(lhs_span),
			};
			let expr = Expr::Idiom(parser.push(expr));
			Ok(Some(expr))
		}
		T![<=] => {
			parse_relation_op(parser, min_bp, BinaryOperator::LessThanEqual, peek.span, lhs).await
		}
		T![>] => {
			// Handle `>` `>..` and `>..=`
			if let Some(peek1) = parser.peek_joined1()?
				&& let T![..] = peek1.token
			{
				if let Some(peek2) = parser.peek_joined2()?
					&& let T![=] = peek2.token
				{
					if RANGE_BP < min_bp {
						return Ok(None);
					}

					let _ = parser.next();
					let _ = parser.next();
					let _ = parser.next();

					let span = peek.span.extend(peek2.span);

					return parse_range_infix_op(
						parser,
						BinaryOperator::RangeSkipInclusive,
						lhs,
						lhs_span,
						span,
					)
					.await;
				}

				if RANGE_BP < min_bp {
					return Ok(None);
				}

				let _ = parser.next();
				let _ = parser.next();

				let span = peek.span.extend(peek1.span);

				if peek_starts_prime(parser)? {
					return parse_range_infix_op(
						parser,
						BinaryOperator::RangeSkip,
						lhs,
						lhs_span,
						span,
					)
					.await;
				} else {
					let lhs = parser.push(lhs);
					let expr = parser.push(PostfixExpr {
						left: lhs,
						op: Spanned {
							value: PostfixOperator::RangeSkip,
							span,
						},
						span: lhs_span,
					});
					return Ok(Some(Expr::Postfix(expr)));
				}
			}

			parse_relation_op(parser, min_bp, BinaryOperator::GreaterThan, peek.span, lhs).await
		}
		T![<~] => {
			if IDIOM_BP < min_bp {
				return Ok(None);
			}
			let _ = parser.next();
			let lhs = parser.push(lhs);

			let lookup = parser.parse().await?;
			let op_span = parser.span_since(peek.span);

			let op = IdiomOperator::Reference(lookup);
			let expr = IdiomExpr {
				left: lhs,
				op: Spanned {
					value: op,
					span: op_span,
				},
				span: parser.span_since(lhs_span),
			};
			let expr = Expr::Idiom(parser.push(expr));
			Ok(Some(expr))
		}
		T![<|] => {
			if RELATION_BP < min_bp {
				return Ok(None);
			}

			let _ = parser.next();
			let k = parser.parse_sync()?;
			let op = if parser.eat(T![,])?.is_some() {
				let expect = "a distance or an integer";
				let peek = parser.peek_expect(expect)?;
				match peek.token {
					T![CHEBYSHEV] => {
						let _ = parser.next();
						BinaryOperator::KNearestNeighbour {
							k,
							distance: ast::Distance::Chebyshev,
						}
					}
					T![COSINE] => {
						let _ = parser.next();
						BinaryOperator::KNearestNeighbour {
							k,
							distance: ast::Distance::Cosine,
						}
					}
					T![EUCLIDEAN] => {
						let _ = parser.next();
						BinaryOperator::KNearestNeighbour {
							k,
							distance: ast::Distance::Euclidean,
						}
					}
					T![HAMMING] => {
						let _ = parser.next();
						BinaryOperator::KNearestNeighbour {
							k,
							distance: ast::Distance::Hamming,
						}
					}
					T![JACCARD] => {
						let _ = parser.next();
						BinaryOperator::KNearestNeighbour {
							k,
							distance: ast::Distance::Jaccard,
						}
					}
					T![MANHATTAN] => {
						let _ = parser.next();
						BinaryOperator::KNearestNeighbour {
							k,
							distance: ast::Distance::Manhattan,
						}
					}
					T![MINKOWSKI] => {
						let _ = parser.next();
						let v = parser.parse_sync()?;
						BinaryOperator::KNearestNeighbour {
							k,
							distance: ast::Distance::Minkowski(v),
						}
					}
					T![PEARSON] => {
						let _ = parser.next();
						BinaryOperator::KNearestNeighbour {
							k,
							distance: ast::Distance::Pearson,
						}
					}
					BaseTokenKind::Int => {
						let ef = parser.parse_sync()?;
						BinaryOperator::KApproximate {
							k,
							ef,
						}
					}
					_ => return Err(parser.unexpected(expect)),
				}
			} else {
				BinaryOperator::KTree {
					k,
				}
			};

			let _ = parser.expect_closing_delimiter(T![|>], peek.span)?;
			let span = parser.span_since(peek.span);

			parse_non_associative_infix_op(
				parser,
				min_bp,
				RELATION_BP,
				op,
				is_relation_op,
				span,
				lhs,
				0,
			)
			.await
		}
		T![>=] => {
			parse_relation_op(parser, min_bp, BinaryOperator::GreaterThanEqual, peek.span, lhs)
				.await
		}
		T![@] => {
			if EQUALITY_BP < min_bp {
				return Ok(None);
			}
			let _ = parser.next();
			let expect = "`AND`, `OR` an integer or `@`";
			let peek = parser.peek_expect(expect)?;
			let (span, op) = match peek.token {
				BaseTokenKind::Int => {
					let int = parser.parse_sync()?;
					let operator = if parser.eat(T![,])?.is_some() {
						let expect = "`AND` or`OR`";
						let peek = parser.peek_expect(expect)?;
						match peek.token {
							T![AND] => {
								let _ = parser.next();
								Some(ast::MatchesOperator::And)
							}
							T![OR] => {
								let _ = parser.next();
								Some(ast::MatchesOperator::Or)
							}
							_ => return Err(parser.unexpected(expect)),
						}
					} else {
						None
					};
					let _ = parser.expect(T![@])?;
					let span = parser.span_since(peek.span);
					(
						span,
						ast::BinaryOperator::Matches {
							reference: Some(int),
							operator,
						},
					)
				}
				T![OR] => {
					let _ = parser.next();
					let _ = parser.expect(T![@])?;
					let span = parser.span_since(peek.span);
					(
						span,
						ast::BinaryOperator::Matches {
							reference: None,
							operator: Some(ast::MatchesOperator::Or),
						},
					)
				}
				T![AND] => {
					let _ = parser.next();
					let _ = parser.expect(T![@])?;
					let span = parser.span_since(peek.span);
					(
						span,
						ast::BinaryOperator::Matches {
							reference: None,
							operator: Some(ast::MatchesOperator::And),
						},
					)
				}
				T![@] => {
					let _ = parser.next();
					let span = parser.span_since(peek.span);
					(
						span,
						ast::BinaryOperator::Matches {
							reference: None,
							operator: None,
						},
					)
				}
				_ => return Err(parser.unexpected(expect)),
			};

			parse_non_associative_infix_op(
				parser,
				min_bp,
				RELATION_BP,
				op,
				is_relation_op,
				span,
				lhs,
				0,
			)
			.await
		}
		BaseTokenKind::Contains | T![CONTAINS] => {
			parse_relation_op(parser, min_bp, BinaryOperator::Contain, peek.span, lhs).await
		}
		BaseTokenKind::NotContains | T![CONTAINSNOT] => {
			parse_relation_op(parser, min_bp, BinaryOperator::NotContain, peek.span, lhs).await
		}
		BaseTokenKind::Inside | T![INSIDE] | T![IN] => {
			parse_relation_op(parser, min_bp, BinaryOperator::Inside, peek.span, lhs).await
		}
		BaseTokenKind::NotInside | T![NOTINSIDE] => {
			parse_relation_op(parser, min_bp, BinaryOperator::NotInside, peek.span, lhs).await
		}
		BaseTokenKind::ContainsAll | T![CONTAINSALL] => {
			parse_relation_op(parser, min_bp, BinaryOperator::ContainAll, peek.span, lhs).await
		}
		BaseTokenKind::ContainsAny | T![CONTAINSANY] => {
			parse_relation_op(parser, min_bp, BinaryOperator::ContainAny, peek.span, lhs).await
		}
		BaseTokenKind::ContainsNone | T![CONTAINSNONE] => {
			parse_relation_op(parser, min_bp, BinaryOperator::ContainNone, peek.span, lhs).await
		}
		BaseTokenKind::AllInside | T![ALLINSIDE] => {
			parse_relation_op(parser, min_bp, BinaryOperator::AllInside, peek.span, lhs).await
		}
		BaseTokenKind::AnyInside | T![ANYINSIDE] => {
			parse_relation_op(parser, min_bp, BinaryOperator::AnyInside, peek.span, lhs).await
		}
		BaseTokenKind::NoneInside | T![NONEINSIDE] => {
			parse_relation_op(parser, min_bp, BinaryOperator::NoneInside, peek.span, lhs).await
		}
		T![OUTSIDE] => {
			parse_relation_op(parser, min_bp, BinaryOperator::Outside, peek.span, lhs).await
		}
		T![INTERSECTS] => {
			parse_relation_op(parser, min_bp, BinaryOperator::Intersects, peek.span, lhs).await
		}
		T![NOT] => {
			if let Some(peek1) = parser.peek1()?
				&& let T![IN] = peek1.token
			{
				let span = peek.span.extend(peek1.span);
				parse_non_associative_infix_op(
					parser,
					min_bp,
					RELATION_BP,
					BinaryOperator::NotInside,
					is_relation_op,
					span,
					lhs,
					2,
				)
				.await
			} else {
				Err(parser.with_error(|parser| {
					Level::Error
						.title(format!(
							"Unexpected token `{}` expected `NOT` to be followed by `IN`",
							parser.slice(peek.span)
						))
						.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(peek.span)))
						.to_diagnostic()
				}))
			}
		}
		T![IS] => {
			if let Some(peek1) = parser.peek1()?
				&& let T![NOT] = peek1.token
			{
				let span = peek.span.extend(peek1.span);
				parse_non_associative_infix_op(
					parser,
					min_bp,
					EQUALITY_BP,
					BinaryOperator::NotEqual,
					is_relation_op,
					span,
					lhs,
					2,
				)
				.await
			} else {
				parse_equality_op(parser, min_bp, BinaryOperator::Equal, peek.span, lhs).await
			}
		}
		T![...] => {
			if IDIOM_BP < min_bp {
				return Ok(None);
			}

			let _ = parser.next();

			let lhs = parser.push(lhs);
			let span = parser.span_since(lhs_span);
			let expr = parser.push(IdiomExpr {
				left: lhs,
				op: Spanned {
					value: IdiomOperator::Flatten,
					span: peek.span,
				},
				span,
			});
			Ok(Some(Expr::Idiom(expr)))
		}
		T![..] => {
			if RANGE_BP < min_bp {
				return Ok(None);
			}

			if let Some(peek1) = parser.peek_joined1()?
				&& let T![=] = peek1.token
			{
				let _ = parser.next();
				let _ = parser.next();

				let op_span = peek.span.extend(peek1.span);
				return parse_range_infix_op(
					parser,
					BinaryOperator::RangeInclusive,
					lhs,
					lhs_span,
					op_span,
				)
				.await;
			}

			let _ = parser.next();

			if peek_starts_prime(parser)? {
				return parse_range_infix_op(
					parser,
					BinaryOperator::Range,
					lhs,
					lhs_span,
					peek.span,
				)
				.await;
			}

			let lhs = parser.push(lhs);
			let span = parser.span_since(lhs_span);
			let expr = parser.push(PostfixExpr {
				left: lhs,
				op: Spanned {
					value: PostfixOperator::Range,
					span: peek.span,
				},
				span,
			});
			Ok(Some(Expr::Postfix(expr)))
		}
		T![.] => {
			if IDIOM_BP < min_bp {
				return Ok(None);
			}

			parse_dot_postfix(parser, lhs, lhs_span).await.map(Some)
		}
		BaseTokenKind::OpenBracket => parse_bracket_postfix(parser, min_bp, lhs, lhs_span).await,
		BaseTokenKind::OpenParen => {
			if IDIOM_BP < min_bp {
				return Ok(None);
			}

			let _ = parser.next();

			let lhs = parser.push(lhs);

			let mut head = None;
			let mut tail = None;
			loop {
				if parser.eat(BaseTokenKind::CloseParen)?.is_some() {
					break;
				}

				let arg = parser.parse_enter().await?;
				parser.push_list(arg, &mut head, &mut tail);

				if parser.eat(T![,])?.is_none() {
					let _ =
						parser.expect_closing_delimiter(BaseTokenKind::CloseParen, peek.span)?;
					break;
				}
			}

			let op_span = parser.span_since(peek.span);
			let span = parser.span_since(lhs_span);
			let idiom = parser.push(IdiomExpr {
				left: lhs,
				op: Spanned {
					value: IdiomOperator::Call(head),
					span: op_span,
				},
				span,
			});

			Ok(Some(Expr::Idiom(idiom)))
		}
		_ => Ok(None),
	}
}

/// Main fuction dispatching the parsing of operators, uses pratt parsing based on binding power to
/// ensure operators are parsed immediatly with correct precedence.
async fn parse_pratt(parser: &mut Parser<'_, '_>, bp: u8) -> ParseResult<Expr> {
	let span = parser.peek_span();
	let mut lhs = parse_prefix_or_prime(parser).await?;

	while let Some(new_lhs) = try_parse_infix_postfix_op(parser, bp, lhs, span).await? {
		lhs = new_lhs;
	}

	Ok(lhs)
}
