use ast::{
	BinaryExpr, BinaryOperator, Expr, IdiomExpr, IdiomOperator, NodeId, PostfixExpr,
	PostfixOperator, PrefixExpr, PrefixOperator, Spanned,
};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use token::{BaseTokenKind, Joined, T};

use super::Parser;
use crate::parse::peek::peek_starts_prime;
use crate::parse::prime::parse_prime;
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

const RANGE_BP: u8 = 7;

const PREFIX_BP: u8 = 14;
const IDIOM_BP: u8 = 15;

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
			Some(T![-] | T![->]) => return parser.todo(),
			_ => {
				let _ = parser.next();
				let ty = parser.parse_push().await?;
				let _ = parser.expect_closing_delimiter(T![>], token.span)?;

				PrefixOperator::Cast(ty)
			}
		},
		T![..] => {
			let _ = parser.next();
			if parser.eat_joined(T![=])?.is_some() {
				PrefixOperator::RangeInclusive(parser.span_since(token.span))
			} else {
				PrefixOperator::Range(token.span)
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

enum ParseFlow {
	Continue,
	Break,
	Next,
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
			let cond = parser.parse_enter_push().await?;
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
			let _ = parser.next();

			let left = parser.push(lhs);
			let index = parser.parse_enter_push().await?;
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
		let field = parser.parse_sync_push()?;

		let peek = parser.peek_expect("`}`")?;
		match peek.token {
			T![:] => {
				let _ = parser.next();
				let expr = parser.parse_enter_push().await?;

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

						return Ok(ast::Destructure {
							field,
							op: Some(Spanned {
								value: ast::DestructureOperator::All,
								span: parser.span_since(peek.span),
							}),
							span: parser.span_since(start),
						});
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

						return Ok(ast::Destructure {
							field,
							op: Some(Spanned {
								value: ast::DestructureOperator::Destructure(head),
								span: parser.span_since(peek.span),
							}),
							span: parser.span_since(start),
						});
					}
					_ => {
						return Err(parser.unexpected("`.*` or `.{`"));
					}
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

async fn parse_dot_brace_postfix(
	parser: &mut Parser<'_, '_>,
	lhs: Expr,
	lhs_span: Span,
	dot_span: Span,
) -> ParseResult<Expr> {
	let brace_token = parser.expect(BaseTokenKind::OpenBrace)?;

	let peek = parser.peek_expect("`*`, `..` or an identifier")?;
	match peek.token {
		T![*] => parser.todo(),
		T![..] => parser.todo(),
		BaseTokenKind::Int => parser.todo(),
		x @ BaseTokenKind::CloseBrace | x if x.is_identifier() => {
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

async fn parse_dot_postfix(
	parser: &mut Parser<'_, '_>,
	lhs: Expr,
	lhs_span: Span,
) -> ParseResult<Option<Expr>> {
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
			Ok(Some(Expr::Idiom(idiom)))
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
			Ok(Some(Expr::Idiom(idiom)))
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
			Ok(Some(Expr::Idiom(idiom)))
		}
		BaseTokenKind::OpenBrace => {
			reject_seperated(parser, dot_token.span, peek.joined)?;

			parse_dot_brace_postfix(parser, lhs, lhs_span, dot_token.span).await.map(Some)
		}
		x if x.is_identifier() => {
			let _ = parser.next();
			let left = parser.push(lhs);
			let slice = parser.slice(peek.span).to_owned();
			let field = parser.push_set(slice);
			let idiom = parser.push(IdiomExpr {
				left,
				op: Spanned {
					span: dot_token.span.extend(peek.span),
					value: IdiomOperator::Field(field),
				},
				span: lhs_span.extend(peek.span),
			});
			Ok(Some(Expr::Idiom(idiom)))
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
		T![<] => {
			if let Some(T![-] | T![->]) = parser.peek_joined1()?.map(|x| x.token) {
				return Ok(None);
			}

			parse_relation_op(parser, min_bp, BinaryOperator::LessThan, peek.span, lhs).await
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
		T![>=] => {
			parse_relation_op(parser, min_bp, BinaryOperator::GreaterThanEqual, peek.span, lhs)
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
				return Err(parser.with_error(|parser| {
					Level::Error
						.title(format!(
							"Unexpected token `{}` expected `NOT` to be followed by `IN`",
							parser.slice(peek.span)
						))
						.snippet(parser.snippet().annotate(AnnotationKind::Primary.span(peek.span)))
						.to_diagnostic()
				}));
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

			parse_dot_postfix(parser, lhs, lhs_span).await
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

async fn parse_pratt(parser: &mut Parser<'_, '_>, bp: u8) -> ParseResult<Expr> {
	let span = parser.peek_span();
	let mut lhs = parse_prefix_or_prime(parser).await?;

	while let Some(new_lhs) = try_parse_infix_postfix_op(parser, bp, lhs, span).await? {
		lhs = new_lhs;
	}

	Ok(lhs)
}
