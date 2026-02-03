use ast::{Expr, PrefixExpr, PrefixOperator};
use token::T;

use crate::parse::{Parse, ParseResult, prime::parse_prime};

use super::Parser;

const BASE_BP: u8 = 0;
const PREFIX_BP: u8 = 10;

impl Parse for ast::Expr {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		parse_pratt(parser, BASE_BP).await
	}
}

async fn try_parse_prefix_op(parser: &mut Parser<'_, '_>) -> ParseResult<Expr> {
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
		T![<] => match parser.peek1()?.map(|x| x.token) {
			Some(T![-] | T![->]) => todo!(),
			_ => todo!(),
		},
		T![..] => todo!(),
		_ => {
			let prime = parse_prime(parser).await?;
			return Ok(prime);
		}
	};

	let expr = parser.enter(async |parser| parse_pratt(parser, PREFIX_BP).await).await?;
	let expr = parser.push(expr);
	let span = parser.span_since(token.span);
	let expr = parser.push(PrefixExpr {
		op,
		left: expr,
		span,
	});
	Ok(Expr::Prefix(expr))
}

async fn parse_pratt(parser: &mut Parser<'_, '_>, bp: u8) -> ParseResult<Expr> {
	return try_parse_prefix_op(parser).await;
}
