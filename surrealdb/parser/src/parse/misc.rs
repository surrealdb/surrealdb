use token::T;

use crate::parse::ParseResult;
use crate::parse::kind::{parse_bare_type, parse_closure_bare_type};
use crate::{Parse, ParseSync, Parser};

impl Parse for ast::Parameter {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		parse_parameter(parser, false).await
	}
}

/// Parse a closure parameter, whose kind is closed by the parameter list's `|`.
pub async fn parse_closure_parameter(parser: &mut Parser<'_, '_>) -> ParseResult<ast::Parameter> {
	parse_parameter(parser, true).await
}

async fn parse_parameter(
	parser: &mut Parser<'_, '_>,
	closure: bool,
) -> ParseResult<ast::Parameter> {
	let start = parser.peek_span();
	let name = parser.parse_sync()?;
	let ty = if parser.eat(T![:])?.is_some() {
		let ty = if closure {
			parse_closure_bare_type(parser).await?
		} else {
			parse_bare_type(parser).await?
		};
		Some(parser.push(ty))
	} else {
		None
	};
	let span = parser.span_since(start);
	Ok(ast::Parameter {
		name,
		ty,
		span,
	})
}

impl ParseSync for ast::Base {
	fn parse_sync(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let peek = parser.peek_expect("`NAMESPACE`, `DATABASE`, or `ROOT`")?;
		let base = match peek.token {
			T![NAMESPACE] => ast::Base::Namespace,
			T![DATABASE] => ast::Base::Database,
			T![ROOT] => ast::Base::Root,
			_ => return Err(parser.unexpected("`NAMESPACE`, `DATABASE`, or `ROOT`")),
		};
		let _ = parser.next();
		Ok(base)
	}
}
