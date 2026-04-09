use token::T;

use crate::parse::ParseResult;
use crate::parse::kind::parse_bare_type;
use crate::{Parse, ParseSync, Parser};

impl Parse for ast::Parameter {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.peek_span();
		let name = parser.parse_sync()?;
		let ty = if parser.eat(T![:])?.is_some() {
			let ty = parse_bare_type(parser).await?;
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
