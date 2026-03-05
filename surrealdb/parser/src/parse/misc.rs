use token::T;

use crate::parse::ParseResult;
use crate::{Parse, Parser};

impl Parse for ast::Parameter {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.peek_span();
		let name = parser.parse_sync()?;
		let _ = parser.expect(T![:])?;
		let ty = parser.parse().await?;
		let span = parser.span_since(start);
		Ok(ast::Parameter {
			name,
			ty,
			span,
		})
	}
}
