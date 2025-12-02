use ast::Spanned;

use super::{Parse, ParseResult, ParseSync, Parser};

impl<T: ParseSync> ParseSync for Spanned<T> {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let start = if let Some(x) = parser.peek()? {
			x.span
		} else {
			parser.eof_span()
		};

		let value = T::parse_sync(parser)?;

		let end = if let Some(x) = parser.peek()? {
			x.span
		} else {
			parser.eof_span()
		};
		Ok(Spanned {
			value,
			span: start.extend(end),
		})
	}
}

impl<T: Parse> Parse for Spanned<T> {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = if let Some(x) = parser.peek()? {
			x.span
		} else {
			parser.eof_span()
		};

		let value = T::parse(parser).await?;

		let end = if let Some(x) = parser.peek()? {
			x.span
		} else {
			parser.eof_span()
		};
		Ok(Spanned {
			value,
			span: start.extend(end),
		})
	}
}
