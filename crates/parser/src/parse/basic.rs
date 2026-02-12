use token::BaseTokenKind;

use super::{ParseResult, ParseSync, Parser};

impl ParseSync for ast::Ident {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::Ident)?;
		let slice = parser.slice(token.span);
		let str_value = parser.unescape_str(slice)?.to_owned();
		let text = parser.push_set(str_value.to_owned());

		Ok(ast::Ident {
			text,
			span: token.span,
		})
	}
}

impl ParseSync for f64 {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let token = parser.expect(BaseTokenKind::Float)?;
		let slice = parser.slice(token.span);
		let float = slice.trim_end_matches("f").parse().expect("lexer should ensure valid floats");
		Ok(float)
	}
}
