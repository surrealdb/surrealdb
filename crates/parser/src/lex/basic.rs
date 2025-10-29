use ast::Ident;

use crate::error::ParseError;
use crate::lex::BaseTokenKind;
use crate::parse::{ParseSync, Parser};

impl ParseSync for Ident {
	fn parse_sync(parser: &mut Parser) -> Result<Self, ParseError> {
		let token = parser.expect(BaseTokenKind::Ident)?;
		let text = parser.slice(token.span);
	}
}
