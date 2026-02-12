use token::BaseTokenKind;

use crate::parse::{ParseResult, Parser};

pub fn peek_starts_prime(parser: &mut Parser<'_, '_>) -> ParseResult<bool> {
	if let Some(x) = parser.peek()? {
		Ok(matches!(
			x.token,
			BaseTokenKind::Ident
				| BaseTokenKind::Float
				| BaseTokenKind::Int
				| BaseTokenKind::Decimal
		))
	} else {
		Ok(false)
	}
}
