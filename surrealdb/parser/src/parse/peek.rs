use token::{BaseTokenKind, T};

use crate::parse::{ParseResult, Parser};

pub fn peek_starts_prime(parser: &mut Parser<'_, '_>) -> ParseResult<bool> {
	if let Some(x) = parser.peek()? {
		Ok(x.token.is_identifier()
			|| matches!(
				x.token,
				BaseTokenKind::Float
					| BaseTokenKind::Int
					| BaseTokenKind::Decimal
					| BaseTokenKind::OpenBrace
					| BaseTokenKind::OpenBracket
					| BaseTokenKind::OpenParen
					| BaseTokenKind::String
					| BaseTokenKind::UuidString
					| BaseTokenKind::RecordIdString
					| BaseTokenKind::DateTimeString
					| BaseTokenKind::Duration
					| BaseTokenKind::NaN
					| BaseTokenKind::PosInfinity
					| BaseTokenKind::NegInfinity
					| T![|]
			))
	} else {
		Ok(false)
	}
}

pub fn peek_starts_record_id_key(parser: &mut Parser<'_, '_>) -> ParseResult<bool> {
	if let Some(x) = parser.peek()? {
		Ok(x.token.is_identifier()
			|| matches!(
				x.token,
				BaseTokenKind::OpenBrace
					| BaseTokenKind::OpenBracket
					| BaseTokenKind::UuidString
					| BaseTokenKind::Int
					| BaseTokenKind::String
			))
	} else {
		Ok(false)
	}
}
