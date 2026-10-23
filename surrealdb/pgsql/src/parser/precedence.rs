//! Operator precedence and associativity hierarchy for PostgreSQL Pratt expression parser.
//!
//! Directly mirrors precedence declarations in PostgreSQL `gram.y`.

use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;

/// Associativity direction for binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Associativity {
	/// Left-to-right associative (`a + b + c` -> `(a + b) + c`).
	Left,
	/// Right-to-left associative (`a op b op c` -> `a op (b op c)`).
	Right,
	/// Non-associative (`a < b < c` is a syntax error).
	None,
}

/// Return the (left_binding_power, right_binding_power, associativity) for an infix operator token.
pub fn infix_binding_power(token: &TokenKind<'_>) -> Option<(u8, u8, Associativity)> {
	if token == &TokenKind::Keyword(Keyword::Or) {
		Some((10, 11, Associativity::Left))
	} else if token == &TokenKind::Keyword(Keyword::And) {
		Some((20, 21, Associativity::Left))
	} else if let TokenKind::Keyword(Keyword::Is | Keyword::Isnull | Keyword::Notnull) = token {
		Some((30, 31, Associativity::None))
	} else if let TokenKind::Keyword(
		Keyword::Between | Keyword::In | Keyword::Like | Keyword::Ilike | Keyword::Similar,
	) = token
	{
		Some((40, 41, Associativity::None))
	} else if let TokenKind::Operator("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") = token {
		Some((40, 41, Associativity::None))
	} else if let TokenKind::Operator("->" | "->>" | "#>" | "#>>" | "@>" | "<@") = token {
		Some((50, 51, Associativity::Left))
	} else if let TokenKind::Operator("<<" | ">>" | "#") = token {
		Some((55, 56, Associativity::Left))
	} else if let TokenKind::Operator("||" | "|" | "&") = token {
		Some((60, 61, Associativity::Left))
	} else if let TokenKind::Operator("+" | "-") = token {
		Some((70, 71, Associativity::Left))
	} else if let TokenKind::Operator("*" | "/" | "%") = token {
		Some((80, 81, Associativity::Left))
	} else if token == &TokenKind::Operator("^") {
		// Precedence 7: Exponentiation (`%left '^'` in gram.y): `a ^ b ^ c` is `(a ^ b) ^ c`.
		Some((90, 91, Associativity::Left))
	} else if token == &TokenKind::Keyword(Keyword::Collate) {
		Some((100, 101, Associativity::Left))
	} else if token == &TokenKind::DoubleColon {
		Some((120, 121, Associativity::Left))
	} else if token == &TokenKind::LBracket {
		Some((130, 131, Associativity::Left))
	} else if let TokenKind::Operator(_) = token {
		Some((65, 66, Associativity::Left))
	} else {
		None
	}
}

/// Prefix binding power for unary operators.
pub fn prefix_binding_power(token: &TokenKind<'_>) -> Option<u8> {
	if token == &TokenKind::Keyword(Keyword::Not) {
		Some(25)
	} else if let TokenKind::Operator("+" | "-" | "~") = token {
		Some(110)
	} else if let TokenKind::Operator(_) = token {
		Some(110)
	} else {
		None
	}
}
