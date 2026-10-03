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
	/// Right-to-left associative (`a ^ b ^ c` -> `a ^ (b ^ c)`).
	Right,
	/// Non-associative (`a < b < c` is a syntax error).
	None,
}

/// Return the (left_binding_power, right_binding_power, associativity) for an infix operator token.
pub fn infix_binding_power(token: &TokenKind<'_>) -> Option<(u8, u8, Associativity)> {
	match token {
		// Precedence 1: OR
		TokenKind::Keyword(Keyword::Or) => Some((10, 11, Associativity::Left)),

		// Precedence 2: AND
		TokenKind::Keyword(Keyword::And) => Some((20, 21, Associativity::Left)),

		// Precedence 3: IS, ISNULL, NOTNULL
		TokenKind::Keyword(Keyword::Is)
		| TokenKind::Keyword(Keyword::Isnull)
		| TokenKind::Keyword(Keyword::Notnull) => Some((30, 31, Associativity::None)),

		// Precedence 4: Comparison & Membership
		TokenKind::Keyword(Keyword::Between)
		| TokenKind::Keyword(Keyword::In)
		| TokenKind::Keyword(Keyword::Like)
		| TokenKind::Keyword(Keyword::Ilike)
		| TokenKind::Keyword(Keyword::Similar) => Some((40, 41, Associativity::None)),

		// Comparison operators
		TokenKind::Operator("=")
		| TokenKind::Operator("<>")
		| TokenKind::Operator("!=")
		| TokenKind::Operator("<")
		| TokenKind::Operator("<=")
		| TokenKind::Operator(">")
		| TokenKind::Operator(">=") => Some((40, 41, Associativity::None)),

		// JSON & Containment operators
		TokenKind::Operator("->")
		| TokenKind::Operator("->>")
		| TokenKind::Operator("#>")
		| TokenKind::Operator("#>>")
		| TokenKind::Operator("@>")
		| TokenKind::Operator("<@") => Some((50, 51, Associativity::Left)),

		// Bitwise shift & XOR
		TokenKind::Operator("<<") | TokenKind::Operator(">>") | TokenKind::Operator("#") => {
			Some((55, 56, Associativity::Left))
		}

		// String concatenation and bitwise OR/AND
		TokenKind::Operator("||") | TokenKind::Operator("|") | TokenKind::Operator("&") => {
			Some((60, 61, Associativity::Left))
		}

		// Precedence 5: Binary addition and subtraction
		TokenKind::Operator("+") | TokenKind::Operator("-") => Some((70, 71, Associativity::Left)),

		// Precedence 6: Multiplication, division, modulo
		TokenKind::Operator("*") | TokenKind::Operator("/") | TokenKind::Operator("%") => {
			Some((80, 81, Associativity::Left))
		}

		// Precedence 7: Exponentiation (right-associative!)
		TokenKind::Operator("^") => Some((91, 90, Associativity::Right)),

		// Precedence 8: COLLATE
		TokenKind::Keyword(Keyword::Collate) => Some((100, 101, Associativity::Left)),

		// Precedence 9: Typecast `::`
		TokenKind::DoubleColon => Some((120, 121, Associativity::Left)),

		// Precedence 10: Array subscripting `[`
		TokenKind::LBracket => Some((130, 131, Associativity::Left)),

		// General / custom infix operator
		TokenKind::Operator(_) => Some((65, 66, Associativity::Left)),

		_ => None,
	}
}

/// Prefix binding power for unary operators.
pub fn prefix_binding_power(token: &TokenKind<'_>) -> Option<u8> {
	match token {
		TokenKind::Keyword(Keyword::Not) => Some(25),
		TokenKind::Operator("+") | TokenKind::Operator("-") | TokenKind::Operator("~") => Some(110),
		TokenKind::Operator(_) => Some(110),
		_ => None,
	}
}
