//! This module defines the pratt parser for operators.

use crate::syn::token::{t, TokenKind};

impl Parser<'_> {
	/// Returns the binding power of an infix operator.
	///
	/// Binding power is the opposite of precendence: a higher binding power means that a token is
	/// more like to operate directly on it's neighbours. Example `*` has a higher binding power
	/// than `-` resulting in 1 - 2 * 3 being parsed as 1 - (2 * 3).
	///
	/// This returns two numbers: the binding power of the left neighbour and the right neighbour.
	/// If the left number is lower then the right it is left associative: i.e. '1 op 2 op 3' will
	/// be parsed as '(1 op 2) op 3'. If the right number is lower the operator is right
	/// associative: i.e. '1 op 2 op 3' will be parsed as '1 op (2 op 3)'. For example: `+=` is
	/// right associative so `a += b += 3` will be parsed as `a += (b += 3)` while `+` is left
	/// associative and will be parsed as `(a + b) + c`.
	fn infix_binding_power(token: TokenKind) -> Option<(u8, u8)> {
		// TODO: Look at ordering of operators.
		match token {
			// assigment operators have the lowes binding power.
			t!("+=") | t!("-=") | t!("-?=") => Some((2, 1)),
			// Equality operators have same binding power.
			t!("=")
			| t!("==")
			| t!("!=")
			| t!("*=")
			| t!("?=")
			| t!("~")
			| t!("!~")
			| t!("*~")
			| t!("?~")
			| t!("@") => Some(todo!()),

			t!("<") | t!("<=") | t!(">") | t!(">=") => Some(todo!()),

			_ => None,
		}
	}

	fn prefix_binding_power(token: TokenKind) -> Option<((), u8)> {
		match token {
			t!("!") => Some(todo!()),
			_ => None,
		}
	}
}
