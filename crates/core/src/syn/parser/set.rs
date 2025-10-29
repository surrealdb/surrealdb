use reblessive::Stk;

use crate::sql::Expr;
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::{Span, t};

impl Parser<'_> {
	/// Parse a set literal: {val, val, val}
	///
	/// # Parser State
	/// Expects the starting `{` to have already been eaten.
	/// Expects to have already determined this is a set (not an object or block).
	/// May be called after the first element has already been parsed.
	pub(super) async fn parse_set(&mut self, stk: &mut Stk, start: Span) -> ParseResult<Vec<Expr>> {
		let mut elements = Vec::new();

		loop {
			if self.eat(t!("}")) {
				return Ok(elements);
			}

			let value = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
			elements.push(value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				return Ok(elements);
			}
		}
	}
}
