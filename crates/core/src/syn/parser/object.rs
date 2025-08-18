use reblessive::Stk;

use super::mac::unexpected;
use crate::sql::literal::ObjectEntry;
use crate::sql::{Block, Expr, Literal};
use crate::syn::lexer::compound;
use crate::syn::parser::mac::expected;
use crate::syn::parser::{ParseResult, Parser, enter_object_recursion};
use crate::syn::token::{Glued, Span, TokenKind, t};
use crate::val::Strand;

impl Parser<'_> {
	/// Parse an production which starts with an `{`
	///
	/// Either a block statemnt, a object or geometry.
	pub(super) async fn parse_object_like(
		&mut self,
		stk: &mut Stk,
		start: Span,
	) -> ParseResult<Expr> {
		if self.eat(t!("}")) {
			// empty object, just return
			enter_object_recursion!(_this = self => {
				return Ok(Expr::Literal(Literal::Object(Vec::new())))
			})
		}

		// Now check first if it can be an object.
		if self.glue_and_peek1()?.kind == t!(":") {
			return self.parse_object(stk, start).await.map(Literal::Object).map(Expr::Literal);
		}

		// not an object so instead parse as a block.
		self.parse_block(stk, start).await.map(Box::new).map(Expr::Block)
	}

	/// Parses an object.
	///
	/// Expects the span of the starting `{` as an argument.
	///
	/// # Parser state
	/// Expects the first `{` to already have been eaten.
	pub(super) async fn parse_object(
		&mut self,
		stk: &mut Stk,
		start: Span,
	) -> ParseResult<Vec<ObjectEntry>> {
		enter_object_recursion!(this = self => {
		   return this.parse_object_inner(stk, start).await;
		})
	}

	async fn parse_object_inner(
		&mut self,
		stk: &mut Stk,
		start: Span,
	) -> ParseResult<Vec<ObjectEntry>> {
		let mut res = Vec::new();
		loop {
			if self.eat(t!("}")) {
				return Ok(res);
			}

			let (key, value) = self.parse_object_entry(stk).await?;
			// TODO: Error on duplicate key?
			res.push(ObjectEntry {
				key,
				value,
			});

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				return Ok(res);
			}
		}
	}

	/// Parses a block of statements
	///
	/// # Parser State
	/// Expects the starting `{` to have already been eaten and its span to be
	/// handed to this functions as the `start` parameter.
	pub async fn parse_block(&mut self, stk: &mut Stk, start: Span) -> ParseResult<Block> {
		let mut statements = Vec::new();
		loop {
			while self.eat(t!(";")) {}
			if self.eat(t!("}")) {
				break;
			}

			let stmt = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
			statements.push(stmt);
			if !self.eat(t!(";")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				break;
			}
		}
		Ok(Block(statements))
	}

	/// Parse a single entry in the object, i.e. `field: value + 1` in the
	/// object `{ field: value + 1 }`
	async fn parse_object_entry(&mut self, stk: &mut Stk) -> ParseResult<(String, Expr)> {
		let text = self.parse_object_key()?;
		expected!(self, t!(":"));
		let value = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
		Ok((text, value))
	}

	/// Parses the key of an object, i.e. `field` in the object `{ field: 1 }`.
	pub(super) fn parse_object_key(&mut self) -> ParseResult<String> {
		let token = self.peek();
		match token.kind {
			x if Self::kind_is_keyword_like(x) => {
				self.pop_peek();
				let str = self.lexer.span_str(token.span);
				Ok(str.to_string())
			}
			TokenKind::Identifier => {
				self.pop_peek();
				let str = self.lexer.string.take().unwrap();
				Ok(str)
			}
			t!("\"") | t!("'") | TokenKind::Glued(Glued::Strand) => {
				let str = self.next_token_value::<Strand>()?.into_string();
				Ok(str)
			}
			TokenKind::Digits => {
				self.pop_peek();
				let span = self.lexer.lex_compound(token, compound::number)?.span;
				let str = self.lexer.span_str(span);
				Ok(str.to_string())
			}
			TokenKind::Glued(Glued::Number) => {
				self.pop_peek();
				let str = self.lexer.span_str(token.span);
				Ok(str.to_string())
			}
			_ => unexpected!(self, token, "an object key"),
		}
	}
}
