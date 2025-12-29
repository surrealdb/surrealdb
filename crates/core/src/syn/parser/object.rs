use reblessive::Stk;

use super::mac::unexpected;
use crate::sql::literal::ObjectEntry;
use crate::sql::{Block, Expr, Literal};
use crate::syn::lexer::compound;
use crate::syn::parser::mac::expected;
use crate::syn::parser::{ParseResult, Parser, enter_object_recursion};
use crate::syn::token::{Span, TokenKind, t};

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
			return Ok(Expr::Literal(Literal::Object(Vec::new())));
		}

		if self.eat(t!(",")) {
			self.expect_closing_delimiter(t!("}"), start)?;
			return Ok(Expr::Literal(Literal::Set(Vec::new())));
		}

		// Try to parse an object if it can be an object.
		if let t!("\"")
		| t!("'")
		| TokenKind::Identifier
		| TokenKind::Digits
		| TokenKind::Keyword(_)
		| TokenKind::Language(_)
		| TokenKind::Algorithm(_)
		| TokenKind::Distance(_)
		| TokenKind::VectorType(_) = self.peek().kind
			&& let Some(x) = self
				.speculate(stk, async |stk, this| {
					enter_object_recursion!(this = this => {
						let key = this.parse_object_key()?;

						if !this.eat(t!(":")){
							return Ok(None)
						}

						let value = stk.run(|stk| this.parse_expr_inherit(stk)).await?;
						let res = vec![ObjectEntry{ key, value }];

						if this.eat(t!(",")){
							this.parse_object_inner(stk, start, res).await.map(Some)
						}else{
							this.expect_closing_delimiter(t!("}"), start)?;
							Ok(Some(res))
						}
					})
				})
				.await?
		{
			return Ok(Expr::Literal(Literal::Object(x)));
		}

		// It's either a set or a block.
		let first_expr = stk.run(|stk| self.parse_block_expr(stk)).await?;

		let next = self.peek();
		match next.kind {
			t!(",") => {
				self.pop_peek();
				let mut exprs = self.parse_set(stk, start).await?;
				exprs.insert(0, first_expr);
				Ok(Expr::Literal(Literal::Set(exprs)))
			}
			t!("}") => {
				self.pop_peek();
				Ok(Expr::Block(Box::new(Block(vec![first_expr]))))
			}
			_ => {
				self.pop_peek();
				let block = self.parse_block_remaining(stk, start, vec![first_expr]).await?;
				Ok(Expr::Block(Box::new(block)))
			}
		}
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
			return this.parse_object_inner(stk, start, Vec::new()).await;
		})
	}

	pub(super) async fn parse_object_inner(
		&mut self,
		stk: &mut Stk,
		start: Span,
		mut res: Vec<ObjectEntry>,
	) -> ParseResult<Vec<ObjectEntry>> {
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

	pub(crate) async fn parse_set(&mut self, stk: &mut Stk, start: Span) -> ParseResult<Vec<Expr>> {
		enter_object_recursion!(this = self => {
			return this.parse_set_inner(stk, start).await;
		})
	}

	async fn parse_set_inner(&mut self, stk: &mut Stk, start: Span) -> ParseResult<Vec<Expr>> {
		let mut res = Vec::new();
		loop {
			if self.eat(t!("}")) {
				return Ok(res);
			}

			let value = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
			res.push(value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				return Ok(res);
			}
		}
	}

	/// Parses a block of statements.
	///
	/// # Parser State
	/// Expects the starting `{` to have already been eaten and its span to be
	/// handed to this functions as the `start` parameter.
	pub async fn parse_block(&mut self, stk: &mut Stk, start: Span) -> ParseResult<Block> {
		self.parse_block_remaining(stk, start, Vec::new()).await
	}

	/// Parses the remaining statements in a block.
	///
	/// # Parser State
	/// Expects the starting `{` to have already been eaten and its span to be
	/// handed to this functions as the `start` parameter.
	///
	/// Any statements which have already been parsed can be passed in as the `existing_stmts`
	/// parameter.
	async fn parse_block_remaining(
		&mut self,
		stk: &mut Stk,
		start: Span,
		mut existing_stmts: Vec<Expr>,
	) -> ParseResult<Block> {
		loop {
			// Eat empty statements.
			while self.eat(t!(";")) {}

			if self.eat(t!("}")) {
				break;
			}

			let stmt = self.parse_block_expr(stk).await?;
			existing_stmts.push(stmt);

			if !self.eat(t!(";")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				break;
			}
		}
		Ok(Block(existing_stmts))
	}

	async fn parse_block_expr(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
		let before = self.recent_span();
		let stmt = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
		let span = before.covers(self.last_span());
		Self::reject_letless_let(&stmt, span)?;
		Ok(stmt)
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
			TokenKind::Identifier => self.parse_ident(),
			t!("\"") | t!("'") => Ok(self.parse_string_lit()?),
			TokenKind::Digits => {
				self.pop_peek();
				let span = self.lexer.lex_compound(token, compound::number)?.span;
				let str = self.lexer.span_str(span);
				Ok(str.to_string())
			}
			_ => unexpected!(self, token, "an object key"),
		}
	}
}
