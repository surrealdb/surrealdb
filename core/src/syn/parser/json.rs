use std::collections::BTreeMap;

use reblessive::Stk;

use crate::{
	sql::{Array, Ident, Object, Strand, Value},
	syn::{
		parser::mac::expected,
		token::{t, QouteKind, Span, TokenKind},
	},
};

use super::{mac::unexpected, ParseResult, Parser};

impl Parser<'_> {
	pub async fn parse_json(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		let token = self.peek();
		match token.kind {
			t!("NULL") => {
				self.pop_peek();
				Ok(Value::Null)
			}
			t!("true") => {
				self.pop_peek();
				Ok(Value::Bool(true))
			}
			t!("false") => {
				self.pop_peek();
				Ok(Value::Bool(false))
			}
			t!("{") => {
				self.pop_peek();
				self.parse_json_object(ctx, token.span).await.map(Value::Object)
			}
			t!("[") => {
				self.pop_peek();
				self.parse_json_array(ctx, token.span).await.map(Value::Array)
			}
			TokenKind::Qoute(QouteKind::Plain | QouteKind::PlainDouble) => {
				let Strand(strand) = self.next_token_value()?;
				if self.legacy_strands {
					if let Some(x) = self.reparse_legacy_strand(ctx, &strand).await {
						return Ok(x);
					}
				}
				Ok(Value::Strand(strand))
			}
			TokenKind::Digits | TokenKind::Number(_) => {
				let peek = self.glue()?;
				match peek.kind {
					TokenKind::Duration => Ok(Value::Duration(self.next_token_value()?)),
					TokenKind::Number(_) => Ok(Value::Number(self.next_token_value()?)),
					x => unexpected!(self, x, "a number"),
				}
			}
			_ => {
				let ident = self.next_token_value::<Ident>()?.0;
				self.parse_thing_from_ident(ctx, ident).await.map(Value::Thing)
			}
		}
	}

	async fn parse_json_object(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Object> {
		let mut obj = BTreeMap::new();
		loop {
			if self.eat(t!("}")) {
				return Ok(Object(obj));
			}
			let key = self.parse_object_key()?;
			expected!(self, t!(":"));
			let value = ctx.run(|ctx| self.parse_json(ctx)).await?;
			obj.insert(key, value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				return Ok(Object(obj));
			}
		}
	}

	async fn parse_json_array(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Array> {
		let mut array = Vec::new();
		loop {
			if self.eat(t!("]")) {
				return Ok(Array(array));
			}
			let value = ctx.run(|ctx| self.parse_json(ctx)).await?;
			array.push(value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("]"), start)?;
				return Ok(Array(array));
			}
		}
	}
}
