use std::collections::BTreeMap;

use reblessive::Stk;

use crate::{
	sql::{Array, Ident, Object, Strand, Value},
	syn::{
		parser::mac::expected,
		token::{t, Span, TokenKind},
	},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	pub async fn parse_json(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		let token = self.next();
		match token.kind {
			t!("NULL") => Ok(Value::Null),
			t!("true") => Ok(Value::Bool(true)),
			t!("false") => Ok(Value::Bool(false)),
			t!("{") => self.parse_json_object(ctx, token.span).await.map(Value::Object),
			t!("[") => self.parse_json_array(ctx, token.span).await.map(Value::Array),
			TokenKind::Duration => self.token_value(token).map(Value::Duration),
			TokenKind::DateTime => self.token_value(token).map(Value::Datetime),
			TokenKind::Strand => {
				if self.legacy_strands {
					self.parse_legacy_strand(ctx).await
				} else {
					Ok(Value::Strand(Strand(self.lexer.string.take().unwrap())))
				}
			}
			TokenKind::Number(_) => self.token_value(token).map(Value::Number),
			TokenKind::Uuid => self.token_value(token).map(Value::Uuid),
			_ => {
				let ident = self.token_value::<Ident>(token)?.0;
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
