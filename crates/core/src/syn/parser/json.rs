use std::collections::BTreeMap;

use reblessive::Stk;

use crate::{
	sql::{Duration, Strand},
	syn::{
		lexer::compound::{self, Numeric},
		parser::mac::{expected, pop_glued},
		token::{Glued, Span, TokenKind, t},
	},
	val::{Array, Number, Object, Value},
};

use super::{ParseResult, Parser};

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
			t!("\"") | t!("'") => {
				let strand: Strand = self.next_token_value()?;
				if self.settings.legacy_strands {
					self.reparse_json_legacy_strand(ctx, strand).await
				} else {
					Ok(Value::Strand(strand.into()))
				}
			}
			t!("-") | t!("+") | TokenKind::Digits => {
				self.pop_peek();
				let compound = self.lexer.lex_compound(token, compound::numeric)?;
				match compound.value {
					Numeric::Duration(x) => Ok(Value::Duration(Duration(x).into())),
					Numeric::Integer(x) => Ok(Value::Number(Number::Int(x))),
					Numeric::Float(x) => Ok(Value::Number(Number::Float(x))),
					Numeric::Decimal(x) => Ok(Value::Number(Number::Decimal(x))),
				}
			}
			TokenKind::Glued(Glued::Strand) => {
				let glued = pop_glued!(self, Strand);
				Ok(Value::Strand(glued.into()))
			}
			TokenKind::Glued(Glued::Duration) => {
				let glued = pop_glued!(self, Duration);
				Ok(Value::Duration(glued.into()))
			}
			_ => {
				//let ident = self.next_token_value::<Ident>()?.0;
				//self.parse_record_id_from_ident(ctx, ident).await.map(|x| Value::Thing(x))
				todo!()
			}
		}
	}

	async fn reparse_json_legacy_strand(
		&mut self,
		_stk: &mut Stk,
		strand: Strand,
	) -> ParseResult<Value> {
		//TODO: Fix record id and others
		Ok(Value::Strand(strand.into()))
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
