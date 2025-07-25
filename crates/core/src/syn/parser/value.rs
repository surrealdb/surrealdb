use std::cmp::Ordering;
use std::collections::BTreeMap;

use reblessive::Stk;

use super::{ParseResult, Parser};

use crate::sql::Ident;
use crate::syn::error::bail;
use crate::syn::lexer::compound::{self, Numeric};
use crate::syn::parser::mac::{expected, pop_glued};
use crate::syn::parser::unexpected;
use crate::syn::token::{Glued, Span, TokenKind, t};
use crate::val::{self, Array, Duration, Number, Object, RecordId, RecordIdKey, Strand, Value};

trait ValueParseFunc {
	async fn parse<'a>(parser: &mut Parser<'a>, ctx: &mut Stk) -> ParseResult<Value>;
}

struct SurrealQL;
struct JSON;

impl ValueParseFunc for SurrealQL {
	async fn parse<'a>(parser: &mut Parser<'a>, ctx: &mut Stk) -> ParseResult<Value> {
		parser.parse_value(ctx).await
	}
}

impl ValueParseFunc for JSON {
	async fn parse<'a>(parser: &mut Parser<'a>, ctx: &mut Stk) -> ParseResult<Value> {
		parser.parse_json(ctx).await
	}
}

impl Parser<'_> {
	/// Parse a complete value which cannot contain non-literal expressions.
	pub async fn parse_value(&mut self, stk: &mut Stk) -> ParseResult<Value> {
		let token = self.peek();
		match token.kind {
			t!("NONE") => {
				self.pop_peek();
				Ok(Value::None)
			}
			t!("NULL") => {
				self.pop_peek();
				Ok(Value::Null)
			}
			TokenKind::NaN => {
				self.pop_peek();
				Ok(Value::Number(Number::Float(f64::NAN)))
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
				self.parse_value_object::<SurrealQL>(stk, token.span).await.map(Value::Object)
			}
			t!("[") => {
				self.pop_peek();
				self.parse_value_array::<SurrealQL>(stk, token.span).await.map(Value::Array)
			}
			t!("\"") | t!("'") => {
				let strand: Strand = self.next_token_value()?;
				if self.settings.legacy_strands {
					self.reparse_json_legacy_strand(stk, strand).await
				} else {
					Ok(Value::Strand(strand.into()))
				}
			}
			t!("d\"") | t!("d'") => {
				let datetime = self.next_token_value()?;
				Ok(Value::Datetime(datetime))
			}
			t!("u\"") | t!("u'") => {
				let uuid = self.next_token_value()?;
				Ok(Value::Uuid(uuid))
			}
			t!("b\"") | t!("b'") | TokenKind::Glued(Glued::Bytes) => {
				Ok(Value::Bytes(self.next_token_value()?))
			}

			t!("f\"") | t!("f'") => {
				if !self.settings.files_enabled {
					unexpected!(self, token, "the experimental files feature to be enabled");
				}

				let file = self.next_token_value::<val::File>()?;
				Ok(Value::File(file))
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
			_ => self.parse_value_record_id_inner::<SurrealQL>(stk).await.map(Value::Thing),
		}
	}

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
				self.parse_value_object::<JSON>(ctx, token.span).await.map(Value::Object)
			}
			t!("[") => {
				self.pop_peek();
				self.parse_value_array::<JSON>(ctx, token.span).await.map(Value::Array)
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
			_ => self.parse_value_record_id_inner::<JSON>(ctx).await.map(Value::Thing),
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

	async fn parse_value_object<VP>(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Object>
	where
		VP: ValueParseFunc,
	{
		let mut obj = BTreeMap::new();
		loop {
			if self.eat(t!("}")) {
				return Ok(Object(obj));
			}
			let key = self.parse_object_key()?;
			expected!(self, t!(":"));
			let value = ctx.run(|ctx| VP::parse(self, ctx)).await?;
			obj.insert(key, value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				return Ok(Object(obj));
			}
		}
	}

	async fn parse_value_array<VP>(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Array>
	where
		VP: ValueParseFunc,
	{
		let mut array = Vec::new();
		loop {
			if self.eat(t!("]")) {
				return Ok(Array(array));
			}
			let value = ctx.run(|ctx| VP::parse(self, ctx)).await?;
			array.push(value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("]"), start)?;
				return Ok(Array(array));
			}
		}
	}

	pub async fn parse_value_record_id(&mut self, ctx: &mut Stk) -> ParseResult<RecordId> {
		self.parse_value_record_id_inner::<SurrealQL>(ctx).await
	}

	async fn parse_value_record_id_inner<VP>(&mut self, ctx: &mut Stk) -> ParseResult<RecordId>
	where
		VP: ValueParseFunc,
	{
		let table = self.next_token_value::<Ident>()?;
		expected!(self, t!(":"));
		let peek = self.peek();
		let key = match peek.kind {
			t!("u'") | t!("u\"") => RecordIdKey::Uuid(self.next_token_value::<val::Uuid>()?.into()),
			t!("{") => {
				let peek = self.pop_peek();
				RecordIdKey::Object(self.parse_value_object::<VP>(ctx, peek.span).await?)
			}
			t!("[") => {
				let peek = self.pop_peek();
				RecordIdKey::Array(self.parse_value_array::<VP>(ctx, peek.span).await?)
			}
			t!("+") => {
				self.pop_peek();
				// starting with a + so it must be a number
				let digits_token = self.peek_whitespace();
				match digits_token.kind {
					TokenKind::Digits => {}
					_ => unexpected!(self, digits_token, "an integer"),
				}

				let next = self.peek_whitespace();
				match next.kind {
					t!(".") => {
						// TODO(delskayn) explain that record-id's cant have matissas,
						// exponents or a number suffix
						unexpected!(self, next, "an integer", => "Numeric Record-id keys can only be integers");
					}
					x if Self::kind_is_identifier(x) => {
						let span = peek.span.covers(next.span);
						bail!("Unexpected token `{x}` expected an integer", @span);
					}
					// allowed
					_ => {}
				}

				let digits_str = self.lexer.span_str(digits_token.span);
				if let Ok(number) = digits_str.parse() {
					RecordIdKey::Number(number)
				} else {
					RecordIdKey::String(digits_str.to_owned())
				}
			}
			t!("-") => {
				self.pop_peek();
				let token = expected!(self, TokenKind::Digits);
				if let Ok(number) = self.lexer.lex_compound(token, compound::integer::<u64>) {
					// Parse to u64 and check if the value is equal to `-i64::MIN` via u64 as
					// `-i64::MIN` doesn't fit in an i64
					match number.value.cmp(&((i64::MAX as u64) + 1)) {
						Ordering::Less => RecordIdKey::Number(-(number.value as i64)),
						Ordering::Equal => RecordIdKey::Number(i64::MIN),
						Ordering::Greater => {
							RecordIdKey::String(format!("-{}", self.lexer.span_str(number.span)))
						}
					}
				} else {
					RecordIdKey::String(format!("-{}", self.lexer.span_str(token.span)))
				}
			}
			TokenKind::Digits => {
				if self.settings.flexible_record_id
					&& Self::kind_is_identifier(self.peek_whitespace1().kind)
				{
					let ident = self.parse_flexible_ident()?;
					RecordIdKey::String(ident.into_string())
				} else {
					self.pop_peek();

					let digits_str = self.lexer.span_str(peek.span);
					if let Ok(number) = digits_str.parse::<i64>() {
						RecordIdKey::Number(number)
					} else {
						RecordIdKey::String(digits_str.to_owned())
					}
				}
			}
			TokenKind::Glued(Glued::Duration) if self.settings.flexible_record_id => {
				let slice = self.lexer.reader.span(peek.span);
				if slice.iter().any(|x| !x.is_ascii()) {
					unexpected!(self, peek, "a identifier");
				}
				// Should be valid utf-8 as it was already parsed by the lexer
				let text = String::from_utf8(slice.to_vec()).unwrap();
				RecordIdKey::String(text)
			}
			_ => {
				let ident = if self.settings.flexible_record_id {
					self.parse_flexible_ident()?
				} else {
					self.next_token_value::<Ident>()?
				};
				RecordIdKey::String(ident.into_string())
			}
			_ => unexpected!(self, peek, "record-id key"),
		};

		Ok(RecordId {
			table: table.into_string(),
			key,
		})
	}
}
