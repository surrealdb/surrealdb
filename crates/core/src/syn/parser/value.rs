use core::f64;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;

use reblessive::Stk;

use super::{ParseResult, Parser};
use crate::syn::error::bail;
use crate::syn::lexer::compound::{self, Numeric};
use crate::syn::parser::mac::{expected, pop_glued};
use crate::syn::parser::unexpected;
use crate::syn::token::{Glued, Span, TokenKind, t};
use crate::types::{
	PublicArray, PublicDuration, PublicFile, PublicGeometry, PublicNumber, PublicObject,
	PublicRange, PublicRecordId, PublicRecordIdKey, PublicTable, PublicUuid, PublicValue,
};

trait ValueParseFunc {
	async fn parse(parser: &mut Parser<'_>, stk: &mut Stk) -> ParseResult<PublicValue>;
}

struct SurrealQL;
struct Json;

impl ValueParseFunc for SurrealQL {
	async fn parse(parser: &mut Parser<'_>, stk: &mut Stk) -> ParseResult<PublicValue> {
		parser.parse_value(stk).await
	}
}

impl ValueParseFunc for Json {
	async fn parse(parser: &mut Parser<'_>, stk: &mut Stk) -> ParseResult<PublicValue> {
		parser.parse_json(stk).await
	}
}

impl Parser<'_> {
	/// Parse a complete value which cannot contain non-literal expressions.
	pub async fn parse_value(&mut self, stk: &mut Stk) -> ParseResult<PublicValue> {
		let token = self.peek();
		let res = match token.kind {
			t!("NONE") => {
				self.pop_peek();
				PublicValue::None
			}
			t!("NULL") => {
				self.pop_peek();
				PublicValue::Null
			}
			TokenKind::NaN => {
				self.pop_peek();
				PublicValue::Number(PublicNumber::Float(f64::NAN))
			}
			TokenKind::Infinity => {
				self.pop_peek();
				PublicValue::Number(PublicNumber::Float(f64::INFINITY))
			}
			t!("true") => {
				self.pop_peek();
				PublicValue::Bool(true)
			}
			t!("false") => {
				self.pop_peek();
				PublicValue::Bool(false)
			}
			t!("{") => {
				self.pop_peek();
				let object = self.parse_value_object::<SurrealQL>(stk, token.span).await?;
				//HACK: This is an annoying hack to have geometries work.
				//
				// Geometries look exactly like objects and are a strict subsect of objects.
				// However in code they are distinct and in surrealql the have different
				// behavior.
				//
				// Geom functions don't work with objects and vice-versa.
				//
				// The previous parse automatically converted an object to geometry if it found
				// an matching object. Now it no longer does that and relies on the
				// 'planning' stage to convert it. But here we still need to do it in the
				// parser.
				if let Some(geom) = PublicGeometry::try_from_object(&object) {
					PublicValue::Geometry(geom)
				} else {
					PublicValue::Object(object)
				}
			}
			t!("[") => {
				self.pop_peek();
				self.parse_value_array::<SurrealQL>(stk, token.span)
					.await
					.map(PublicValue::Array)?
			}
			t!("\"") | t!("'") => {
				let strand = self.parse_string_lit()?;
				if self.settings.legacy_strands {
					self.reparse_json_legacy_strand(stk, strand).await
				} else {
					PublicValue::String(strand)
				}
			}
			t!("d\"") | t!("d'") => PublicValue::Datetime(self.next_token_value()?),
			t!("u\"") | t!("u'") => PublicValue::Uuid(self.next_token_value()?),
			t!("b\"") | t!("b'") => PublicValue::Bytes(self.next_token_value()?),
			//TODO: Implement record id for value parsing
			t!("f\"") | t!("f'") => {
				if !self.settings.files_enabled {
					unexpected!(self, token, "the experimental files feature to be enabled");
				}

				let file = self.next_token_value::<PublicFile>()?;
				PublicValue::File(file)
			}
			t!("/") => {
				let regex = self.next_token_value()?;
				PublicValue::Regex(regex)
			}
			t!("(") => {
				let open = self.pop_peek().span;
				let peek = self.peek();
				match peek.kind {
					t!("+") | t!("-") | TokenKind::Digits => {
						let before = peek.span;
						let number = self.next_token_value::<Numeric>()?;
						let number_span = before.covers(self.last_span());
						if self.peek().kind == t!(",") {
							let x = match number {
								Numeric::Duration(_) | Numeric::Decimal(_) => {
									bail!("Unexpected token, expected a non-decimal, non-NaN, number",
										@number_span => "Coordinate numbers can't be NaN or a decimal");
								}
								Numeric::Float(x) if x.is_nan() => {
									bail!("Unexpected token, expected a non-decimal, non-NaN, number",
										@number_span => "Coordinate numbers can't be NaN or a decimal");
								}
								Numeric::Float(x) => x,
								Numeric::Integer(x) => x as f64,
							};

							self.pop_peek();

							let y = self.next_token_value::<f64>()?;
							self.expect_closing_delimiter(t!(")"), open)?;
							PublicValue::Geometry(PublicGeometry::Point(geo::Point::new(x, y)))
						} else {
							self.expect_closing_delimiter(t!(")"), open)?;

							match number {
								Numeric::Float(x) => PublicValue::Number(PublicNumber::Float(x)),
								Numeric::Integer(x) => PublicValue::Number(PublicNumber::Int(x)),
								Numeric::Decimal(x) => {
									PublicValue::Number(PublicNumber::Decimal(x))
								}
								Numeric::Duration(duration) => {
									PublicValue::Duration(PublicDuration::from(duration))
								}
							}
						}
					}
					_ => {
						let res = stk.run(|stk| self.parse_value(stk)).await?;
						self.expect_closing_delimiter(t!(")"), open)?;
						res
					}
				}
			}
			t!("..") => {
				self.pop_peek();
				let peek = self.peek_whitespace().kind;
				if peek == t!("=") {
					self.pop_peek();
					let v = stk.run(|stk| self.parse_value(stk)).await?;
					PublicValue::Range(Box::new(PublicRange {
						start: Bound::Unbounded,
						end: Bound::Included(v),
					}))
				} else if Self::kind_starts_expression(peek) {
					let v = stk.run(|stk| self.parse_value(stk)).await?;
					PublicValue::Range(Box::new(PublicRange {
						start: Bound::Unbounded,
						end: Bound::Excluded(v),
					}))
				} else {
					PublicValue::Range(Box::new(PublicRange {
						start: Bound::Unbounded,
						end: Bound::Unbounded,
					}))
				}
			}
			t!("-") | t!("+") | TokenKind::Digits => {
				self.pop_peek();
				let compound = self.lexer.lex_compound(token, compound::numeric)?;
				match compound.value {
					Numeric::Duration(x) => PublicValue::Duration(PublicDuration::from(x)),
					Numeric::Integer(x) => PublicValue::Number(PublicNumber::Int(x)),
					Numeric::Float(x) => PublicValue::Number(PublicNumber::Float(x)),
					Numeric::Decimal(x) => PublicValue::Number(PublicNumber::Decimal(x)),
				}
			}
			_ => self
				.parse_value_record_id_inner::<SurrealQL>(stk)
				.await
				.map(PublicValue::RecordId)?,
		};

		match self.peek_whitespace().kind {
			t!(">") => {
				self.pop_peek();
				expected!(self, t!(".."));
				let peek = self.peek_whitespace().kind;
				if peek == t!("=") {
					self.pop_peek();
					let v = stk.run(|stk| self.parse_value(stk)).await?;
					Ok(PublicValue::Range(Box::new(PublicRange {
						start: Bound::Excluded(res),
						end: Bound::Included(v),
					})))
				} else if Self::kind_starts_expression(peek) {
					let v = stk.run(|stk| self.parse_value(stk)).await?;
					Ok(PublicValue::Range(Box::new(PublicRange {
						start: Bound::Excluded(res),
						end: Bound::Excluded(v),
					})))
				} else {
					Ok(PublicValue::Range(Box::new(PublicRange {
						start: Bound::Excluded(res),
						end: Bound::Unbounded,
					})))
				}
			}
			t!("..") => {
				self.pop_peek();

				let peek = self.peek_whitespace().kind;
				if peek == t!("=") {
					self.pop_peek();
					let v = stk.run(|stk| self.parse_value(stk)).await?;
					Ok(PublicValue::Range(Box::new(PublicRange {
						start: Bound::Included(res),
						end: Bound::Included(v),
					})))
				} else if Self::kind_starts_expression(peek) {
					let v = stk.run(|stk| self.parse_value(stk)).await?;
					Ok(PublicValue::Range(Box::new(PublicRange {
						start: Bound::Included(res),
						end: Bound::Excluded(v),
					})))
				} else {
					Ok(PublicValue::Range(Box::new(PublicRange {
						start: Bound::Included(res),
						end: Bound::Unbounded,
					})))
				}
			}
			_ => Ok(res),
		}
	}

	pub async fn parse_json(&mut self, stk: &mut Stk) -> ParseResult<PublicValue> {
		let token = self.peek();
		match token.kind {
			t!("NULL") => {
				self.pop_peek();
				Ok(PublicValue::Null)
			}
			t!("true") => {
				self.pop_peek();
				Ok(PublicValue::Bool(true))
			}
			t!("false") => {
				self.pop_peek();
				Ok(PublicValue::Bool(false))
			}
			t!("{") => {
				self.pop_peek();
				self.parse_value_object::<Json>(stk, token.span).await.map(PublicValue::Object)
			}
			t!("[") => {
				self.pop_peek();
				self.parse_value_array::<Json>(stk, token.span).await.map(PublicValue::Array)
			}
			t!("\"") | t!("'") => {
				let strand = self.parse_string_lit()?;
				if self.settings.legacy_strands {
					Ok(self.reparse_json_legacy_strand(stk, strand).await)
				} else {
					Ok(PublicValue::String(strand))
				}
			}
			t!("-") | t!("+") | TokenKind::Digits => {
				self.pop_peek();
				let compound = self.lexer.lex_compound(token, compound::numeric)?;
				match compound.value {
					Numeric::Duration(x) => Ok(PublicValue::Duration(PublicDuration::from(x))),
					Numeric::Integer(x) => Ok(PublicValue::Number(PublicNumber::Int(x))),
					Numeric::Float(x) => Ok(PublicValue::Number(PublicNumber::Float(x))),
					Numeric::Decimal(x) => Ok(PublicValue::Number(PublicNumber::Decimal(x))),
				}
			}
			TokenKind::Glued(Glued::Duration) => {
				let glued = pop_glued!(self, Duration);
				Ok(PublicValue::Duration(glued))
			}
			_ => {
				match self.parse_value_record_id_inner::<Json>(stk).await.map(PublicValue::RecordId)
				{
					Ok(x) => Ok(x),
					Err(err) => {
						tracing::debug!("Error parsing record id: {err:?}");
						self.parse_value_table().await.map(PublicValue::Table)
					}
				}
			}
		}
	}

	async fn reparse_json_legacy_strand(&mut self, stk: &mut Stk, strand: String) -> PublicValue {
		if let Ok(x) = Parser::new(strand.as_bytes()).parse_value_record_id(stk).await {
			return PublicValue::RecordId(x);
		}
		if let Ok(x) = Parser::new(strand.as_bytes()).next_token_value() {
			return PublicValue::Datetime(x);
		}
		// TODO: Fix this, uuid's don't actually work since it expects a 'u"'
		if let Ok(x) = Parser::new(strand.as_bytes()).next_token_value() {
			return PublicValue::Uuid(x);
		}

		//TODO: Fix record id and others
		PublicValue::String(strand)
	}

	async fn parse_value_object<VP>(
		&mut self,
		stk: &mut Stk,
		start: Span,
	) -> ParseResult<PublicObject>
	where
		VP: ValueParseFunc,
	{
		let mut obj = BTreeMap::new();
		loop {
			if self.eat(t!("}")) {
				return Ok(PublicObject::from(obj));
			}
			let key = self.parse_object_key()?;
			expected!(self, t!(":"));
			let value = stk.run(|ctx| VP::parse(self, ctx)).await?;
			obj.insert(key, value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				return Ok(PublicObject::from(obj));
			}
		}
	}

	async fn parse_value_array<VP>(
		&mut self,
		stk: &mut Stk,
		start: Span,
	) -> ParseResult<PublicArray>
	where
		VP: ValueParseFunc,
	{
		let mut array = Vec::new();
		loop {
			if self.eat(t!("]")) {
				return Ok(PublicArray::from(array));
			}
			let value = stk.run(|stk| VP::parse(self, stk)).await?;
			array.push(value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("]"), start)?;
				return Ok(PublicArray::from(array));
			}
		}
	}

	async fn parse_value_table(&mut self) -> ParseResult<PublicTable> {
		let table = self.parse_ident()?;
		Ok(PublicTable::new(table))
	}

	pub async fn parse_value_record_id(&mut self, stk: &mut Stk) -> ParseResult<PublicRecordId> {
		self.parse_value_record_id_inner::<SurrealQL>(stk).await
	}

	async fn parse_value_record_id_inner<VP>(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<PublicRecordId>
	where
		VP: ValueParseFunc,
	{
		let table = self.parse_ident()?;
		expected!(self, t!(":"));
		let peek = self.peek();
		let key = match peek.kind {
			t!("u'") | t!("u\"") => PublicRecordIdKey::Uuid(self.next_token_value::<PublicUuid>()?),
			t!("{") => {
				let peek = self.pop_peek();
				PublicRecordIdKey::Object(self.parse_value_object::<VP>(stk, peek.span).await?)
			}
			t!("[") => {
				let peek = self.pop_peek();
				PublicRecordIdKey::Array(self.parse_value_array::<VP>(stk, peek.span).await?)
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
					PublicRecordIdKey::Number(number)
				} else {
					PublicRecordIdKey::String(digits_str.to_owned())
				}
			}
			t!("-") => {
				self.pop_peek();
				let token = expected!(self, TokenKind::Digits);
				if let Ok(number) = self.lexer.lex_compound(token, compound::integer::<u64>) {
					// Parse to u64 and check if the value is equal to `-i64::MIN` via u64 as
					// `-i64::MIN` doesn't fit in an i64
					match number.value.cmp(&((i64::MAX as u64) + 1)) {
						Ordering::Less => PublicRecordIdKey::Number(-(number.value as i64)),
						Ordering::Equal => PublicRecordIdKey::Number(i64::MIN),
						Ordering::Greater => PublicRecordIdKey::String(format!(
							"-{}",
							self.lexer.span_str(number.span)
						)),
					}
				} else {
					PublicRecordIdKey::String(format!("-{}", self.lexer.span_str(token.span)))
				}
			}
			TokenKind::Digits => {
				if self.settings.flexible_record_id
					&& Self::kind_is_identifier(self.peek_whitespace1().kind)
				{
					let ident = self.parse_flexible_ident()?;
					PublicRecordIdKey::String(ident)
				} else {
					self.pop_peek();

					let digits_str = self.lexer.span_str(peek.span);
					if let Ok(number) = digits_str.parse::<i64>() {
						PublicRecordIdKey::Number(number)
					} else {
						PublicRecordIdKey::String(digits_str.to_owned())
					}
				}
			}
			TokenKind::Glued(Glued::Duration) if self.settings.flexible_record_id => {
				let slice = self.lexer.reader.span(peek.span);
				if slice.iter().any(|x| !x.is_ascii()) {
					unexpected!(self, peek, "a identifier");
				}
				// Should be valid utf-8 as it was already parsed by the lexer
				let text = String::from_utf8(slice.to_vec()).expect("parser validated utf8");
				PublicRecordIdKey::String(text)
			}
			_ => {
				let ident = if self.settings.flexible_record_id {
					self.parse_flexible_ident()?
				} else {
					self.parse_ident()?
				};
				PublicRecordIdKey::String(ident)
			}
		};

		Ok(PublicRecordId::new(table, key))
	}
}
