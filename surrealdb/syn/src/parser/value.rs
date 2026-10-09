use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;

use surrealdb_types::{
	Array as PublicArray, Duration as PublicDuration, File as PublicFile,
	Geometry as PublicGeometry, Number as PublicNumber, Object as PublicObject,
	Range as PublicRange, RecordId as PublicRecordId, RecordIdKey as PublicRecordIdKey,
	Set as PublicSet, Table as PublicTable, Uuid as PublicUuid, Value as PublicValue,
};

use super::{ParseResult, Parser};
use crate::error::bail;
use crate::lexer::Lexer;
use crate::lexer::compound::{self, Numeric};
use crate::parser::mac::{expected, expected_whitespace};
use crate::parser::{enter_object_recursion, unexpected};
use crate::token::{Span, TokenKind, t};

trait ValueParseFunc {
	fn parse(parser: &mut Parser<'_>) -> ParseResult<PublicValue>;
}

struct SurrealQL;
struct Json;

impl ValueParseFunc for SurrealQL {
	fn parse(parser: &mut Parser<'_>) -> ParseResult<PublicValue> {
		parser.parse_value()
	}
}

impl ValueParseFunc for Json {
	fn parse(parser: &mut Parser<'_>) -> ParseResult<PublicValue> {
		parser.parse_json()
	}
}

impl Parser<'_> {
	/// Parse a complete value which cannot contain non-literal expressions.
	pub fn parse_value(&mut self) -> ParseResult<PublicValue> {
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
				let open = self.pop_peek().span;

				if self.eat(t!("}")) {
					return Ok(PublicValue::Object(PublicObject::new()));
				}

				// First, check if it's an empty set. `{,}` is an empty set.
				if self.eat(t!(",")) {
					self.expect_closing_delimiter(t!("}"), open)?;
					return Ok(PublicValue::Set(PublicSet::new()));
				}

				enter_object_recursion!(this = self => {
					if let t!("\"")
					| t!("'")
					| TokenKind::Identifier
					| TokenKind::Digits
					| TokenKind::Keyword(_)
					| TokenKind::Language(_)
					| TokenKind::Algorithm(_)
					| TokenKind::Distance(_)
					| TokenKind::VectorType(_) = this.peek().kind
						&& let Some(x) = this
							.speculate(|this| {
								let key = this.parse_object_key()?;
								if !this.eat(t!(":")) {
									return Ok(None);
								}
								let value = this.parse_value()?;
								let mut res = BTreeMap::new();
								res.insert(key, value);

								if this.eat(t!(",")) {
									this.parse_value_object::<SurrealQL>(open, res).map(Some)
								} else {
									this.expect_closing_delimiter(t!("}"), open)?;
									Ok(Some(PublicObject::from(res)))
								}
							})?
					{
						if let Some(x) = PublicGeometry::try_from_object(&x) {
							return Ok(PublicValue::Geometry(x));
						} else {
							return Ok(PublicValue::Object(x));
						}
					}

					// It must be a set: `{1, 2, 3}` or `{value}`
					let set = this.parse_value_set::<SurrealQL>(token.span)?;
					PublicValue::Set(set)
				})
			}
			t!("[") => {
				self.pop_peek();
				enter_object_recursion!(this = self => {
					this.parse_value_array::<SurrealQL>(token.span)
						.map(PublicValue::Array)?
				})
			}
			t!("\"") | t!("'") => {
				let strand = self.parse_string_lit()?;
				if self.settings.legacy_strands {
					self.reparse_json_legacy_strand(strand)
				} else {
					PublicValue::String(strand)
				}
			}
			t!("d\"") | t!("d'") => PublicValue::Datetime(self.next_token_value()?),
			t!("u\"") | t!("u'") => PublicValue::Uuid(self.next_token_value()?),
			t!("b\"") | t!("b'") => PublicValue::Bytes(self.next_token_value()?),
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
								Numeric::Integer(x) => x.into_int(number_span)? as f64,
							};

							self.pop_peek();

							let y = self.next_token_value::<f64>()?;
							self.expect_closing_delimiter(t!(")"), open)?;
							PublicValue::Geometry(PublicGeometry::Point(geo_types::Point::new(
								x, y,
							)))
						} else {
							self.expect_closing_delimiter(t!(")"), open)?;

							match number {
								Numeric::Float(x) => PublicValue::Number(PublicNumber::Float(x)),
								Numeric::Integer(x) => {
									PublicValue::Number(PublicNumber::Int(x.into_int(number_span)?))
								}
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
						enter_object_recursion!(this = self => {
							let res = this.parse_value()?;
							this.expect_closing_delimiter(t!(")"), open)?;
							res
						})
					}
				}
			}
			t!("..") => {
				self.pop_peek();
				match self.peek_whitespace().map(|x| x.kind) {
					Some(t!("=")) => {
						self.pop_peek();
						enter_object_recursion!(this = self => {
							let v = this.parse_value()?;
							PublicValue::Range(Box::new(PublicRange {
								start: Bound::Unbounded,
								end: Bound::Included(v),
							}))
						})
					}
					Some(x) if Self::kind_starts_expression(x) => {
						enter_object_recursion!(this = self => {
							let v = this.parse_value()?;
							PublicValue::Range(Box::new(PublicRange {
								start: Bound::Unbounded,
								end: Bound::Excluded(v),
							}))
						})
					}
					_ => PublicValue::Range(Box::new(PublicRange {
						start: Bound::Unbounded,
						end: Bound::Unbounded,
					})),
				}
			}
			t!("-") | t!("+") | TokenKind::Digits => {
				self.pop_peek();
				let compound = self.lex_compound(token, compound::numeric)?;
				match compound.value {
					Numeric::Duration(x) => PublicValue::Duration(PublicDuration::from(x)),
					Numeric::Integer(x) => {
						PublicValue::Number(PublicNumber::Int(x.into_int(compound.span)?))
					}
					Numeric::Float(x) => PublicValue::Number(PublicNumber::Float(x)),
					Numeric::Decimal(x) => PublicValue::Number(PublicNumber::Decimal(x)),
				}
			}
			_ => self.parse_value_record_id_inner::<SurrealQL>().map(PublicValue::RecordId)?,
		};

		match self.peek_whitespace().map(|x| x.kind) {
			Some(t!(">")) => {
				self.pop_peek();
				expected_whitespace!(self, t!(".."));
				match self.peek_whitespace().map(|x| x.kind) {
					Some(t!("=")) => {
						self.pop_peek();
						enter_object_recursion!(this = self => {
							let v = this.parse_value()?;
							Ok(PublicValue::Range(Box::new(PublicRange {
								start: Bound::Excluded(res),
								end: Bound::Included(v),
							})))
						})
					}
					Some(x) if Self::kind_starts_expression(x) => {
						enter_object_recursion!(this = self => {
							let v = this.parse_value()?;
							Ok(PublicValue::Range(Box::new(PublicRange {
								start: Bound::Excluded(res),
								end: Bound::Excluded(v),
							})))
						})
					}
					_ => Ok(PublicValue::Range(Box::new(PublicRange {
						start: Bound::Excluded(res),
						end: Bound::Unbounded,
					}))),
				}
			}
			Some(t!("..")) => {
				self.pop_peek();

				match self.peek_whitespace().map(|x| x.kind) {
					Some(t!("=")) => {
						self.pop_peek();
						enter_object_recursion!(this = self => {
							let v = this.parse_value()?;
							Ok(PublicValue::Range(Box::new(PublicRange {
								start: Bound::Included(res),
								end: Bound::Included(v),
							})))
						})
					}
					Some(x) if Self::kind_starts_expression(x) => {
						enter_object_recursion!(this = self => {
							let v = this.parse_value()?;
							Ok(PublicValue::Range(Box::new(PublicRange {
								start: Bound::Included(res),
								end: Bound::Excluded(v),
							})))
						})
					}
					_ => Ok(PublicValue::Range(Box::new(PublicRange {
						start: Bound::Included(res),
						end: Bound::Unbounded,
					}))),
				}
			}
			_ => Ok(res),
		}
	}

	pub fn parse_json(&mut self) -> ParseResult<PublicValue> {
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
				let open = self.pop_peek().span;

				if self.eat(t!("}")) {
					return Ok(PublicValue::Object(PublicObject::new()));
				}

				enter_object_recursion!(this = self => {
					this.parse_value_object::<Json>(open, BTreeMap::new())
						.map(PublicValue::Object)
				})
			}
			t!("[") => {
				self.pop_peek();
				enter_object_recursion!(this = self => {
					this.parse_value_array::<Json>(token.span).map(PublicValue::Array)
				})
			}
			t!("\"") | t!("'") => {
				let strand = self.parse_string_lit()?;
				if self.settings.legacy_strands {
					Ok(self.reparse_json_legacy_strand(strand))
				} else {
					Ok(PublicValue::String(strand))
				}
			}
			t!("-") | t!("+") | TokenKind::Digits => {
				self.pop_peek();
				let compound = self.lex_compound(token, compound::numeric)?;
				match compound.value {
					Numeric::Duration(x) => Ok(PublicValue::Duration(PublicDuration::from(x))),
					Numeric::Integer(x) => {
						Ok(PublicValue::Number(PublicNumber::Int(x.into_int(compound.span)?)))
					}
					Numeric::Float(x) => Ok(PublicValue::Number(PublicNumber::Float(x))),
					Numeric::Decimal(x) => Ok(PublicValue::Number(PublicNumber::Decimal(x))),
				}
			}
			_ => match self.parse_value_record_id_inner::<Json>().map(PublicValue::RecordId) {
				Ok(x) => Ok(x),
				Err(err) => {
					tracing::debug!("Error parsing record id: {err:?}");
					self.parse_value_table().map(PublicValue::Table)
				}
			},
		}
	}

	fn reparse_json_legacy_strand(&mut self, strand: String) -> PublicValue {
		if let Ok(x) = Parser::new(strand.as_bytes()).parse_value_record_id() {
			return PublicValue::RecordId(x);
		}

		if let Ok(x) = Lexer::lex_datetime(&strand) {
			return PublicValue::Datetime(x);
		}

		if let Ok(x) = Lexer::lex_uuid(&strand) {
			return PublicValue::Uuid(x);
		}

		PublicValue::String(strand)
	}

	fn parse_value_object<VP>(
		&mut self,
		start: Span,
		mut obj: BTreeMap<String, PublicValue>,
	) -> ParseResult<PublicObject>
	where
		VP: ValueParseFunc,
	{
		loop {
			if self.eat(t!("}")) {
				return Ok(PublicObject::from(obj));
			}
			let key = self.parse_object_key()?;
			expected!(self, t!(":"));
			let value = VP::parse(self)?;
			obj.insert(key, value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				return Ok(PublicObject::from(obj));
			}
		}
	}

	fn parse_value_set<VP>(&mut self, start: Span) -> ParseResult<PublicSet>
	where
		VP: ValueParseFunc,
	{
		let mut set = PublicSet::new();
		loop {
			if self.eat(t!("}")) {
				return Ok(set);
			}

			let value = VP::parse(self)?;
			set.insert(value);

			if !self.eat(t!(",")) {
				if set.len() <= 1 {
					// Single-element object: `{value}`
					// We could parse this in SQON, but in SurrealQL this is a block statement.
					// So we instead throw an error and require the user to add a trailing
					// comma for a set.
					unexpected!(
						self,
						self.peek(),
						"`,`",
						=> "Sets with a single value must have at least a single comma"
					);
				}

				self.expect_closing_delimiter(t!("}"), start)?;

				return Ok(set);
			}
		}
	}

	fn parse_value_array<VP>(&mut self, start: Span) -> ParseResult<PublicArray>
	where
		VP: ValueParseFunc,
	{
		let mut array = Vec::new();
		loop {
			if self.eat(t!("]")) {
				return Ok(PublicArray::from(array));
			}
			let value = VP::parse(self)?;
			array.push(value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("]"), start)?;
				return Ok(PublicArray::from(array));
			}
		}
	}

	fn parse_value_table(&mut self) -> ParseResult<PublicTable> {
		let table = self.parse_ident()?.into_string();
		Ok(PublicTable::new(table))
	}

	pub fn parse_value_record_id(&mut self) -> ParseResult<PublicRecordId> {
		self.parse_value_record_id_inner::<SurrealQL>()
	}

	fn parse_value_record_id_inner<VP>(&mut self) -> ParseResult<PublicRecordId>
	where
		VP: ValueParseFunc,
	{
		let table = self.parse_ident()?.into_string();
		expected!(self, t!(":"));
		let peek = self.peek();
		let key = match peek.kind {
			t!("u'") | t!("u\"") => PublicRecordIdKey::Uuid(self.next_token_value::<PublicUuid>()?),
			t!("{") => {
				let peek = self.pop_peek();
				enter_object_recursion!(this = self => {
					PublicRecordIdKey::Object(
						this.parse_value_object::<VP>(peek.span, BTreeMap::new())?,
					)
				})
			}
			t!("[") => {
				let peek = self.pop_peek();
				enter_object_recursion!(this = self => {
					PublicRecordIdKey::Array(this.parse_value_array::<VP>(peek.span)?)
				})
			}
			t!("+") => {
				self.pop_peek();
				// starting with a + so it must be a number
				let digits_token = if let Some(digits_token) = self.peek_whitespace() {
					match digits_token.kind {
						TokenKind::Digits => digits_token,
						_ => unexpected!(self, digits_token, "an integer"),
					}
				} else {
					bail!("Unexpected whitespace",@self.last_span() => "No whitespace allowed after this token")
				};

				match self.peek_whitespace().map(|x| x.kind) {
					Some(t!(".")) => {
						// Numeric record-id keys are stored as `i64`, so a fractional part
						// (`.`), an exponent (`e`/`E`), or a numeric type suffix is not
						// allowed here.
						unexpected!(self, self.peek(), "an integer", => "Numeric Record-id keys can only be integers");
					}
					Some(x) if Self::kind_is_identifier(x) => {
						let span = peek.span.covers(self.peek().span);
						bail!("Unexpected token `{x}` expected an integer", @span);
					}
					// allowed
					_ => {}
				}

				let digits_str = self.span_str(digits_token.span);
				if let Ok(number) = digits_str.parse() {
					PublicRecordIdKey::Number(number)
				} else {
					PublicRecordIdKey::String(digits_str.to_owned())
				}
			}
			t!("-") => {
				self.pop_peek();
				let token = expected!(self, TokenKind::Digits);
				if let Ok(number) = self.lex_compound(token, compound::integer::<u64>) {
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
					&& let Some(peek) = self.peek_whitespace1()
					&& Self::kind_is_identifier(peek.kind)
				{
					let ident = self.parse_flexible_ident()?;
					PublicRecordIdKey::String(ident)
				} else {
					self.pop_peek();

					let digits_str = self.span_str(peek.span);
					if let Ok(number) = digits_str.parse::<i64>() {
						PublicRecordIdKey::Number(number)
					} else {
						PublicRecordIdKey::String(digits_str.to_owned())
					}
				}
			}
			_ => {
				let ident = if self.settings.flexible_record_id {
					self.parse_flexible_ident()?
				} else {
					self.parse_ident()?.into_string()
				};
				PublicRecordIdKey::String(ident)
			}
		};

		Ok(PublicRecordId::new(table, key))
	}
}
