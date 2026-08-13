//! Parser for SQON, the SurrealQL Object Notation: the value literal subset of SurrealQL.
//!
//! The API follows the same design as serde: [`SqonDeserialize`] drives a [`Parser`], which
//! feeds the parsed values into visitors. This allows SQON text to be deserialized directly
//! into structured types without an intermediate value tree.

use core::fmt;
use std::ops::Bound;

use common::decimal::DecimalExt;
use logos::Lexer;
use parse_common::duration;
use rust_decimal::Decimal;
use surrealdb_types::{File, Table};

mod de;
mod visit;
pub use visit::{SqonKeyValueVisitor, SqonKeyVisitor, SqonValueVisitor, SqonVisitor};
mod token;
use token::Token;

use crate::token::RecordIdKeyToken;

macro_rules! try_some {
	($expr:expr) => {
		match $expr {
			Ok(x) => x,
			Err(e) => return Some(Err(e)),
		}
	};
}

/// Parse a value from SQON source text.
///
/// The source must contain exactly one value; trailing tokens after the value are an error.
pub fn from_str<T: SqonDeserialize>(source: &str) -> Result<T, Error> {
	let mut common = Common::new(source);
	let res = ValueAccess(&mut common).parse(T::visitor())?;
	common.expect_eof()?;
	Ok(res)
}

/// The kind of value the parser encountered, used in type mismatch errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueKind {
	None,
	Null,
	Bool,
	F64,
	I64,
	Decimal,
	String,
	Bytes,
	Duration,
	Datetime,
	Uuid,
	File,
	Array,
	Object,
	Set,
	RecordId,
	Range,
}

impl fmt::Display for ValueKind {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let name = match self {
			ValueKind::None => "none",
			ValueKind::Null => "null",
			ValueKind::Bool => "boolean",
			ValueKind::F64 => "float",
			ValueKind::I64 => "integer",
			ValueKind::Decimal => "decimal",
			ValueKind::String => "string",
			ValueKind::Bytes => "bytes",
			ValueKind::Duration => "duration",
			ValueKind::Datetime => "datetime",
			ValueKind::Uuid => "uuid",
			ValueKind::File => "file",
			ValueKind::Array => "array",
			ValueKind::Object => "object",
			ValueKind::Set => "set",
			ValueKind::RecordId => "record-id",
			ValueKind::Range => "range",
		};
		f.write_str(name)
	}
}

/// The kind of record id key the parser encountered, used in type mismatch errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordIdKeyKind {
	String,
	Number,
	Uuid,
	Array,
	Object,
	Range,
}

impl fmt::Display for RecordIdKeyKind {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let name = match self {
			RecordIdKeyKind::String => "string",
			RecordIdKeyKind::Number => "number",
			RecordIdKeyKind::Uuid => "uuid",
			RecordIdKeyKind::Array => "array",
			RecordIdKeyKind::Object => "object",
			RecordIdKeyKind::Range => "range",
		};
		f.write_str(name)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Kind {
	Value(ValueKind),
	Key(RecordIdKeyKind),
	Single,
}

impl fmt::Display for Kind {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Kind::Value(x) => x.fmt(f),
			Kind::Key(x) => x.fmt(f),
			Kind::Single => write!(f, "single value"),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
	UnexpectedType {
		found: Kind,
		expected: String,
	},
	InvalidSqon {
		message: String,
	},
	RecursionLimit,
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Error::UnexpectedType {
				found,
				expected,
			} => {
				write!(f, "Unexpected {found}, expected {expected}")
			}
			Error::InvalidSqon {
				message,
			} => f.write_str(message),
			Error::RecursionLimit => f.write_str("Value exceeded the parser recursion limit"),
		}
	}
}

impl std::error::Error for Error {}

pub trait Parser: Sized {
	fn parse<V: SqonVisitor>(self, v: V) -> Result<V::Value, Error>;
}

pub trait SqonDeserialize: Sized {
	/// The visitor which drives deserialization of this type.
	///
	/// The visitor is the single definition of how the type deserializes: generic impls
	/// compose through it directly, for example to parse a range start bound with this
	/// type's value visitor and convert it with [`SqonVisitor::finish`].
	type Visitor: SqonVisitor<Value = Self>;

	fn visitor() -> Self::Visitor;
}

/// Strip the surrounding quotes, and the optional `s` prefix, from a string token's source
/// text.
fn strip_string_quotes(slice: &str) -> &str {
	let slice = slice.strip_prefix('s').unwrap_or(slice);
	&slice[1..slice.len() - 1]
}

/// Parse the contents of a file string, with the prefix, quotes, and escape sequences already
/// removed.
///
/// The format is `bucket:/key`, where the bucket allows only alphanumeric characters and `_`,
/// `-`, and `.`, and the key additionally allows `/`.
fn parse_file(source: &str) -> Result<File, parse_common::Error> {
	let mut chars = source.char_indices();

	let mut bucket = String::new();
	loop {
		let Some((idx, c)) = chars.next() else {
			return Err(parse_common::Error {
				span: source.len()..source.len(),
				message: "Unexpected end of file string, missing bucket separator `:`".to_string(),
			});
		};

		match c {
			'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '.' => bucket.push(c),
			':' => break,
			c => {
				return Err(parse_common::Error {
					span: idx..idx + c.len_utf8(),
					message: format!(
						"Unexpected character `{c}`, file string buckets only allow alphanumeric characters and `_`, `-`, and `.`"
					),
				});
			}
		}
	}

	match chars.next() {
		Some((_, '/')) => {}
		Some((idx, c)) => {
			return Err(parse_common::Error {
				span: idx..idx + c.len_utf8(),
				message: format!("Unexpected character `{c}`, expected `/`"),
			});
		}
		None => {
			return Err(parse_common::Error {
				span: source.len()..source.len(),
				message: "Unexpected end of file string, missing file string key".to_string(),
			});
		}
	}

	let mut key = String::from("/");
	for (idx, c) in chars {
		match c {
			'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '.' | '/' => key.push(c),
			c => {
				return Err(parse_common::Error {
					span: idx..idx + c.len_utf8(),
					message: format!(
						"Unexpected character `{c}`, file string keys only allow alphanumeric characters and `_`, `-`, `.`, and `/`"
					),
				});
			}
		}
	}

	Ok(File::new(bucket, key))
}

struct Common<'a> {
	lexer: Lexer<'a, Token>,
	peek: Option<Result<Token, Error>>,
	scratch: String,
	depth: u32,
}

impl<'a> Common<'a> {
	fn new(source: &'a str) -> Common<'a> {
		Common {
			lexer: Lexer::new(source),
			peek: None,
			depth: 128,
			scratch: String::new(),
		}
	}

	fn peek(&mut self) -> Option<Result<Token, Error>> {
		if let Some(p) = self.peek.as_ref() {
			return Some(p.clone());
		}
		let t = match self.lexer.next()? {
			Ok(x) => Ok(x),
			Err(_) => Err(Error::InvalidSqon {
				message: format!("Invalid token `{}`", self.lexer.slice()),
			}),
		};
		self.peek = Some(t.clone());
		Some(t)
	}

	fn peek_expect(&mut self) -> Result<Token, Error> {
		let Some(token) = self.peek() else {
			return Err(Error::InvalidSqon {
				message: "Unexpected end of source".to_string(),
			});
		};
		token
	}

	/// Consume the token previously returned by `peek`.
	fn eat_peek(&mut self) {
		self.peek = None;
	}

	fn next(&mut self) -> Option<Result<Token, Error>> {
		if let Some(p) = self.peek.take() {
			return Some(p);
		};
		match self.lexer.next()? {
			Ok(x) => Some(Ok(x)),
			Err(_) => Some(Err(Error::InvalidSqon {
				message: format!("Invalid token `{}`", self.lexer.slice()),
			})),
		}
	}

	fn next_expect(&mut self) -> Result<Token, Error> {
		let Some(token) = self.next() else {
			return Err(Error::InvalidSqon {
				message: "Unexpected end of source".to_string(),
			});
		};
		token
	}

	fn expect_eof(&mut self) -> Result<(), Error> {
		match self.peek() {
			None => Ok(()),
			Some(Ok(_)) => Err(self.unexpected()),
			Some(Err(e)) => Err(e),
		}
	}

	fn unexpected(&self) -> Error {
		Error::InvalidSqon {
			message: format!("Unexpected token `{}`", self.lexer.slice()),
		}
	}

	fn parse<V: SqonVisitor>(&mut self, v: V) -> Result<V::Value, Error> {
		let token = self.next_expect()?;
		let slice = self.lexer.slice();
		self.parse_from(token, slice, v)
	}

	/// Continue `parse` from an already consumed first token; `slice` must be the source text
	/// of that token.
	fn parse_from<V: SqonVisitor>(
		&mut self,
		token: Token,
		slice: &'a str,
		mut v: V,
	) -> Result<V::Value, Error> {
		match token {
			Token::DotDot => {
				return if let Some(Token::CloseBrace | Token::CloseBracket | Token::Comma) | None =
					self.peek().transpose()?
				{
					v.finish_range(Bound::Unbounded, Bound::Unbounded)
				} else {
					self.enter_depth(|this| {
						v.finish_range(Bound::Unbounded, Bound::Excluded(ValueAccess(this)))
					})
				};
			}
			Token::DotDotEqual => {
				return self.enter_depth(|this| {
					v.finish_range(Bound::Unbounded, Bound::Included(ValueAccess(this)))
				});
			}
			_ => {}
		}

		let bound = self.parse_token_value(token, slice, v.visitor())?;

		match self.peek().transpose()? {
			Some(Token::DotDot) => {
				self.eat_peek();

				match self.peek().transpose()? {
					Some(Token::CloseBrace | Token::CloseBracket | Token::Comma) | None => {
						v.finish_range(Bound::Included(bound), Bound::Unbounded)
					}
					_ => self.enter_depth(|this| {
						v.finish_range(Bound::Included(bound), Bound::Excluded(ValueAccess(this)))
					}),
				}
			}
			Some(Token::ShevronDotDot) => {
				self.eat_peek();

				match self.peek().transpose()? {
					Some(Token::CloseBrace | Token::CloseBracket | Token::Comma) | None => {
						v.finish_range(Bound::Excluded(bound), Bound::Unbounded)
					}
					_ => self.enter_depth(|this| {
						v.finish_range(Bound::Excluded(bound), Bound::Excluded(ValueAccess(this)))
					}),
				}
			}
			Some(Token::ShevronDotDotEqual) => {
				self.eat_peek();

				self.enter_depth(|this| {
					v.finish_range(Bound::Excluded(bound), Bound::Included(ValueAccess(this)))
				})
			}
			Some(Token::DotDotEqual) => {
				self.eat_peek();

				self.enter_depth(|this| {
					v.finish_range(Bound::Included(bound), Bound::Included(ValueAccess(this)))
				})
			}
			_ => v.finish(bound),
		}
	}

	/// Parse a value from an already consumed first token; `slice` must be the source text of
	/// that token.
	fn parse_token_value<V>(
		&mut self,
		token: Token,
		slice: &'a str,
		visitor: V,
	) -> Result<V::Value, Error>
	where
		V: SqonValueVisitor,
	{
		match token {
			Token::OpenBrace => self.parse_brace(visitor),
			Token::OpenBracket => self.parse_bracket(visitor),
			Token::String => {
				let inner = strip_string_quotes(slice);
				let string = match parse_common::unescape(inner, &mut self.scratch) {
					Ok(x) => x.to_string(),
					Err(e) => {
						return Err(Error::InvalidSqon {
							message: e.message,
						});
					}
				};

				visitor.visit_string(string)
			}
			Token::RecordIdString => {
				let inner = &slice[2..(slice.len() - 1)];
				let inner = match parse_common::unescape(inner, &mut self.scratch) {
					Ok(x) => x,
					Err(e) => {
						return Err(Error::InvalidSqon {
							message: e.message,
						});
					}
				};

				// The sub-parser inherits the remaining recursion budget so nested record id
				// strings cannot reset the limit.
				let mut sub_parser = Common::new(inner);
				sub_parser.depth = self.depth;
				let Token::Ident = sub_parser.next_expect()? else {
					return Err(sub_parser.unexpected());
				};
				let ident = sub_parser.lexer.slice();
				let ident = sub_parser.unescape_ident(ident)?;

				let Token::Colon = sub_parser.next_expect()? else {
					return Err(sub_parser.unexpected());
				};

				let table = Table::new(ident);

				let res = visitor.visit_record_id(table, RecordIdKeyAccess(&mut sub_parser))?;
				sub_parser.expect_eof()?;
				Ok(res)
			}
			Token::UuidString => self
				.unescape_parse::<1, _, _>(slice, parse_common::uuid)
				.and_then(|x| visitor.visit_uuid(x)),
			Token::DateTimeString => self
				.unescape_parse::<1, _, _>(slice, parse_common::datetime)
				.and_then(|x| visitor.visit_datetime(x)),
			Token::FileString => self
				.unescape_parse::<1, _, _>(slice, parse_file)
				.and_then(|x| visitor.visit_file(x)),
			Token::ByteString => self
				.unescape_parse::<1, _, _>(slice, parse_common::bytes)
				.and_then(|x| visitor.visit_bytes(x)),
			Token::NaN => visitor.visit_f64(f64::NAN),
			Token::Infinity | Token::PosInfinity => visitor.visit_f64(f64::INFINITY),
			Token::NegInfinity => visitor.visit_f64(f64::NEG_INFINITY),
			Token::Float => {
				let float_slice = self.deunderscore(slice.trim_end_matches('f'));
				match float_slice.parse() {
					Ok(x) => visitor.visit_f64(x),
					Err(e) => Err(Error::InvalidSqon {
						message: format!("Failed to parse float: {e}"),
					}),
				}
			}
			Token::Decimal => {
				let decimal_slice = self.deunderscore(slice.trim_end_matches("dec"));
				let decimal = if decimal_slice.contains(['e', 'E']) {
					Decimal::from_scientific(decimal_slice)
				} else {
					Decimal::from_str_normalized(decimal_slice)
				};
				match decimal {
					Ok(x) => visitor.visit_decimal(x),
					Err(e) => Err(Error::InvalidSqon {
						message: format!("Failed to parse decimal: {e}"),
					}),
				}
			}
			Token::Int => {
				let int_slice = self.deunderscore(slice);
				match int_slice.parse() {
					Ok(x) => visitor.visit_i64(x),
					Err(e) => Err(Error::InvalidSqon {
						message: format!("Failed to parse integer: {e}"),
					}),
				}
			}
			Token::Duration => match duration(slice) {
				Err(e) => Err(Error::InvalidSqon {
					message: e.message,
				}),
				Ok(x) => visitor.visit_duration(x),
			},
			Token::Ident => {
				let Token::Colon = self.next_expect()? else {
					return Err(self.unexpected());
				};

				let ident = self.unescape_ident(slice)?;
				let table = Table::new(ident);

				self.enter_depth(|this| visitor.visit_record_id(table, RecordIdKeyAccess(this)))
			}
			Token::KwNull => visitor.visit_null(),
			Token::KwNone => visitor.visit_none(),
			Token::KwTrue => visitor.visit_bool(true),
			Token::KwFalse => visitor.visit_bool(false),
			_ => Err(Error::InvalidSqon {
				message: format!("Unexpected token `{slice}`"),
			}),
		}
	}

	fn unescape<'b>(&'b mut self, slice: &'b str) -> Result<&'b str, Error> {
		parse_common::unescape(slice, &mut self.scratch).map_err(|e| Error::InvalidSqon {
			message: e.message,
		})
	}

	/// Unescape the contents of a prefixed string token and parse it with `f`.
	///
	/// `slice` must be the token's source text; `TRUNCATE` is the length of the prefix before
	/// the opening quote.
	fn unescape_parse<
		const TRUNCATE: usize,
		T,
		F: FnOnce(&str) -> Result<T, parse_common::Error>,
	>(
		&mut self,
		slice: &str,
		f: F,
	) -> Result<T, Error> {
		let inner = &slice[TRUNCATE + 1..slice.len() - 1];
		let slice = self.unescape(inner)?;
		f(slice).map_err(|e| Error::InvalidSqon {
			message: e.message,
		})
	}

	fn unescape_ident(&mut self, slice: &str) -> Result<String, Error> {
		if slice.starts_with("`") {
			let slice = self.unescape(&slice[1..(slice.len() - 1)])?;
			Ok(slice.to_string())
		} else if slice.starts_with('⟨') {
			let start_offset = const { '⟨'.len_utf8() };
			let end_offset = const { '⟩'.len_utf8() };

			let slice = &slice[start_offset..(slice.len() - end_offset)];
			let slice = self.unescape(slice)?;

			Ok(slice.to_string())
		} else {
			Ok(slice.to_string())
		}
	}

	fn deunderscore<'b>(&'b mut self, slice: &'b str) -> &'b str {
		let Some((k, mut rest)) = slice.split_once("_") else {
			return slice;
		};

		self.scratch.clear();
		self.scratch.push_str(k);
		loop {
			let Some((k, new_rest)) = rest.split_once("_") else {
				self.scratch.push_str(rest);
				return &self.scratch;
			};

			self.scratch.push_str(k);
			rest = new_rest
		}
	}

	fn enter_depth<R, F: FnOnce(&mut Self) -> Result<R, Error>>(
		&mut self,
		f: F,
	) -> Result<R, Error> {
		let Some(depth) = self.depth.checked_sub(1) else {
			return Err(Error::RecursionLimit);
		};

		self.depth = depth;

		let res = f(self);

		self.depth += 1;

		res
	}

	fn parse_bracket<V>(&mut self, visitor: V) -> Result<V::Value, Error>
	where
		V: SqonValueVisitor,
	{
		self.enter_depth(move |this| {
			visitor.visit_array(ArrayAccess {
				done: false,
				parser: this,
			})
		})
	}

	/// Parse a brace-enclosed value, with the opening brace already consumed.
	///
	/// A brace value is an object if it is empty (`{}`) or starts with an object key followed
	/// by a colon, otherwise it is a set (`{,}` is the empty set).
	fn parse_brace<V>(&mut self, visitor: V) -> Result<V::Value, Error>
	where
		V: SqonValueVisitor,
	{
		self.enter_depth(move |this| {
			match this.peek_expect()? {
				Token::CloseBrace => {
					this.eat_peek();

					visitor.visit_object(ObjectAccess {
						parser: this,
						done: true,
					})
				}
				Token::Comma => {
					this.eat_peek();

					let Token::CloseBrace = this.next_expect()? else {
						return Err(this.unexpected());
					};

					visitor.visit_set(SetAccess {
						parser: this,
						done: true,
					})
				}
				Token::Ident
				| Token::String
				| Token::Int
				| Token::NaN
				| Token::Infinity
				| Token::KwNull
				| Token::KwNone
				| Token::KwTrue
				| Token::KwFalse => {
					let lexer = this.lexer.clone();
					// Pops the peeked token.
					let peek = this.peek.take();

					if let Token::Colon = this.next_expect()? {
						// Reset the lexer to before popping the key token.
						this.peek = peek;
						this.lexer = lexer;

						visitor.visit_object(ObjectAccess {
							parser: this,
							done: false,
						})
					} else {
						// Reset the lexer to before popping the key token.
						this.peek = peek;
						this.lexer = lexer;

						visitor.visit_set(SetAccess {
							parser: this,
							done: false,
						})
					}
				}
				_ => visitor.visit_set(SetAccess {
					parser: this,
					done: false,
				}),
			}
		})
	}

	/// Convert an already consumed object key token into the key string; `slice` must be the
	/// source text of that token.
	fn object_key(&mut self, token: Token, slice: &'a str) -> Result<String, Error> {
		match token {
			Token::Ident => self.unescape_ident(slice),
			Token::String => {
				let slice = strip_string_quotes(slice);
				let slice = self.unescape(slice)?;
				Ok(slice.to_string())
			}
			// Numbers and keyword-like tokens are valid object keys, taken verbatim.
			Token::Int
			| Token::NaN
			| Token::Infinity
			| Token::KwNull
			| Token::KwNone
			| Token::KwTrue
			| Token::KwFalse => Ok(slice.to_string()),
			_ => Err(Error::InvalidSqon {
				message: format!("Unexpected token `{slice}`, expected an object key"),
			}),
		}
	}
}

pub struct ArrayAccess<'p, 'src> {
	done: bool,
	parser: &'p mut Common<'src>,
}

impl<'p, 'src> ArrayAccess<'p, 'src> {
	pub fn next_entry<V: SqonDeserialize>(&mut self) -> Option<Result<V, Error>> {
		if self.done {
			return None;
		}

		if let Token::CloseBracket = try_some!(self.parser.peek_expect()) {
			self.parser.eat_peek();
			self.done = true;
			return None;
		}

		let v = try_some!(ValueAccess(self.parser).parse(V::visitor()));

		match try_some!(self.parser.next_expect()) {
			Token::CloseBracket => {
				self.done = true;
			}
			Token::Comma => {}
			_ => {
				return Some(Err(self.parser.unexpected()));
			}
		}
		Some(Ok(v))
	}
}

pub struct ObjectAccess<'p, 'src> {
	parser: &'p mut Common<'src>,
	done: bool,
}

impl<'p, 'src> ObjectAccess<'p, 'src> {
	pub fn next_entry<V: SqonDeserialize>(&mut self) -> Option<Result<(String, V), Error>> {
		if self.done {
			return None;
		}

		let token = try_some!(self.parser.next_expect());

		if let Token::CloseBrace = token {
			self.done = true;
			return None;
		}

		let slice = self.parser.lexer.slice();
		let field = try_some!(self.parser.object_key(token, slice));

		let Token::Colon = try_some!(self.parser.next_expect()) else {
			return Some(Err(self.parser.unexpected()));
		};

		let value = try_some!(ValueAccess(self.parser).parse(V::visitor()));

		match try_some!(self.parser.next_expect()) {
			Token::CloseBrace => {
				self.done = true;
			}
			Token::Comma => {}
			_ => {
				return Some(Err(self.parser.unexpected()));
			}
		};

		Some(Ok((field, value)))
	}
}

pub struct SetAccess<'p, 'src> {
	parser: &'p mut Common<'src>,
	done: bool,
}

impl<'p, 'src> SetAccess<'p, 'src> {
	pub fn next_entry<V: SqonDeserialize>(&mut self) -> Option<Result<V, Error>> {
		if self.done {
			return None;
		}

		if let Token::CloseBrace = try_some!(self.parser.peek_expect()) {
			self.parser.eat_peek();
			self.done = true;
			return None;
		};

		let value = try_some!(ValueAccess(self.parser).parse(V::visitor()));

		match try_some!(self.parser.next_expect()) {
			Token::Comma => {}
			Token::CloseBrace => self.done = true,
			_ => {
				return Some(Err(self.parser.unexpected()));
			}
		};

		Some(Ok(value))
	}
}

pub struct ValueAccess<'p, 'src>(&'p mut Common<'src>);

impl<'p, 'src> Parser for ValueAccess<'p, 'src> {
	fn parse<V: SqonVisitor>(self, v: V) -> Result<V::Value, Error> {
		self.0.parse(v)
	}
}

pub struct RecordIdKeyAccess<'p, 'src>(&'p mut Common<'src>);

impl<'p, 'src> RecordIdKeyAccess<'p, 'src> {
	pub fn parse_key<V: SqonKeyVisitor>(mut self, mut v: V) -> Result<V::Value, Error> {
		assert!(self.0.peek.is_none());

		let mut lexer = self.0.lexer.clone().morph::<RecordIdKeyToken>();
		let token = lexer.next().transpose().map_err(|_| Error::InvalidSqon {
			message: format!("Invalid token `{}`", lexer.slice()),
		})?;

		let start = match token {
			Some(RecordIdKeyToken::DotDot) => {
				self.0.lexer = lexer.clone().morph();

				if let None | Some(Token::CloseBrace | Token::CloseBracket | Token::Comma) =
					self.0.peek().transpose()?
				{
					return v.finish_range(Bound::Unbounded, Bound::Unbounded);
				} else {
					self.0.lexer = lexer.morph();
					self.0.peek = None;
					return self.0.enter_depth(|this| {
						v.finish_range(Bound::Unbounded, Bound::Excluded(RecordIdKeyAccess(this)))
					});
				}
			}
			Some(RecordIdKeyToken::DotDotEqual) => {
				self.0.lexer = lexer.clone().morph();

				return self.0.enter_depth(|this| {
					v.finish_range(Bound::Unbounded, Bound::Included(RecordIdKeyAccess(this)))
				});
			}
			Some(x) => {
				let slice = lexer.slice();
				self.0.lexer = lexer.morph();
				self.parse_key_value(x, slice, v.visitor())?
			}
			None => {
				return Err(Error::InvalidSqon {
					message: "Unexpected end of source".to_string(),
				});
			}
		};

		match self.0.peek().transpose()? {
			Some(Token::DotDot) => {
				self.0.eat_peek();

				let lexer = self.0.lexer.clone();

				if let None | Some(Token::CloseBrace | Token::CloseBracket | Token::Comma) =
					self.0.peek().transpose()?
				{
					v.finish_range(Bound::Included(start), Bound::Unbounded)
				} else {
					self.0.lexer = lexer;
					self.0.peek = None;
					self.0.enter_depth(|this| {
						v.finish_range(
							Bound::Included(start),
							Bound::Excluded(RecordIdKeyAccess(this)),
						)
					})
				}
			}
			Some(Token::ShevronDotDot) => {
				self.0.eat_peek();

				let lexer = self.0.lexer.clone();

				if let None | Some(Token::CloseBrace | Token::CloseBracket | Token::Comma) =
					self.0.peek().transpose()?
				{
					v.finish_range(Bound::Excluded(start), Bound::Unbounded)
				} else {
					self.0.lexer = lexer;
					self.0.peek = None;
					self.0.enter_depth(|this| {
						v.finish_range(
							Bound::Excluded(start),
							Bound::Excluded(RecordIdKeyAccess(this)),
						)
					})
				}
			}
			Some(Token::DotDotEqual) => {
				self.0.eat_peek();

				self.0.enter_depth(|this| {
					v.finish_range(Bound::Included(start), Bound::Included(RecordIdKeyAccess(this)))
				})
			}
			Some(Token::ShevronDotDotEqual) => {
				self.0.eat_peek();

				self.0.enter_depth(|this| {
					v.finish_range(Bound::Excluded(start), Bound::Included(RecordIdKeyAccess(this)))
				})
			}
			_ => v.finish(start),
		}
	}

	fn parse_key_value<V: SqonKeyValueVisitor>(
		&mut self,
		token: RecordIdKeyToken,
		slice: &str,
		v: V,
	) -> Result<V::Value, Error> {
		match token {
			RecordIdKeyToken::Ident => {
				let slice = self.0.unescape_ident(slice)?;
				v.visit_string(slice)
			}
			RecordIdKeyToken::String => {
				let slice = strip_string_quotes(slice);
				let slice = self.0.unescape(slice)?;
				v.visit_string(slice.to_string())
			}
			RecordIdKeyToken::Brace => self.0.enter_depth(|this| {
				v.visit_object(ObjectAccess {
					parser: this,
					done: false,
				})
			}),
			RecordIdKeyToken::Bracket => self.0.enter_depth(|this| {
				v.visit_array(ArrayAccess {
					parser: this,
					done: false,
				})
			}),
			RecordIdKeyToken::Integer => {
				let number = self.0.deunderscore(slice);
				if let Ok(n) = number.parse() {
					v.visit_number(n)
				} else {
					v.visit_string(slice.to_string())
				}
			}
			RecordIdKeyToken::UuidString => {
				let uuid = self.0.unescape_parse::<1, _, _>(slice, parse_common::uuid)?;
				v.visit_uuid(uuid)
			}
			RecordIdKeyToken::DotDot | RecordIdKeyToken::DotDotEqual => unreachable!(),
		}
	}
}
