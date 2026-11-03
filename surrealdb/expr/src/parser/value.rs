//! The literal-value grammars: SurrealQL values ([`value`]), JSON ([`json`])
//! and record ids ([`record_id`]), parsed straight into public values.
//!
//! The grammars nest through arrays, objects, sets, parentheses, ranges and
//! record-id keys. Nesting is held on an explicit stack and bounded by the
//! object budget, charged where earlier releases charged it, so it costs no
//! native stack. A failure is returned unrendered: rendering it against the
//! source is the caller's concern.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::Duration;

use surrealdb_parser_next::decode;
use surrealdb_parser_next::kw::Kw;
use surrealdb_parser_next::lex::{
	Compound, Lexer, NumKind, PREFIX_DATE, PREFIX_INTERVAL, PREFIX_TIME, PREFIX_TIMESTAMP, T, Tok,
	compound_error, decode_duration, invalid_reason,
};
use surrealdb_parser_next::parse::starts_expression;
use surrealdb_types::{
	Array, Geometry, Number, Object, Range, RecordId, RecordIdKey, Regex, Set, Table, Value, Wide,
};

use super::entry::{Error, Settings};
use super::{Reject, diagnostic as render, lit};
use crate::expr::Literal;

/// Parses a SurrealQL value. Input after the value is not read.
pub fn value(src: &str, settings: &Settings) -> Result<Value, Error> {
	Parser::new(src, settings, false, false).run()
}

/// Parses a JSON value, with `\uXXXX` surrogate pairs. Input after the value
/// is not read.
pub fn json(src: &str, settings: &Settings) -> Result<Value, Error> {
	Parser::new(src, settings, true, true).run()
}

/// Parses a record id, `table:key`, with a value key. The record id must be
/// the whole of `src`: text after it is an error rather than ignored, so a
/// string that only begins with a record id never names that record.
pub fn record_id(src: &str, settings: &Settings) -> Result<RecordId, Error> {
	let mut p = Parser::new(src, settings, false, false);
	let Value::RecordId(r) = p.run_record_id()? else {
		unreachable!("a record-id parse yields a record id");
	};
	if p.tok.kind != T::Eof {
		return Err(p.unexpected("the end of the record id"));
	}
	Ok(r)
}

type R<T> = Result<T, Error>;

/// A value under construction, waiting for its next element.
/// Each collection frame holds the position of its opening delimiter, which
/// a missing closer's error points at.
enum Frame {
	Array(Vec<Value>, u32),
	/// An object, waiting for the value of `key`.
	Object(BTreeMap<String, Value>, String, u32),
	/// A set; its length is after deduplication, which decides whether a
	/// single element needs a trailing comma, as earlier releases read it.
	Set(Set, u32),
	/// `( value )`.
	Paren(u32),
	/// A range waiting for its end. `postfix` when it followed a start
	/// value (`a..`), which makes the range final: no range follows it.
	RangeEnd {
		start: Bound<Value>,
		inclusive: bool,
		postfix: bool,
	},
	/// A record id whose key is an array or object under construction.
	KeyArray(String, Vec<Value>, u32),
	KeyObject(String, BTreeMap<String, Value>, String, u32),
}

/// What starting a value produced.
enum Start {
	/// A finished value, and whether a range may follow it.
	Done(Value, bool),
	/// A frame was pushed; an element is wanted.
	Pushed,
}

struct Parser<'a> {
	src: &'a str,
	lex: Lexer<'a>,
	tok: Tok,
	/// The last consumed token.
	prev: Tok,
	settings: &'a Settings,
	json: bool,
	json_escapes: bool,
	/// Object levels left (`object_recursion_limit`).
	budget: u16,
	frames: Vec<Frame>,
	/// Parsing a record id alone: no range follows the outermost one.
	record_only: bool,
}

impl<'a> Parser<'a> {
	fn new(src: &'a str, settings: &'a Settings, json: bool, json_escapes: bool) -> Self {
		let mut lex = Lexer::new(src.as_bytes());
		let tok = lex.next_token();
		Parser {
			src,
			lex,
			tok,
			prev: tok,
			settings,
			json,
			json_escapes,
			budget: settings.limits.object_depth,
			frames: Vec::new(),
			record_only: false,
		}
	}

	fn bump(&mut self) -> Tok {
		let t = self.tok;
		self.prev = t;
		self.tok = self.lex.next_token();
		t
	}

	/// An error over `start..end`.
	fn err_at(
		&self,
		start: u32,
		end: u32,
		msg: impl Into<std::borrow::Cow<'static, str>>,
	) -> Error {
		Error::Lower(Reject::new(msg).at(start, end))
	}

	/// The established unexpected-token error at the current token, with
	/// `expected` in its wording. An invalid token reports why the lexer rejected it.
	fn unexpected(&self, expected: &str) -> Error {
		let t = self.tok;
		if t.kind == T::Invalid {
			return Error::Syntax(invalid_reason(self.src.as_bytes(), t.start));
		}
		let msg = if t.kind == T::Eof {
			format!("Unexpected end of file, expected {expected}")
		} else {
			format!(
				"Unexpected token `{}`, expected {expected}",
				render::token_name(self.src, t.start)
			)
		};
		self.err_at(t.start, self.syn_token_end(t), msg)
	}

	/// A missing closing delimiter `closer` for the opener at `open`.
	fn unclosed(&self, closer: &str, open: u32) -> Error {
		let t = self.tok;
		let msg = format!(
			"Unexpected token `{}` expected delimiter `{closer}`",
			render::token_name(self.src, t.start)
		);
		let r = Reject::new(msg).at(t.start, self.syn_token_end(t));
		Error::Lower(Reject {
			extra: Some((open, open + 1, "expected this delimiter to close".into())),
			..r
		})
	}

	/// Where the token at `t` ends for error spans, as clients have always
	/// seen them: `??` is two `?` tokens and a digits token runs on over `_`.
	fn syn_token_end(&self, t: Tok) -> u32 {
		if t.kind == T::QQ {
			t.start + 1
		} else if t.kind == T::Digits {
			self.digits_end(t.start)
		} else {
			t.end
		}
	}

	/// The established error for the malformed literal at `start`, read as `mode`.
	fn number_err(&self, start: u32, mode: Compound) -> Error {
		match compound_error(self.src.as_bytes(), start, mode) {
			Some(d) => Error::Syntax(d),
			None => self.err_at(start, self.tok.end, "invalid number"),
		}
	}

	fn peek1(&self) -> Tok {
		let mut l = self.lex;
		l.next_token()
	}

	fn reset_to(&mut self, pos: u32) {
		self.lex.pos = pos;
		self.tok = self.lex.next_token();
	}

	fn text(&self, t: Tok) -> &'a str {
		&self.src[t.start as usize..t.end as usize]
	}

	fn eat(&mut self, k: T) -> bool {
		if self.tok.kind == k {
			self.bump();
			true
		} else {
			false
		}
	}

	/// Consumes a token of kind `k`, or fails having consumed the token in
	/// its place. `expected` is `k` as the established error names it.
	fn expect(&mut self, k: T, expected: &'static str) -> R<()> {
		if self.eat(k) {
			Ok(())
		} else {
			let e = self.unexpected(expected);
			self.consume_failed();
			Err(e)
		}
	}

	/// Consumes the closing delimiter `k` of the opener at `open`, or fails
	/// without consuming.
	fn expect_close(&mut self, k: T, open: u32) -> R<()> {
		if self.eat(k) {
			return Ok(());
		}
		let closer = if k == T::RBracket {
			"]"
		} else if k == T::RBrace {
			"}"
		} else {
			")"
		};
		Err(self.unclosed(closer, open))
	}

	/// Consumes the token a failure stopped at. `??` counts as two tokens
	/// here, so only its first `?` is consumed.
	fn consume_failed(&mut self) {
		if self.tok.kind == T::QQ {
			let at = self.tok.start + 1;
			self.reset_to(at);
		} else if self.tok.kind == T::Digits {
			// The digits token runs on over `_`, as it always has.
			let at = self.digits_end(self.tok.start);
			self.reset_to(at);
		} else if self.tok.kind == T::Invalid {
			// An unexpected character, or an unterminated string, is one
			// token.
			let mut at = (self.tok.start as usize + 1).max(self.tok.end as usize);
			while !self.src.is_char_boundary(at) {
				at += 1;
			}
			self.reset_to(at as u32);
		} else {
			self.bump();
		}
	}

	/// An identifier's name at the current token, consumed whether or not it
	/// is one.
	fn take_ident(&mut self) -> R<String> {
		let t = self.tok;
		match self.ident(t) {
			Ok(name) => {
				self.bump();
				Ok(name)
			}
			// An escaped identifier whose escapes do not decode, consumed.
			Err(e) if t.kind == T::EscIdent => {
				self.bump();
				Err(e)
			}
			Err(_) => {
				let e = self.unexpected("an identifier");
				self.consume_failed();
				Err(e)
			}
		}
	}

	/// Enters one object level.
	fn enter(&mut self) -> R<()> {
		if self.budget == 0 {
			return Err(self.err_at(
				self.prev.start,
				self.prev.end,
				"Exceeded object recursion depth limit",
			));
		}
		self.budget -= 1;
		Ok(())
	}

	fn leave(&mut self) {
		self.budget += 1;
	}

	fn push(&mut self, f: Frame) -> R<Start> {
		self.frames.push(f);
		Ok(Start::Pushed)
	}

	fn run(&mut self) -> R<Value> {
		let s = if self.json {
			self.start_json()?
		} else {
			self.start()?
		};
		self.drive(s)
	}

	fn run_record_id(&mut self) -> R<Value> {
		self.record_only = true;
		let s = self.record_id_start()?;
		self.drive(s)
	}

	/// Finishes the value `s` began: parses elements for pushed frames and
	/// closes them until the stack is empty.
	fn drive(&mut self, mut s: Start) -> R<Value> {
		'start: loop {
			let (mut v, mut rangeable) = match s {
				Start::Done(v, r) => (v, r),
				Start::Pushed => {
					s = self.element()?;
					continue 'start;
				}
			};
			loop {
				if rangeable
					&& !self.json && let Some(next) = self.postfix_range(&mut v)?
				{
					s = next;
					continue 'start;
				}
				let Some(f) = self.frames.pop() else {
					return Ok(v);
				};
				match f {
					Frame::Array(mut items, open) => {
						items.push(v);
						if self.eat(T::Comma) {
							if !self.eat(T::RBracket) {
								self.frames.push(Frame::Array(items, open));
								s = self.element()?;
								continue 'start;
							}
						} else {
							self.expect_close(T::RBracket, open)?;
						}
						self.leave();
						v = Value::Array(Array::from(items));
						rangeable = true;
					}
					Frame::KeyArray(table, mut items, open) => {
						items.push(v);
						if self.eat(T::Comma) {
							if !self.eat(T::RBracket) {
								self.frames.push(Frame::KeyArray(table, items, open));
								s = self.element()?;
								continue 'start;
							}
						} else {
							self.expect_close(T::RBracket, open)?;
						}
						self.leave();
						v = Value::RecordId(RecordId::new(table, RecordIdKey::Array(items.into())));
						rangeable = !(self.record_only && self.frames.is_empty());
					}
					Frame::Object(mut map, key, open) => {
						map.insert(key, v);
						match self.object_next(open)? {
							Some(key) => {
								self.frames.push(Frame::Object(map, key, open));
								s = self.element()?;
								continue 'start;
							}
							None => {
								self.leave();
								let obj = Object::from(map);
								// An object is never a range start.
								rangeable = false;
								v = match Geometry::try_from_object(&obj) {
									Some(g) if !self.json => Value::Geometry(g),
									_ => Value::Object(obj),
								};
							}
						}
					}
					Frame::KeyObject(table, mut map, key, open) => {
						map.insert(key, v);
						match self.object_next(open)? {
							Some(key) => {
								self.frames.push(Frame::KeyObject(table, map, key, open));
								s = self.element()?;
								continue 'start;
							}
							None => {
								self.leave();
								v = Value::RecordId(RecordId::new(
									table,
									RecordIdKey::Object(Object::from(map)),
								));
								rangeable = !(self.record_only && self.frames.is_empty());
							}
						}
					}
					Frame::Set(mut items, open) => {
						items.insert(v);
						if self.eat(T::Comma) {
							if !self.eat(T::RBrace) {
								self.frames.push(Frame::Set(items, open));
								s = self.element()?;
								continue 'start;
							}
						} else if items.len() <= 1 {
							let Error::Lower(r) = self.unexpected("`,`") else {
								return Err(self.unexpected("`,`"));
							};
							return Err(Error::Lower(r.with_label(
								"Sets with a single value must have at least a single comma",
							)));
						} else {
							self.expect_close(T::RBrace, open)?;
						}
						self.leave();
						v = Value::Set(items);
						rangeable = true;
					}
					Frame::Paren(open) => {
						self.expect_close(T::RParen, open)?;
						self.leave();
						rangeable = true;
					}
					Frame::RangeEnd {
						start,
						inclusive,
						postfix,
					} => {
						self.leave();
						let end = if inclusive {
							Bound::Included(v)
						} else {
							Bound::Excluded(v)
						};
						v = Value::Range(Box::new(Range::from((start, end))));
						rangeable = !postfix;
					}
				}
			}
		}
	}

	/// Starts the next element of the frame on top.
	fn element(&mut self) -> R<Start> {
		if self.json {
			self.start_json()
		} else {
			self.start()
		}
	}

	/// After an object entry: the next key, or `None` at the closing brace.
	fn object_next(&mut self, open: u32) -> R<Option<String>> {
		if !self.eat(T::Comma) {
			self.expect_close(T::RBrace, open)?;
			return Ok(None);
		}
		if self.eat(T::RBrace) {
			return Ok(None);
		}
		let key = self.object_key()?;
		self.expect(T::Colon, ":")?;
		Ok(Some(key))
	}

	/// A range after a value: `>..`, `..`, `..=`, each directly after it.
	fn postfix_range(&mut self, v: &mut Value) -> R<Option<Start>> {
		let excluded = if self.tok.kind == T::Gt && !self.tok.ws() {
			self.bump();
			if self.tok.ws() {
				let gt = self.prev;
				return Err(Error::Lower(
					Reject::new("Unexpected whitespace")
						.at(gt.start, gt.end)
						.with_label("No whitespace allowed after this token"),
				));
			}
			if self.tok.kind != T::DotDot {
				return Err(self.unexpected(".."));
			}
			true
		} else if self.tok.kind == T::DotDot && !self.tok.ws() {
			false
		} else {
			return Ok(None);
		};
		self.bump();
		let start = std::mem::replace(v, Value::None);
		let start = if excluded {
			Bound::Excluded(start)
		} else {
			Bound::Included(start)
		};
		self.range_end(start, true).map(Some)
	}

	/// The end of a range after its `..`.
	fn range_end(&mut self, start: Bound<Value>, postfix: bool) -> R<Start> {
		let inclusive = if self.tok.kind == T::Eq && !self.tok.ws() {
			self.bump();
			true
		} else if !self.tok.ws() && starts_expression(&self.tok) {
			false
		} else {
			let r = Value::Range(Box::new(Range::from((start, Bound::Unbounded))));
			return Ok(Start::Done(r, !postfix));
		};
		self.enter()?;
		self.push(Frame::RangeEnd {
			start,
			inclusive,
			postfix,
		})?;
		self.start()
	}

	fn string(&self, t: Tok) -> R<String> {
		let mut s = String::new();
		if !decode::unescape_with(decode::strip_delims(self.text(t)), &mut s, self.json_escapes) {
			return Err(match decode::escape_error(self.text(t), self.json_escapes) {
				Some((a, b, msg, label)) => {
					Error::Lower(Reject::new(msg).at(t.start + a, t.start + b).with_label(label))
				}
				None => self.err_at(t.start, t.end, "Invalid escape sequence"),
			});
		}
		Ok(s)
	}

	/// An object key.
	fn object_key(&mut self) -> R<String> {
		let t = self.tok;
		if t.kind == T::Ident {
			self.bump();
			Ok(self.text(t).to_owned())
		} else if t.kind == T::EscIdent || (t.kind == T::Str && matches!(t.aux(), 0 | b's')) {
			self.bump();
			self.string(t)
		} else if t.kind == T::Digits {
			let mut l = self.lex;
			match l.numeric(t.start) {
				Ok(NumKind::Duration) | Err(_) => Err(self.number_err(t.start, Compound::Number)),
				Ok(kind) => {
					let end = l.pos;
					self.reset_to(end);
					let text = &self.src[t.start as usize..end as usize];
					// The key is spelled as written, but must be a valid
					// number.
					match kind {
						NumKind::Int
						| NumKind::I8
						| NumKind::U8
						| NumKind::I16
						| NumKind::U16
						| NumKind::I32
						| NumKind::U32
						| NumKind::I64
						| NumKind::U64
						| NumKind::Float
						| NumKind::F16
						| NumKind::F32
						| NumKind::F64 => {
							if kind == NumKind::Int {
								// A key's magnitude is bounded by `i64::MIN`'s.
								let digits: String = text.chars().filter(|&c| c != '_').collect();
								if let Ok(u) = digits.parse::<u64>()
									&& u <= i64::MAX as u64 + 1
								{
								} else {
									return Err(self.number_err(t.start, Compound::Number));
								}
							} else if parse_number_kind(kind, text).is_none() {
								return Err(self.number_err(t.start, Compound::Number));
							}
						}
						NumKind::Decimal => {
							lit::decimal(text).map_err(Error::Lower)?;
						}
						NumKind::BigDecimal => {
							lit::bigdecimal(text).map_err(Error::Lower)?;
						}
						NumKind::Duration => {}
					}
					Ok(text.to_owned())
				}
			}
		} else {
			Err(self.unexpected("an object key"))
		}
	}

	/// A number, duration or `±Infinity` at a sign or digits.
	fn number(&mut self) -> R<Value> {
		let t = self.tok;
		if t.kind != T::Digits {
			let n = self.peek1();
			if n.kind == T::Infinity && !n.ws() {
				self.bump();
				self.bump();
				let f = if t.kind == T::Minus {
					f64::NEG_INFINITY
				} else {
					f64::INFINITY
				};
				return Ok(Value::Number(Number::Float(f)));
			}
			if n.kind != T::Digits || n.ws() {
				return Err(self.number_err(t.start, Compound::Numeric));
			}
		}
		let mut l = self.lex;
		let Ok(kind) = l.numeric(t.start) else {
			return Err(self.number_err(t.start, Compound::Numeric));
		};
		let end = l.pos;
		let text = &self.src[t.start as usize..end as usize];
		if kind == NumKind::Duration && t.kind != T::Digits {
			return Err(self.number_err(t.start, Compound::Numeric));
		}
		self.reset_to(end);
		self.prev = Tok {
			start: t.start,
			end,
			..t
		};
		let parsed_num = match kind {
			NumKind::Decimal | NumKind::BigDecimal | NumKind::Duration => None,
			NumKind::Int
			| NumKind::Float
			| NumKind::I8
			| NumKind::U8
			| NumKind::I16
			| NumKind::U16
			| NumKind::I32
			| NumKind::U32
			| NumKind::I64
			| NumKind::U64
			| NumKind::F16
			| NumKind::F32
			| NumKind::F64 => match parse_number_kind(kind, text) {
				Some(n) => Some(n),
				None => return Err(self.number_err(t.start, Compound::Numeric)),
			},
		};
		Ok(match kind {
			NumKind::Decimal => {
				let Literal::Decimal(d) = lit::decimal(text).map_err(Error::Lower)? else {
					unreachable!("a decimal literal");
				};
				Value::Number(Number::Decimal(d))
			}
			NumKind::BigDecimal => Value::Number(Number::BigDecimal(
				lit::bigdecimal_value(text).map_err(Error::Lower)?,
			)),
			NumKind::Duration => {
				let (s, ns) = decode_duration(text.as_bytes())
					.ok_or_else(|| self.number_err(t.start, Compound::Numeric))?;
				Value::Duration(Duration::new(s, ns).into())
			}
			NumKind::Int
			| NumKind::I8
			| NumKind::U8
			| NumKind::I16
			| NumKind::U16
			| NumKind::I32
			| NumKind::U32
			| NumKind::I64
			| NumKind::U64
			| NumKind::Float
			| NumKind::F16
			| NumKind::F32
			| NumKind::F64 => Value::Number(parsed_num.expect("parsed above")),
		})
	}

	/// Starts a SurrealQL value.
	fn start(&mut self) -> R<Start> {
		let t = self.tok;
		let done = |v| Ok(Start::Done(v, true));
		if t.kind == T::Ident && t.kw == Kw::None_ {
			self.bump();
			done(Value::None)
		} else if t.kind == T::Ident && t.kw == Kw::Null {
			self.bump();
			done(Value::Null)
		} else if t.kind == T::Ident && t.kw == Kw::True {
			self.bump();
			done(Value::Bool(true))
		} else if t.kind == T::Ident && t.kw == Kw::False {
			self.bump();
			done(Value::Bool(false))
		} else if t.kind == T::NaN {
			self.bump();
			done(Value::Number(Number::Float(f64::NAN)))
		} else if t.kind == T::Infinity {
			self.bump();
			done(Value::Number(Number::Float(f64::INFINITY)))
		} else if t.kind == T::Lt
			&& let Some(v) = self.try_nonfinite_float_cast()
		{
			done(v)
		} else if t.kind == T::LBrace {
			self.bump();
			if self.eat(T::RBrace) {
				return Ok(Start::Done(Value::Object(Object::new()), false));
			}
			if self.eat(T::Comma) {
				self.expect_close(T::RBrace, t.start)?;
				return Ok(Start::Done(Value::Set(Set::new()), false));
			}
			self.enter()?;
			let keyish = matches!(self.tok.kind, T::Ident | T::EscIdent | T::Digits)
				|| (self.tok.kind == T::Str && matches!(self.tok.aux(), 0 | b's'));
			if keyish {
				let (pos, tok, prev) = (self.lex.pos, self.tok, self.prev);
				let key = self.object_key()?;
				if self.eat(T::Colon) {
					return self.push(Frame::Object(BTreeMap::new(), key, t.start));
				}
				self.lex.pos = pos;
				self.tok = tok;
				self.prev = prev;
			}
			if self.eat(T::RBrace) {
				self.leave();
				return done(Value::Set(Set::new()));
			}
			self.push(Frame::Set(Set::new(), t.start))?;
			self.start()
		} else if t.kind == T::LBracket {
			self.bump();
			self.enter()?;
			if self.eat(T::RBracket) {
				self.leave();
				return done(Value::Array(Array::new()));
			}
			self.push(Frame::Array(Vec::new(), t.start))?;
			self.start()
		} else if t.kind == T::Str {
			match t.aux() {
				0 | b's' => {
					self.bump();
					let s = self.string(t)?;
					if self.settings.opts.legacy_strands {
						return done(legacy_strand(s));
					}
					done(Value::String(s))
				}
				PREFIX_DATE | PREFIX_TIME | PREFIX_TIMESTAMP | PREFIX_INTERVAL => {
					self.bump();
					let body = self.string(t)?;
					let token = &self.src[t.start as usize..t.end as usize];
					let at = |r| Error::Lower(lit::in_token(r, token, t.start));
					let v = match t.aux() {
						PREFIX_DATE => {
							let Literal::Date(v) = lit::date(&body).map_err(at)? else {
								unreachable!("a date literal");
							};
							Value::Date(v.into())
						}
						PREFIX_TIME => {
							let Literal::Time(v) = lit::time(&body).map_err(at)? else {
								unreachable!("a time literal");
							};
							Value::Time(v.into())
						}
						PREFIX_TIMESTAMP => {
							let Literal::Timestamp(v) = lit::timestamp(&body).map_err(at)? else {
								unreachable!("a timestamp literal");
							};
							Value::Timestamp(v.into())
						}
						_ => {
							let Literal::Interval(v) = lit::interval(&body).map_err(at)? else {
								unreachable!("an interval literal");
							};
							Value::Interval(v.into())
						}
					};
					done(v)
				}
				b'd' | b'u' | b'b' | b'f' => {
					self.bump();
					let body = self.string(t)?;
					let token = &self.src[t.start as usize..t.end as usize];
					let at = |r| Error::Lower(lit::in_token(r, token, t.start));
					let v = match t.aux() {
						b'f' => Value::File(lit::file(&body).map_err(at)?),
						b'd' => {
							let Literal::Datetime(d) = lit::datetime(&body).map_err(at)? else {
								unreachable!("a datetime literal");
							};
							Value::Datetime(d.into())
						}
						b'u' => {
							let Literal::Uuid(u) = lit::uuid(&body).map_err(at)? else {
								unreachable!("a uuid literal");
							};
							Value::Uuid(u.into())
						}
						_ => {
							let Literal::Bytes(b) = lit::bytes(&body).map_err(at)? else {
								unreachable!("a bytes literal");
							};
							Value::Bytes(b.into())
						}
					};
					done(v)
				}
				_ => self.record_id_start(),
			}
		} else if t.kind == T::Slash {
			let mut l = self.lex;
			if l.regex(t.start).is_err() {
				let end = self.src.len() as u32;
				return Err(self.err_at(t.start, end, "Failed to lex regex, unexpected eof"));
			}
			let end = l.pos;
			self.reset_to(end);
			let Literal::Regex(r) =
				lit::regex_at(&self.src[t.start as usize..end as usize], t.start, end)
					.map_err(Error::Lower)?
			else {
				unreachable!("a regex literal");
			};
			done(Value::Regex(Regex::from(r)))
		} else if t.kind == T::LParen {
			self.bump();
			if matches!(self.tok.kind, T::Plus | T::Minus | T::Digits) {
				let num_start = self.tok.start;
				let n = self.number()?;
				let num_end = self.prev_end();
				if self.eat(T::Comma) {
					let x = if let Value::Number(num) = n {
						match num.wide() {
							Wide::Int(i) => i as f64,
							Wide::Float(f) if !f.is_nan() => f,
							Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
								return Err(Error::Lower(
									Reject::new(
										"Unexpected token, expected a non-decimal, non-NaN, number",
									)
									.at(num_start, num_end)
									.with_label("Coordinate numbers can't be NaN or a decimal"),
								));
							}
						}
					} else {
						return Err(Error::Lower(
							Reject::new(
								"Unexpected token, expected a non-decimal, non-NaN, number",
							)
							.at(num_start, num_end)
							.with_label("Coordinate numbers can't be NaN or a decimal"),
						));
					};
					let y = self.float()?;
					self.expect_close(T::RParen, t.start)?;
					return done(Value::Geometry(Geometry::Point((x, y).into())));
				}
				self.expect_close(T::RParen, t.start)?;
				return done(n);
			}
			self.enter()?;
			self.push(Frame::Paren(t.start))?;
			self.start()
		} else if t.kind == T::DotDot {
			self.bump();
			self.range_end(Bound::Unbounded, false)
		} else if matches!(t.kind, T::Plus | T::Minus | T::Digits) {
			let v = self.number()?;
			done(v)
		} else if self.record_only {
			self.record_id_start()
		} else {
			self.record_or_table()
		}
	}

	/// A record id, or a table name. A value of type `table` renders as its
	/// bare name, so an identifier that ends a value - followed by the end of
	/// input, or inside a collection by a `,` or a closing delimiter - is a
	/// table; one followed by `:` begins a record id, and any other token is
	/// an error. Within a record id's key an identifier always begins a record
	/// id, as the key grammar has it.
	fn record_or_table(&mut self) -> R<Start> {
		let table = self.take_ident()?;
		let ends_value = if self.tok.kind == T::Eof {
			true
		} else if matches!(self.tok.kind, T::Comma | T::RBracket | T::RBrace | T::RParen) {
			!self.frames.is_empty()
		} else {
			false
		};
		let in_key =
			self.frames.iter().any(|f| matches!(f, Frame::KeyArray(..) | Frame::KeyObject(..)));
		if ends_value && !in_key {
			return Ok(Start::Done(Value::Table(Table::new(table)), false));
		}
		self.expect(T::Colon, ":")?;
		self.record_key(table)
	}

	/// An `f64` token value: a number with an optional adjacent sign,
	/// `NaN` or `Infinity`.
	fn float(&mut self) -> R<f64> {
		let t = self.tok;
		if t.kind == T::NaN {
			self.bump();
			Ok(f64::NAN)
		} else if t.kind == T::Infinity {
			self.bump();
			Ok(f64::INFINITY)
		} else if matches!(t.kind, T::Plus | T::Minus | T::Digits) {
			// A float reads on over a `.`, which must start a
			// fraction.
			if compound_error(self.src.as_bytes(), t.start, Compound::Float).is_some() {
				return Err(self.number_err(t.start, Compound::Float));
			}
			let mut l = self.lex;
			if let Ok(
				NumKind::Int
				| NumKind::I8
				| NumKind::U8
				| NumKind::I16
				| NumKind::U16
				| NumKind::I32
				| NumKind::U32
				| NumKind::I64
				| NumKind::U64
				| NumKind::Float
				| NumKind::F16
				| NumKind::F32
				| NumKind::F64,
			) = l.numeric(t.start)
			{
				let val = self.number()?;
				if let Value::Number(num) = val {
					match num.wide() {
						Wide::Int(i) => Ok(i as f64),
						Wide::Float(f) => Ok(f),
						Wide::Decimal(_) | Wide::BigDecimal(_) => {
							Err(self.number_err(t.start, Compound::Float))
						}
					}
				} else {
					Err(self.number_err(t.start, Compound::Float))
				}
			} else {
				Err(self.number_err(t.start, Compound::Float))
			}
		} else {
			Err(self.unexpected("a floating point number"))
		}
	}

	/// Starts a JSON value.
	fn start_json(&mut self) -> R<Start> {
		let t = self.tok;
		let done = |v| Ok(Start::Done(v, false));
		if t.kind == T::Ident && t.kw == Kw::Null {
			self.bump();
			done(Value::Null)
		} else if t.kind == T::Ident && t.kw == Kw::True {
			self.bump();
			done(Value::Bool(true))
		} else if t.kind == T::Ident && t.kw == Kw::False {
			self.bump();
			done(Value::Bool(false))
		} else if t.kind == T::LBrace {
			self.bump();
			if self.eat(T::RBrace) {
				return done(Value::Object(Object::new()));
			}
			self.enter()?;
			if self.eat(T::RBrace) {
				self.leave();
				return done(Value::Object(Object::new()));
			}
			let key = self.object_key()?;
			self.expect(T::Colon, ":")?;
			self.push(Frame::Object(BTreeMap::new(), key, t.start))?;
			self.start_json()
		} else if t.kind == T::LBracket {
			self.bump();
			self.enter()?;
			if self.eat(T::RBracket) {
				self.leave();
				return done(Value::Array(Array::new()));
			}
			self.push(Frame::Array(Vec::new(), t.start))?;
			self.start_json()
		} else if t.kind == T::Str && matches!(t.aux(), 0 | b's') {
			self.bump();
			let s = self.string(t)?;
			if self.settings.opts.legacy_strands {
				return done(legacy_strand(s));
			}
			done(Value::String(s))
		} else if matches!(t.kind, T::Plus | T::Minus | T::Digits) {
			let v = self.number()?;
			done(v)
		} else {
			self.json_record_or_table()
		}
	}

	/// A record id, or else a table name. The table name is read from
	/// where the record id failed, past the token it failed on, so the record
	/// id is parsed whole before falling back: this recurses once per record
	/// id nested in a key, each of which charges the object budget.
	fn json_record_or_table(&mut self) -> R<Start> {
		if let Ok(table) = self.take_ident()
			&& self.expect(T::Colon, ":").is_ok()
		{
			let outer = std::mem::take(&mut self.frames);
			let budget = self.budget;
			let res = self.record_key(table).and_then(|s| self.drive(s));
			self.frames = outer;
			match res {
				Ok(v) => return Ok(Start::Done(v, false)),
				Err(_) => self.budget = budget,
			}
		}
		let name = self.take_ident()?;
		Ok(Start::Done(Value::Table(Table::new(name)), false))
	}

	/// An identifier's name: the text of a word, unescaped when quoted.
	fn ident(&self, t: Tok) -> R<String> {
		if t.kind == T::Ident {
			Ok(self.text(t).to_owned())
		} else if t.kind == T::EscIdent {
			self.string(t)
		} else {
			Err(self.unexpected("an identifier"))
		}
	}

	/// Starts a record id with a value key.
	fn record_id_start(&mut self) -> R<Start> {
		let table = self.take_ident()?;
		self.expect(T::Colon, ":")?;
		self.record_key(table)
	}

	/// A record id's key after its `:`.
	fn record_key(&mut self, table: String) -> R<Start> {
		let k = self.tok;
		let key = if k.kind == T::Str && k.aux() == b'u' {
			self.bump();
			let body = self.string(k)?;
			let token = &self.src[k.start as usize..k.end as usize];
			let Literal::Uuid(u) =
				lit::uuid(&body).map_err(|r| Error::Lower(lit::in_token(r, token, k.start)))?
			else {
				unreachable!("a uuid literal");
			};
			RecordIdKey::Uuid(u.into())
		} else if k.kind == T::LBrace {
			self.bump();
			self.enter()?;
			if self.eat(T::RBrace) {
				self.leave();
				RecordIdKey::Object(Object::new())
			} else {
				let key = self.object_key()?;
				self.expect(T::Colon, ":")?;
				self.push(Frame::KeyObject(table, BTreeMap::new(), key, k.start))?;
				return self.element();
			}
		} else if k.kind == T::LBracket {
			self.bump();
			self.enter()?;
			if self.eat(T::RBracket) {
				self.leave();
				RecordIdKey::Array(Array::new())
			} else {
				self.push(Frame::KeyArray(table, Vec::new(), k.start))?;
				return self.element();
			}
		} else if k.kind == T::Plus {
			self.bump();
			let d = self.tok;
			if d.ws() {
				return Err(Error::Lower(
					Reject::new("Unexpected whitespace")
						.at(k.start, k.end)
						.with_label("No whitespace allowed after this token"),
				));
			}
			if d.kind != T::Digits {
				return Err(self.unexpected("an integer"));
			}
			// The digits are read but stay the next token.
			let end = self.digits_end(d.start);
			let after = self.token_at(end);
			if !after.ws() && after.kind == T::Dot {
				let msg = format!(
					"Unexpected token `{}`, expected an integer",
					render::token_name(self.src, d.start)
				);
				return Err(Error::Lower(
					Reject::new(msg)
						.at(d.start, end)
						.with_label("Numeric Record-id keys can only be integers"),
				));
			}
			if !after.ws() && after.is_ident() {
				let msg = format!(
					"Unexpected token `{}` expected an integer",
					render::token_name(self.src, after.start)
				);
				return Err(self.err_at(k.start, after.end, msg));
			}
			let text = &self.src[d.start as usize..end as usize];
			match text.parse::<i64>() {
				Ok(n) => RecordIdKey::Number(n),
				Err(_) => RecordIdKey::String(text.to_owned()),
			}
		} else if k.kind == T::Minus {
			self.bump();
			let d = self.tok;
			if d.kind != T::Digits {
				let e = self.unexpected("a number");
				self.consume_failed();
				return Err(e);
			}
			let end = self.digits_end(d.start);
			let text = &self.src[d.start as usize..end as usize];
			let next = self.src.as_bytes().get(end as usize).copied();
			if next.is_some_and(|b| b.is_ascii_alphabetic()) {
				// The letter ends a failed integer and is dropped, as
				// earlier releases read it.
				self.reset_to(end + 1);
				RecordIdKey::String(format!("-{text}"))
			} else if next == Some(b'.')
				&& self.src.as_bytes().get(end as usize + 1).is_some_and(u8::is_ascii_digit)
			{
				self.reset_to(end);
				RecordIdKey::String(format!("-{text}"))
			} else {
				self.reset_to(end);
				let digits: String = text.chars().filter(|&c| c != '_').collect();
				if let Ok(u) = digits.parse::<u64>() {
					if u <= i64::MAX as u64 {
						RecordIdKey::Number(-(u as i64))
					} else if u == i64::MAX as u64 + 1 {
						RecordIdKey::Number(i64::MIN)
					} else {
						RecordIdKey::String(format!("-{text}"))
					}
				} else {
					RecordIdKey::String(format!("-{text}"))
				}
			}
		} else if k.kind == T::Digits {
			let end = self.digits_end(k.start);
			let after = self.token_at(end);
			let flexible = !self.settings.limits.strict_record_ids;
			// Unlike in a query, `NaN` or `Infinity` after the digits ends
			// the key rather than joining it, as earlier releases read values.
			if flexible && !after.ws() && matches!(after.kind, T::Ident | T::EscIdent) {
				self.reset_to(after.end);
				RecordIdKey::String(self.src[k.start as usize..after.end as usize].to_owned())
			} else {
				self.reset_to(end);
				let text = &self.src[k.start as usize..end as usize];
				match text.parse::<i64>() {
					Ok(n) => RecordIdKey::Number(n),
					Err(_) => RecordIdKey::String(text.to_owned()),
				}
			}
		} else if matches!(k.kind, T::NaN | T::Infinity) && !self.settings.limits.strict_record_ids
		{
			self.bump();
			RecordIdKey::String(self.text(k).to_owned())
		} else {
			RecordIdKey::String(self.take_ident()?)
		};
		let rangeable = !(self.record_only && self.frames.is_empty());
		Ok(Start::Done(Value::RecordId(RecordId::new(table, key)), rangeable))
	}

	/// The end of the last consumed token.
	fn prev_end(&self) -> u32 {
		self.prev.end
	}

	/// Where the digits token starting at `start` ends: digits and `_`.
	fn digits_end(&self, start: u32) -> u32 {
		let b = self.src.as_bytes();
		let mut i = start as usize;
		while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'_') {
			i += 1;
		}
		i as u32
	}

	/// The token at byte `pos`, with whether whitespace precedes it.
	fn token_at(&self, pos: u32) -> Tok {
		let mut l = self.lex;
		l.pos = pos;
		l.next_token()
	}

	/// Parses `<f16>NaN`, `<f16>Infinity`, `<f16>-Infinity`, `<f32>NaN`,
	/// `<f32>Infinity` or `<f32>-Infinity` at `<`, restoring the lexer when
	/// the tokens do not form one.
	fn try_nonfinite_float_cast(&mut self) -> Option<Value> {
		let saved = (self.lex.pos, self.tok, self.prev);
		self.bump(); // `<`
		let kind_tok = self.tok;
		let is_f16 = if kind_tok.kind == T::Ident {
			let ktext = self.text(kind_tok);
			if ktext.eq_ignore_ascii_case("f16") {
				true
			} else if ktext.eq_ignore_ascii_case("f32") {
				false
			} else {
				(self.lex.pos, self.tok, self.prev) = saved;
				return None;
			}
		} else {
			(self.lex.pos, self.tok, self.prev) = saved;
			return None;
		};
		self.bump();
		if !self.eat(T::Gt) {
			(self.lex.pos, self.tok, self.prev) = saved;
			return None;
		}
		let v = if self.eat(T::NaN) {
			if is_f16 {
				Value::Number(Number::F16(half::f16::NAN))
			} else {
				Value::Number(Number::F32(f32::NAN))
			}
		} else if self.eat(T::Infinity) {
			if is_f16 {
				Value::Number(Number::F16(half::f16::INFINITY))
			} else {
				Value::Number(Number::F32(f32::INFINITY))
			}
		} else if matches!(self.tok.kind, T::Minus | T::Plus) {
			let neg = self.tok.kind == T::Minus;
			self.bump();
			if self.tok.kind == T::Infinity && !self.tok.ws() {
				self.bump();
				if is_f16 {
					Value::Number(Number::F16(if neg {
						half::f16::NEG_INFINITY
					} else {
						half::f16::INFINITY
					}))
				} else {
					Value::Number(Number::F32(if neg {
						f32::NEG_INFINITY
					} else {
						f32::INFINITY
					}))
				}
			} else {
				(self.lex.pos, self.tok, self.prev) = saved;
				return None;
			}
		} else {
			(self.lex.pos, self.tok, self.prev) = saved;
			return None;
		};
		Some(v)
	}
}

/// A string under `legacy_strands` in a value: a record id when one starts
/// it, then a datetime or uuid when the whole string is one.
fn legacy_strand(s: String) -> Value {
	let settings = Settings {
		limits: Default::default(),
		opts: super::Options {
			legacy_strands: false,
			surrealism: false,
		},
	};
	if let Ok(r) = record_id(&s, &settings) {
		return Value::RecordId(r);
	}
	match lit::legacy_scalar(&s) {
		Some(Literal::Datetime(d)) => Value::Datetime(d.into()),
		Some(Literal::Uuid(u)) => Value::Uuid(u.into()),
		_ => Value::String(s),
	}
}

fn parse_number_kind(kind: NumKind, text: &str) -> Option<Number> {
	use surrealdb_parser_next::lex::DecodedNumber;
	Some(match surrealdb_parser_next::lex::decode_number(kind, text.as_bytes())? {
		DecodedNumber::Int(v) => Number::Int(v),
		DecodedNumber::I8(v) => Number::I8(v),
		DecodedNumber::U8(v) => Number::U8(v),
		DecodedNumber::I16(v) => Number::I16(v),
		DecodedNumber::U16(v) => Number::U16(v),
		DecodedNumber::I32(v) => Number::I32(v),
		DecodedNumber::U32(v) => Number::U32(v),
		DecodedNumber::U64(v) => Number::U64(v),
		DecodedNumber::Float(v) => Number::Float(v),
		DecodedNumber::F16(v) => Number::F16(v),
		DecodedNumber::F32(v) => Number::F32(v),
	})
}
