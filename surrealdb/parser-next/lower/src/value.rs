//! `syn`'s literal-value grammars: SurrealQL values (`parse_value`), JSON
//! (`parse_json`) and record ids (`parse_value_record_id`), parsed straight
//! into public values.
//!
//! The grammars nest through arrays, objects, sets, parentheses, ranges and
//! record-id keys. Nesting is held on an explicit stack and bounded by the
//! object budget, charged where `syn` charges it, so it costs no native
//! stack. A failure reports a message only: callers that render errors take
//! them from `syn`.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::Duration;

use surrealdb_parser_next::decode;
use surrealdb_parser_next::kw::Kw;
use surrealdb_parser_next::lex::{Lexer, NumKind, T, Tok, decode_duration};
use surrealdb_parser_next::parse::starts_expression;
use surrealdb_sql::Literal;
use surrealdb_types::{
	Array, Geometry, Number, Object, Range, RecordId, RecordIdKey, Regex, Set, Table, Value,
};

use crate::entry::{Error, Settings};
use crate::lit;

/// Parses a SurrealQL value. Input after the value is not read.
pub fn value(src: &str, settings: &Settings) -> Result<Value, Error> {
	Parser::new(src, settings, false, false).run()
}

/// Parses a JSON value, with `\uXXXX` surrogate pairs. Input after the value
/// is not read.
pub fn json(src: &str, settings: &Settings) -> Result<Value, Error> {
	Parser::new(src, settings, true, true).run()
}

/// Parses a record id, `table:key`, with a value key. Input after it is not
/// read.
pub fn record_id(src: &str, settings: &Settings) -> Result<RecordId, Error> {
	let mut p = Parser::new(src, settings, false, false);
	match p.run_record_id()? {
		Value::RecordId(r) => Ok(r),
		_ => unreachable!("a record-id parse yields a record id"),
	}
}

type R<T> = Result<T, Error>;

fn fail<T>(msg: &'static str) -> R<T> {
	Err(Error::Lower(msg))
}

/// A value under construction, waiting for its next element.
enum Frame {
	Array(Vec<Value>),
	/// An object, waiting for the value of `key`.
	Object(BTreeMap<String, Value>, String),
	/// A set; its length is after deduplication, which decides whether a
	/// single element needs a trailing comma, as in `syn`.
	Set(Set),
	/// `( value )`.
	Paren,
	/// A range waiting for its end. `postfix` when it followed a start
	/// value (`a..`), which makes the range final: no range follows it.
	RangeEnd {
		start: Bound<Value>,
		inclusive: bool,
		postfix: bool,
	},
	/// A record id whose key is an array or object under construction.
	KeyArray(String, Vec<Value>),
	KeyObject(String, BTreeMap<String, Value>, String),
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
	settings: &'a Settings,
	json: bool,
	json_escapes: bool,
	/// Object levels left (`syn`'s `object_recursion_limit`).
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
		self.tok = self.lex.next_token();
		t
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
	/// its place, as `syn`'s `expected!`.
	fn expect(&mut self, k: T, msg: &'static str) -> R<()> {
		if self.eat(k) {
			Ok(())
		} else {
			self.consume_failed();
			fail(msg)
		}
	}

	/// Consumes a closing delimiter, or fails without consuming.
	fn expect_close(&mut self, k: T, msg: &'static str) -> R<()> {
		if self.eat(k) {
			Ok(())
		} else {
			fail(msg)
		}
	}

	/// Consumes the token a failure stopped at. `syn` has no `??` token, so
	/// only its first `?` is consumed.
	fn consume_failed(&mut self) {
		if self.tok.kind == T::QQ {
			let at = self.tok.start + 1;
			self.reset_to(at);
		} else if self.tok.kind == T::Digits {
			// `syn`'s digits token runs on over `_`.
			let at = self.digits_end(self.tok.start);
			self.reset_to(at);
		} else if self.tok.kind == T::Invalid {
			// `syn` reads an unexpected character, or an unterminated
			// string, as one token.
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
	/// is one, as `syn`'s `parse_ident`.
	fn take_ident(&mut self) -> R<String> {
		let t = self.tok;
		match self.ident(t) {
			Ok(name) => {
				self.bump();
				Ok(name)
			}
			Err(e) => {
				self.consume_failed();
				Err(e)
			}
		}
	}

	/// Enters one object level.
	fn enter(&mut self) -> R<()> {
		if self.budget == 0 {
			return fail("Exceeded object recursion depth limit");
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
					Frame::Array(mut items) => {
						items.push(v);
						if self.eat(T::Comma) {
							if !self.eat(T::RBracket) {
								self.frames.push(Frame::Array(items));
								s = self.element()?;
								continue 'start;
							}
						} else {
							self.expect_close(T::RBracket, "expected `]`")?;
						}
						self.leave();
						v = Value::Array(Array::from(items));
						rangeable = true;
					}
					Frame::KeyArray(table, mut items) => {
						items.push(v);
						if self.eat(T::Comma) {
							if !self.eat(T::RBracket) {
								self.frames.push(Frame::KeyArray(table, items));
								s = self.element()?;
								continue 'start;
							}
						} else {
							self.expect_close(T::RBracket, "expected `]`")?;
						}
						self.leave();
						v = Value::RecordId(RecordId::new(table, RecordIdKey::Array(items.into())));
						rangeable = !(self.record_only && self.frames.is_empty());
					}
					Frame::Object(mut map, key) => {
						map.insert(key, v);
						match self.object_next()? {
							Some(key) => {
								self.frames.push(Frame::Object(map, key));
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
					Frame::KeyObject(table, mut map, key) => {
						map.insert(key, v);
						match self.object_next()? {
							Some(key) => {
								self.frames.push(Frame::KeyObject(table, map, key));
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
					Frame::Set(mut items) => {
						items.insert(v);
						if self.eat(T::Comma) {
							if !self.eat(T::RBrace) {
								self.frames.push(Frame::Set(items));
								s = self.element()?;
								continue 'start;
							}
						} else if items.len() <= 1 {
							return fail(
								"Sets with a single value must have at least a single comma",
							);
						} else {
							self.expect_close(T::RBrace, "expected `}`")?;
						}
						self.leave();
						v = Value::Set(items);
						rangeable = true;
					}
					Frame::Paren => {
						self.expect_close(T::RParen, "expected `)`")?;
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
	fn object_next(&mut self) -> R<Option<String>> {
		if !self.eat(T::Comma) {
			self.expect_close(T::RBrace, "expected `}`")?;
			return Ok(None);
		}
		if self.eat(T::RBrace) {
			return Ok(None);
		}
		let key = self.object_key()?;
		self.expect(T::Colon, "expected `:`")?;
		Ok(Some(key))
	}

	/// A range after a value: `>..`, `..`, `..=`, each directly after it.
	fn postfix_range(&mut self, v: &mut Value) -> R<Option<Start>> {
		let excluded = match self.tok.kind {
			T::Gt if !self.tok.ws() => {
				self.bump();
				if self.tok.kind != T::DotDot || self.tok.ws() {
					return fail("expected `..`");
				}
				true
			}
			T::DotDot if !self.tok.ws() => false,
			_ => return Ok(None),
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
			return fail("invalid escape");
		}
		Ok(s)
	}

	/// `syn`'s `parse_object_key`.
	fn object_key(&mut self) -> R<String> {
		let t = self.tok;
		match t.kind {
			T::Ident => {
				self.bump();
				Ok(self.text(t).to_owned())
			}
			T::EscIdent => {
				self.bump();
				self.string(t)
			}
			T::Str if matches!(t.aux(), 0 | b's') => {
				self.bump();
				self.string(t)
			}
			T::Digits => {
				let mut l = self.lex;
				match l.numeric(t.start) {
					Ok(NumKind::Duration) | Err(_) => fail("invalid number"),
					Ok(kind) => {
						let end = l.pos;
						self.reset_to(end);
						let text = &self.src[t.start as usize..end as usize];
						// The key is spelled as written, but must be a valid
						// number.
						match kind {
							NumKind::Int => {
								// `syn` bounds a key's magnitude by `i64::MIN`'s.
								let digits: String = text.chars().filter(|&c| c != '_').collect();
								match digits.parse::<u64>() {
									Ok(u) if u <= i64::MAX as u64 + 1 => {}
									_ => return fail("number does not fit in an i64"),
								}
							}
							NumKind::Decimal => {
								lit::decimal(text).map_err(|r| Error::Lower(r.0))?;
							}
							_ => {}
						}
						Ok(text.to_owned())
					}
				}
			}
			_ => fail("expected an object key"),
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
				return fail("expected a number");
			}
		}
		let mut l = self.lex;
		let kind = l.numeric(t.start).map_err(|_| Error::Lower("invalid number"))?;
		let end = l.pos;
		self.reset_to(end);
		let text = &self.src[t.start as usize..end as usize];
		Ok(match kind {
			NumKind::Int => Value::Number(Number::Int(int(text)?)),
			NumKind::Float => Value::Number(Number::Float(float(text))),
			NumKind::Decimal => match lit::decimal(text).map_err(|r| Error::Lower(r.0))? {
				Literal::Decimal(d) => Value::Number(Number::Decimal(d)),
				_ => unreachable!("a decimal literal"),
			},
			NumKind::Duration => {
				if t.kind != T::Digits {
					return fail("a duration takes no sign");
				}
				let body = text;
				let (s, ns) =
					decode_duration(body.as_bytes()).ok_or(Error::Lower("invalid duration"))?;
				Value::Duration(Duration::new(s, ns).into())
			}
		})
	}

	/// Starts a SurrealQL value (`syn`'s `parse_value`).
	fn start(&mut self) -> R<Start> {
		let t = self.tok;
		let done = |v| Ok(Start::Done(v, true));
		match t.kind {
			T::Ident if t.kw == Kw::None_ => {
				self.bump();
				done(Value::None)
			}
			T::Ident if t.kw == Kw::Null => {
				self.bump();
				done(Value::Null)
			}
			T::Ident if t.kw == Kw::True => {
				self.bump();
				done(Value::Bool(true))
			}
			T::Ident if t.kw == Kw::False => {
				self.bump();
				done(Value::Bool(false))
			}
			T::NaN => {
				self.bump();
				done(Value::Number(Number::Float(f64::NAN)))
			}
			T::Infinity => {
				self.bump();
				done(Value::Number(Number::Float(f64::INFINITY)))
			}
			T::LBrace => {
				self.bump();
				if self.eat(T::RBrace) {
					return Ok(Start::Done(Value::Object(Object::new()), false));
				}
				if self.eat(T::Comma) {
					self.expect_close(T::RBrace, "expected `}`")?;
					return Ok(Start::Done(Value::Set(Set::new()), false));
				}
				self.enter()?;
				let keyish = matches!(self.tok.kind, T::Ident | T::EscIdent | T::Digits)
					|| (self.tok.kind == T::Str && matches!(self.tok.aux(), 0 | b's'));
				if keyish {
					let (pos, tok) = (self.lex.pos, self.tok);
					let key = self.object_key()?;
					if self.eat(T::Colon) {
						return self.push(Frame::Object(BTreeMap::new(), key));
					}
					self.lex.pos = pos;
					self.tok = tok;
				}
				if self.eat(T::RBrace) {
					self.leave();
					return done(Value::Set(Set::new()));
				}
				self.push(Frame::Set(Set::new()))?;
				self.start()
			}
			T::LBracket => {
				self.bump();
				self.enter()?;
				if self.eat(T::RBracket) {
					self.leave();
					return done(Value::Array(Array::new()));
				}
				self.push(Frame::Array(Vec::new()))?;
				self.start()
			}
			T::Str => match t.aux() {
				0 | b's' => {
					self.bump();
					let s = self.string(t)?;
					if self.settings.opts.legacy_strands {
						return done(legacy_strand(s));
					}
					done(Value::String(s))
				}
				b'd' | b'u' | b'b' | b'f' => {
					self.bump();
					let body = self.string(t)?;
					let v = match t.aux() {
						b'f' => {
							if !self.settings.opts.files {
								return fail(crate::FILES_DISABLED);
							}
							Value::File(crate::file_value(&body).map_err(|r| Error::Lower(r.0))?)
						}
						b'd' => match lit::datetime(&body).map_err(|r| Error::Lower(r.0))? {
							Literal::Datetime(d) => Value::Datetime(d.into()),
							_ => unreachable!("a datetime literal"),
						},
						b'u' => match lit::uuid(&body).map_err(|r| Error::Lower(r.0))? {
							Literal::Uuid(u) => Value::Uuid(u.into()),
							_ => unreachable!("a uuid literal"),
						},
						_ => match lit::bytes(&body).map_err(|r| Error::Lower(r.0))? {
							Literal::Bytes(b) => Value::Bytes(b.into()),
							_ => unreachable!("a bytes literal"),
						},
					};
					done(v)
				}
				_ => self.record_id_start(),
			},
			T::Slash => {
				let mut l = self.lex;
				l.regex(t.start).map_err(|_| Error::Lower("invalid regex"))?;
				let end = l.pos;
				self.reset_to(end);
				match lit::regex(&self.src[t.start as usize..end as usize])
					.map_err(|r| Error::Lower(r.0))?
				{
					Literal::Regex(r) => done(Value::Regex(Regex::from(r))),
					_ => unreachable!("a regex literal"),
				}
			}
			T::LParen => {
				self.bump();
				if matches!(self.tok.kind, T::Plus | T::Minus | T::Digits) {
					let n = self.number()?;
					if self.eat(T::Comma) {
						let x = match n {
							Value::Number(Number::Int(i)) => i as f64,
							Value::Number(Number::Float(f)) if !f.is_nan() => f,
							_ => return fail("Coordinate numbers can't be NaN or a decimal"),
						};
						let y = self.float()?;
						self.expect_close(T::RParen, "expected `)`")?;
						return done(Value::Geometry(Geometry::Point((x, y).into())));
					}
					self.expect_close(T::RParen, "expected `)`")?;
					return done(n);
				}
				self.enter()?;
				self.push(Frame::Paren)?;
				self.start()
			}
			T::DotDot => {
				self.bump();
				self.range_end(Bound::Unbounded, false)
			}
			T::Plus | T::Minus | T::Digits => {
				let v = self.number()?;
				done(v)
			}
			_ => self.record_id_start(),
		}
	}

	/// `syn`'s `f64` token value: a number with an optional adjacent sign,
	/// `NaN` or `Infinity`.
	fn float(&mut self) -> R<f64> {
		let t = self.tok;
		match t.kind {
			T::NaN => {
				self.bump();
				Ok(f64::NAN)
			}
			T::Infinity => {
				self.bump();
				Ok(f64::INFINITY)
			}
			T::Plus | T::Minus | T::Digits => match self.number()? {
				Value::Number(Number::Int(i)) => Ok(i as f64),
				Value::Number(Number::Float(f)) => Ok(f),
				_ => fail("expected a float"),
			},
			_ => fail("expected a float"),
		}
	}

	/// Starts a JSON value (`syn`'s `parse_json`).
	fn start_json(&mut self) -> R<Start> {
		let t = self.tok;
		let done = |v| Ok(Start::Done(v, false));
		match t.kind {
			T::Ident if t.kw == Kw::Null => {
				self.bump();
				done(Value::Null)
			}
			T::Ident if t.kw == Kw::True => {
				self.bump();
				done(Value::Bool(true))
			}
			T::Ident if t.kw == Kw::False => {
				self.bump();
				done(Value::Bool(false))
			}
			T::LBrace => {
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
				self.expect(T::Colon, "expected `:`")?;
				self.push(Frame::Object(BTreeMap::new(), key))?;
				self.start_json()
			}
			T::LBracket => {
				self.bump();
				self.enter()?;
				if self.eat(T::RBracket) {
					self.leave();
					return done(Value::Array(Array::new()));
				}
				self.push(Frame::Array(Vec::new()))?;
				self.start_json()
			}
			T::Str if matches!(t.aux(), 0 | b's') => {
				self.bump();
				let s = self.string(t)?;
				if self.settings.opts.legacy_strands {
					return done(legacy_strand(s));
				}
				done(Value::String(s))
			}
			T::Plus | T::Minus | T::Digits => {
				let v = self.number()?;
				done(v)
			}
			_ => self.json_record_or_table(),
		}
	}

	/// A record id, or else a table name. `syn` reads the table name from
	/// where the record id failed, past the token it failed on, so the record
	/// id is parsed whole before falling back: this recurses once per record
	/// id nested in a key, each of which charges the object budget.
	fn json_record_or_table(&mut self) -> R<Start> {
		if let Ok(table) = self.take_ident()
			&& self.expect(T::Colon, "expected `:`").is_ok()
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
		match t.kind {
			T::Ident => Ok(self.text(t).to_owned()),
			T::EscIdent => self.string(t),
			_ => fail("expected an identifier"),
		}
	}

	/// Starts a record id with a value key (`syn`'s
	/// `parse_value_record_id_inner`).
	fn record_id_start(&mut self) -> R<Start> {
		let table = self.take_ident()?;
		self.expect(T::Colon, "expected `:`")?;
		self.record_key(table)
	}

	/// A record id's key after its `:`.
	fn record_key(&mut self, table: String) -> R<Start> {
		let k = self.tok;
		let key = match k.kind {
			T::Str if k.aux() == b'u' => {
				self.bump();
				let body = self.string(k)?;
				match lit::uuid(&body).map_err(|r| Error::Lower(r.0))? {
					Literal::Uuid(u) => RecordIdKey::Uuid(u.into()),
					_ => unreachable!("a uuid literal"),
				}
			}
			T::LBrace => {
				self.bump();
				self.enter()?;
				if self.eat(T::RBrace) {
					self.leave();
					RecordIdKey::Object(Object::new())
				} else {
					let key = self.object_key()?;
					self.expect(T::Colon, "expected `:`")?;
					self.push(Frame::KeyObject(table, BTreeMap::new(), key))?;
					return self.element();
				}
			}
			T::LBracket => {
				self.bump();
				self.enter()?;
				if self.eat(T::RBracket) {
					self.leave();
					RecordIdKey::Array(Array::new())
				} else {
					self.push(Frame::KeyArray(table, Vec::new()))?;
					return self.element();
				}
			}
			T::Plus => {
				self.bump();
				let d = self.tok;
				if d.ws() {
					return fail("Unexpected whitespace");
				}
				if d.kind != T::Digits {
					return fail("expected an integer");
				}
				// The digits are read but stay the next token.
				let end = self.digits_end(d.start);
				let after = self.token_at(end);
				if !after.ws() && (after.kind == T::Dot || after.is_ident()) {
					return fail("Numeric Record-id keys can only be integers");
				}
				let text = &self.src[d.start as usize..end as usize];
				match text.parse::<i64>() {
					Ok(n) => RecordIdKey::Number(n),
					Err(_) => RecordIdKey::String(text.to_owned()),
				}
			}
			T::Minus => {
				self.bump();
				let d = self.tok;
				if d.kind != T::Digits {
					self.consume_failed();
					return fail("expected digits");
				}
				let end = self.digits_end(d.start);
				let text = &self.src[d.start as usize..end as usize];
				let next = self.src.as_bytes().get(end as usize).copied();
				if next.is_some_and(|b| b.is_ascii_alphabetic()) {
					// `syn` reads the letter as the end of a failed integer and
					// drops it.
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
					match digits.parse::<u64>() {
						Ok(u) if u <= i64::MAX as u64 => RecordIdKey::Number(-(u as i64)),
						Ok(u) if u == i64::MAX as u64 + 1 => RecordIdKey::Number(i64::MIN),
						_ => RecordIdKey::String(format!("-{text}")),
					}
				}
			}
			T::Digits => {
				let end = self.digits_end(k.start);
				let after = self.token_at(end);
				let flexible = !self.settings.limits.strict_record_ids;
				if flexible
					&& !after.ws() && matches!(
					after.kind,
					T::Ident | T::EscIdent | T::NaN | T::Infinity
				) {
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
			}
			T::NaN | T::Infinity if !self.settings.limits.strict_record_ids => {
				self.bump();
				RecordIdKey::String(self.text(k).to_owned())
			}
			_ => RecordIdKey::String(self.take_ident()?),
		};
		let rangeable = !(self.record_only && self.frames.is_empty());
		Ok(Start::Done(Value::RecordId(RecordId::new(table, key)), rangeable))
	}

	/// Where `syn`'s digits token starting at `start` ends: digits and `_`.
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
}

/// A string under `legacy_strands` in a value: a record id when one starts
/// it, then a datetime or uuid when the whole string is one.
fn legacy_strand(s: String) -> Value {
	let settings = Settings {
		limits: Default::default(),
		opts: crate::Options {
			legacy_strands: false,
			files: false,
			surrealism: false,
		},
	};
	if let Ok(r) = record_id(&s, &settings) {
		return Value::RecordId(r);
	}
	if let Ok(Literal::Datetime(d)) = lit::datetime(&s) {
		return Value::Datetime(d.into());
	}
	if let Ok(Literal::Uuid(u)) = lit::uuid(&s) {
		return Value::Uuid(u.into());
	}
	Value::String(s)
}

fn int(text: &str) -> R<i64> {
	let digits: String = text.chars().filter(|&c| c != '_').collect();
	let (neg, body) = match digits.strip_prefix('-') {
		Some(b) => (true, b),
		None => (false, digits.strip_prefix('+').unwrap_or(&digits)),
	};
	let u: u64 = body.parse().map_err(|_| Error::Lower("number does not fit in an i64"))?;
	if neg {
		if u > i64::MAX as u64 + 1 {
			return fail("number does not fit in an i64");
		}
		Ok((u as i64).wrapping_neg())
	} else {
		i64::try_from(u).map_err(|_| Error::Lower("number does not fit in an i64"))
	}
}

fn float(text: &str) -> f64 {
	let s: String = text.chars().filter(|&c| c != '_' && c != 'f').collect();
	s.parse().unwrap_or(f64::NAN)
}
