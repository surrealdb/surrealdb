//! Kinds (types): `int | string`, `option<record<a | b>>`, `array<int, 4>`,
//! and literal kinds such as `{ a: 1 | 'x' }`.
//!
//! Kinds nest through `option<…>`, `array<…>`, `set<…>` and literal objects
//! and arrays, so they are parsed on the explicit frame stack like
//! expressions: nesting costs no native stack.

use super::*;
use crate::ast::kind as k;

impl<'a, 's> Parser<'a, 's> {
	/// A kind without enclosing `<` `>`: `any`, or one kind or `option<…>`
	/// optionally followed by `| kind …`.
	pub(crate) fn kind(&mut self) -> R<NodeId> {
		self.kind_with(false)
	}

	/// `any`, `option<…>` or one concrete kind, with no `| kind` after it.
	pub(crate) fn single_kind(&mut self) -> R<NodeId> {
		self.kind_with(true)
	}

	/// The `TYPE` of a field named `id`: a kind whose outermost tuple may name
	/// its components, as in `[city: string, time: datetime]`. A tuple that is
	/// not the first token of the kind, or whose first element is not a name
	/// followed by `:`, is read as an ordinary kind.
	pub(crate) fn id_kind(&mut self) -> R<NodeId> {
		self.keyed_at = self.tok.start;
		let res = self.kind();
		self.keyed_at = u32::MAX;
		res
	}

	/// A component name and its `:`, pushing the `Name`. Names are identifiers,
	/// escaped or not, keywords included; a name not followed by `:` is not a
	/// component name.
	fn component_name(&mut self) -> R<()> {
		let t = self.tok;
		if !t.is_ident() || self.peek1().kind != T::Colon {
			return Err(self.unexpected("a component name"));
		}
		self.bump();
		let op = if t.kind == T::EscIdent {
			1
		} else {
			0
		};
		let name = self.leaf(Tag::Name, op, t);
		self.vals.push(name);
		self.bump();
		Ok(())
	}

	fn kind_with(&mut self, single: bool) -> R<NodeId> {
		let base = self.frames.len();
		self.push_frame(Frame::Atom {
			table_as_field: self.table_as_field,
		})?;
		let res = self.kind_run(base, single);
		if res.is_err() {
			self.truncate_frames(base);
		}
		res
	}

	/// A cast's kind, after its `<`, through the closing `>`.
	pub(crate) fn cast_kind(&mut self) -> R<NodeId> {
		let open = self.prev_start;
		let k = self.kind()?;
		self.close(T::Gt, open)?;
		Ok(k)
	}

	fn push_alts(&mut self, option: bool) -> R<()> {
		self.push_frame(Frame::KindAlts {
			start: self.tok.start,
			open: self.prev_start,
			mark: self.vals.len() as u32,
			option,
			single: false,
		})
	}

	fn kind_run(&mut self, base: usize, single: bool) -> R<NodeId> {
		self.push_frame(Frame::KindAlts {
			start: self.tok.start,
			open: self.prev_start,
			mark: self.vals.len() as u32,
			option: false,
			single,
		})?;
		'want: loop {
			// The first alternative of an inner kind may be `any` or
			// `option<…>`; every other position takes a concrete kind.
			let inner_first = if let Some(Frame::KindAlts {
				mark,
				option: false,
				..
			}) = self.frames.last()
			{
				*mark as usize == self.vals.len()
			} else {
				false
			};
			let t = self.tok;
			let mut v = if inner_first && t.is_kw(Kw::Any) {
				self.bump();
				self.leaf(Tag::Kind, k::ANY, t)
			} else if inner_first && t.is_kw(Kw::Option) {
				self.bump();
				self.expect(T::Lt, "<")?;
				self.push_alts(true)?;
				continue 'want;
			} else {
				match self.concrete_kind()? {
					Some(v) => v,
					None => continue 'want,
				}
			};
			loop {
				let Some(&f) = self.frames.last() else {
					unreachable!("the base frame is never popped early")
				};
				match f {
					Frame::Atom {
						..
					} => {
						self.pop_frame();
						debug_assert_eq!(self.frames.len(), base);
						return Ok(v);
					}
					Frame::KindAlts {
						start,
						open,
						mark,
						option,
						single,
					} => {
						// `any` as the whole inner kind is not followed by
						// alternatives.
						let lone_any = !option
							&& self.vals.len() == mark as usize
							&& self.ast.nodes[v as usize].op == k::ANY
							&& self.ast.nodes[v as usize].tag == Tag::Kind;
						self.vals.push(v);
						if !lone_any && !single && self.eat(T::Pipe) {
							continue 'want;
						}
						self.pop_frame();
						if option {
							self.close(T::Gt, open)?;
							let (list, d) = self.list_from(mark);
							v = self.node(Tag::Kind, k::OPTION, list, 0, start, d)?;
						} else if self.vals.len() - mark as usize == 1 {
							v = self.vals.pop().expect("one alternative");
						} else {
							let (list, d) = self.list_from(mark);
							v = self.node(Tag::Kind, k::EITHER, list, 0, start, d)?;
						}
					}
					Frame::KindSized {
						start,
						open,
						code,
					} => {
						self.pop_frame();
						let size = if self.eat(T::Comma) {
							self.uint_lit()?
						} else {
							NONE
						};
						self.close(T::Gt, open)?;
						let d = self.depth_of(v);
						v = self.node(Tag::Kind, code, v, size, start, d)?;
					}
					Frame::KindObject {
						start,
						mark,
					} => {
						self.vals.push(v);
						self.eat(T::Comma);
						if self.eat(T::RBrace) {
							self.pop_frame();
							let (list, d) = self.list_from(mark);
							v = self.node(Tag::Kind, k::LIT_OBJECT, list, 0, start, d)?;
						} else {
							self.object_key()?;
							self.push_alts(false)?;
							continue 'want;
						}
					}
					Frame::KindTuple {
						start,
						mark,
						named,
					} => {
						self.vals.push(v);
						// A named tuple separates its elements with commas and
						// allows one after the last; an unnamed tuple takes them
						// or leaves them.
						let closed = if !named {
							self.eat(T::Comma);
							self.eat(T::RBracket)
						} else if self.eat(T::Comma) {
							self.eat(T::RBracket)
						} else if self.eat(T::RBracket) {
							true
						} else {
							return Err(self.unexpected("`,` or `]`"));
						};
						if closed {
							self.pop_frame();
							let (list, d) = self.list_from(mark);
							let code = if named {
								k::LIT_KEYED
							} else {
								k::LIT_ARRAY
							};
							v = self.node(Tag::Kind, code, list, 0, start, d)?;
						} else {
							if named {
								self.component_name()?;
							}
							self.push_alts(false)?;
							continue 'want;
						}
					}
					Frame::Base {
						..
					}
					| Frame::Path {
						..
					}
					| Frame::Infix {
						..
					}
					| Frame::Prefix {
						..
					}
					| Frame::Cast {
						..
					}
					| Frame::Paren {
						..
					}
					| Frame::Array {
						..
					}
					| Frame::Object {
						..
					}
					| Frame::Args {
						..
					}
					| Frame::Idiom {
						..
					}
					| Frame::Bracket {
						..
					}
					| Frame::BlockOrSet {
						..
					}
					| Frame::Block {
						..
					}
					| Frame::Set {
						..
					}
					| Frame::Closure {
						..
					} => unreachable!("only kind frames are above a kind's base"),
				}
			}
		}
	}

	/// A kind other than `any` and `option<…>`. Returns `None` when it pushed a
	/// frame and wants an inner kind.
	fn concrete_kind(&mut self) -> R<Option<NodeId>> {
		// Every concrete kind is one object level, even a leaf, as earlier
		// releases charge it.
		if self.o >= self.limits.object_depth {
			return Err(self.depth_error("Exceeded object recursion depth limit", ""));
		}
		let t = self.tok;
		let code = if t.kind == T::Str && (t.aux() == 0 || t.aux() == b's') {
			self.bump();
			let lit = self.leaf(Tag::Str, t.aux(), t);
			return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
		} else if t.kind == T::NaN || t.kind == T::Infinity {
			self.bump();
			let tag = if t.kind == T::NaN {
				Tag::NaN
			} else {
				Tag::Infinity
			};
			let lit = self.leaf(tag, 0, t);
			return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
		} else if t.kind == T::Plus || t.kind == T::Minus || t.kind == T::Digits {
			let signed = t.kind != T::Digits;
			if signed {
				self.bump();
				if self.tok.kind == T::Infinity && !self.tok.ws() {
					// `-Infinity` is one number.
					let end = self.bump().end;
					let f = if t.kind == T::Minus {
						f64::NEG_INFINITY
					} else {
						f64::INFINITY
					};
					let bits = f.to_bits();
					let lit = self.leaf_span(
						Tag::Float,
						0,
						bits as u32,
						(bits >> 32) as u32,
						t.start,
						end,
					);
					return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
				}
				if self.tok.kind != T::Digits || self.tok.ws() {
					return Err(self.number_error(
						t.start,
						LexError::InvalidNumber,
						Compound::Number,
						t.start,
					));
				}
			}
			let lit = self.number(t.start, signed)?;
			let n = self.ast.nodes[lit as usize];
			if signed && n.tag == Tag::Prefix {
				// A signed literal kind is a number, never a duration.
				if let Some(d) = crate::lex::compound_error(self.src, t.start, Compound::Number) {
					return Err(self.diag(d));
				}
				return Err(self.error(n.start, n.end, "expected a number"));
			}
			return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
		} else if t.kind == T::LBrace {
			self.bump();
			if self.eat(T::RBrace) {
				let idx = self.empty_list();
				return self.node(Tag::Kind, k::LIT_OBJECT, idx, 0, t.start, 0).map(Some);
			}
			self.push_frame(Frame::KindObject {
				start: t.start,
				mark: self.vals.len() as u32,
			})?;
			self.object_key()?;
			self.push_alts(false)?;
			return Ok(None);
		} else if t.kind == T::LBracket {
			self.bump();
			if self.eat(T::RBracket) {
				let idx = self.empty_list();
				return self.node(Tag::Kind, k::LIT_ARRAY, idx, 0, t.start, 0).map(Some);
			}
			// Only the opening tuple of an `id` field's `TYPE` names its
			// components; the lookahead runs before the first element is read
			// as a kind, since component names may be kind keywords.
			let named =
				self.keyed_at == t.start && self.tok.is_ident() && self.peek1().kind == T::Colon;
			self.push_frame(Frame::KindTuple {
				start: t.start,
				mark: self.vals.len() as u32,
				named,
			})?;
			if named {
				self.component_name()?;
			}
			self.push_alts(false)?;
			return Ok(None);
		} else if t.kind == T::Ident {
			if t.kw == Kw::True {
				self.bump();
				let lit = self.leaf(Tag::True, 0, t);
				return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
			} else if t.kw == Kw::False {
				self.bump();
				let lit = self.leaf(Tag::False, 0, t);
				return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
			} else if t.kw == Kw::Bool {
				k::BOOL
			} else if t.kw == Kw::None_ {
				k::NONE
			} else if t.kw == Kw::Null {
				k::NULL
			} else if t.kw == Kw::Bytes {
				k::BYTES
			} else if t.kw == Kw::Datetime {
				k::DATETIME
			} else if t.kw == Kw::Decimal {
				k::DECIMAL
			} else if t.kw == Kw::Duration {
				k::DURATION
			} else if t.kw == Kw::Float {
				k::FLOAT
			} else if t.kw == Kw::Int {
				k::INT
			} else if t.kw == Kw::Number {
				k::NUMBER
			} else if t.kw == Kw::Object {
				k::OBJECT
			} else if t.kw == Kw::Point {
				k::POINT
			} else if t.kw == Kw::String {
				k::STRING
			} else if t.kw == Kw::Uuid {
				k::UUID
			} else if t.kw == Kw::Range {
				k::RANGE
			} else if t.kw == Kw::Regex {
				k::REGEX
			} else if t.kw == Kw::Function {
				k::FUNCTION
			} else if t.kw == Kw::Record
				|| t.kw == Kw::Table
				|| t.kw == Kw::Tb
				|| t.kw == Kw::Geometry
				|| t.kw == Kw::File
			{
				self.bump();
				let code = if t.kw == Kw::Record {
					k::RECORD
				} else if t.kw == Kw::Geometry {
					k::GEOMETRY
				} else if t.kw == Kw::File {
					k::FILE
				} else {
					k::TABLE
				};
				let list = if self.eat(T::Lt) {
					let open = self.prev_start;
					let mark = self.vals.len() as u32;
					loop {
						if code == k::GEOMETRY
							&& !(self.tok.kind == T::Ident && is_geometry_kind(self.tok.kw))
						{
							return Err(self.unexpected("a geometry kind name"));
						}
						let n = self.ident()?;
						let name = self.leaf(Tag::Name, 0, n);
						self.vals.push(name);
						if !self.eat(T::Pipe) {
							break;
						}
					}
					self.close(T::Gt, open)?;
					self.list_from(mark).0
				} else {
					NONE
				};
				return self.node(Tag::Kind, code, list, 0, t.start, 1).map(Some);
			} else if t.kw == Kw::Array || t.kw == Kw::Set {
				self.bump();
				let code = if t.kw == Kw::Array {
					k::ARRAY
				} else {
					k::SET
				};
				if !self.eat(T::Lt) {
					return self.node(Tag::Kind, code, NONE, NONE, t.start, 0).map(Some);
				}
				self.push_frame(Frame::KindSized {
					start: t.start,
					open: self.prev_start,
					code,
				})?;
				self.push_alts(false)?;
				return Ok(None);
			} else {
				match &self.text[t.start as usize..t.end as usize] {
					s if s.eq_ignore_ascii_case("i8") => k::I8,
					s if s.eq_ignore_ascii_case("u8") => k::U8,
					s if s.eq_ignore_ascii_case("i16") => k::I16,
					s if s.eq_ignore_ascii_case("u16") => k::U16,
					s if s.eq_ignore_ascii_case("i32") => k::I32,
					s if s.eq_ignore_ascii_case("u32") => k::U32,
					s if s.eq_ignore_ascii_case("i64") => k::INT,
					s if s.eq_ignore_ascii_case("u64") => k::U64,
					s if s.eq_ignore_ascii_case("f16") => k::F16,
					s if s.eq_ignore_ascii_case("f32") => k::F32,
					s if s.eq_ignore_ascii_case("f64") => k::FLOAT,
					s if s.eq_ignore_ascii_case("bigdecimal") => k::BIGDECIMAL,
					s if s.eq_ignore_ascii_case("date") => k::DATE,
					s if s.eq_ignore_ascii_case("time") => k::TIME,
					s if s.eq_ignore_ascii_case("timestamp") => k::TIMESTAMP,
					s if s.eq_ignore_ascii_case("interval") => k::INTERVAL,
					s if s.eq_ignore_ascii_case("enum") => k::ENUM,
					s if crate::is_builtin_kind_name(s) => {
						return Err(self.unexpected("a kind name"));
					}
					_ => return self.named_kind(t).map(Some),
				}
			}
		} else if t.kind == T::EscIdent {
			// A built-in kind's name is no type's name, escaped or not: a type of that name is
			// never defined, and its stored text would read back as the built-in kind.
			let mut name = String::new();
			if crate::decode::unescape(crate::decode::strip_delims(self.text_of(t)), &mut name)
				&& crate::is_builtin_kind_name(&name)
			{
				return Err(self.unexpected("a kind name"));
			}
			return self.named_kind(t).map(Some);
		} else {
			return Err(self.unexpected("a kind name"));
		};
		if code == k::ENUM {
			return self.enum_kind(t).map(Some);
		}
		self.bump();
		Ok(Some(self.leaf(Tag::Kind, code, t)))
	}

	/// `enum<'a', 'b'>` at the `enum` token `t`.
	fn enum_kind(&mut self, t: Tok) -> R<NodeId> {
		self.bump();
		let open = self.tok;
		self.expect(T::Lt, "<")?;
		let mark = self.vals.len() as u32;
		loop {
			let item = self.string_lit()?;
			self.vals.push(item);
			if !self.eat(T::Comma) {
				break;
			}
		}
		self.close(T::Gt, open.start)?;
		let list = self.list_from(mark).0;
		self.node(Tag::Kind, k::ENUM, list, 0, t.start, 1)
	}

	/// The name of a type declared by `DEFINE TYPE`, at the identifier `t`.
	fn named_kind(&mut self, t: Tok) -> R<NodeId> {
		self.bump();
		let op = if t.kind == T::EscIdent {
			1
		} else {
			0
		};
		let name = self.leaf(Tag::Name, op, t);
		self.node(Tag::Kind, k::NAMED, name, 0, t.start, 1)
	}
}

fn is_geometry_kind(kw: Kw) -> bool {
	kw == Kw::Point
		|| kw == Kw::Line
		|| kw == Kw::Polygon
		|| kw == Kw::MultiPoint
		|| kw == Kw::MultiLine
		|| kw == Kw::MultiPolygon
		|| kw == Kw::Collection
}
