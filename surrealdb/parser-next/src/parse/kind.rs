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
			let inner_first = matches!(
				self.frames.last(),
				Some(Frame::KindAlts { mark, option: false, .. }) if *mark as usize == self.vals.len()
			);
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
					_ => unreachable!("only kind frames are above a kind's base"),
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
		let code = match t.kind {
			T::Str if t.aux() == 0 || t.aux() == b's' => {
				self.bump();
				let lit = self.leaf(Tag::Str, t.aux(), t);
				return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
			}
			T::NaN | T::Infinity => {
				self.bump();
				let tag = if t.kind == T::NaN {
					Tag::NaN
				} else {
					Tag::Infinity
				};
				let lit = self.leaf(tag, 0, t);
				return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
			}
			T::Plus | T::Minus | T::Digits => {
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
					if let Some(d) = crate::lex::compound_error(self.src, t.start, Compound::Number)
					{
						return Err(self.diag(d));
					}
					return Err(self.error(n.start, n.end, "expected a number"));
				}
				return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
			}
			T::LBrace => {
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
			}
			T::LBracket => {
				self.bump();
				if self.eat(T::RBracket) {
					let idx = self.empty_list();
					return self.node(Tag::Kind, k::LIT_ARRAY, idx, 0, t.start, 0).map(Some);
				}
				// Only the opening tuple of an `id` field's `TYPE` names its
				// components; the lookahead runs before the first element is read
				// as a kind, since component names may be kind keywords.
				let named = self.keyed_at == t.start
					&& self.tok.is_ident()
					&& self.peek1().kind == T::Colon;
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
			}
			T::Ident => match t.kw {
				Kw::True => {
					self.bump();
					let lit = self.leaf(Tag::True, 0, t);
					return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
				}
				Kw::False => {
					self.bump();
					let lit = self.leaf(Tag::False, 0, t);
					return self.node(Tag::Kind, k::LITERAL, lit, 0, t.start, 1).map(Some);
				}
				Kw::Bool => k::BOOL,
				Kw::None_ => k::NONE,
				Kw::Null => k::NULL,
				Kw::Bytes => k::BYTES,
				Kw::Datetime => k::DATETIME,
				Kw::Decimal => k::DECIMAL,
				Kw::Duration => k::DURATION,
				Kw::Float => k::FLOAT,
				Kw::Int => k::INT,
				Kw::Number => k::NUMBER,
				Kw::Object => k::OBJECT,
				Kw::Point => k::POINT,
				Kw::String => k::STRING,
				Kw::Uuid => k::UUID,
				Kw::Range => k::RANGE,
				Kw::Regex => k::REGEX,
				Kw::Function => k::FUNCTION,
				Kw::Record | Kw::Table | Kw::Tb | Kw::Geometry | Kw::File => {
					self.bump();
					let code = match t.kw {
						Kw::Record => k::RECORD,
						Kw::Geometry => k::GEOMETRY,
						Kw::File => k::FILE,
						_ => k::TABLE,
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
				}
				Kw::Array | Kw::Set => {
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
				}
				_ => return Err(self.unexpected("a kind name")),
			},
			_ => return Err(self.unexpected("a kind name")),
		};
		self.bump();
		Ok(Some(self.leaf(Tag::Kind, code, t)))
	}
}

fn is_geometry_kind(kw: Kw) -> bool {
	matches!(
		kw,
		Kw::Point
			| Kw::Line
			| Kw::Polygon
			| Kw::MultiPoint
			| Kw::MultiLine
			| Kw::MultiPolygon
			| Kw::Collection
	)
}
