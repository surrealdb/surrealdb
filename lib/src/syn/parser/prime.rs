use crate::{
	sql::{
		id::Gen, Array, Cond, Dir, Edges, Fields, Future, Graph, Id, Idiom, Mock, Part, Subquery,
		Table, Tables, Thing, Value,
	},
	syn::{
		parser::mac::{expected, to_do},
		token::{t, Span, TokenKind},
	},
};

use super::{mac::unexpected, ParseResult, Parser};

impl Parser<'_> {
	/// Parses a value that operators operate on.
	pub(super) fn parse_prime_value(&mut self) -> ParseResult<Value> {
		if let Some(x) = self.parse_determined_prime_value()? {
			return Ok(x);
		}

		if self.peek_token_at(1).kind == t!("::") {
			// production like a::b::c a path
			return self.parse_path_like();
		}
		if self.peek_token_at(1).kind == t!(":") {
			// production like a:b a thing
			return self.parse_thing_like();
		}

		if matches!(
			self.peek_token_at(1).kind,
			t!(".") | t!("...") | t!("[") | t!("->") | t!("<->") | t!("<-")
		) {
			// Next is a the start of an idiom so try an idiom.
			return self.parse_idiom().map(Value::Idiom);
		}

		let peek = self.peek_token();
		let parse_as_field = |this: &mut Parser| Ok(Value::Table(Table(this.parse_raw_ident()?)));
		match peek.kind {
			t!("IF") => self.recover(
				peek.span,
				|this| this.parse_if_stmt().map(|s| Value::Subquery(Box::new(Subquery::Ifelse(s)))),
				parse_as_field,
			),
			t!("RETURN") => self.recover(
				peek.span,
				|this| {
					this.parse_return_stmt().map(|s| Value::Subquery(Box::new(Subquery::Output(s))))
				},
				parse_as_field,
			),
			t!("SELECT") => self.recover(
				peek.span,
				|this| {
					this.parse_select_stmt().map(|s| Value::Subquery(Box::new(Subquery::Select(s))))
				},
				parse_as_field,
			),
			t!("CREATE") => self.recover(
				peek.span,
				|this| {
					this.parse_create_stmt().map(|s| Value::Subquery(Box::new(Subquery::Create(s))))
				},
				parse_as_field,
			),
			t!("UPDATE") => self.recover(
				peek.span,
				|this| {
					this.parse_update_stmt().map(|s| Value::Subquery(Box::new(Subquery::Update(s))))
				},
				parse_as_field,
			),
			t!("DELETE") => self.recover(
				peek.span,
				|this| {
					this.parse_delete_stmt().map(|s| Value::Subquery(Box::new(Subquery::Delete(s))))
				},
				parse_as_field,
			),
			t!("RELATE") => self.recover(
				peek.span,
				|this| {
					this.parse_relate_stmt().map(|s| Value::Subquery(Box::new(Subquery::Relate(s))))
				},
				parse_as_field,
			),
			t!("INSERT") => self.recover(
				peek.span,
				|this| {
					this.parse_insert_stmt().map(|s| Value::Subquery(Box::new(Subquery::Insert(s))))
				},
				parse_as_field,
			),
			t!("DEFINE") => self.recover(
				peek.span,
				|this| {
					this.parse_define_stmt().map(|s| Value::Subquery(Box::new(Subquery::Define(s))))
				},
				parse_as_field,
			),
			t!("REMOVE") => self.recover(
				peek.span,
				|this| {
					this.parse_remove_stmt().map(|s| Value::Subquery(Box::new(Subquery::Remove(s))))
				},
				parse_as_field,
			),
			_ => Ok(Value::Table(Table(self.parse_raw_ident()?))),
		}
	}

	fn parse_determined_prime_value(&mut self) -> ParseResult<Option<Value>> {
		let token = self.peek_token();
		let res = match token.kind {
			t!("<-") => {
				self.pop_peek();
				let part = self.parse_graph(Dir::In)?;
				Value::Idiom(Idiom(vec![part]))
			}
			t!("<->") => {
				self.pop_peek();
				let part = self.parse_graph(Dir::Both)?;
				Value::Idiom(Idiom(vec![part]))
			}
			t!("->") => {
				self.pop_peek();
				let part = self.parse_graph(Dir::Out)?;
				Value::Idiom(Idiom(vec![part]))
			}
			t!("<") => {
				self.pop_peek();
				// At this point casting should already have been parsed.
				// So this must be a future
				expected!(self, "FUTURE");
				self.expect_closing_delimiter(t!(">"), token.span)?;
				let span = expected!(self, "{").span;
				let block = self.parse_block(span)?;
				let future = Box::new(Future(block));
				// future can't start an idiom so return immediately.
				Value::Future(future)
			}
			t!("|") => {
				self.pop_peek();
				self.parse_mock().map(Value::Mock)?
			}
			t!("[") => self.parse_array().map(Value::Array)?,
			t!("{") => self.parse_object_like()?,
			t!("/") => {
				// regex
				to_do!(self)
			}
			t!("$param") => self.parse_param().map(Value::Param)?,
			t!("(") => {
				let span = self.pop_peek().span;
				Value::Subquery(Box::new(self.parse_delimited_subquery(span)?))
			}
			TokenKind::Strand => self.parse_strand().map(Value::Strand)?,
			TokenKind::Duration {
				valid_identifier: false,
			} => self.parse_duration().map(Value::Duration)?,
			x if !x.can_be_identifier() => unexpected!(self, x, "a value"),
			_ => return Ok(None),
		};
		Ok(Some(res))
	}

	fn parse_thing_like(&mut self) -> ParseResult<Value> {
		let tb = self.parse_raw_ident()?;
		self.pop_peek();
		let id = self.parse_thing_tail()?;
		let thing = Thing {
			tb,
			id,
		};
		if matches!(self.peek_token().kind, t!("<-") | t!("<->") | t!("->")) {
			self.parse_edge(thing).map(Box::new).map(Value::Edges)
		} else {
			Ok(Value::Thing(thing))
		}
	}

	fn parse_edge(&mut self, thing: Thing) -> ParseResult<Edges> {
		let dir = self.parse_dir()?;
		let start = self.next_token();
		match start.kind {
			t!("?") => Ok(Edges {
				dir,
				from: thing,
				what: Tables::default(),
			}),
			t!("(") => {
				if self.eat(t!("?")) {
					self.expect_closing_delimiter(t!(")"), start.span)?;
					return Ok(Edges {
						dir,
						from: thing,
						what: Tables::default(),
					});
				}

				let mut tables = vec![Table(self.parse_raw_ident()?)];
				while self.eat(t!(",")) {
					tables.push(Table(self.parse_raw_ident()?));
				}
				self.expect_closing_delimiter(t!(")"), start.span)?;
				Ok(Edges {
					dir,
					from: thing,
					what: Tables(tables),
				})
			}
			x => unexpected!(self, x, "an edge value"),
		}
	}

	fn parse_path_like(&mut self) -> ParseResult<Value> {
		to_do!(self)
	}

	fn parse_mock(&mut self) -> ParseResult<Mock> {
		let t = self.parse_raw_ident()?;
		expected!(self, ":");
		let number = self.parse_u64()?;
		// mock can't start an idiom so return immediately.
		if self.eat(t!("|")) {
			return Ok(Mock::Count(t, number));
		} else {
			expected!(self, "..");
			let to = self.parse_u64()?;
			expected!(self, "|");
			return Ok(Mock::Range(t, number, to));
		}
	}

	fn parse_delimited_subquery(&mut self, span: Span) -> ParseResult<Subquery> {
		let token = self.peek_token();
		let v = match token.kind {
			t!("RETURN") => self.recover(
				token.span,
				|this| this.parse_return_stmt().map(Subquery::Output),
				|this| this.parse_value().map(Subquery::Value),
			)?,
			t!("SELECT") => self.recover(
				token.span,
				|this| this.parse_select_stmt().map(Subquery::Select),
				|this| this.parse_value().map(Subquery::Value),
			)?,
			t!("CREATE") => self.recover(
				token.span,
				|this| this.parse_create_stmt().map(Subquery::Create),
				|this| this.parse_value().map(Subquery::Value),
			)?,
			t!("UPDATE") => self.recover(
				token.span,
				|this| this.parse_update_stmt().map(Subquery::Update),
				|this| this.parse_value().map(Subquery::Value),
			)?,
			t!("DELETE") => self.recover(
				token.span,
				|this| this.parse_delete_stmt().map(Subquery::Delete),
				|this| this.parse_value().map(Subquery::Value),
			)?,
			t!("RELATE") => self.recover(
				token.span,
				|this| this.parse_relate_stmt().map(Subquery::Relate),
				|this| this.parse_value().map(Subquery::Value),
			)?,
			t!("INSERT") => self.recover(
				token.span,
				|this| this.parse_insert_stmt().map(Subquery::Insert),
				|this| this.parse_value().map(Subquery::Value),
			)?,
			t!("DEFINE") => self.recover(
				token.span,
				|this| this.parse_define_stmt().map(Subquery::Define),
				|this| this.parse_value().map(Subquery::Value),
			)?,
			t!("REMOVE") => self.recover(
				token.span,
				|this| this.parse_remove_stmt().map(Subquery::Remove),
				|this| this.parse_value().map(Subquery::Value),
			)?,
			_ => self.parse_value().map(Subquery::Value)?,
		};
		self.expect_closing_delimiter(t!(")"), span)?;
		Ok(v)
	}

	pub(super) fn parse_thing_tail(&mut self) -> ParseResult<Id> {
		match self.peek_token().kind {
			t!("RAND") if self.peek_token_at(1).kind == t!("(") => {
				self.pop_peek();
				let close = self.pop_peek().span;
				self.expect_closing_delimiter(t!(")"), close)?;
				Ok(Id::Generate(Gen::Rand))
			}
			t!("ULID") if self.peek_token_at(1).kind == t!("(") => {
				self.pop_peek();
				let close = self.pop_peek().span;
				self.expect_closing_delimiter(t!(")"), close)?;
				Ok(Id::Generate(Gen::Ulid))
			}
			t!("UUID") if self.peek_token_at(1).kind == t!("(") => {
				self.pop_peek();
				let close = self.pop_peek().span;
				self.expect_closing_delimiter(t!(")"), close)?;
				Ok(Id::Generate(Gen::Uuid))
			}
			t!("-") => {
				self.pop_peek();
				let number = self.parse_u64()?;
				let number = if number == i64::MAX as u64 + 1 {
					i64::MIN
				} else {
					-(number as i64)
				};

				Ok(Id::Number(number))
			}
			t!("+") => {
				self.pop_peek();
				let number = self.parse_u64()?;
				let Some(number) = number.try_into().ok() else {
					to_do!(self)
				};
				Ok(Id::Number(number))
			}
			t!("123") => {
				let number = self.parse_u64()?;
				let Some(number) = number.try_into().ok() else {
					to_do!(self)
				};
				Ok(Id::Number(number))
			}
			t!("{") => {
				let s = self.pop_peek().span;
				let object = self.parse_object(s)?;
				Ok(Id::Object(object))
			}
			t!("[") => {
				let array = self.parse_array()?;
				Ok(Id::Array(array))
			}
			x => unexpected!(self, x, "a thing id"),
		}
	}

	pub(super) fn parse_graph(&mut self, dir: Dir) -> ParseResult<Part> {
		let next = self.next_token();
		match next.kind {
			t!("(") => {
				if self.eat(t!("?")) {
					self.expect_closing_delimiter(t!(")"), next.span)?;
					Ok(Part::Graph(Graph {
						dir,
						expr: Fields::all(),
						..Default::default()
					}))
				} else {
					// TODO: Better error message here.
					let mut what = vec![self.parse_raw_ident().map(Table)?];
					while self.eat(t!(",")) {
						what.push(self.parse_raw_ident().map(Table)?);
					}

					let cond =
						self.eat(t!("WHERE")).then(|| self.parse_value()).transpose()?.map(Cond);
					let alias = self
						.eat(t!("AS"))
						.then(|| {
							// TODO: Check idiom type
							self.parse_plain_idiom()
						})
						.transpose()?;

					Ok(Part::Graph(Graph {
						dir,
						expr: Fields::all(),
						what: Tables(what),
						alias,
						cond,
						..Default::default()
					}))
				}
			}
			t!("?") => Ok(Part::Graph(Graph {
				dir,
				expr: Fields::all(),
				..Default::default()
			})),
			TokenKind::Keyword(_)
			| TokenKind::Number
			| TokenKind::Duration {
				valid_identifier: true,
			} => {
				let str = self.lexer.reader.span(next.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				Ok(Part::Graph(Graph {
					dir,
					expr: Fields::all(),
					what: Tables(vec![Table(str)]),
					..Default::default()
				}))
			}
			TokenKind::Identifier => {
				let data_index = next.data_index.unwrap();
				let idx = u32::from(data_index) as usize;
				let str = self.lexer.strings[idx].clone();
				Ok(Part::Graph(Graph {
					dir,
					expr: Fields::all(),
					what: Tables(vec![Table(str)]),
					..Default::default()
				}))
			}
			x => unexpected!(self, x, "a graph start"),
		}
	}

	fn parse_array(&mut self) -> ParseResult<Array> {
		let start = expected!(self, "[").span;
		let mut res = Vec::new();
		// basic parsing of a delimited list with optional trailing separator.
		// First check if the list ends, then parse a value followed by asserting that the list
		// ends if there is no next separator.
		loop {
			if self.eat(t!("]")) {
				break;
			}
			let value = self.parse_value()?;
			res.push(value);
			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("]"), start)?;
				break;
			}
		}
		Ok(Array(res))
	}
}
