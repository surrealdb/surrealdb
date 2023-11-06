use crate::{
	sql::{Dir, Edges, Field, Fields, Graph, Ident, Idiom, Part, Table, Tables, Value},
	syn::{
		parser::mac::to_do,
		token::{t, Span, TokenKind},
	},
};

use super::{mac::unexpected, ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_fields(&mut self) -> ParseResult<Fields> {
		if self.eat(t!("VALUE")) {
			let expr = self.parse_value_field()?;
			let alias = self.eat(t!("AS")).then(|| self.parse_plain_idiom()).transpose()?;
			Ok(Fields(
				vec![Field::Single {
					expr,
					alias,
				}],
				true,
			))
		} else {
			let mut fields = Vec::new();
			loop {
				let field = if self.eat(t!("*")) {
					Field::All
				} else {
					let expr = self.parse_value_field()?;
					let alias = self.eat(t!("AS")).then(|| self.parse_plain_idiom()).transpose()?;
					Field::Single {
						expr,
						alias,
					}
				};
				fields.push(field);
				if !self.eat(t!(",")) {
					break;
				}
			}
			Ok(Fields(fields, false))
		}
	}

	/// Parses a list of idioms seperated by a `,`
	pub fn parse_idiom_list(&mut self) -> ParseResult<Vec<Idiom>> {
		let mut res = vec![self.parse_plain_idiom()?];
		while self.eat(t!(",")) {
			res.push(self.parse_plain_idiom()?);
		}
		Ok(res)
	}

	/// Parses the remaining idiom parts after the start.
	pub(crate) fn parse_remaining_idiom(&mut self, start: Vec<Part>) -> ParseResult<Idiom> {
		let mut res = start;
		loop {
			match self.peek_kind() {
				t!("...") => {
					self.pop_peek();
					res.push(Part::Flatten);
				}
				t!(".") => {
					self.pop_peek();
					res.push(self.parse_dot_part()?)
				}
				t!("[") => {
					let span = self.pop_peek().span;
					res.push(self.parse_bracket_part(span)?)
				}
				t!("->") => {
					self.pop_peek();
					res.push(Part::Graph(self.parse_graph(Dir::Out)?))
				}
				t!("<->") => {
					self.pop_peek();
					res.push(Part::Graph(self.parse_graph(Dir::Both)?))
				}
				t!("<-") => {
					self.pop_peek();
					res.push(Part::Graph(self.parse_graph(Dir::In)?))
				}
				t!("..") => {
					// TODO: error message suggesting `..`
					to_do!(self)
				}
				_ => break,
			}
		}
		Ok(Idiom(res))
	}

	/// Parses the remaining idiom parts after the start.
	pub(crate) fn parse_remaining_value_idiom(&mut self, start: Vec<Part>) -> ParseResult<Value> {
		let mut res = start;
		loop {
			match self.peek_kind() {
				t!("...") => {
					self.pop_peek();
					res.push(Part::Flatten);
				}
				t!(".") => {
					self.pop_peek();
					res.push(self.parse_dot_part()?)
				}
				t!("[") => {
					let span = self.pop_peek().span;
					res.push(self.parse_bracket_part(span)?)
				}
				t!("->") => {
					self.pop_peek();
					if let Some(x) = self.parse_graph_idiom(&mut res, Dir::Out)? {
						return Ok(x);
					}
				}
				t!("<->") => {
					self.pop_peek();
					if let Some(x) = self.parse_graph_idiom(&mut res, Dir::Out)? {
						return Ok(x);
					}
				}
				t!("<-") => {
					self.pop_peek();
					if let Some(x) = self.parse_graph_idiom(&mut res, Dir::Out)? {
						return Ok(x);
					}
				}
				t!("..") => {
					// TODO: error message suggesting `..`
					to_do!(self)
				}
				_ => break,
			}
		}
		Ok(Value::Idiom(Idiom(res)))
	}

	fn parse_graph_idiom(&mut self, res: &mut Vec<Part>, dir: Dir) -> ParseResult<Option<Value>> {
		let graph = self.parse_graph(dir)?;
		// the production `Thing Graph` is reparsed as an edge if the graph does not contain an
		// alias or a condition.
		if res.len() == 1 && graph.alias.is_none() && graph.cond.is_none() {
			match std::mem::replace(&mut res[0], Part::All) {
				Part::Value(Value::Thing(t)) => {
					let edge = Edges {
						dir: graph.dir,
						from: t,
						what: graph.what,
					};
					let value = Value::Edges(Box::new(edge));

					if !Self::continues_idiom(self.peek_kind()) {
						return Ok(Some(value));
					}
					res[0] = Part::Value(value);
					return Ok(None);
				}
				x => {
					res[0] = x;
				}
			}
		}
		res.push(Part::Graph(graph));
		Ok(None)
	}

	/// Returns if the token kind could continua an idiom
	pub fn continues_idiom(kind: TokenKind) -> bool {
		matches!(kind, t!("->") | t!("<->") | t!("<-") | t!("[") | t!(".") | t!("..."))
	}

	/// Parse a idiom which can only start with a graph or an identifier.
	/// Other expressions are not allowed as start of this idiom
	pub fn parse_plain_idiom(&mut self) -> ParseResult<Idiom> {
		let start = match self.peek_kind() {
			t!("->") => {
				self.pop_peek();
				Part::Graph(self.parse_graph(Dir::Out)?)
			}
			t!("<->") => {
				self.pop_peek();
				Part::Graph(self.parse_graph(Dir::Both)?)
			}
			t!("<-") => {
				self.pop_peek();
				Part::Graph(self.parse_graph(Dir::In)?)
			}
			_ => Part::Field(self.parse_token_value()?),
		};
		let start = vec![start];
		self.parse_remaining_idiom(start)
	}

	pub fn parse_dot_part(&mut self) -> ParseResult<Part> {
		let res = match self.peek_kind() {
			t!("*") => {
				self.pop_peek();
				Part::All
			}
			_ => Part::Field(self.parse_token_value()?),
		};
		Ok(res)
	}

	pub fn parse_bracket_part(&mut self, start: Span) -> ParseResult<Part> {
		let res = match self.peek_kind() {
			t!("*") => {
				self.pop_peek();
				Part::All
			}
			t!("$") => {
				self.pop_peek();
				Part::Last
			}
			t!("123") => Part::Index(self.parse_token_value()?),
			t!("?") | t!("WHERE") => {
				self.pop_peek();
				Part::Where(self.parse_value()?)
			}
			t!("$param") => Part::Value(Value::Param(self.parse_token_value()?)),
			TokenKind::Strand => Part::Value(Value::Strand(self.parse_token_value()?)),
			_ => {
				let idiom = self.parse_basic_idiom()?;
				Part::Value(Value::Idiom(idiom))
			}
		};
		self.expect_closing_delimiter(t!("]"), start)?;
		Ok(res)
	}

	pub fn parse_basic_idiom_list(&mut self) -> ParseResult<Vec<Idiom>> {
		let mut res = vec![self.parse_basic_idiom()?];
		while self.eat(t!(",")) {
			res.push(self.parse_basic_idiom()?);
		}
		Ok(res)
	}

	pub fn parse_basic_idiom(&mut self) -> ParseResult<Idiom> {
		let start = self.parse_token_value::<Ident>()?;
		let mut parts = vec![Part::Field(start)];
		loop {
			let token = self.peek();
			let part = match token.kind {
				t!(".") => {
					self.pop_peek();
					self.parse_dot_part()?
				}
				t!("[") => {
					self.pop_peek();
					let res = match self.peek_kind() {
						t!("*") => {
							self.pop_peek();
							Part::All
						}
						t!("$") => {
							self.pop_peek();
							Part::Last
						}
						t!("123") => {
							let number = self.from_token(token)?;
							Part::Index(number)
						}
						x => unexpected!(self, x, "$, * or a number"),
					};
					self.expect_closing_delimiter(t!("]"), token.span)?;
					res
				}
				_ => break,
			};
			parts.push(part);
		}
		Ok(Idiom(parts))
	}

	pub fn parse_local_idiom(&mut self) -> ParseResult<Idiom> {
		let start = self.parse_token_value()?;
		let mut parts = vec![Part::Field(start)];
		loop {
			let token = self.peek();
			let part = match token.kind {
				t!(".") => {
					self.pop_peek();
					self.parse_dot_part()?
				}
				t!("[") => {
					self.pop_peek();
					let res = match self.peek_kind() {
						t!("*") => {
							self.pop_peek();
							Part::All
						}
						t!("123") => {
							let number = self.parse_token_value()?;
							Part::Index(number)
						}
						x => unexpected!(self, x, "$, * or a number"),
					};
					self.expect_closing_delimiter(t!("]"), token.span)?;
					res
				}
				_ => break,
			};

			parts.push(part);
		}

		if self.eat(t!("...")) {
			parts.push(Part::Flatten);
			if let t!(".") | t!("[") = self.peek_kind() {
				// TODO: Error message that flatten can only be last.
				to_do!(self)
			}
		}

		Ok(Idiom(parts))
	}

	/// Parses a list of what values seperated by comma's
	///
	/// # Parser state
	/// Expects to be at the start of a what list.
	pub fn parse_what_list(&mut self) -> ParseResult<Vec<Value>> {
		let mut res = vec![self.parse_what_value()?];
		while self.eat(t!(",")) {
			res.push(self.parse_what_value()?)
		}
		Ok(res)
	}

	/// Parses a single what value,
	///
	/// # Parser state
	/// Expects to be at the start of a what value
	pub fn parse_what_value(&mut self) -> ParseResult<Value> {
		let start = self.parse_what_primary()?;
		if start.can_start_idiom() && Self::continues_idiom(self.peek_kind()) {
			let start = match start {
				Value::Table(Table(x)) => vec![Part::Field(Ident(x))],
				Value::Idiom(Idiom(x)) => x,
				x => vec![Part::Value(x)],
			};

			let idiom = self.parse_remaining_value_idiom(start)?;
			Ok(idiom)
		} else {
			Ok(start)
		}
	}

	/// Parses a graph value
	///
	/// # Parser state
	/// Expects to just have eaten a direction (e.g. <-, <->, or ->) and be at the field like part
	/// of the graph
	pub fn parse_graph(&mut self, dir: Dir) -> ParseResult<Graph> {
		match self.peek_kind() {
			t!("?") => {
				self.pop_peek();
				Ok(Graph {
					dir,
					..Default::default()
				})
			}
			t!("(") => {
				let span = self.pop_peek().span;
				let what = match self.peek_kind() {
					t!("?") => {
						self.pop_peek();
						Tables::default()
					}
					x if x.can_be_identifier() => {
						// The following function should always succeed here,
						// returning an error here would be a bug, so unwrap.
						let table = self.parse_token_value().unwrap();
						let mut tables = Tables(vec![table]);
						while self.eat(t!(",")) {
							tables.0.push(self.parse_token_value()?);
						}
						tables
					}
					x => unexpected!(self, x, "`?` or an identifier"),
				};

				let cond = self.try_parse_condition()?;
				let alias = self.eat(t!("AS")).then(|| self.parse_plain_idiom()).transpose()?;

				self.expect_closing_delimiter(t!(")"), span)?;

				Ok(Graph {
					dir,
					what,
					cond,
					alias,
					..Default::default()
				})
			}
			x if x.can_be_identifier() => {
				// The following function should always succeed here,
				// returning an error here would be a bug, so unwrap.
				let table = self.parse_token_value().unwrap();
				Ok(Graph {
					dir,
					what: Tables(vec![table]),
					..Default::default()
				})
			}
			x => unexpected!(self, x, "`?`, `(` or an identifier"),
		}
	}
}
