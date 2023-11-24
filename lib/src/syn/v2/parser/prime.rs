use std::ops::Bound;

use crate::{
	sql::{Array, Dir, Id, Ident, Idiom, Mock, Part, Range, Subquery, Table, Thing, Value},
	syn::v2::{
		parser::mac::{expected, to_do, unexpected},
		token::{t, Span, TokenKind},
	},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	/// Parse a what primary.
	///
	/// What's are values which are more restricted in what expressions they can contain.
	pub fn parse_what_primary(&mut self) -> ParseResult<Value> {
		match self.peek_kind() {
			TokenKind::Duration => {
				let duration = self.parse_token_value()?;
				Ok(Value::Duration(duration))
			}
			TokenKind::DateTime => {
				let datetime = self.parse_token_value()?;
				Ok(Value::Datetime(datetime))
			}
			t!("$param") => {
				let param = self.parse_token_value()?;
				Ok(Value::Param(param))
			}
			t!("IF") => {
				let stmt = self.parse_if_stmt()?;
				Ok(Value::Subquery(Box::new(Subquery::Ifelse(stmt))))
			}
			t!("(") => {
				let token = self.pop_peek();
				self.parse_inner_subquery(Some(token.span)).map(|x| Value::Subquery(Box::new(x)))
			}
			t!("<") => {
				self.pop_peek();
				expected!(self, "FUTURE");
				expected!(self, ">");
				let start = expected!(self, "{").span;
				let block = self.parse_block(start)?;
				Ok(Value::Future(Box::new(crate::sql::Future(block))))
			}
			t!("|") => {
				let start = self.pop_peek().span;
				self.parse_mock(start).map(Value::Mock)
			}
			t!("/") => {
				let token = self.pop_peek();
				let regex = self.lexer.relex_regex(token);
				self.token_value(regex).map(Value::Regex)
			}
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE") => self.parse_inner_subquery(None).map(|x| Value::Subquery(Box::new(x))),
			_ => {
				let table = self.parse_token_value::<Table>()?;
				if self.peek_kind() == t!(":") {
					return self.parse_thing_or_range(table.0);
				}
				Ok(Value::Table(table))
			}
		}
	}

	/// Parse an expressions
	pub fn parse_idiom_expression(&mut self) -> ParseResult<Value> {
		let token = self.next();
		let value = match token.kind {
			t!("NONE") => return Ok(Value::None),
			t!("NULL") => return Ok(Value::Null),
			t!("true") => return Ok(Value::Bool(true)),
			t!("false") => return Ok(Value::Bool(false)),
			t!("<") => {
				// Casting should already have been parsed.
				expected!(self, "FUTURE");
				self.expect_closing_delimiter(t!(">"), token.span)?;
				let next = expected!(self, "{").span;
				let block = self.parse_block(next)?;
				return Ok(Value::Future(Box::new(crate::sql::Future(block))));
			}
			TokenKind::Strand => {
				let strand = self.token_value(token)?;
				return Ok(Value::Strand(strand));
			}
			TokenKind::Duration => {
				let duration = self.token_value(token)?;
				Value::Duration(duration)
			}
			TokenKind::Number => {
				let number = self.token_value(token)?;
				Value::Number(number)
			}
			TokenKind::Uuid => {
				let uuid = self.token_value(token)?;
				Value::Uuid(uuid)
			}
			TokenKind::DateTime => {
				let datetime = self.token_value(token)?;
				Value::Datetime(datetime)
			}
			t!("$param") => {
				let param = self.token_value(token)?;
				Value::Param(param)
			}
			t!("FUNCTION") => {
				to_do!(self)
			}
			t!("->") => {
				let graph = self.parse_graph(Dir::Out)?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("<->") => {
				let graph = self.parse_graph(Dir::Both)?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("<-") => {
				let graph = self.parse_graph(Dir::In)?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("[") => self.parse_array(token.span).map(Value::Array)?,
			t!("{") => self.parse_object_like(token.span)?,
			t!("|") => self.parse_mock(token.span).map(Value::Mock)?,
			t!("IF") => {
				let stmt = self.parse_if_stmt()?;
				Value::Subquery(Box::new(Subquery::Ifelse(stmt)))
			}
			t!("(") => {
				self.parse_inner_subquery(Some(token.span)).map(|x| Value::Subquery(Box::new(x)))?
			}
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE") => self.parse_inner_subquery(None).map(|x| Value::Subquery(Box::new(x)))?,
			_ => {
				let name: Ident = self.token_value(token)?;
				if self.peek_kind() == t!(":") {
					return self.parse_thing_or_range(name.0);
				}

				if self.table_as_field {
					Value::Idiom(Idiom(vec![Part::Field(name)]))
				} else {
					Value::Table(Table(name.0))
				}
			}
		};

		// Parse the rest of the idiom if it is being continued.
		if Self::continues_idiom(self.peek_kind()) {
			match value {
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Future(_)
				| Value::Strand(_) => unreachable!(),
				Value::Idiom(Idiom(x)) => self.parse_remaining_value_idiom(x),
				Value::Table(Table(x)) => {
					self.parse_remaining_value_idiom(vec![Part::Field(Ident(x))])
				}
				x => self.parse_remaining_value_idiom(vec![Part::Value(x)]),
			}
		} else {
			Ok(value)
		}
	}

	/// Parses an array production
	///
	/// # Parser state
	/// Expects the starting `[` to already be eaten and its span passed as an argument.
	pub fn parse_array(&mut self, start: Span) -> ParseResult<Array> {
		let mut values = Vec::new();
		loop {
			if self.eat(t!("]")) {
				break;
			}
			values.push(self.parse_value()?);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("]"), start)?;
				break;
			}
		}

		Ok(Array(values))
	}

	pub fn parse_mock(&mut self, start: Span) -> ParseResult<Mock> {
		let name = self.parse_token_value::<Ident>()?.0;
		expected!(self, ":");
		let from = self.parse_token_value()?;
		let to = self.eat(t!("..")).then(|| self.parse_token_value()).transpose()?;
		self.expect_closing_delimiter(t!("|"), start)?;
		if let Some(to) = to {
			Ok(Mock::Range(name, from, to))
		} else {
			Ok(Mock::Count(name, from))
		}
	}

	pub fn parse_thing_or_range(&mut self, ident: String) -> ParseResult<Value> {
		expected!(self, ":");
		let exclusive = self.eat(t!(">"));
		let id = self.parse_id()?;
		if self.eat(t!("..")) {
			let inclusive = self.eat(t!("="));
			let end = self.parse_id()?;
			Ok(Value::Range(Box::new(Range {
				tb: ident,
				beg: if exclusive {
					Bound::Excluded(id)
				} else {
					Bound::Included(id)
				},
				end: if inclusive {
					Bound::Included(end)
				} else {
					Bound::Excluded(end)
				},
			})))
		} else {
			if exclusive {
				unexpected!(self, self.peek_kind(), "the range operator '..'")
			}
			Ok(Value::Thing(Thing {
				tb: ident,
				id,
			}))
		}
	}

	pub fn parse_range(&mut self) -> ParseResult<Range> {
		let tb = self.parse_token_value::<Ident>()?.0;
		expected!(self, ":");
		let exclusive = self.eat(t!(">"));
		let id = self.parse_id()?;
		expected!(self, "..");
		let inclusive = self.eat(t!("="));
		let end = self.parse_id()?;
		Ok(Range {
			tb,
			beg: if exclusive {
				Bound::Excluded(id)
			} else {
				Bound::Included(id)
			},
			end: if inclusive {
				Bound::Included(end)
			} else {
				Bound::Excluded(end)
			},
		})
	}

	pub fn parse_thing(&mut self) -> ParseResult<Thing> {
		let ident = self.parse_token_value::<Ident>()?.0;
		self.parse_thing_from_ident(ident)
	}

	pub fn parse_thing_from_ident(&mut self, ident: String) -> ParseResult<Thing> {
		expected!(self, ":");
		let id = self.parse_id()?;
		Ok(Thing {
			tb: ident,
			id,
		})
	}

	pub fn parse_id(&mut self) -> ParseResult<Id> {
		match self.peek_kind() {
			t!("{") => {
				let start = self.pop_peek().span;
				let object = self.parse_object(start)?;
				Ok(Id::Object(object))
			}
			t!("[") => {
				let start = self.pop_peek().span;
				let array = self.parse_array(start)?;
				Ok(Id::Array(array))
			}
			// TODO: negative numbers.
			TokenKind::Number => {
				let number = self.parse_token_value::<u64>()?;
				Ok(Id::Number(number as i64))
			}
			_ => {
				let ident = self.parse_token_value::<Ident>()?.0;
				Ok(Id::String(ident))
			}
		}
	}

	pub fn parse_full_subquery(&mut self) -> ParseResult<Subquery> {
		let peek = self.peek();
		match peek.kind {
			t!("(") => {
				self.pop_peek();
				self.parse_inner_subquery(Some(peek.span))
			}
			t!("IF") => {
				self.pop_peek();
				let if_stmt = self.parse_if_stmt()?;
				Ok(Subquery::Ifelse(if_stmt))
			}
			_ => self.parse_inner_subquery(None),
		}
	}

	pub fn parse_inner_subquery(&mut self, start: Option<Span>) -> ParseResult<Subquery> {
		let res = match self.peek().kind {
			t!("RETURN") => {
				self.pop_peek();
				let stmt = self.parse_return_stmt()?;
				Subquery::Output(stmt)
			}
			t!("SELECT") => {
				self.pop_peek();
				let stmt = self.parse_select_stmt()?;
				Subquery::Select(stmt)
			}
			t!("CREATE") => {
				self.pop_peek();
				let stmt = self.parse_create_stmt()?;
				Subquery::Create(stmt)
			}
			t!("UPDATE") => {
				self.pop_peek();
				let stmt = self.parse_update_stmt()?;
				Subquery::Update(stmt)
			}
			t!("DELETE") => {
				self.pop_peek();
				let stmt = self.parse_delete_stmt()?;
				Subquery::Delete(stmt)
			}
			t!("RELATE") => {
				self.pop_peek();
				let stmt = self.parse_relate_stmt()?;
				Subquery::Relate(stmt)
			}
			t!("DEFINE") => {
				self.pop_peek();
				let stmt = self.parse_define_stmt()?;
				Subquery::Define(stmt)
			}
			t!("REMOVE") => {
				self.pop_peek();
				let stmt = self.parse_remove_stmt()?;
				Subquery::Remove(stmt)
			}
			_ => {
				let value = self.parse_value()?;
				Subquery::Value(value)
			}
		};
		if let Some(start) = start {
			self.expect_closing_delimiter(t!(")"), start)?;
		}
		Ok(res)
	}
}
