use crate::{
	sql::{
		Array, Dir, Duration, Id, Ident, Idiom, Mock, Param, Part, Strand, Subquery, Table, Thing,
		Value,
	},
	syn::{
		parser::mac::{expected, to_do},
		token::{t, Span, TokenKind},
	},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_what_primary(&mut self) -> ParseResult<Value> {
		match self.peek_kind() {
			TokenKind::Duration => {
				let duration = self.parse_duration()?;
				Ok(Value::Duration(duration))
			}
			t!("$param") => {
				let param = self.parse_param()?;
				Ok(Value::Param(param))
			}
			t!("IF") => {
				let stmt = self.parse_if_stmt()?;
				Ok(Value::Subquery(Box::new(Subquery::Ifelse(stmt))))
			}
			t!("(") => {
				let token = self.pop_peek();
				self.parse_subquery(Some(token.span)).map(|x| Value::Subquery(Box::new(x)))
			}
			t!("<") => {
				self.pop_peek();
				expected!(self, "FUTURE");
				expected!(self, ">");
				let start = expected!(self, "{").span;
				let block = self.parse_block(start)?;
				Ok(Value::Future(Box::new(crate::sql::Future(block))))
			}
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE") => self.parse_subquery(None).map(|x| Value::Subquery(Box::new(x))),
			_ => self.parse_raw_ident().map(|x| Value::Table(Table(x))),
		}
	}

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
				let index = u32::from(token.data_index.unwrap());
				let strand = Strand(self.lexer.strings[index as usize].clone());
				return Ok(Value::Strand(strand));
			}
			TokenKind::Duration => {
				let index = u32::from(token.data_index.unwrap());
				let duration = Duration(self.lexer.durations[index as usize]);
				Value::Duration(duration)
			}
			TokenKind::Number => {
				let index = u32::from(token.data_index.unwrap());
				let number = self.lexer.numbers[index as usize].clone();
				Value::Number(number)
			}
			t!("$param") => {
				let index = u32::from(token.data_index.unwrap());
				let param = Param(Ident(self.lexer.strings[index as usize].clone()));
				Value::Param(param)
			}
			t!("FUNCTION") => {
				to_do!(self)
			}
			t!("->") => {
				let graph = self.parse_graph(Dir::In)?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("<->") => {
				let graph = self.parse_graph(Dir::Both)?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("<-") => {
				let graph = self.parse_graph(Dir::Out)?;
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
				self.parse_subquery(Some(token.span)).map(|x| Value::Subquery(Box::new(x)))?
			}
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE") => self.parse_subquery(None).map(|x| Value::Subquery(Box::new(x)))?,
			_ => {
				let identifier = self.token_as_raw_ident(token)?;
				Value::Table(Table(identifier))
			}
		};

		if Self::continues_idiom(self.peek_kind()) {
			match value {
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Future(_)
				| Value::Strand(_) => unreachable!(),
				Value::Idiom(Idiom(x)) => self.parse_remaining_idiom(x).map(Value::Idiom),
				Value::Table(Table(x)) => {
					self.parse_remaining_idiom(vec![Part::Field(Ident(x))]).map(Value::Idiom)
				}
				x => self.parse_remaining_idiom(vec![Part::Value(x)]).map(Value::Idiom),
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
		let name = self.parse_raw_ident()?;
		expected!(self, ":");
		let from = self.parse_u64()?;
		let to = self.eat(t!("..")).then(|| self.parse_u64()).transpose()?;
		self.expect_closing_delimiter(t!("|"), start)?;
		if let Some(to) = to {
			Ok(Mock::Range(name, from, to))
		} else {
			Ok(Mock::Count(name, from))
		}
	}

	pub fn parse_thing(&mut self) -> ParseResult<Thing> {
		let ident = self.parse_raw_ident()?;
		expected!(self, ":");
		let id = match self.peek_kind() {
			t!("{") => {
				let start = self.pop_peek().span;
				let object = self.parse_object(start)?;
				Id::Object(object)
			}
			t!("[") => {
				let start = self.pop_peek().span;
				let array = self.parse_array(start)?;
				Id::Array(array)
			}
			// TODO: negative numbers.
			TokenKind::Number => {
				let number = self.parse_u64()?;
				Id::Number(number as i64)
			}
			_ => {
				let ident = self.parse_raw_ident()?;
				Id::String(ident)
			}
		};
		Ok(Thing {
			tb: ident,
			id,
		})
	}

	pub fn parse_subquery(&mut self, start: Option<Span>) -> ParseResult<Subquery> {
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
