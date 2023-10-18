use crate::{
	sql::{Array, Dir, Duration, Ident, Idiom, Param, Part, Strand, Value},
	syn::{
		parser::mac::to_do,
		token::{t, Span, TokenKind},
	},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_what_primary(&mut self) -> ParseResult<Value> {
		to_do!(self)
	}

	pub fn parse_primary(&mut self) -> ParseResult<Value> {
		let token = self.next();
		match token.kind {
			t!("NONE") => Ok(Value::None),
			t!("NULL") => Ok(Value::Null),
			t!("TRUE") => Ok(Value::Bool(true)),
			t!("FALSE") => Ok(Value::Bool(false)),
			t!("<") => {
				// future
				to_do!(self)
			}
			TokenKind::Strand => {
				let index = u32::from(token.data_index.unwrap());
				let strand = Strand(self.lexer.strings[index as usize]);
				Ok(Value::Strand(strand))
			}
			TokenKind::Duration => {
				let index = u32::from(token.data_index.unwrap());
				let duration = Duration(self.lexer.durations[index as usize]);
				Ok(Value::Duration(duration))
			}
			TokenKind::Number => {
				let index = u32::from(token.data_index.unwrap());
				let number = self.lexer.numbers[index as usize];
				Ok(Value::Number(number))
			}
			t!("$param") => {
				let index = u32::from(token.data_index.unwrap());
				let param = Param(Ident(self.lexer.strings[index as usize]));
				Ok(Value::Param(param))
			}
			t!("FUNCTION") => {
				to_do!(self)
			}
			t!("->") => {
				let graph = self.parse_graph(Dir::In)?;
				Ok(Value::Idiom(Idiom(vec![Part::Graph(graph)])))
			}
			t!("<->") => {
				let graph = self.parse_graph(Dir::Both)?;
				Ok(Value::Idiom(Idiom(vec![Part::Graph(graph)])))
			}
			t!("<-") => {
				let graph = self.parse_graph(Dir::Out)?;
				Ok(Value::Idiom(Idiom(vec![Part::Graph(graph)])))
			}
			t!("[") => self.parse_array(token.span).map(Value::Array),
			_ => to_do!(self),
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
}
