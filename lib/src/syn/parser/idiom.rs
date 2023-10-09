use crate::{
	sql::{Dir, Ident, Idiom, Part, Value},
	syn::{
		parser::mac::{expected, to_do, unexpected},
		token::{t, TokenKind},
	},
};

use super::{ParseResult, Parser};

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum IdiomKind {
	Plain,
	Basic,
	Local,
}

impl Parser<'_> {
	pub fn parse_idiom(&mut self, kind: IdiomKind) -> ParseResult<Value> {
		let start = self.parse_prime_value()?;
		let peek = self.peek_token();
		if !start.can_start_idiom() {
			return Ok(start);
		}
		if let Some(next_part) = self.parse_next_part(kind)? {
			let mut idiom = match start {
				Value::Idiom(mut x) => {
					x.0.push(next_part);
					x
				}
				Value::Table(x) => Idiom(vec![Part::Field(Ident(x.0)), next_part]),
				x => Idiom(vec![Part::Start(x), next_part]),
			};

			while let Some(x) = self.parse_next_part(kind)? {
				idiom.push(next_part);
			}

			return Ok(Value::Idiom(idiom));
		} else {
			return Ok(start);
		}
	}

	pub fn parse_next_part(&mut self, kind: IdiomKind) -> ParseResult<Option<Part>> {
		let peek = self.peek_token();
		let part = match peek.kind {
			t!(".") => {
				self.pop_peek();
				if self.eat(t!("*")) {
					Part::All
				} else {
					let ident = self.parse_ident()?;
					Part::Field(ident)
				}
			}
			t!("...") => {
				self.pop_peek();
				if kind == IdiomKind::Basic {
					unexpected!(self, t!("..."), "a basic idiom");
				}
				Part::Flatten
			}
			t!("[") => {
				let token = self.next_token();
				let part = match token.kind {
					t!("*") => {
						self.next_token();
						Part::All
					}
					t!("$") => {
						if kind == IdiomKind::Local {
							unexpected(self, token.kind, "a local idiom");
						}
						self.next_token();
						Part::Last
					}
					t!("WHERE") => {
						// recover from WHERE condition when WHERE is a idiom
						self.recover(
							token.span,
							|this| this.parse_value().map(Part::Where),
							|this| this.parse_bracketed_value().map(Part::Where),
						)?
					}
					t!("?") => self.parse_value().map(Part::Where)?,
					TokenKind::Number => Part::Index(self.parse_number()?),
					// TODO: Value.
					x => unexpected!(self, x, "*, $, WHERE, ?, or a simple value"),
				};
				self.expect_closing_delimiter(t!("]"), peek.span)?;
				part
			}
			t!("->") => {
				match kind {
					// TODO: Check direction
					IdiomKind::Plain => self.parse_graph(Dir::Out)?,
					IdiomKind::Basic => unexpected!(self, peek.kind, "a basic idiom"),
					IdiomKind::Local => unexpected!(self, peek.kind, "a local idiom"),
				}
			}
			t!("<->") => match kind {
				IdiomKind::Plain => self.parse_graph(Dir::Both)?,
				IdiomKind::Basic => unexpected!(self, peek.kind, "a basic idiom"),
				IdiomKind::Local => unexpected!(self, peek.kind, "a local idiom"),
			},
			t!("<-") => match kind {
				IdiomKind::Plain => self.parse_graph(Dir::In)?,
				IdiomKind::Basic => unexpected!(self, peek.kind, "a basic idiom"),
				IdiomKind::Local => unexpected!(self, peek.kind, "a local idiom"),
			},
			_ => return Ok(None),
		};
		Ok(Some(part))
	}

	pub fn parse_bracketed_value(&mut self) -> ParseResult<Value> {
		to_do!(self)
	}
}
