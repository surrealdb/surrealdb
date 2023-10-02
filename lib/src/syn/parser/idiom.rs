use crate::{
	sql::{Cond, Dir, Fields, Graph, Idiom, Part, Table, Tables},
	syn::{
		parser::mac::{expected, to_do, unexpected},
		token::{t, TokenKind},
	},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_plain_idiom(&mut self) -> ParseResult<Idiom> {
		let first = match self.next_token().kind {
			t!("<-") => {
				let res = self.parse_graph(Dir::In)?;
				Part::Graph(res)
			}
			t!("<->") => {
				let res = self.parse_graph(Dir::Both)?;
				Part::Graph(res)
			}
			t!("->") => {
				let res = self.parse_graph(Dir::Out)?;
				Part::Graph(res)
			}
			_ => {
				let res = self.parse_ident()?;
				Part::Field(res)
			}
		};

		let mut parts = vec![first];

		loop {
			let part = match self.peek_token().kind {
				t!("<-") => {
					self.next_token();
					let res = self.parse_graph(Dir::In)?;
					Part::Graph(res)
				}
				t!("<->") => {
					self.next_token();
					let res = self.parse_graph(Dir::Both)?;
					Part::Graph(res)
				}
				t!("->") => {
					self.next_token();
					let res = self.parse_graph(Dir::Out)?;
					Part::Graph(res)
				}
				t!("...") => {
					self.next_token();
					Part::Flatten
				}
				t!(".") => {
					if self.eat(t!("*")) {
						Part::All
					} else {
						let res = self.parse_ident()?;
						Part::Field(res)
					}
				}
				t!("[") => {
					let part = match self.next_token().kind {
						t!("*") => {
							self.next_token();
							Part::All
						}
						t!("$") => {
							self.next_token();
							Part::Last
						}
						TokenKind::Number => to_do!(self),
						t!("WHERE") | t!("?") => to_do!(self),
						// TODO: Value.
						x => unexpected!(self, x, "*, $, WHERE, ?, or a simple value"),
					};
					expected!(self, "]");
					part
				}
				_ => break,
			};
			parts.push(part);
		}

		Ok(Idiom::from(parts))
	}

	fn parse_graph(&mut self, dir: Dir) -> ParseResult<Graph> {
		let res = match self.next_token().kind {
			t!("?") => Graph {
				dir,
				what: Tables::default(),
				..Default::default()
			},
			t!("(") => {
				let what = if self.eat(t!("?")) {
					Tables::default()
				} else {
					let mut head = vec![Table(self.parse_raw_ident()?)];
					while self.eat(t!(",")) {
						head.push(Table(self.parse_raw_ident()?));
					}
					Tables(head)
				};
				let cond =
					self.eat(t!("WHERE")).then(|| self.parse_value().map(Cond)).transpose()?;
				let alias = self.eat(t!("AS")).then(|| self.parse_plain_idiom()).transpose()?;
				Graph {
					dir,
					expr: Fields::all(),
					what,
					cond,
					alias,
					..Default::default()
				}
			}
			_ => {
				let text = self.parse_raw_ident()?;
				Graph {
					dir,
					expr: Fields::all(),
					what: Tables::from(Table(text)),
					..Default::default()
				}
			}
		};
		Ok(res)
	}
}
