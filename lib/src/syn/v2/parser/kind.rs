use crate::{
	sql::Kind,
	syn::v2::{
		parser::mac::expected,
		token::{t, Keyword, Span, TokenKind},
	},
};

use super::{mac::unexpected, ParseResult, Parser};

impl Parser<'_> {
	/// Parse a kind production.
	///
	/// # Parser State
	/// expects the first `<` to already be eaten
	pub fn parse_kind(&mut self, delim: Span) -> ParseResult<Kind> {
		let kind = self.parse_inner_kind()?;
		self.expect_closing_delimiter(t!(">"), delim)?;
		Ok(kind)
	}

	pub fn parse_inner_kind(&mut self) -> ParseResult<Kind> {
		match self.peek_kind() {
			t!("ANY") => {
				self.pop_peek();
				Ok(Kind::Any)
			}
			t!("OPTION") => {
				self.pop_peek();
				let delim = expected!(self, "<").span;
				let mut first = self.parse_concrete_kind()?;
				if self.peek_kind() == t!("|") {
					let mut kind = vec![first];
					while self.eat(t!("|")) {
						kind.push(self.parse_concrete_kind()?);
					}
					first = Kind::Either(kind);
				}
				self.expect_closing_delimiter(t!(">"), delim)?;
				Ok(Kind::Option(Box::new(first)))
			}
			_ => {
				let first = self.parse_concrete_kind()?;
				if self.peek_kind() == t!("|") {
					let mut kind = vec![first];
					while self.eat(t!("|")) {
						kind.push(self.parse_concrete_kind()?);
					}
					Ok(Kind::Either(kind))
				} else {
					Ok(first)
				}
			}
		}
	}

	pub fn parse_concrete_kind(&mut self) -> ParseResult<Kind> {
		match self.next().kind {
			t!("BOOL") => Ok(Kind::Bool),
			t!("NULL") => Ok(Kind::Null),
			t!("BYTES") => Ok(Kind::Bytes),
			t!("DATETIME") => Ok(Kind::Datetime),
			t!("DECIMAL") => Ok(Kind::Decimal),
			t!("DURATION") => Ok(Kind::Duration),
			t!("FLOAT") => Ok(Kind::Float),
			t!("INT") => Ok(Kind::Int),
			t!("NUMBER") => Ok(Kind::Number),
			t!("OBJECT") => Ok(Kind::Object),
			t!("POINT") => Ok(Kind::Point),
			t!("STRING") => Ok(Kind::String),
			t!("UUID") => Ok(Kind::Uuid),
			t!("RECORD") => {
				let tables = match self.peek_kind() {
					t!("<") => {
						let next = self.next();
						let mut tables = vec![self.parse_token_value()?];
						while self.eat(t!("|")) {
							tables.push(self.parse_token_value()?);
						}
						self.expect_closing_delimiter(t!(">"), next.span)?;
						tables
					}
					t!("(") => {
						let next = self.next();
						let mut tables = vec![self.parse_token_value()?];
						while self.eat(t!(",")) {
							tables.push(self.parse_token_value()?);
						}
						self.expect_closing_delimiter(t!(")"), next.span)?;
						tables
					}
					_ => Vec::new(),
				};
				Ok(Kind::Record(tables))
			}
			t!("GEOMETRY") => {
				let kind = match self.peek_kind() {
					t!("<") => {
						let delim = self.pop_peek().span;
						let mut kind = vec![self.parse_geometry_kind()?];
						while self.eat(t!("|")) {
							kind.push(self.parse_geometry_kind()?);
						}
						self.expect_closing_delimiter(t!(">"), delim)?;
						kind
					}
					t!("(") => {
						let delim = self.pop_peek().span;
						let mut kind = vec![self.parse_geometry_kind()?];
						loop {
							if self.eat(t!(")")) {
								break;
							}

							kind.push(self.parse_geometry_kind()?);

							if !self.eat(t!(",")) {
								self.expect_closing_delimiter(t!(")"), delim)?;
								break;
							}
						}
						kind
					}
					_ => Vec::new(),
				};
				Ok(Kind::Geometry(kind))
			}
			t!("ARRAY") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let kind = self.parse_inner_kind()?;
					let size = self.eat(t!(",")).then(|| self.parse_token_value()).transpose()?;
					self.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::Array(Box::new(kind), size))
				} else {
					Ok(Kind::Array(Box::new(Kind::Any), None))
				}
			}
			t!("SET") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let kind = self.parse_inner_kind()?;
					let size = self.eat(t!(",")).then(|| self.parse_token_value()).transpose()?;
					self.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::Set(Box::new(kind), size))
				} else {
					Ok(Kind::Set(Box::new(Kind::Any), None))
				}
			}
			x => unexpected!(self, x, "a kind name"),
		}
	}

	pub fn parse_geometry_kind(&mut self) -> ParseResult<String> {
		match self.next().kind {
			TokenKind::Keyword(
				x @ (Keyword::Feature
				| Keyword::Point
				| Keyword::Line
				| Keyword::Polygon
				| Keyword::MultiPoint
				| Keyword::MultiLine
				| Keyword::MultiPolygon
				| Keyword::Collection),
			) => Ok(x.as_str().to_ascii_lowercase()),
			x => unexpected!(self, x, "a geometry kind name"),
		}
	}
}
