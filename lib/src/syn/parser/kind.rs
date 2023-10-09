use crate::{sql::Kind, syn::parser::mac::to_do};

use super::{ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_kind(&mut self) -> ParseResult<Kind> {
		to_do!(self)
	}
}
