use crate::syn::token::{t, TokenKind};

use super::Parser;

impl Parser<'_> {
	pub fn peek_value_keyword(&mut self) -> bool {
		matches!(
			self.peek_token().kind,
			t!(")") | t!("]") | t!("}") | t!(";") | t!(",") | TokenKind::Eof
		)
	}
}
