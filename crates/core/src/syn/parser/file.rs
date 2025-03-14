use crate::sql::{File, Ident};

use super::{ParseResult, Parser};
use crate::syn::token::t;

// TODO(kearfy): This is so cursed but just want to get the rest working first
// TODO(kearfy): Also, make it file://... instead of file:/...

impl Parser<'_> {
	/// Expects `file:/` to be parsed already
	pub(crate) async fn parse_file(&mut self) -> ParseResult<File> {
		// expected!(self, t!("/"));
		let bucket: Ident = self.next_token_value()?;
		let mut key = String::new();
		loop {
			match self.peek_kind() {
				t!("/") => key.push('/'),
				t!(".") => key.push('.'),
				_ => {
					break;
				}
			}

			self.pop_peek();
			let segment: Ident = self.next_token_value()?;
			key += &segment.to_string();
		}

		Ok(File {
			bucket,
			key,
		})
	}
}
