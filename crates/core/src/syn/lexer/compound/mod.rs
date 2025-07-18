use crate::syn::error::SyntaxError;
use crate::syn::lexer::Lexer;
use crate::syn::token::{Span, Token};

mod bytes;
mod datetime;
mod file;
mod ident;
mod js;
mod number;
mod regex;
mod strand;
mod uuid;

pub use bytes::bytes;
pub use datetime::{datetime, datetime_inner};
pub use file::file;
pub use ident::flexible_ident;
pub use js::javascript;
pub use number::{
	NumberKind, Numeric, NumericKind, duration, float, integer, number, numeric, numeric_kind,
};
pub use regex::regex;
pub use strand::strand;
pub use uuid::uuid;

#[derive(Debug)]
pub struct CompoundToken<T> {
	pub value: T,
	pub span: Span,
}

impl Lexer<'_> {
	/// Lex a more complex token from the start token.
	/// The start token should already be consumed.
	pub fn lex_compound<F, R>(
		&mut self,
		start: Token,
		f: F,
	) -> Result<CompoundToken<R>, SyntaxError>
	where
		F: Fn(&mut Self, Token) -> Result<R, SyntaxError>,
	{
		assert_eq!(
			self.last_offset,
			start.span.offset + start.span.len,
			"The start token given to compound was not the last token consumed."
		);

		self.last_offset = start.span.offset;

		let res = f(self, start)?;

		Ok(CompoundToken {
			value: res,
			span: self.advance_span(),
		})
	}
}
