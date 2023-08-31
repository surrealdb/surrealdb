use std::fmt;

use nom::{error::ParseError, Err, Parser};

use super::error::IResult;

#[derive(Debug)]
pub enum Error<I> {
	/// An error because a keyword
	Keyword{
		input: I,
		kind: nom::error::ErrorKind,
	},
	Base {
		kind: nom::error::ErrorKind,
		input: I,
	},
}

pub fn cut_keyword<I, K, P, O, O1>(
	mut keyword: K,
	mut parser: P,
) -> impl FnMut(I) -> IResult<I, O, Error<I>>
where
	K: Parser<I, I, Error<I>>,
	P: Parser<I, O, Error<I>>,
	I: Clone,
{
	move |input: I| {
		let (input, keyword) = keyword.parse(input)?;

		match parser.parse(input) {
			Err(Err::Error(Error::Base {
				kind,
				input,
			})) => Err(Err::Failure(Error::KeywordError {
				keyword,
				input,
				kind,
			})),
			x => x,
		}
	}
}

pub fn recover_keyword<P, W, I, O>(
	mut parser: P,
	mut with: W,
) -> impl FnMut(I) -> IResult<I, O, Error<I>>
where
	I: Clone,
	P: Parser<I, O, Error<I>>,
	W: Parser<I, O, Error<I>>,
{
	move |input: I| match parser.parse(input.clone()) {
		Err(Err::Failure(Error::KeywordError {
			keyword,
			failure_input,
			kind,
		})) => {
			match parser.parse()
		}
		x => x,
	}
}

impl<I: fmt::Display> fmt::Display for Error<I> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		writeln!(f, "todo")
	}
}

impl<I> ParseError<I> for Error<I> {
	fn from_error_kind(input: I, kind: nom::error::ErrorKind) -> Self {
		Error::Base {
			input,
			kind,
		}
	}

	fn append(input: I, kind: nom::error::ErrorKind, other: Self) -> Self {
		other
	}
}
