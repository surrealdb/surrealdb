use nom::error::ErrorKind;
use nom::error::ParseError;
use nom::Err;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error<I> {
	ParserError(I),
}

pub type IResult<I, O, E = Error<I>> = Result<(I, O), Err<E>>;

impl<I> ParseError<I> for Error<I> {
	fn from_error_kind(input: I, _: ErrorKind) -> Self {
		Self::ParserError(input)
	}
	fn append(_: I, _: ErrorKind, other: Self) -> Self {
		other
	}
}
