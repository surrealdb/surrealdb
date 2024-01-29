use super::{IResult, ParseError};
use nom::bytes::complete::tag_no_case;
use nom::Err;
use nom::Parser;

pub fn expected<I, O, P>(expect: &'static str, mut parser: P) -> impl FnMut(I) -> IResult<I, O>
where
	P: Parser<I, O, ParseError<I>>,
{
	move |input: I| match parser.parse(input) {
		Err(Err::Error(err)) => match err {
			ParseError::Base(tried) => Err(Err::Error(ParseError::Expected {
				tried,
				expected: expect,
			})),
			ParseError::Explained {
				tried,
				explained,
			} => Err(Err::Error(ParseError::ExplainedExpected {
				tried,
				expected: expect,
				explained,
			})),
			ParseError::Expected {
				tried,
				..
			} => Err(Err::Error(ParseError::Expected {
				tried,
				expected: expect,
			})),
			x => Err(Err::Error(x)),
		},
		Err(Err::Failure(err)) => match err {
			ParseError::Base(tried) => Err(Err::Failure(ParseError::Expected {
				tried,
				expected: expect,
			})),
			ParseError::Explained {
				tried,
				explained,
			} => Err(Err::Failure(ParseError::ExplainedExpected {
				tried,
				expected: expect,
				explained,
			})),
			ParseError::Expected {
				tried: input,
				..
			} => Err(Err::Failure(ParseError::Expected {
				tried: input,
				expected: expect,
			})),
			x => Err(Err::Failure(x)),
		},
		rest => rest,
	}
}

pub trait ExplainResultExt<I, O> {
	/// A function which adds a explanation to an error if the parser fails at a place which can
	/// be parsed with the given parser.
	fn explain<P, O1>(self, explain: &'static str, condition: P) -> Self
	where
		P: Parser<I, O1, ParseError<I>>;
}

impl<I: Clone, O> ExplainResultExt<I, O> for IResult<I, O> {
	fn explain<P, O1>(self, explain: &'static str, mut condition: P) -> Self
	where
		P: Parser<I, O1, ParseError<I>>,
	{
		let error = match self {
			Ok(x) => return Ok(x),
			Err(e) => e,
		};

		let mut was_failure = false;
		let error = match error {
			Err::Error(e) => e,
			Err::Failure(e) => {
				was_failure = true;
				e
			}
			Err::Incomplete(e) => return Err(Err::Incomplete(e)),
		};

		let new_error = match error {
			ParseError::Base(tried) => {
				if condition.parse(tried.clone()).is_ok() {
					ParseError::Explained {
						tried,
						explained: explain,
					}
				} else {
					ParseError::Base(tried)
				}
			}
			ParseError::Expected {
				tried,
				expected,
			} => {
				if condition.parse(tried.clone()).is_ok() {
					ParseError::ExplainedExpected {
						tried,
						expected,
						explained: explain,
					}
				} else {
					ParseError::Expected {
						tried,
						expected,
					}
				}
			}
			e => e,
		};

		if was_failure {
			Err(Err::Failure(new_error))
		} else {
			Err(Err::Error(new_error))
		}
	}
}

pub fn expect_tag_no_case(tag: &'static str) -> impl FnMut(&str) -> IResult<&str, &str> {
	move |input: &str| match tag_no_case(tag).parse(input) {
		Result::Err(_) => Err(Err::Failure(ParseError::Expected {
			tried: input,
			expected: tag,
		})),
		rest => rest,
	}
}
