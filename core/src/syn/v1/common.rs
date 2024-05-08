use super::{
	comment::{mightbespace, shouldbespace},
	error::ParseError,
	IResult,
};
use nom::{
	branch::alt,
	bytes::complete::{take_while, take_while_m_n},
	character::complete::char,
	combinator::map_res,
	multi::many1,
	Err, InputLength, Parser,
};
use std::ops::RangeBounds;

pub fn colons(i: &str) -> IResult<&str, ()> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = many1(char(';'))(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, ()))
}

pub fn commas(i: &str) -> IResult<&str, ()> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(',')(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, ()))
}

pub fn verbar(i: &str) -> IResult<&str, ()> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('|')(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, ()))
}

pub fn commasorspace(i: &str) -> IResult<&str, ()> {
	alt((commas, shouldbespace))(i)
}

pub fn openparentheses(s: &str) -> IResult<&str, &str> {
	let (i, _) = char('(')(s)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, s))
}

pub fn closeparentheses(i: &str) -> IResult<&str, &str> {
	let (s, _) = mightbespace(i)?;
	let (i, _) = char(')')(s)?;
	Ok((i, s))
}

pub fn openbraces(s: &str) -> IResult<&str, &str> {
	let (i, _) = char('{')(s)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, s))
}

pub fn closebraces(i: &str) -> IResult<&str, &str> {
	let (s, _) = mightbespace(i)?;
	let (i, _) = char('}')(s)?;
	Ok((i, s))
}

pub fn openbracket(s: &str) -> IResult<&str, &str> {
	let (i, _) = char('[')(s)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, s))
}

pub fn closebracket(i: &str) -> IResult<&str, &str> {
	let (s, _) = mightbespace(i)?;
	let (i, _) = char(']')(s)?;
	Ok((i, s))
}

pub fn openchevron(s: &str) -> IResult<&str, &str> {
	let (i, _) = char('<')(s)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, s))
}

pub fn closechevron(i: &str) -> IResult<&str, &str> {
	let (s, _) = mightbespace(i)?;
	let (i, _) = char('>')(s)?;
	Ok((i, s))
}

#[inline]
pub fn is_hex(chr: char) -> bool {
	chr.is_ascii_hexdigit()
}

#[inline]
pub fn is_digit(chr: char) -> bool {
	chr.is_ascii_digit()
}

#[inline]
pub fn val_char(chr: char) -> bool {
	chr.is_ascii_alphanumeric() || chr == '_'
}

pub fn take_u64(i: &str) -> IResult<&str, u64> {
	map_res(take_while(is_digit), |s: &str| s.parse::<u64>())(i)
}

pub fn take_digits(i: &str, n: usize) -> IResult<&str, u32> {
	map_res(take_while_m_n(n, n, is_digit), |s: &str| s.parse::<u32>())(i)
}

pub fn take_digits_range(i: &str, n: usize, range: impl RangeBounds<u32>) -> IResult<&str, u32> {
	let (i, v) = take_while_m_n(n, n, is_digit)(i)?;
	match v.parse::<u32>() {
		Ok(v) => {
			if range.contains(&v) {
				Ok((i, v))
			} else {
				Result::Err(Err::Error(ParseError::RangeError {
					tried: i,
					lower: range.start_bound().cloned(),
					upper: range.end_bound().cloned(),
				}))
			}
		}
		Err(error) => Result::Err(Err::Error(ParseError::ParseInt {
			tried: v,
			error,
		})),
	}
}

/// Parses a parser delimited by two other parsers.
///
/// This parser fails (not errors) if the second delimiting parser returns an error.
pub fn expect_delimited<I, D, V, T, O, O1>(
	mut prefix: D,
	mut value: V,
	mut terminator: T,
) -> impl FnMut(I) -> IResult<I, O, ParseError<I>>
where
	I: Clone + InputLength,
	V: Parser<I, O, ParseError<I>>,
	D: Parser<I, I, ParseError<I>>,
	T: Parser<I, O1, ParseError<I>>,
{
	move |i| {
		let (i, s) = prefix.parse(i)?;
		let (i, res) = value.parse(i)?;
		match terminator.parse(i) {
			Ok((i, _)) => Result::Ok((i, res)),
			Result::Err(Err::Failure(e)) | Result::Err(Err::Error(e)) => {
				Result::Err(Err::Failure(ParseError::MissingDelimiter {
					opened: s,
					tried: e.tried(),
				}))
			}
			Result::Err(Err::Incomplete(e)) => Result::Err(Err::Incomplete(e)),
		}
	}
}

pub fn expect_terminator<P, I, O>(
	open_span: I,
	mut terminator: P,
) -> impl FnMut(I) -> IResult<I, O, ParseError<I>>
where
	I: Clone,
	P: Parser<I, O, ParseError<I>>,
{
	move |i| match terminator.parse(i) {
		Ok((i, x)) => Ok((i, x)),
		Result::Err(Err::Failure(e)) | Result::Err(Err::Error(e)) => {
			Result::Err(Err::Failure(ParseError::MissingDelimiter {
				opened: open_span.clone(),
				tried: e.tried(),
			}))
		}
		Result::Err(Err::Incomplete(e)) => Result::Err(Err::Incomplete(e)),
	}
}

/// Parses a delimited list with an option trailing separator in the form of:
///
///```text
/// PREFIX $(PARSER)SEPARATOR* $(SEPARATOR)? TERMINATOR
///```
///
/// Which parsers productions like
/// (a,b,c,) or [a,b]
///
/// First parses the prefix and returns it's error if there is one.
/// The tries to parse the terminator. If there is one the parser completes else it tries to parse
/// the value, else it returns the parsed values.
/// Then it tries to parse the separator, if there is one it start again trying to parse the
/// terminator followed by a value if there is no terminator. Else it tries to parse the terminator
/// and if there is none it returns a failure. Otherwise completes with an vec of the parsed
/// values.
///
pub fn delimited_list0<I, D, S, V, T, O, O1, O2>(
	mut prefix: D,
	mut separator: S,
	mut value: V,
	mut terminator: T,
) -> impl FnMut(I) -> IResult<I, Vec<O>, ParseError<I>>
where
	I: Clone + InputLength,
	V: Parser<I, O, ParseError<I>>,
	D: Parser<I, I, ParseError<I>>,
	S: Parser<I, O1, ParseError<I>>,
	T: Parser<I, O2, ParseError<I>>,
{
	move |i| {
		let (i, s) = prefix.parse(i)?;
		let mut res = Vec::new();
		let mut input = i;
		loop {
			match terminator.parse(input.clone()) {
				Err(Err::Error(_)) => {}
				Err(e) => return Err(e),
				Ok((i, _)) => {
					input = i;
					break;
				}
			}
			let (i, value) = value.parse(input)?;
			res.push(value);
			match separator.parse(i.clone()) {
				Ok((i, _)) => {
					input = i;
				}
				Err(Err::Error(_)) => match terminator.parse(i.clone()) {
					Ok((i, _)) => {
						input = i;
						break;
					}
					Result::Err(Err::Error(_)) => {
						return Err(Err::Failure(ParseError::MissingDelimiter {
							opened: s,
							tried: i,
						}))
					}
					Result::Err(e) => return Err(e),
				},
				Err(e) => return Err(e),
			}
		}
		Ok((input, res))
	}
}

/// Parses a delimited list with an option trailing separator in the form of:
///
///```text
/// PREFIX $(PARSER)SEPARATOR+ $(SEPARATOR)? TERMINATOR
///```
///
/// Which parsers productions like
/// (a,b,c,) or [a,b] but not empty lists
///
/// First parses the prefix and returns it's error if there is one.
/// The tries to parse the terminator. If there is one the parser completes else it tries to parse
/// the value, else it returns the parsed values.
/// Then it tries to parse the separator, if there is one it start again trying to parse the
/// terminator followed by a value if there is no terminator. Else it tries to parse the terminator
/// and if there is none it returns a failure. Otherwise completes with an vec of the parsed
/// values.
///
pub fn delimited_list1<I, D, S, V, T, O, O1, O2>(
	mut prefix: D,
	mut separator: S,
	mut value: V,
	mut terminator: T,
) -> impl FnMut(I) -> IResult<I, Vec<O>, ParseError<I>>
where
	I: Clone + InputLength,
	V: Parser<I, O, ParseError<I>>,
	D: Parser<I, I, ParseError<I>>,
	S: Parser<I, O1, ParseError<I>>,
	T: Parser<I, O2, ParseError<I>>,
{
	move |i| {
		let (i, s) = prefix.parse(i)?;
		let mut input = i;
		let (i, v) = value.parse(input)?;
		let mut res = vec![v];

		match separator.parse(i.clone()) {
			Ok((i, _)) => {
				input = i;
			}
			Err(Err::Error(_)) => match terminator.parse(i.clone()) {
				Ok((i, _)) => return Ok((i, res)),
				Result::Err(Err::Error(_)) => {
					return Err(Err::Failure(ParseError::MissingDelimiter {
						opened: s,
						tried: i,
					}))
				}
				Result::Err(e) => return Err(e),
			},
			Err(e) => return Err(e),
		}

		loop {
			match terminator.parse(input.clone()) {
				Err(Err::Error(_)) => {}
				Err(e) => return Err(e),
				Ok((i, _)) => {
					input = i;
					break;
				}
			}
			let (i, v) = value.parse(input)?;
			res.push(v);
			match separator.parse(i.clone()) {
				Ok((i, _)) => {
					input = i;
				}
				Err(Err::Error(_)) => match terminator.parse(i.clone()) {
					Ok((i, _)) => {
						input = i;
						break;
					}
					Result::Err(Err::Error(_)) => {
						return Err(Err::Failure(ParseError::MissingDelimiter {
							opened: s,
							tried: i,
						}))
					}
					Result::Err(e) => return Err(e),
				},
				Err(e) => return Err(e),
			}
		}
		Ok((input, res))
	}
}
