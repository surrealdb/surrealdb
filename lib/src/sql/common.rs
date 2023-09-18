use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::error::{IResult, ParseError};
use nom::branch::alt;
use nom::bytes::complete::take_while;
use nom::bytes::complete::take_while_m_n;
use nom::character::complete::char;
use nom::character::is_alphanumeric;
use nom::combinator::map_res;
use nom::multi::many1;
use nom::Err;
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
pub fn val_u8(chr: u8) -> bool {
	is_alphanumeric(chr) || chr == b'_'
}

#[inline]
pub fn val_char(chr: char) -> bool {
	chr.is_ascii_alphanumeric() || chr == '_'
}

pub fn take_u64(i: &str) -> IResult<&str, u64> {
	map_res(take_while(is_digit), |s: &str| s.parse::<u64>())(i)
}

pub fn take_u32_len(i: &str) -> IResult<&str, (u32, usize)> {
	map_res(take_while(is_digit), |s: &str| s.parse::<u32>().map(|x| (x, s.len())))(i)
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
