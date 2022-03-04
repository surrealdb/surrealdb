use crate::sql::comment::mightbespace;
use crate::sql::error::Error::ParserError;
use crate::sql::error::IResult;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_while;
use nom::bytes::complete::take_while_m_n;
use nom::character::is_alphanumeric;
use nom::combinator::map;
use nom::multi::many1;
use nom::Err::Error;
use std::ops::RangeBounds;
use std::str;

pub fn colons(i: &str) -> IResult<&str, ()> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = many1(tag(";"))(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, ()))
}

pub fn commas(i: &str) -> IResult<&str, ()> {
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag(",")(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, ()))
}

#[inline]
pub fn is_digit(chr: char) -> bool {
	(0x30..=0x39).contains(&(chr as u8))
}

#[inline]
pub fn to_u32(s: &str) -> u32 {
	str::FromStr::from_str(s).unwrap()
}

#[inline]
pub fn to_u64(s: &str) -> u64 {
	str::FromStr::from_str(s).unwrap()
}

#[inline]
pub fn to_usize(s: &str) -> usize {
	str::FromStr::from_str(s).unwrap()
}

#[inline]
pub fn val_char(chr: char) -> bool {
	is_alphanumeric(chr as u8) || chr == '_'
}

#[inline]
pub fn escape(s: &str, f: &dyn Fn(char) -> bool, c: &str) -> String {
	for x in s.chars() {
		if !f(x) {
			return format!("{}{}{}", c, s, c);
		}
	}
	s.to_owned()
}

pub fn take_u32(i: &str) -> IResult<&str, u32> {
	let (i, v) = map(take_while(is_digit), to_u32)(i)?;
	Ok((i, v))
}

pub fn take_u64(i: &str) -> IResult<&str, u64> {
	let (i, v) = map(take_while(is_digit), to_u64)(i)?;
	Ok((i, v))
}

pub fn take_usize(i: &str) -> IResult<&str, usize> {
	let (i, v) = map(take_while(is_digit), to_usize)(i)?;
	Ok((i, v))
}

pub fn take_digits(i: &str, n: usize) -> IResult<&str, u32> {
	let (i, v) = map(take_while_m_n(n, n, is_digit), to_u32)(i)?;
	Ok((i, v))
}

pub fn take_digits_range(i: &str, n: usize, range: impl RangeBounds<u32>) -> IResult<&str, u32> {
	let (i, v) = map(take_while_m_n(n, n, is_digit), to_u32)(i)?;
	if range.contains(&v) {
		Ok((i, v))
	} else {
		Err(Error(ParserError(i)))
	}
}
