use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::error::Error::ParserError;
use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::take_while;
use nom::bytes::complete::take_while_m_n;
use nom::character::complete::char;
use nom::character::is_alphanumeric;
use nom::multi::many1;
use nom::Err::Error;
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

pub fn commasorspace(i: &str) -> IResult<&str, ()> {
	alt((commas, shouldbespace))(i)
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

#[inline]
pub fn val_u8(chr: u8) -> bool {
	is_alphanumeric(chr) || chr == b'_'
}

pub fn take_u64(i: &str) -> IResult<&str, u64> {
	let (i, v) = take_while(is_digit)(i)?;
	match v.parse::<u64>() {
		Ok(v) => Ok((i, v)),
		_ => Err(Error(ParserError(i))),
	}
}

pub fn take_usize(i: &str) -> IResult<&str, usize> {
	let (i, v) = take_while(is_digit)(i)?;
	match v.parse::<usize>() {
		Ok(v) => Ok((i, v)),
		_ => Err(Error(ParserError(i))),
	}
}

pub fn take_u32_len(i: &str) -> IResult<&str, (u32, usize)> {
	let (i, v) = take_while(is_digit)(i)?;
	match v.parse::<u32>() {
		Ok(n) => Ok((i, (n, v.len()))),
		_ => Err(Error(ParserError(i))),
	}
}

pub fn take_digits(i: &str, n: usize) -> IResult<&str, u32> {
	let (i, v) = take_while_m_n(n, n, is_digit)(i)?;
	match v.parse::<u32>() {
		Ok(v) => Ok((i, v)),
		_ => Err(Error(ParserError(i))),
	}
}

pub fn take_digits_range(i: &str, n: usize, range: impl RangeBounds<u32>) -> IResult<&str, u32> {
	let (i, v) = take_while_m_n(n, n, is_digit)(i)?;
	match v.parse::<u32>() {
		Ok(v) if range.contains(&v) => Ok((i, v)),
		_ => Err(Error(ParserError(i))),
	}
}
