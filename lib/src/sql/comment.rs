use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::take_until;
use nom::character::complete::char;
use nom::character::complete::multispace0;
use nom::character::complete::multispace1;
use nom::character::complete::not_line_ending;
use nom::multi::many1;

pub fn mightbespace(i: &str) -> IResult<&str, ()> {
	let (i, _) = alt((comment, blank))(i)?;
	Ok((i, ()))
}

pub fn shouldbespace(i: &str) -> IResult<&str, ()> {
	let (i, _) = alt((comment, space))(i)?;
	Ok((i, ()))
}

pub fn comment(i: &str) -> IResult<&str, ()> {
	let (i, _) = multispace0(i)?;
	let (i, _) = many1(alt((block, slash, dash, hash)))(i)?;
	let (i, _) = multispace0(i)?;
	Ok((i, ()))
}

pub fn block(i: &str) -> IResult<&str, ()> {
	let (i, _) = multispace0(i)?;
	let (i, _) = char('/')(i)?;
	let (i, _) = char('*')(i)?;
	let (i, _) = take_until("*/")(i)?;
	let (i, _) = char('*')(i)?;
	let (i, _) = char('/')(i)?;
	let (i, _) = multispace0(i)?;
	Ok((i, ()))
}

pub fn slash(i: &str) -> IResult<&str, ()> {
	let (i, _) = multispace0(i)?;
	let (i, _) = char('/')(i)?;
	let (i, _) = char('/')(i)?;
	let (i, _) = not_line_ending(i)?;
	Ok((i, ()))
}

pub fn dash(i: &str) -> IResult<&str, ()> {
	let (i, _) = multispace0(i)?;
	let (i, _) = char('-')(i)?;
	let (i, _) = char('-')(i)?;
	let (i, _) = not_line_ending(i)?;
	Ok((i, ()))
}

pub fn hash(i: &str) -> IResult<&str, ()> {
	let (i, _) = multispace0(i)?;
	let (i, _) = char('#')(i)?;
	let (i, _) = not_line_ending(i)?;
	Ok((i, ()))
}

fn blank(i: &str) -> IResult<&str, ()> {
	let (i, _) = multispace0(i)?;
	Ok((i, ()))
}

fn space(i: &str) -> IResult<&str, ()> {
	let (i, _) = multispace1(i)?;
	Ok((i, ()))
}
