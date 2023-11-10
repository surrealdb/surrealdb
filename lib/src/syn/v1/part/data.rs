use super::super::{
	comment::{mightbespace, shouldbespace},
	common::{commas, commasorspace},
	error::{expect_tag_no_case, expected},
	idiom::plain,
	literal::{duration, ident, scoring, tables},
	operator::{assigner, dir},
	thing::thing,
	// TODO: go through and check every import for alias.
	value::value,
	IResult,
};
use crate::sql::{Base, ChangeFeed, Cond, Data, Edges};
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, map, map_res, opt, recognize, value as map_value},
	multi::separated_list1,
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};

pub fn data(i: &str) -> IResult<&str, Data> {
	alt((set, unset, patch, merge, replace, content))(i)
}

fn set(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("SET")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(separated_list1(
		commas,
		cut(|i| {
			let (i, l) = plain(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, o) = assigner(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, r) = value(i)?;
			Ok((i, (l, o, r)))
		}),
	))(i)?;
	Ok((i, Data::SetExpression(v)))
}

fn unset(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("UNSET")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(separated_list1(commas, plain))(i)?;
	Ok((i, Data::UnsetExpression(v)))
}

fn patch(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("PATCH")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, Data::PatchExpression(v)))
}

fn merge(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("MERGE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, Data::MergeExpression(v)))
}

fn replace(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("REPLACE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, Data::ReplaceExpression(v)))
}

fn content(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("CONTENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, Data::ContentExpression(v)))
}

pub fn single(i: &str) -> IResult<&str, Data> {
	let (i, v) = value(i)?;
	Ok((i, Data::SingleExpression(v)))
}

pub fn values(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("(")(i)?;
	// TODO: look at call tree here.
	let (i, fields) = separated_list1(commas, plain)(i)?;
	let (i, _) = tag_no_case(")")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUES")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, values) = separated_list1(commas, |i| {
		let (i, _) = tag_no_case("(")(i)?;
		let (i, v) = separated_list1(commas, value)(i)?;
		let (i, _) = tag_no_case(")")(i)?;
		Ok((i, v))
	})(i)?;
	Ok((
		i,
		Data::ValuesExpression(
			values.into_iter().map(|row| fields.iter().cloned().zip(row).collect()).collect(),
		),
	))
}

pub fn update(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("ON DUPLICATE KEY UPDATE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_list1(commas, |i| {
		let (i, l) = plain(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, o) = assigner(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, r) = value(i)?;
		Ok((i, (l, o, r)))
	})(i)?;
	Ok((i, Data::UpdateExpression(v)))
}
