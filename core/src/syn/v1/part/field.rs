use super::super::{common::commas, ending::field as ending, idiom::plain, value::value, IResult};
use crate::{
	sql::{Field, Fields},
	syn::v1::comment::shouldbespace,
};
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	combinator::{cut, opt},
	multi::separated_list1,
	sequence::delimited,
};

pub fn fields(i: &str) -> IResult<&str, Fields> {
	alt((field_one, field_many))(i)
}

fn field_one(i: &str) -> IResult<&str, Fields> {
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, f) = alone(i)?;
		let (i, _) = ending(i)?;
		Ok((i, Fields(vec![f], true)))
	})(i)
}

pub fn field(i: &str) -> IResult<&str, Field> {
	alt((all, alone))(i)
}

fn field_many(i: &str) -> IResult<&str, Fields> {
	let (i, v) = separated_list1(commas, field)(i)?;
	Ok((i, Fields(v, false)))
}

pub fn all(i: &str) -> IResult<&str, Field> {
	let (i, _) = tag_no_case("*")(i)?;
	Ok((i, Field::All))
}

pub fn alone(i: &str) -> IResult<&str, Field> {
	let (i, expr) = value(i)?;
	let (i, alias) =
		if let (i, Some(_)) = opt(delimited(shouldbespace, tag_no_case("AS"), shouldbespace))(i)? {
			let (i, alias) = cut(plain)(i)?;
			(i, Some(alias))
		} else {
			(i, None)
		};
	Ok((
		i,
		Field::Single {
			expr,
			alias,
		},
	))
}
