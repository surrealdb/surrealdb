use super::super::super::{
	comment::shouldbespace,
	ending,
	error::expected,
	literal::{ident, strand},
	IResult,
};
use crate::sql::{statements::DefineNamespaceStatement, Strand};
use nom::{branch::alt, bytes::complete::tag_no_case, combinator::cut, multi::many0};
#[cfg(feature = "sql2")]
use nom::{combinator::opt, sequence::tuple};

pub fn namespace(i: &str) -> IResult<&str, DefineNamespaceStatement> {
	let (i, _) = alt((tag_no_case("NS"), tag_no_case("NAMESPACE")))(i)?;
	#[cfg(feature = "sql2")]
	let (i, if_not_exists) = opt(tuple((
		shouldbespace,
		tag_no_case("IF"),
		cut(tuple((shouldbespace, tag_no_case("NOT"), shouldbespace, tag_no_case("EXISTS")))),
	)))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(namespace_opts)(i)?;
	let (i, _) = expected("COMMENT", ending::query)(i)?;
	// Create the base statement
	let mut res = DefineNamespaceStatement {
		name,
		#[cfg(feature = "sql2")]
		if_not_exists: if_not_exists.is_some(),
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineNamespaceOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineNamespaceOption {
	Comment(Strand),
}

fn namespace_opts(i: &str) -> IResult<&str, DefineNamespaceOption> {
	alt((namespace_comment,))(i)
}

fn namespace_comment(i: &str) -> IResult<&str, DefineNamespaceOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineNamespaceOption::Comment(v)))
}
