#[cfg(all(not(feature = "jwks"), feature = "sql2"))]
use super::super::super::error::ParseError::Expected;
use super::super::super::{
	comment::shouldbespace,
	ending,
	error::{expect_tag_no_case, expected},
	literal::{algorithm, ident, strand, strand::strand_raw},
	part::base_or_scope,
	IResult,
};
use crate::{
	sql::{statements::DefineTokenStatement, Algorithm, Strand},
	syn::v1::ParseError,
};
use nom::Err;
use nom::{branch::alt, bytes::complete::tag_no_case, combinator::cut, multi::many0};

pub fn token(i: &str) -> IResult<&str, DefineTokenStatement> {
	let (i, _) = tag_no_case("TOKEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, base, opts)) = cut(|i| {
		let (i, name) = ident(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, base) = base_or_scope(i)?;
		let (i, opts) = many0(token_opts)(i)?;
		let (i, _) = expected("TYPE, VALUE, or COMMENT", ending::query)(i)?;
		Ok((i, (name, base, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineTokenStatement {
		name,
		base,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineTokenOption::Type(v) => {
				#[cfg(all(not(feature = "jwks"), feature = "sql2"))]
				if matches!(v, Algorithm::Jwks) {
					return Err(Err::Error(Expected {
						tried: i,
						expected: "the 'jwks' feature to be enabled",
					}));
				}
				res.kind = v;
			}
			DefineTokenOption::Value(v) => {
				res.code = v;
			}
			DefineTokenOption::Comment(v) => {
				res.comment = Some(v);
			}
			#[cfg(feature = "sql2")]
			DefineTokenOption::IfNotExists(v) => {
				res.if_not_exists = v;
			}
		}
	}
	// Check necessary options
	if res.code.is_empty() {
		return Err(Err::Failure(ParseError::ExplainedExpected {
			tried: i,
			expected: "a VALUE clause",
			explained: "A token requires a VALUE clause to be defined.",
		}));
	}
	// Return the statement
	Ok((i, res))
}

enum DefineTokenOption {
	Type(Algorithm),
	Value(String),
	Comment(Strand),
	#[cfg(feature = "sql2")]
	IfNotExists(bool),
}

fn token_opts(i: &str) -> IResult<&str, DefineTokenOption> {
	alt((
		token_type,
		token_value,
		token_comment,
		#[cfg(feature = "sql2")]
		token_if_not_exists
	))(i)
}

fn token_type(i: &str) -> IResult<&str, DefineTokenOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TYPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(algorithm)(i)?;
	Ok((i, DefineTokenOption::Type(v)))
}

fn token_value(i: &str) -> IResult<&str, DefineTokenOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand_raw)(i)?;
	Ok((i, DefineTokenOption::Value(v)))
}

fn token_comment(i: &str) -> IResult<&str, DefineTokenOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineTokenOption::Comment(v)))
}

#[cfg(feature = "sql2")]
fn token_if_not_exists(i: &str) -> IResult<&str, DefineTokenOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("IF")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("NOT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("EXISTS")(i)?;
	Ok((i, DefineTokenOption::IfNotExists(true)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn define_token_without_value_clause() {
		let sql = "TOKEN test ON test";
		let res = token(sql);

		assert_eq!(res.is_err(), true)
	}
}
