use super::super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	ending,
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{self, basic, plain},
	literal::{
		datetime, duration, filters, ident, param, scoring, strand, strand::strand_raw, table,
		tables, timeout, tokenizer,
	},
	operator::{assigner, dir},
	part::{
		base, cond, data,
		data::{single, update},
		output,
		permission::permissions,
	},
	thing::thing,
	value::{value, values, whats},
	IResult, ParseError,
};
use crate::{
	iam::Role,
	sql::{
		statements::DefineUserStatement, Ident, Idioms, Index, Kind, Permissions, Strand, Value,
	},
};
use argon2::{password_hash::SaltString, Argon2, PasswordHasher};
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, into, map, map_res, opt, recognize, value as map_value},
	multi::{many0, separated_list1},
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};
use rand::{distributions::Alphanumeric, rngs::OsRng, Rng};

pub fn user(i: &str) -> IResult<&str, DefineUserStatement> {
	let (i, _) = tag_no_case("USER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, base, opts)) = cut(|i| {
		let (i, name) = ident(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, base) = base(i)?;
		let (i, opts) = user_opts(i)?;
		let (i, _) = expected("PASSWORD, PASSHASH, ROLES, or COMMENT", ending::query)(i)?;
		Ok((i, (name, base, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineUserStatement {
		name,
		base,
		roles: vec!["Viewer".into()], // New users get the viewer role by default
		code: rand::thread_rng()
			.sample_iter(&Alphanumeric)
			.take(128)
			.map(char::from)
			.collect::<String>(),
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineUserOption::Password(v) => {
				res.hash = Argon2::default()
					.hash_password(v.as_ref(), &SaltString::generate(&mut OsRng))
					.unwrap()
					.to_string()
			}
			DefineUserOption::Passhash(v) => {
				res.hash = v;
			}
			DefineUserOption::Roles(v) => {
				res.roles = v;
			}
			DefineUserOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineUserOption {
	Password(String),
	Passhash(String),
	Roles(Vec<Ident>),
	Comment(Strand),
}

fn user_opts(i: &str) -> IResult<&str, Vec<DefineUserOption>> {
	many0(alt((user_pass, user_hash, user_roles, user_comment)))(i)
}

fn user_pass(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PASSWORD")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand_raw)(i)?;
	Ok((i, DefineUserOption::Password(v)))
}

fn user_hash(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PASSHASH")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand_raw)(i)?;
	Ok((i, DefineUserOption::Passhash(v)))
}

fn user_comment(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineUserOption::Comment(v)))
}

fn user_roles(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ROLES")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, roles) = separated_list1(commas, |i| {
		let (i, v) = cut(ident)(i)?;
		// Verify the role is valid
		Role::try_from(v.as_str()).map_err(|_| Err::Failure(ParseError::Role(i, v.to_string())))?;

		Ok((i, v))
	})(i)?;

	Ok((i, DefineUserOption::Roles(roles)))
}
