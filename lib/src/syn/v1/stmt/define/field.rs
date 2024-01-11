use super::super::super::{
	comment::shouldbespace,
	ending,
	error::{expect_tag_no_case, expected},
	idiom::{self},
	kind::kind,
	literal::{ident, strand},
	part::permission::permissions,
	value::value,
	IResult,
};
use crate::sql::{statements::DefineFieldStatement, Kind, Permission, Permissions, Strand, Value};
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	combinator::{cut, opt},
	multi::many0,
	sequence::tuple,
};

pub fn field(i: &str) -> IResult<&str, DefineFieldStatement> {
	let (i, _) = tag_no_case("FIELD")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, what, opts)) = cut(|i| {
		let (i, name) = idiom::local(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, what) = ident(i)?;
		let (i, opts) = many0(field_opts)(i)?;
		let (i, _) = expected(
			"one of FLEX(IBLE), TYPE, VALUE, ASSERT, DEFAULT, or COMMENT",
			cut(ending::query),
		)(i)?;
		Ok((i, (name, what, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineFieldStatement {
		name,
		what,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineFieldOption::Flex => {
				res.flex = true;
			}
			DefineFieldOption::Kind(v) => {
				res.kind = Some(v);
			}
			DefineFieldOption::Value(v) => {
				res.value = Some(v);
			}
			DefineFieldOption::Assert(v) => {
				res.assert = Some(v);
			}
			DefineFieldOption::Default(v) => {
				res.default = Some(v);
			}
			DefineFieldOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineFieldOption::Permissions(v) => {
				res.permissions = v;
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineFieldOption {
	Flex,
	Kind(Kind),
	Value(Value),
	Assert(Value),
	Default(Value),
	Comment(Strand),
	Permissions(Permissions),
}

fn field_opts(i: &str) -> IResult<&str, DefineFieldOption> {
	alt((
		field_flex,
		field_kind,
		field_value,
		field_assert,
		field_default,
		field_comment,
		field_permissions,
	))(i)
}

fn field_flex(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("FLEXIBLE"), tag_no_case("FLEXI"), tag_no_case("FLEX")))(i)?;
	Ok((i, DefineFieldOption::Flex))
}

fn field_kind(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TYPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(kind)(i)?;
	Ok((i, DefineFieldOption::Kind(v)))
}

fn field_value(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineFieldOption::Value(v)))
}

fn field_assert(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ASSERT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineFieldOption::Assert(v)))
}

fn field_default(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("DEFAULT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineFieldOption::Default(v)))
}

fn field_comment(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = strand(i)?;
	Ok((i, DefineFieldOption::Comment(v)))
}

fn field_permissions(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = permissions(i, Permission::Full)?;
	Ok((i, DefineFieldOption::Permissions(v)))
}

#[cfg(test)]
mod test {
	use super::field;

	fn assert_parsable(sql: &str) {
		let res = field(sql);
		assert!(res.is_ok());
		let (_, out) = res.unwrap();
		assert_eq!(format!("DEFINE {}", sql), format!("{}", out))
	}

	#[test]
	fn define_field_record_type_permissions() {
		assert_parsable("FIELD attributes[*] ON listing TYPE record PERMISSIONS FULL")
	}
}
