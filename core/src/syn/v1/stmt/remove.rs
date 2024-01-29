use super::super::{
	comment::{mightbespace, shouldbespace},
	error::expect_tag_no_case,
	idiom::{self},
	literal::{ident, ident_path},
	part::{base, base_or_scope},
	IResult,
};
use crate::sql::statements::{
	RemoveAnalyzerStatement, RemoveDatabaseStatement, RemoveEventStatement, RemoveFieldStatement,
	RemoveFunctionStatement, RemoveIndexStatement, RemoveNamespaceStatement, RemoveParamStatement,
	RemoveScopeStatement, RemoveStatement, RemoveTableStatement, RemoveTokenStatement,
	RemoveUserStatement,
};
use nom::{
	branch::alt,
	bytes::complete::{tag, tag_no_case},
	character::complete::char,
	combinator::{cut, map, opt},
	sequence::tuple,
};

pub fn remove(i: &str) -> IResult<&str, RemoveStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((
		map(namespace, RemoveStatement::Namespace),
		map(database, RemoveStatement::Database),
		map(function, RemoveStatement::Function),
		map(token, RemoveStatement::Token),
		map(scope, RemoveStatement::Scope),
		map(param, RemoveStatement::Param),
		map(table, RemoveStatement::Table),
		map(event, RemoveStatement::Event),
		map(field, RemoveStatement::Field),
		map(index, RemoveStatement::Index),
		map(analyzer, RemoveStatement::Analyzer),
		map(user, RemoveStatement::User),
	))(i)
}

pub fn analyzer(i: &str) -> IResult<&str, RemoveAnalyzerStatement> {
	let (i, _) = tag_no_case("ANALYZER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveAnalyzerStatement {
			name,
		},
	))
}

pub fn database(i: &str) -> IResult<&str, RemoveDatabaseStatement> {
	let (i, _) = alt((tag_no_case("DB"), tag_no_case("DATABASE")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveDatabaseStatement {
			name,
		},
	))
}

pub fn event(i: &str) -> IResult<&str, RemoveEventStatement> {
	let (i, _) = tag_no_case("EVENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = expect_tag_no_case("ON")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = cut(ident)(i)?;
	Ok((
		i,
		RemoveEventStatement {
			name,
			what,
		},
	))
}

pub fn field(i: &str) -> IResult<&str, RemoveFieldStatement> {
	let (i, _) = tag_no_case("FIELD")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(idiom::local)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = expect_tag_no_case("ON")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = cut(ident)(i)?;
	Ok((
		i,
		RemoveFieldStatement {
			name,
			what,
		},
	))
}

pub fn function(i: &str) -> IResult<&str, RemoveFunctionStatement> {
	let (i, _) = tag_no_case("FUNCTION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag("fn::")(i)?;
	let (i, name) = ident_path(i)?;
	let (i, _) = opt(|i| {
		let (i, _) = mightbespace(i)?;
		let (i, _) = char('(')(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = char(')')(i)?;
		Ok((i, ()))
	})(i)?;
	Ok((
		i,
		RemoveFunctionStatement {
			name,
		},
	))
}

pub fn index(i: &str) -> IResult<&str, RemoveIndexStatement> {
	let (i, _) = tag_no_case("INDEX")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = expect_tag_no_case("ON")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = cut(ident)(i)?;
	Ok((
		i,
		RemoveIndexStatement {
			name,
			what,
		},
	))
}

pub fn namespace(i: &str) -> IResult<&str, RemoveNamespaceStatement> {
	let (i, _) = alt((tag_no_case("NS"), tag_no_case("NAMESPACE")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveNamespaceStatement {
			name,
		},
	))
}

pub fn param(i: &str) -> IResult<&str, RemoveParamStatement> {
	let (i, _) = tag_no_case("PARAM")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = cut(char('$'))(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveParamStatement {
			name,
		},
	))
}

pub fn scope(i: &str) -> IResult<&str, RemoveScopeStatement> {
	let (i, _) = tag_no_case("SCOPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveScopeStatement {
			name,
		},
	))
}

pub fn table(i: &str) -> IResult<&str, RemoveTableStatement> {
	let (i, _) = tag_no_case("TABLE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveTableStatement {
			name,
		},
	))
}

pub fn token(i: &str) -> IResult<&str, RemoveTokenStatement> {
	let (i, _) = tag_no_case("TOKEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = expect_tag_no_case("ON")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, base) = cut(base_or_scope)(i)?;
	Ok((
		i,
		RemoveTokenStatement {
			name,
			base,
		},
	))
}

pub fn user(i: &str) -> IResult<&str, RemoveUserStatement> {
	let (i, _) = tag_no_case("USER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = expect_tag_no_case("ON")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, base) = cut(base)(i)?;
	Ok((
		i,
		RemoveUserStatement {
			name,
			base,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::Ident;

	#[test]
	fn check_remove_serialize() {
		let stm = RemoveStatement::Namespace(RemoveNamespaceStatement {
			name: Ident::from("test"),
		});
		let enc: Vec<u8> = stm.try_into().unwrap();
		assert_eq!(9, enc.len());
	}

	#[test]
	fn remove_long_function() {
		let sql = "REMOVE FUNCTION fn::foo::bar::baz::bac";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE FUNCTION fn::foo::bar::baz::bac", format!("{}", out))
	}
}
