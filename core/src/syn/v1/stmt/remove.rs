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
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveAnalyzerStatement {
			name,
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn database(i: &str) -> IResult<&str, RemoveDatabaseStatement> {
	let (i, _) = alt((tag_no_case("DB"), tag_no_case("DATABASE")))(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveDatabaseStatement {
			name,
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn event(i: &str) -> IResult<&str, RemoveEventStatement> {
	let (i, _) = tag_no_case("EVENT")(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
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
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn field(i: &str) -> IResult<&str, RemoveFieldStatement> {
	let (i, _) = tag_no_case("FIELD")(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
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
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn function(i: &str) -> IResult<&str, RemoveFunctionStatement> {
	let (i, _) = tag_no_case("FUNCTION")(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
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
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn index(i: &str) -> IResult<&str, RemoveIndexStatement> {
	let (i, _) = tag_no_case("INDEX")(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
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
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn namespace(i: &str) -> IResult<&str, RemoveNamespaceStatement> {
	let (i, _) = alt((tag_no_case("NS"), tag_no_case("NAMESPACE")))(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveNamespaceStatement {
			name,
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn param(i: &str) -> IResult<&str, RemoveParamStatement> {
	let (i, _) = tag_no_case("PARAM")(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = cut(char('$'))(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveParamStatement {
			name,
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn scope(i: &str) -> IResult<&str, RemoveScopeStatement> {
	let (i, _) = tag_no_case("SCOPE")(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveScopeStatement {
			name,
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn table(i: &str) -> IResult<&str, RemoveTableStatement> {
	let (i, _) = tag_no_case("TABLE")(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	Ok((
		i,
		RemoveTableStatement {
			name,
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn token(i: &str) -> IResult<&str, RemoveTokenStatement> {
	let (i, _) = tag_no_case("TOKEN")(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
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
			if_exists: if_exists.is_some(),
		},
	))
}

pub fn user(i: &str) -> IResult<&str, RemoveUserStatement> {
	let (i, _) = tag_no_case("USER")(i)?;
	let (i, if_exists) =
		opt(tuple((shouldbespace, tag_no_case("IF"), shouldbespace, tag_no_case("EXISTS"))))(i)?;
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
			if_exists: if_exists.is_some(),
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
			if_exists: false,
		});
		let enc: Vec<u8> = stm.into();
		assert_eq!(10, enc.len());
	}

	/// REMOVE ANALYZER tests

	#[test]
	fn remove_analyzer() {
		let sql = "REMOVE ANALYZER test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE ANALYZER test", format!("{}", out))
	}

	#[test]
	fn remove_analyzer_if_exists() {
		let sql = "REMOVE ANALYZER IF EXISTS test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE ANALYZER IF EXISTS test", format!("{}", out))
	}

	#[test]
	fn remove_analyzer_if() {
		let sql = "REMOVE ANALYZER IF test";
		let res = remove(sql);
		assert!(res.is_err());
	}

	/// REMOVE DATABASE tests

	#[test]
	fn remove_database() {
		let sql = "REMOVE DATABASE test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE DATABASE test", format!("{}", out))
	}

	#[test]
	fn remove_database_if_exists() {
		let sql = "REMOVE DATABASE IF EXISTS test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE DATABASE IF EXISTS test", format!("{}", out))
	}

	#[test]
	fn remove_database_if() {
		let sql = "REMOVE DATABASE IF test";
		let res = remove(sql);
		assert!(res.is_err());
	}

	/// REMOVE EVENT tests

	#[test]
	fn remove_event() {
		let sql = "REMOVE EVENT test ON test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE EVENT test ON test", format!("{}", out))
	}

	#[test]
	fn remove_event_if_exists() {
		let sql = "REMOVE EVENT IF EXISTS test ON test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE EVENT IF EXISTS test ON test", format!("{}", out))
	}

	#[test]
	fn remove_event_if() {
		let sql = "REMOVE EVENT IF test ON test";
		let res = remove(sql);
		assert!(res.is_err());
	}

	/// REMOVE FIELD tests

	#[test]
	fn remove_field() {
		let sql = "REMOVE FIELD test ON test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE FIELD test ON test", format!("{}", out))
	}

	#[test]
	fn remove_field_if_exists() {
		let sql = "REMOVE FIELD IF EXISTS test ON test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE FIELD IF EXISTS test ON test", format!("{}", out))
	}

	#[test]
	fn remove_field_if() {
		let sql = "REMOVE FIELD IF test ON test";
		let res = remove(sql);
		assert!(res.is_err());
	}

	/// REMOVE FUNCTION tests

	#[test]
	fn remove_function() {
		let sql = "REMOVE FUNCTION fn::test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE FUNCTION fn::test", format!("{}", out))
	}

	#[test]
	fn remove_function_if_exists() {
		let sql = "REMOVE FUNCTION IF EXISTS fn::test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE FUNCTION IF EXISTS fn::test", format!("{}", out))
	}

	#[test]
	fn remove_function_if() {
		let sql = "REMOVE FUNCTION IF fn::test";
		let res = remove(sql);
		assert!(res.is_err());
	}

	#[test]
	fn remove_long_function() {
		let sql = "REMOVE FUNCTION fn::foo::bar::baz::bac";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE FUNCTION fn::foo::bar::baz::bac", format!("{}", out))
	}

	/// REMOVE INDEX tests

	#[test]
	fn remove_index() {
		let sql = "REMOVE INDEX test ON test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE INDEX test ON test", format!("{}", out))
	}

	#[test]
	fn remove_index_if_exists() {
		let sql = "REMOVE INDEX IF EXISTS test ON test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE INDEX IF EXISTS test ON test", format!("{}", out))
	}

	#[test]
	fn remove_index_if() {
		let sql = "REMOVE INDEX IF test ON test";
		let res = remove(sql);
		assert!(res.is_err());
	}

	/// REMOVE NAMESPACE tests

	#[test]
	fn remove_namespace() {
		let sql = "REMOVE NAMESPACE test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE NAMESPACE test", format!("{}", out))
	}

	#[test]
	fn remove_namespace_if_exists() {
		let sql = "REMOVE NAMESPACE IF EXISTS test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE NAMESPACE IF EXISTS test", format!("{}", out))
	}

	#[test]
	fn remove_namespace_if() {
		let sql = "REMOVE NAMESPACE IF test";
		let res = remove(sql);
		assert!(res.is_err());
	}

	/// REMOVE PARAM tests

	#[test]
	fn remove_param() {
		let sql = "REMOVE PARAM $test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE PARAM $test", format!("{}", out))
	}

	#[test]
	fn remove_param_if_exists() {
		let sql = "REMOVE PARAM IF EXISTS $test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE PARAM IF EXISTS $test", format!("{}", out))
	}

	#[test]
	fn remove_param_if() {
		let sql = "REMOVE PARAM IF $test";
		let res = remove(sql);
		assert!(res.is_err());
	}

	/// REMOVE SCOPE tests

	#[test]
	fn remove_scope() {
		let sql = "REMOVE SCOPE test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE SCOPE test", format!("{}", out))
	}

	#[test]
	fn remove_scope_if_exists() {
		let sql = "REMOVE SCOPE IF EXISTS test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE SCOPE IF EXISTS test", format!("{}", out))
	}

	#[test]
	fn remove_scope_if() {
		let sql = "REMOVE SCOPE IF test";
		let res = remove(sql);
		assert!(res.is_err());
	}

	/// REMOVE TABLE tests

	#[test]
	fn remove_table() {
		let sql = "REMOVE TABLE test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE TABLE test", format!("{}", out))
	}

	#[test]
	fn remove_table_if_exists() {
		let sql = "REMOVE TABLE IF EXISTS test";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE TABLE IF EXISTS test", format!("{}", out))
	}

	#[test]
	fn remove_table_if() {
		let sql = "REMOVE TABLE IF test";
		let res = remove(sql);
		assert!(res.is_err());
	}

	/// REMOVE TOKEN tests

	#[test]
	fn remove_token() {
		let sql = "REMOVE TOKEN test ON NAMESPACE";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE TOKEN test ON NAMESPACE", format!("{}", out))
	}

	#[test]
	fn remove_token_if_exists() {
		let sql = "REMOVE TOKEN IF EXISTS test ON NAMESPACE";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE TOKEN IF EXISTS test ON NAMESPACE", format!("{}", out))
	}

	#[test]
	fn remove_token_if() {
		let sql = "REMOVE TOKEN IF test ON NAMESPACE";
		let res = remove(sql);
		assert!(res.is_err());
	}

	/// REMOVE USER tests

	#[test]
	fn remove_user() {
		let sql = "REMOVE USER test ON ROOT";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE USER test ON ROOT", format!("{}", out))
	}

	#[test]
	fn remove_user_if_exists() {
		let sql = "REMOVE USER IF EXISTS test ON ROOT";
		let res = remove(sql);
		let out = res.unwrap().1;
		assert_eq!("REMOVE USER IF EXISTS test ON ROOT", format!("{}", out))
	}

	#[test]
	fn remove_user_if() {
		let sql = "REMOVE USER IF test ON ROOT";
		let res = remove(sql);
		assert!(res.is_err());
	}
}
