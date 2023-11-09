use super::super::{comment::shouldbespace, IResult};
use crate::sql::statement::DefineStatement;
use nom::{branch::alt, bytes::complete::tag_no_case, combinator::map};

mod analyzer;
mod database;
mod event;
mod field;
mod function;
mod index;
mod namespace;
mod param;
mod scope;
mod table;
mod token;
mod user;

pub fn define(i: &str) -> IResult<&str, DefineStatement> {
	let (i, _) = tag_no_case("DEFINE")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((
		map(namespace, DefineStatement::Namespace),
		map(database, DefineStatement::Database),
		map(function, DefineStatement::Function),
		map(user, DefineStatement::User),
		map(token, DefineStatement::Token),
		map(scope, DefineStatement::Scope),
		map(param, DefineStatement::Param),
		map(table, DefineStatement::Table),
		map(event, DefineStatement::Event),
		map(field, DefineStatement::Field),
		map(index, DefineStatement::Index),
		map(analyzer, DefineStatement::Analyzer),
	))(i)
}
