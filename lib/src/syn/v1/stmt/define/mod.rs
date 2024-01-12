use super::super::{comment::shouldbespace, IResult};
use crate::sql::statements::DefineStatement;
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
		map(namespace::namespace, DefineStatement::Namespace),
		map(database::database, DefineStatement::Database),
		map(function::function, DefineStatement::Function),
		map(user::user, DefineStatement::User),
		map(token::token, DefineStatement::Token),
		map(scope::scope, DefineStatement::Scope),
		map(param::param, DefineStatement::Param),
		map(table::table, DefineStatement::Table),
		map(event::event, DefineStatement::Event),
		map(field::field, DefineStatement::Field),
		map(index::index, DefineStatement::Index),
		map(analyzer::analyzer, DefineStatement::Analyzer),
	))(i)
}
