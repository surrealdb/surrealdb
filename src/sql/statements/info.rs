use crate::dbs;
use crate::dbs::Executor;
use crate::dbs::Runtime;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::ident::ident_raw;
use crate::sql::literal::Literal;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum InfoStatement {
	Namespace,
	Database,
	Scope(String),
	Table(String),
}

impl fmt::Display for InfoStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			InfoStatement::Namespace => write!(f, "INFO FOR NAMESPACE"),
			InfoStatement::Database => write!(f, "INFO FOR DATABASE"),
			InfoStatement::Scope(ref s) => write!(f, "INFO FOR SCOPE {}", s),
			InfoStatement::Table(ref t) => write!(f, "INFO FOR TABLE {}", t),
		}
	}
}

impl dbs::Process for InfoStatement {
	fn process(
		&self,
		ctx: &Runtime,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		todo!()
	}
}

pub fn info(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = tag_no_case("INFO")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((namespace, database, scope, table))(i)
}

fn namespace(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("NAMESPACE"), tag_no_case("NS")))(i)?;
	Ok((i, InfoStatement::Namespace))
}

fn database(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("DATABASE"), tag_no_case("DB")))(i)?;
	Ok((i, InfoStatement::Database))
}

fn scope(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("SCOPE"), tag_no_case("SC")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, scope) = ident_raw(i)?;
	Ok((i, InfoStatement::Scope(String::from(scope))))
}

fn table(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("TABLE"), tag_no_case("TB")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, table) = ident_raw(i)?;
	Ok((i, InfoStatement::Table(String::from(table))))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn info_query_ns() {
		let sql = "INFO FOR NAMESPACE";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Namespace);
		assert_eq!("INFO FOR NAMESPACE", format!("{}", out));
	}

	#[test]
	fn info_query_db() {
		let sql = "INFO FOR DATABASE";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Database);
		assert_eq!("INFO FOR DATABASE", format!("{}", out));
	}

	#[test]
	fn info_query_sc() {
		let sql = "INFO FOR SCOPE test";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Scope(String::from("test")));
		assert_eq!("INFO FOR SCOPE test", format!("{}", out));
	}

	#[test]
	fn info_query_tb() {
		let sql = "INFO FOR TABLE test";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Table(String::from("test")));
		assert_eq!("INFO FOR TABLE test", format!("{}", out));
	}
}
