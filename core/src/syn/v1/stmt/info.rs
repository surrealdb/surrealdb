use super::super::{
	comment::shouldbespace, error::expected, error::ExplainResultExt, literal::ident, part::base,
	IResult,
};
use crate::sql::statements::InfoStatement;
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	combinator::{cut, opt},
};

pub fn info(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = tag_no_case("INFO")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = cut(shouldbespace)(i)?;
	let (i, stm) = expected(
		"ROOT, NAMESPACE, DATABASE, SCOPE, TABLE or USER",
		cut(alt((root, ns, db, sc, tb, user))),
	)(i)?;

	let (i, structure) = opt(tag_no_case("STRUCTURE"))(i)?;
	Ok((
		i,
		match structure {
			Some(_) => stm.structurize(),
			None => stm,
		},
	))
}

fn root(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("ROOT"), tag_no_case("KV")))(i)?;
	Ok((i, InfoStatement::Root(false)))
}

fn ns(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("NAMESPACE"), tag_no_case("NS")))(i)?;
	Ok((i, InfoStatement::Ns(false)))
}

fn db(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("DATABASE"), tag_no_case("DB")))(i)?;
	Ok((i, InfoStatement::Db(false)))
}

fn sc(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("SCOPE"), tag_no_case("SC")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, scope) = cut(ident)(i)?;
	Ok((i, InfoStatement::Sc(scope, false)))
}

fn tb(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("TABLE"), tag_no_case("TB")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, table) = cut(ident)(i)?;
	Ok((i, InfoStatement::Tb(table, false)))
}

fn user(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("USER"), tag_no_case("US")))(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, user) = ident(i)?;
		let (i, base) = opt(|i| {
			let (i, _) = shouldbespace(i)?;
			let (i, _) = tag_no_case("ON")(i)?;
			cut(|i| {
				let (i, _) = shouldbespace(i)?;
				let (i, base) =
					base(i).explain("scopes are not allowed here", tag_no_case("SCOPE"))?;
				Ok((i, base))
			})(i)
		})(i)?;

		Ok((i, InfoStatement::User(user, base, false)))
	})(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::{Base, Ident};

	#[test]
	fn info_query_root() {
		let sql = "INFO FOR ROOT";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Root(false));
		assert_eq!("INFO FOR ROOT", format!("{}", out));
	}

	#[test]
	fn info_query_ns() {
		let sql = "INFO FOR NAMESPACE";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Ns(false));
		assert_eq!("INFO FOR NAMESPACE", format!("{}", out));
	}

	#[test]
	fn info_query_db() {
		let sql = "INFO FOR DATABASE";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Db(false));
		assert_eq!("INFO FOR DATABASE", format!("{}", out));
	}

	#[test]
	fn info_query_sc() {
		let sql = "INFO FOR SCOPE test";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Sc(Ident::from("test"), false));
		assert_eq!("INFO FOR SCOPE test", format!("{}", out));
	}

	#[test]
	fn info_query_tb() {
		let sql = "INFO FOR TABLE test";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Tb(Ident::from("test"), false));
		assert_eq!("INFO FOR TABLE test", format!("{}", out));
	}

	#[test]
	fn info_query_user() {
		let sql = "INFO FOR USER test ON ROOT";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), Some(Base::Root), false));
		assert_eq!("INFO FOR USER test ON ROOT", format!("{}", out));

		let sql = "INFO FOR USER test ON NS";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), Some(Base::Ns), false));
		assert_eq!("INFO FOR USER test ON NAMESPACE", format!("{}", out));

		let sql = "INFO FOR USER test ON DB";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), Some(Base::Db), false));
		assert_eq!("INFO FOR USER test ON DATABASE", format!("{}", out));

		let sql = "INFO FOR USER test";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), None, false));
		assert_eq!("INFO FOR USER test", format!("{}", out));
	}

	#[test]
	fn info_query_root_structure() {
		let sql = "INFO FOR ROOT STRUCTURE";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Root(true));
		assert_eq!("INFO FOR ROOT", format!("{}", out));
	}
}
