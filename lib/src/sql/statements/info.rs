use crate::ctx::Context;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::Base;
use crate::sql::base::base;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::object::Object;
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub enum InfoStatement {
	Kv,
	Ns,
	Db,
	Sc(Ident),
	Tb(Ident),
	User(Ident, Option<Base>),
}

impl InfoStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		match self {
			InfoStatement::Kv => {
				// No need for NS/DB
				opt.needs(Level::Kv)?;
				// Allowed to run?
				opt.check(Level::Kv)?;
				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the namespaces
				let mut tmp = Object::default();
				for v in run.all_ns().await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("namespaces".to_owned(), tmp.into());
				// Process the users
				let mut tmp = Object::default();
				for v in run.all_kv_users().await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("users".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Ns => {
				// Selected NS?
				opt.needs(Level::Ns)?;
				// Allowed to run?
				opt.check(Level::Ns)?;
				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the databases
				let mut tmp = Object::default();
				for v in run.all_db(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("databases".to_owned(), tmp.into());
				// Process the logins
				let mut tmp = Object::default();
				for v in run.all_nl(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("logins".to_owned(), tmp.into());
				// Process the users
				let mut tmp = Object::default();
				for v in run.all_ns_users(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("users".to_owned(), tmp.into());
				// Process the tokens
				let mut tmp = Object::default();
				for v in run.all_nt(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tokens".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Db => {
				// Selected DB?
				opt.needs(Level::Db)?;
				// Allowed to run?
				opt.check(Level::Db)?;
				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the logins
				let mut tmp = Object::default();
				for v in run.all_dl(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("logins".to_owned(), tmp.into());
				// Process the users
				let mut tmp = Object::default();
				for v in run.all_db_users(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("users".to_owned(), tmp.into());
				// Process the tokens
				let mut tmp = Object::default();
				for v in run.all_dt(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tokens".to_owned(), tmp.into());
				// Process the functions
				let mut tmp = Object::default();
				for v in run.all_fc(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("functions".to_owned(), tmp.into());
				// Process the params
				let mut tmp = Object::default();
				for v in run.all_pa(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("params".to_owned(), tmp.into());
				// Process the scopes
				let mut tmp = Object::default();
				for v in run.all_sc(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("scopes".to_owned(), tmp.into());
				// Process the tables
				let mut tmp = Object::default();
				for v in run.all_tb(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tables".to_owned(), tmp.into());
				// Process the analyzers
				let mut tmp = Object::default();
				for v in run.all_az(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("analyzers".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Sc(sc) => {
				// Selected DB?
				opt.needs(Level::Db)?;
				// Allowed to run?
				opt.check(Level::Db)?;
				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the tokens
				let mut tmp = Object::default();
				for v in run.all_st(opt.ns(), opt.db(), sc).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tokens".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Tb(tb) => {
				// Selected DB?
				opt.needs(Level::Db)?;
				// Allowed to run?
				opt.check(Level::Db)?;
				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the events
				let mut tmp = Object::default();
				for v in run.all_ev(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("events".to_owned(), tmp.into());
				// Process the fields
				let mut tmp = Object::default();
				for v in run.all_fd(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("fields".to_owned(), tmp.into());
				// Process the tables
				let mut tmp = Object::default();
				for v in run.all_ft(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tables".to_owned(), tmp.into());
				// Process the indexes
				let mut tmp = Object::default();
				for v in run.all_ix(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("indexes".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			},
			InfoStatement::User(user, base) => {
				let level = match base {
					// Get the level from the provided user statement
					Some(val) => val.to_level(),
					// If no level is provided, use the current selected level
					None => opt.current_level(),
				};

				// Check if all the necessary options are set for the given level
				opt.needs(level.to_owned())?;
				// Check if the user is allowed to run the statement on the given level
				opt.check(level.to_owned())?;

				// Clone transaction
				let run = txn.clone();
				// Claim transaction
				let mut run = run.lock().await;
				// Process the user
				let res = match level {
					Level::Kv => run.get_kv_user(user).await?,
					Level::Ns => run.get_ns_user(opt.ns(), user).await?,
					Level::Db => run.get_db_user(opt.ns(), opt.db(), user).await?,
					_ => return Err(Error::QueryPermissions),
				};
				// Ok all good
				Value::from(res.to_string()).ok()
			}
		}
	}
}

impl fmt::Display for InfoStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Kv => f.write_str("INFO FOR KV"),
			Self::Ns => f.write_str("INFO FOR NAMESPACE"),
			Self::Db => f.write_str("INFO FOR DATABASE"),
			Self::Sc(ref s) => write!(f, "INFO FOR SCOPE {s}"),
			Self::Tb(ref t) => write!(f, "INFO FOR TABLE {t}"),
			Self::User(ref u, ref b) => {
				match b {
					Some(ref b) => write!(f, "INFO FOR USER {u} ON {b}"),
					None => write!(f, "INFO FOR USER {u}"),
				}
			},
		}
	}
}

pub fn info(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = tag_no_case("INFO")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((kv, ns, db, sc, tb, user))(i)
}

fn kv(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = tag_no_case("KV")(i)?;
	Ok((i, InfoStatement::Kv))
}

fn ns(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("NAMESPACE"), tag_no_case("NS")))(i)?;
	Ok((i, InfoStatement::Ns))
}

fn db(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("DATABASE"), tag_no_case("DB")))(i)?;
	Ok((i, InfoStatement::Db))
}

fn sc(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("SCOPE"), tag_no_case("SC")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, scope) = ident(i)?;
	Ok((i, InfoStatement::Sc(scope)))
}

fn tb(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("TABLE"), tag_no_case("TB")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, table) = ident(i)?;
	Ok((i, InfoStatement::Tb(table)))
}

fn user(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("USER"), tag_no_case("US")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, user) = ident(i)?;
	let (i, base) = opt(|i| {
		let (i, _) = shouldbespace(i)?;
		let (i, _) = tag_no_case("ON")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, base) = base(i)?;
		Ok((i, base))
	})(i)?;

	Ok((i, InfoStatement::User(user, base)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn info_query_kv() {
		let sql = "INFO FOR KV";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Kv);
		assert_eq!("INFO FOR KV", format!("{}", out));
	}

	#[test]
	fn info_query_ns() {
		let sql = "INFO FOR NAMESPACE";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Ns);
		assert_eq!("INFO FOR NAMESPACE", format!("{}", out));
	}

	#[test]
	fn info_query_db() {
		let sql = "INFO FOR DATABASE";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Db);
		assert_eq!("INFO FOR DATABASE", format!("{}", out));
	}

	#[test]
	fn info_query_sc() {
		let sql = "INFO FOR SCOPE test";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Sc(Ident::from("test")));
		assert_eq!("INFO FOR SCOPE test", format!("{}", out));
	}

	#[test]
	fn info_query_tb() {
		let sql = "INFO FOR TABLE test";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Tb(Ident::from("test")));
		assert_eq!("INFO FOR TABLE test", format!("{}", out));
	}

	#[test]
	fn info_query_user() {
		let sql = "INFO FOR USER test ON KV";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), Some(Base::Kv)));
		assert_eq!("INFO FOR USER test ON KV", format!("{}", out));

		let sql = "INFO FOR USER test ON NS";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), Some(Base::Ns)));
		assert_eq!("INFO FOR USER test ON NAMESPACE", format!("{}", out));

		let sql = "INFO FOR USER test ON DB";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), Some(Base::Db)));
		assert_eq!("INFO FOR USER test ON DATABASE", format!("{}", out));

		let sql = "INFO FOR USER test";
		let res = info(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), None));
		assert_eq!("INFO FOR USER test", format!("{}", out));
	}
}
