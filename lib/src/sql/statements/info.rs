use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::base::base;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::object::Object;
use crate::sql::value::Value;
use crate::sql::Base;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use nom::combinator::opt;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub enum InfoStatement {
	Root,
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
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		match self {
			InfoStatement::Root => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Root)?;
				// Claim transaction
				let mut run = txn.lock().await;
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
				for v in run.all_root_users().await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("users".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Ns => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the databases
				let mut tmp = Object::default();
				for v in run.all_db(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("databases".to_owned(), tmp.into());
				// Process the users
				let mut tmp = Object::default();
				for v in run.all_ns_users(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("users".to_owned(), tmp.into());
				// Process the tokens
				let mut tmp = Object::default();
				for v in run.all_ns_tokens(opt.ns()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tokens".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Db => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the users
				let mut tmp = Object::default();
				for v in run.all_db_users(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("users".to_owned(), tmp.into());
				// Process the tokens
				let mut tmp = Object::default();
				for v in run.all_db_tokens(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tokens".to_owned(), tmp.into());
				// Process the functions
				let mut tmp = Object::default();
				for v in run.all_db_functions(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("functions".to_owned(), tmp.into());
				// Process the params
				let mut tmp = Object::default();
				for v in run.all_db_params(opt.ns(), opt.db()).await?.iter() {
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
				for v in run.all_db_analyzers(opt.ns(), opt.db()).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("analyzers".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Sc(sc) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the tokens
				let mut tmp = Object::default();
				for v in run.all_sc_tokens(opt.ns(), opt.db(), sc).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tokens".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::Tb(tb) => {
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Any, &Base::Db)?;
				// Claim transaction
				let mut run = txn.lock().await;
				// Create the result set
				let mut res = Object::default();
				// Process the events
				let mut tmp = Object::default();
				for v in run.all_tb_events(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("events".to_owned(), tmp.into());
				// Process the fields
				let mut tmp = Object::default();
				for v in run.all_tb_fields(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("fields".to_owned(), tmp.into());
				// Process the tables
				let mut tmp = Object::default();
				for v in run.all_tb_views(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("tables".to_owned(), tmp.into());
				// Process the indexes
				let mut tmp = Object::default();
				for v in run.all_tb_indexes(opt.ns(), opt.db(), tb).await?.iter() {
					tmp.insert(v.name.to_string(), v.to_string().into());
				}
				res.insert("indexes".to_owned(), tmp.into());
				// Ok all good
				Value::from(res).ok()
			}
			InfoStatement::User(user, base) => {
				let base = base.clone().unwrap_or(opt.selected_base()?);
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Actor, &base)?;

				// Claim transaction
				let mut run = txn.lock().await;
				// Process the user
				let res = match base {
					Base::Root => run.get_root_user(user).await?,
					Base::Ns => run.get_ns_user(opt.ns(), user).await?,
					Base::Db => run.get_db_user(opt.ns(), opt.db(), user).await?,
					_ => return Err(Error::InvalidLevel(base.to_string())),
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
			Self::Root => f.write_str("INFO FOR ROOT"),
			Self::Ns => f.write_str("INFO FOR NAMESPACE"),
			Self::Db => f.write_str("INFO FOR DATABASE"),
			Self::Sc(ref s) => write!(f, "INFO FOR SCOPE {s}"),
			Self::Tb(ref t) => write!(f, "INFO FOR TABLE {t}"),
			Self::User(ref u, ref b) => match b {
				Some(ref b) => write!(f, "INFO FOR USER {u} ON {b}"),
				None => write!(f, "INFO FOR USER {u}"),
			},
		}
	}
}

pub fn info(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = tag_no_case("INFO")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FOR")(i)?;
	cut(|i| {
		let (i, _) = shouldbespace(i)?;
		alt((root, ns, db, sc, tb, user))(i)
	})(i)
}

fn root(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("ROOT"), tag_no_case("KV")))(i)?;
	Ok((i, InfoStatement::Root))
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
	cut(|i| {
		let (i, scope) = ident(i)?;
		Ok((i, InfoStatement::Sc(scope)))
	})(i)
}

fn tb(i: &str) -> IResult<&str, InfoStatement> {
	let (i, _) = alt((tag_no_case("TABLE"), tag_no_case("TB")))(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, table) = ident(i)?;
		Ok((i, InfoStatement::Tb(table)))
	})(i)
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
				let (i, base) = base(i)?;
				Ok((i, base))
			})(i)
		})(i)?;

		Ok((i, InfoStatement::User(user, base)))
	})(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn info_query_root() {
		let sql = "INFO FOR ROOT";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Root);
		assert_eq!("INFO FOR ROOT", format!("{}", out));
	}

	#[test]
	fn info_query_ns() {
		let sql = "INFO FOR NAMESPACE";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Ns);
		assert_eq!("INFO FOR NAMESPACE", format!("{}", out));
	}

	#[test]
	fn info_query_db() {
		let sql = "INFO FOR DATABASE";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Db);
		assert_eq!("INFO FOR DATABASE", format!("{}", out));
	}

	#[test]
	fn info_query_sc() {
		let sql = "INFO FOR SCOPE test";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Sc(Ident::from("test")));
		assert_eq!("INFO FOR SCOPE test", format!("{}", out));
	}

	#[test]
	fn info_query_tb() {
		let sql = "INFO FOR TABLE test";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::Tb(Ident::from("test")));
		assert_eq!("INFO FOR TABLE test", format!("{}", out));
	}

	#[test]
	fn info_query_user() {
		let sql = "INFO FOR USER test ON ROOT";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), Some(Base::Root)));
		assert_eq!("INFO FOR USER test ON ROOT", format!("{}", out));

		let sql = "INFO FOR USER test ON NS";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), Some(Base::Ns)));
		assert_eq!("INFO FOR USER test ON NAMESPACE", format!("{}", out));

		let sql = "INFO FOR USER test ON DB";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), Some(Base::Db)));
		assert_eq!("INFO FOR USER test ON DATABASE", format!("{}", out));

		let sql = "INFO FOR USER test";
		let res = info(sql);
		let out = res.unwrap().1;
		assert_eq!(out, InfoStatement::User(Ident::from("test"), None));
		assert_eq!("INFO FOR USER test", format!("{}", out));
	}
}
