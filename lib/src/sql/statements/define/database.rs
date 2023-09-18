use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::base::Base;
use crate::sql::changefeed::{changefeed, ChangeFeed};
use crate::sql::comment::shouldbespace;
use crate::sql::ending;
use crate::sql::error::expected;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::strand::{strand, Strand};
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use nom::multi::many0;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineDatabaseStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub comment: Option<Strand>,
	pub changefeed: Option<ChangeFeed>,
}

impl DefineDatabaseStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Process the statement
		let key = crate::key::namespace::db::new(opt.ns(), &self.name);
		let ns = run.add_ns(opt.ns(), opt.strict).await?;
		// Set the id
		if self.id.is_none() && ns.id.is_some() {
			let mut db = self.clone();
			db.id = Some(run.get_next_db_id(ns.id.unwrap()).await?);
			// Store the db
			run.set(key, db).await?;
		} else {
			// Store the db
			run.set(key, self).await?;
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineDatabaseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE DATABASE {}", self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		if let Some(ref v) = self.changefeed {
			write!(f, " {v}")?;
		}
		Ok(())
	}
}

pub fn database(i: &str) -> IResult<&str, DefineDatabaseStatement> {
	let (i, _) = alt((tag_no_case("DB"), tag_no_case("DATABASE")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(database_opts)(i)?;
	let (i, _) = expected("COMMENT or CHANGEFEED", ending::query)(i)?;

	// Create the base statement
	let mut res = DefineDatabaseStatement {
		name,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineDatabaseOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineDatabaseOption::ChangeFeed(v) => {
				res.changefeed = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineDatabaseOption {
	Comment(Strand),
	ChangeFeed(ChangeFeed),
}

fn database_opts(i: &str) -> IResult<&str, DefineDatabaseOption> {
	alt((database_comment, database_changefeed))(i)
}

fn database_comment(i: &str) -> IResult<&str, DefineDatabaseOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineDatabaseOption::Comment(v)))
}

fn database_changefeed(i: &str) -> IResult<&str, DefineDatabaseOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = changefeed(i)?;
	Ok((i, DefineDatabaseOption::ChangeFeed(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn define_database_with_changefeed() {
		let sql = "DATABASE mydatabase CHANGEFEED 1h";
		let res = database(sql);
		let out = res.unwrap().1;
		assert_eq!(format!("DEFINE {sql}"), format!("{}", out));

		let serialized: Vec<u8> = (&out).try_into().unwrap();
		let deserialized = DefineDatabaseStatement::try_from(&serialized).unwrap();
		assert_eq!(out, deserialized);
	}
}
