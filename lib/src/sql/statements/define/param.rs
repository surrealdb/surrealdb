use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::base::Base;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::strand::{strand, Strand};
use crate::sql::value::{value, Value};
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::multi::many0;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineParamStatement {
	pub name: Ident,
	pub value: Value,
	pub comment: Option<Strand>,
}

impl DefineParamStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Parameter, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Process the statement
		let ns = run.add_ns(opt.ns(), opt.strict).await?;
		let ns = ns.id.unwrap();
		let db = run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		let db = db.id.unwrap();
		let key = crate::key::database::pa::new(ns, db, &self.name);
		run.set(key, self).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineParamStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE PARAM ${} VALUE {}", self.name, self.value)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

pub fn param(i: &str) -> IResult<&str, DefineParamStatement> {
	let (i, _) = tag_no_case("DEFINE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PARAM")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = char('$')(i)?;
	let (i, name) = ident(i)?;
	let (i, opts) = many0(param_opts)(i)?;
	// Create the base statement
	let mut res = DefineParamStatement {
		name,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineParamOption::Value(v) => {
				res.value = v;
			}
			DefineParamOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Check necessary options
	if res.value.is_none() {
		// TODO throw error
	}
	// Return the statement
	Ok((i, res))
}

enum DefineParamOption {
	Value(Value),
	Comment(Strand),
}

fn param_opts(i: &str) -> IResult<&str, DefineParamOption> {
	alt((param_value, param_comment))(i)
}

fn param_value(i: &str) -> IResult<&str, DefineParamOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	Ok((i, DefineParamOption::Value(v)))
}

fn param_comment(i: &str) -> IResult<&str, DefineParamOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = strand(i)?;
	Ok((i, DefineParamOption::Comment(v)))
}
