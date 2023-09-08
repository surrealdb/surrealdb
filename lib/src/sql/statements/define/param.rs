use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::base::Base;
use crate::sql::comment::shouldbespace;
use crate::sql::ending;
use crate::sql::error::expected;
use crate::sql::error::IResult;
use crate::sql::fmt::is_pretty;
use crate::sql::fmt::pretty_indent;
use crate::sql::ident::{ident, Ident};
use crate::sql::permission::{permission, Permission};
use crate::sql::strand::{strand, Strand};
use crate::sql::value::{value, Value};
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::cut;
use nom::multi::many0;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineParamStatement {
	pub name: Ident,
	pub value: Value,
	pub comment: Option<Strand>,
	pub permissions: Permission,
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
		let key = crate::key::database::pa::new(opt.ns(), opt.db(), &self.name);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
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
		if !self.permissions.is_full() {
			let _indent = if is_pretty() {
				Some(pretty_indent())
			} else {
				f.write_char(' ')?;
				None
			};
			write!(f, "PERMISSIONS {}", self.permissions)?;
		}
		Ok(())
	}
}

pub fn param(i: &str) -> IResult<&str, DefineParamStatement> {
	let (i, _) = tag_no_case("PARAM")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = cut(char('$'))(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(param_opts)(i)?;
	let (i, _) = expected("VALUE, PERMISSIONS, or COMMENT", ending::query)(i)?;
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
			DefineParamOption::Permissions(v) => {
				res.permissions = v;
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
	Permissions(Permission),
}

fn param_opts(i: &str) -> IResult<&str, DefineParamOption> {
	alt((param_value, param_comment, param_permissions))(i)
}

fn param_value(i: &str) -> IResult<&str, DefineParamOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineParamOption::Value(v)))
}

fn param_comment(i: &str) -> IResult<&str, DefineParamOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineParamOption::Comment(v)))
}

fn param_permissions(i: &str) -> IResult<&str, DefineParamOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PERMISSIONS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(permission)(i)?;
	Ok((i, DefineParamOption::Permissions(v)))
}
