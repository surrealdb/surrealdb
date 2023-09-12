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
pub struct DefineNamespaceStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub comment: Option<Strand>,
}

impl DefineNamespaceStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;
		// Process the statement
		let key = crate::key::root::ns::new(&self.name);
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Set the id
		if self.id.is_none() {
			let mut ns = self.clone();
			ns.id = Some(run.get_next_ns_id().await?);
			run.set(key, ns).await?;
		} else {
			run.set(key, self).await?;
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineNamespaceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE NAMESPACE {}", self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

pub fn namespace(i: &str) -> IResult<&str, DefineNamespaceStatement> {
	let (i, _) = alt((tag_no_case("NS"), tag_no_case("NAMESPACE")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(namespace_opts)(i)?;
	let (i, _) = expected("COMMENT", ending::query)(i)?;
	// Create the base statement
	let mut res = DefineNamespaceStatement {
		name,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineNamespaceOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineNamespaceOption {
	Comment(Strand),
}

fn namespace_opts(i: &str) -> IResult<&str, DefineNamespaceOption> {
	namespace_comment(i)
}

fn namespace_comment(i: &str) -> IResult<&str, DefineNamespaceOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineNamespaceOption::Comment(v)))
}
