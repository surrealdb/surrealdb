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
use crate::sql::error::expect_tag_no_case;
use crate::sql::error::expected;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::strand::{strand, Strand};
use crate::sql::value::{value, values, Value, Values};
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use nom::combinator::opt;
use nom::multi::many0;
use nom::sequence::tuple;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineEventStatement {
	pub name: Ident,
	pub what: Ident,
	pub when: Value,
	pub then: Values,
	pub comment: Option<Strand>,
}

impl DefineEventStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Process the statement
		let key = crate::key::table::ev::new(opt.ns(), opt.db(), &self.what, &self.name);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		run.add_tb(opt.ns(), opt.db(), &self.what, opt.strict).await?;
		run.set(key, self).await?;
		// Clear the cache
		let key = crate::key::table::ev::prefix(opt.ns(), opt.db(), &self.what);
		run.clr(key).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineEventStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"DEFINE EVENT {} ON {} WHEN {} THEN {}",
			self.name, self.what, self.when, self.then
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

pub fn event(i: &str) -> IResult<&str, DefineEventStatement> {
	let (i, _) = tag_no_case("EVENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, what, opts)) = cut(|i| {
		let (i, name) = ident(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, what) = ident(i)?;
		let (i, opts) = many0(event_opts)(i)?;
		let (i, _) = expected("WHEN, THEN, or COMMENT", ending::query)(i)?;
		Ok((i, (name, what, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineEventStatement {
		name,
		what,
		when: Value::Bool(true),
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineEventOption::When(v) => {
				res.when = v;
			}
			DefineEventOption::Then(v) => {
				res.then = v;
			}
			DefineEventOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Check necessary options
	if res.then.is_empty() {
		// TODO throw error
	}
	// Return the statement
	Ok((i, res))
}

enum DefineEventOption {
	When(Value),
	Then(Values),
	Comment(Strand),
}

fn event_opts(i: &str) -> IResult<&str, DefineEventOption> {
	alt((event_when, event_then, event_comment))(i)
}

fn event_when(i: &str) -> IResult<&str, DefineEventOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("WHEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineEventOption::When(v)))
}

fn event_then(i: &str) -> IResult<&str, DefineEventOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("THEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(values)(i)?;
	Ok((i, DefineEventOption::Then(v)))
}

fn event_comment(i: &str) -> IResult<&str, DefineEventOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineEventOption::Comment(v)))
}
