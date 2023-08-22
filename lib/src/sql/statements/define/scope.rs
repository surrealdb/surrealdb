use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::base::Base;
use crate::sql::comment::shouldbespace;
use crate::sql::duration::{duration, Duration};
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::strand::{strand, Strand};
use crate::sql::value::{value, Value};
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::multi::many0;
use rand::distributions::Alphanumeric;
use rand::Rng;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineScopeStatement {
	pub name: Ident,
	pub code: String,
	pub session: Option<Duration>,
	pub signup: Option<Value>,
	pub signin: Option<Value>,
	pub comment: Option<Strand>,
}

impl DefineScopeStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Scope, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Process the statement
		let ns = run.add_ns(opt.ns(), opt.strict).await?;
		let ns = ns.id.unwrap();
		let db = run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		let db = db.id.unwrap();
		let key = crate::key::database::sc::new(ns, db, &self.name);
		run.set(key, self).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineScopeStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE SCOPE {}", self.name)?;
		if let Some(ref v) = self.session {
			write!(f, " SESSION {v}")?
		}
		if let Some(ref v) = self.signup {
			write!(f, " SIGNUP {v}")?
		}
		if let Some(ref v) = self.signin {
			write!(f, " SIGNIN {v}")?
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

pub fn scope(i: &str) -> IResult<&str, DefineScopeStatement> {
	let (i, _) = tag_no_case("DEFINE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SCOPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident(i)?;
	let (i, opts) = many0(scope_opts)(i)?;
	// Create the base statement
	let mut res = DefineScopeStatement {
		name,
		code: rand::thread_rng()
			.sample_iter(&Alphanumeric)
			.take(128)
			.map(char::from)
			.collect::<String>(),
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineScopeOption::Session(v) => {
				res.session = Some(v);
			}
			DefineScopeOption::Signup(v) => {
				res.signup = Some(v);
			}
			DefineScopeOption::Signin(v) => {
				res.signin = Some(v);
			}
			DefineScopeOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineScopeOption {
	Session(Duration),
	Signup(Value),
	Signin(Value),
	Comment(Strand),
}

fn scope_opts(i: &str) -> IResult<&str, DefineScopeOption> {
	alt((scope_session, scope_signup, scope_signin, scope_comment))(i)
}

fn scope_session(i: &str) -> IResult<&str, DefineScopeOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SESSION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = duration(i)?;
	Ok((i, DefineScopeOption::Session(v)))
}

fn scope_signup(i: &str) -> IResult<&str, DefineScopeOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SIGNUP")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	Ok((i, DefineScopeOption::Signup(v)))
}

fn scope_signin(i: &str) -> IResult<&str, DefineScopeOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SIGNIN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	Ok((i, DefineScopeOption::Signin(v)))
}

fn scope_comment(i: &str) -> IResult<&str, DefineScopeOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = strand(i)?;
	Ok((i, DefineScopeOption::Comment(v)))
}
