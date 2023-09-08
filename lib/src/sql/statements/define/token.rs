use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::algorithm::{algorithm, Algorithm};
use crate::sql::base::{base_or_scope, Base};
use crate::sql::comment::shouldbespace;
use crate::sql::ending;
use crate::sql::error::expect_tag_no_case;
use crate::sql::error::expected;
use crate::sql::error::IResult;
use crate::sql::escape::quote_str;
use crate::sql::ident::{ident, Ident};
use crate::sql::strand::{strand, strand_raw, Strand};
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
pub struct DefineTokenStatement {
	pub name: Ident,
	pub base: Base,
	pub kind: Algorithm,
	pub code: String,
	pub comment: Option<Strand>,
}

impl DefineTokenStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

		match &self.base {
			Base::Ns => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::namespace::tk::new(opt.ns(), &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.set(key, self).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::database::tk::new(opt.ns(), opt.db(), &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.add_db(opt.ns(), opt.db(), opt.strict).await?;
				run.set(key, self).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Sc(sc) => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::scope::tk::new(opt.ns(), opt.db(), sc, &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.add_db(opt.ns(), opt.db(), opt.strict).await?;
				run.add_sc(opt.ns(), opt.db(), sc, opt.strict).await?;
				run.set(key, self).await?;
				// Ok all good
				Ok(Value::None)
			}
			// Other levels are not supported
			_ => Err(Error::InvalidLevel(self.base.to_string())),
		}
	}
}

impl Display for DefineTokenStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"DEFINE TOKEN {} ON {} TYPE {} VALUE {}",
			self.name,
			self.base,
			self.kind,
			quote_str(&self.code)
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

pub fn token(i: &str) -> IResult<&str, DefineTokenStatement> {
	let (i, _) = tag_no_case("TOKEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, base, opts)) = cut(|i| {
		let (i, name) = ident(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, base) = base_or_scope(i)?;
		let (i, opts) = many0(token_opts)(i)?;
		let (i, _) = expected("TYPE, VALUE, or COMMENT", ending::query)(i)?;
		Ok((i, (name, base, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineTokenStatement {
		name,
		base,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineTokenOption::Type(v) => {
				res.kind = v;
			}
			DefineTokenOption::Value(v) => {
				res.code = v;
			}
			DefineTokenOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Check necessary options
	if res.code.is_empty() {
		// TODO throw error
	}
	// Return the statement
	Ok((i, res))
}

enum DefineTokenOption {
	Type(Algorithm),
	Value(String),
	Comment(Strand),
}

fn token_opts(i: &str) -> IResult<&str, DefineTokenOption> {
	alt((token_type, token_value, token_comment))(i)
}

fn token_type(i: &str) -> IResult<&str, DefineTokenOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TYPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(algorithm)(i)?;
	Ok((i, DefineTokenOption::Type(v)))
}

fn token_value(i: &str) -> IResult<&str, DefineTokenOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand_raw)(i)?;
	Ok((i, DefineTokenOption::Value(v)))
}

fn token_comment(i: &str) -> IResult<&str, DefineTokenOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineTokenOption::Comment(v)))
}
