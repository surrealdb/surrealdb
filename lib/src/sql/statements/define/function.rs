use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::base::Base;
use crate::sql::block::{block, Block};
use crate::sql::comment::{mightbespace, shouldbespace};
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::ident;
use crate::sql::ident::{ident, Ident};
use crate::sql::kind::{kind, Kind};
use crate::sql::strand::{strand, Strand};
use crate::sql::value::Value;
use derive::Store;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::multi::many0;
use nom::multi::separated_list0;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineFunctionStatement {
	pub name: Ident,
	pub args: Vec<(Ident, Kind)>,
	pub block: Block,
	pub comment: Option<Strand>,
}

impl DefineFunctionStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Function, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Process the statement
		let db = run.add_and_cache_db(opt.ns(), opt.db(), opt.strict).await?;
		let db = db.id.unwrap();
		let ns = run.add_and_cache_ns(opt.ns(), opt.strict).await?;
		let ns = ns.id.unwrap();
		let key = crate::key::database::fc::new(ns, db, &self.name);
		run.set(key, self).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl fmt::Display for DefineFunctionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FUNCTION fn::{}(", self.name)?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "${name}: {kind}")?;
		}
		f.write_str(") ")?;
		Display::fmt(&self.block, f)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

pub fn function(i: &str) -> IResult<&str, DefineFunctionStatement> {
	let (i, _) = tag_no_case("DEFINE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FUNCTION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag("fn::")(i)?;
	let (i, name) = ident::multi(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('(')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, args) = separated_list0(commas, |i| {
		let (i, _) = char('$')(i)?;
		let (i, name) = ident(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = char(':')(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, kind) = kind(i)?;
		Ok((i, (name, kind)))
	})(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(')')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, block) = block(i)?;
	let (i, opts) = many0(function_opts)(i)?;
	// Create the base statement
	let mut res = DefineFunctionStatement {
		name,
		args,
		block,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineFunctionOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineFunctionOption {
	Comment(Strand),
}

fn function_opts(i: &str) -> IResult<&str, DefineFunctionOption> {
	function_comment(i)
}

fn function_comment(i: &str) -> IResult<&str, DefineFunctionOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = strand(i)?;
	Ok((i, DefineFunctionOption::Comment(v)))
}
