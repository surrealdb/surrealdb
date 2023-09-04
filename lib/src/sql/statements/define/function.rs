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
use crate::sql::common::closeparentheses;
use crate::sql::common::commas;
use crate::sql::common::openparentheses;
use crate::sql::ending;
use crate::sql::error::expected;
use crate::sql::error::IResult;
use crate::sql::fmt::is_pretty;
use crate::sql::fmt::pretty_indent;
use crate::sql::ident;
use crate::sql::ident::{ident, Ident};
use crate::sql::kind::{kind, Kind};
use crate::sql::permission::{permission, Permission};
use crate::sql::strand::{strand, Strand};
use crate::sql::util::delimited_list0;
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::cut;
use nom::multi::many0;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineFunctionStatement {
	pub name: Ident,
	pub args: Vec<(Ident, Kind)>,
	pub block: Block,
	pub comment: Option<Strand>,
	pub permissions: Permission,
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
		let key = crate::key::database::fc::new(opt.ns(), opt.db(), &self.name);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
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

pub fn function(i: &str) -> IResult<&str, DefineFunctionStatement> {
	let (i, _) = tag_no_case("FUNCTION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag("fn::")(i)?;
	let (i, name) = ident::multi(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, args) = delimited_list0(
		openparentheses,
		commas,
		|i| {
			let (i, _) = char('$')(i)?;
			let (i, name) = ident(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = char(':')(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, kind) = kind(i)?;
			Ok((i, (name, kind)))
		},
		closeparentheses,
	)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, block) = block(i)?;
	let (i, opts) = many0(function_opts)(i)?;
	let (i, _) = expected("PERMISSIONS or COMMENT", ending::query)(i)?;
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
			DefineFunctionOption::Permissions(v) => {
				res.permissions = v;
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineFunctionOption {
	Comment(Strand),
	Permissions(Permission),
}

fn function_opts(i: &str) -> IResult<&str, DefineFunctionOption> {
	alt((function_comment, function_permissions))(i)
}

fn function_comment(i: &str) -> IResult<&str, DefineFunctionOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineFunctionOption::Comment(v)))
}

fn function_permissions(i: &str) -> IResult<&str, DefineFunctionOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PERMISSIONS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(permission)(i)?;
	Ok((i, DefineFunctionOption::Permissions(v)))
}
