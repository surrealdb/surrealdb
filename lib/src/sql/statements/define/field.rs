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
use crate::sql::fmt::is_pretty;
use crate::sql::fmt::pretty_indent;
use crate::sql::ident::{ident, Ident};
use crate::sql::idiom;
use crate::sql::idiom::Idiom;
use crate::sql::kind::{kind, Kind};
use crate::sql::permission::{permissions, Permissions};
use crate::sql::strand::{strand, Strand};
use crate::sql::value::{value, Value};
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use nom::combinator::opt;
use nom::multi::many0;
use nom::sequence::tuple;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	pub flex: bool,
	pub kind: Option<Kind>,
	pub value: Option<Value>,
	pub assert: Option<Value>,
	pub default: Option<Value>,
	pub permissions: Permissions,
	pub comment: Option<Strand>,
}

impl DefineFieldStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Process the statement
		let fd = self.name.to_string();
		let key = crate::key::table::fd::new(opt.ns(), opt.db(), &self.what, &fd);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		run.add_tb(opt.ns(), opt.db(), &self.what, opt.strict).await?;
		run.set(key, self).await?;
		// Clear the cache
		let key = crate::key::table::fd::prefix(opt.ns(), opt.db(), &self.what);
		run.clr(key).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineFieldStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FIELD {} ON {}", self.name, self.what)?;
		if self.flex {
			write!(f, " FLEXIBLE")?
		}
		if let Some(ref v) = self.kind {
			write!(f, " TYPE {v}")?
		}
		if let Some(ref v) = self.default {
			write!(f, " DEFAULT {v}")?
		}
		if let Some(ref v) = self.value {
			write!(f, " VALUE {v}")?
		}
		if let Some(ref v) = self.assert {
			write!(f, " ASSERT {v}")?
		}
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
			write!(f, "{}", self.permissions)?;
		}
		Ok(())
	}
}

pub fn field(i: &str) -> IResult<&str, DefineFieldStatement> {
	let (i, _) = tag_no_case("FIELD")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, what, opts)) = cut(|i| {
		let (i, name) = idiom::local(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, what) = ident(i)?;
		let (i, opts) = many0(field_opts)(i)?;
		let (i, _) = expected(
			"one of FLEX(IBLE), TYPE, VALUE, ASSERT, DEFAULT, or COMMENT",
			cut(ending::query),
		)(i)?;
		Ok((i, (name, what, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineFieldStatement {
		name,
		what,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineFieldOption::Flex => {
				res.flex = true;
			}
			DefineFieldOption::Kind(v) => {
				res.kind = Some(v);
			}
			DefineFieldOption::Value(v) => {
				res.value = Some(v);
			}
			DefineFieldOption::Assert(v) => {
				res.assert = Some(v);
			}
			DefineFieldOption::Default(v) => {
				res.default = Some(v);
			}
			DefineFieldOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineFieldOption::Permissions(v) => {
				res.permissions = v;
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineFieldOption {
	Flex,
	Kind(Kind),
	Value(Value),
	Assert(Value),
	Default(Value),
	Comment(Strand),
	Permissions(Permissions),
}

fn field_opts(i: &str) -> IResult<&str, DefineFieldOption> {
	alt((
		field_flex,
		field_kind,
		field_value,
		field_assert,
		field_default,
		field_comment,
		field_permissions,
	))(i)
}

fn field_flex(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("FLEXIBLE"), tag_no_case("FLEXI"), tag_no_case("FLEX")))(i)?;
	Ok((i, DefineFieldOption::Flex))
}

fn field_kind(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TYPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(kind)(i)?;
	Ok((i, DefineFieldOption::Kind(v)))
}

fn field_value(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineFieldOption::Value(v)))
}

fn field_assert(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ASSERT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineFieldOption::Assert(v)))
}

fn field_default(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("DEFAULT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineFieldOption::Default(v)))
}

fn field_comment(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = strand(i)?;
	Ok((i, DefineFieldOption::Comment(v)))
}

fn field_permissions(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = permissions(i)?;
	Ok((i, DefineFieldOption::Permissions(v)))
}
