use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::ident_raw;
use crate::sql::value::{value, Value};
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::cut;
use nom::combinator::opt;
use nom::sequence::{preceded, terminated};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct SetStatement {
	pub name: String,
	pub what: Value,
}

impl SetStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.what.writeable()
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Check if the variable is a protected variable
		match PROTECTED_PARAM_NAMES.contains(&self.name.as_str()) {
			// The variable isn't protected and can be stored
			false => self.what.compute(ctx, opt, txn, doc).await,
			// The user tried to set a protected variable
			true => Err(Error::InvalidParam {
				// Move the parameter name, as we no longer need it
				name: self.name.to_owned(),
			}),
		}
	}
}

impl fmt::Display for SetStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LET ${} = {}", self.name, self.what)
	}
}

pub fn set(i: &str) -> IResult<&str, SetStatement> {
	let (i, _) = opt(terminated(tag_no_case("LET"), shouldbespace))(i)?;
	let (i, n) = preceded(char('$'), cut(ident_raw))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('=')(i)?;
	let (i, w) = cut(|i| {
		let (i, _) = mightbespace(i)?;
		value(i)
	})(i)?;
	Ok((
		i,
		SetStatement {
			name: n,
			what: w,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn let_statement() {
		let sql = "LET $name = NULL";
		let res = set(sql);
		let out = res.unwrap().1;
		assert_eq!("LET $name = NULL", format!("{}", out));
	}
}
