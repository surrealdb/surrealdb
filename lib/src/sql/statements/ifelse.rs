use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::fmt::{fmt_separated_by, Fmt};
use crate::sql::value::{value, Value};
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::multi::separated_list0;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct IfelseStatement {
	pub exprs: Vec<(Value, Value)>,
	pub close: Option<Value>,
}

impl IfelseStatement {
	pub(crate) fn writeable(&self) -> bool {
		for (cond, then) in self.exprs.iter() {
			if cond.writeable() || then.writeable() {
				return true;
			}
		}
		self.close.as_ref().map_or(false, |v| v.writeable())
	}

	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		for (ref cond, ref then) in &self.exprs {
			let v = cond.compute(ctx, opt, txn, doc).await?;
			if v.is_truthy() {
				return then.compute(ctx, opt, txn, doc).await;
			}
		}
		match self.close {
			Some(ref v) => v.compute(ctx, opt, txn, doc).await,
			None => Ok(Value::None),
		}
	}
}

impl Display for IfelseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		Display::fmt(
			&Fmt::new(
				self.exprs.iter().map(|args| {
					Fmt::new(args, |(cond, then), f| write!(f, "IF {cond} THEN {then}"))
				}),
				fmt_separated_by(" ELSE "),
			),
			f,
		)?;
		if let Some(ref v) = self.close {
			write!(f, " ELSE {v}")?
		}
		f.write_str(" END")?;
		Ok(())
	}
}

pub fn ifelse(i: &str) -> IResult<&str, IfelseStatement> {
	let (i, exprs) = separated_list0(split, exprs)(i)?;
	let (i, close) = opt(close)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("END")(i)?;
	Ok((
		i,
		IfelseStatement {
			exprs,
			close,
		},
	))
}

fn exprs(i: &str) -> IResult<&str, (Value, Value)> {
	let (i, _) = tag_no_case("IF")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, cond) = value(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("THEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, then) = value(i)?;
	Ok((i, (cond, then)))
}

fn close(i: &str) -> IResult<&str, Value> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ELSE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, then) = value(i)?;
	Ok((i, then))
}

fn split(i: &str) -> IResult<&str, ()> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ELSE")(i)?;
	let (i, _) = shouldbespace(i)?;
	Ok((i, ()))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn ifelse_statement_first() {
		let sql = "IF this THEN that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_statement_close() {
		let sql = "IF this THEN that ELSE that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_statement_other() {
		let sql = "IF this THEN that ELSE IF this THEN that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_statement_other_close() {
		let sql = "IF this THEN that ELSE IF this THEN that ELSE that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
