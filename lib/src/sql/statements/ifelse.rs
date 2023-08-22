use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::block::block;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::fmt::{fmt_separated_by, is_pretty, pretty_indent, Fmt, Pretty};
use crate::sql::value::{value, Value};
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::combinator::opt;
use nom::multi::separated_list1;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct IfelseStatement {
	pub exprs: Vec<(Value, Value)>,
	pub close: Option<Value>,
}

impl IfelseStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		for (cond, then) in self.exprs.iter() {
			if cond.writeable() || then.writeable() {
				return true;
			}
		}
		self.close.as_ref().map_or(false, |v| v.writeable())
	}
	/// Check if we require a writeable transaction
	pub(crate) fn bracketed(&self) -> bool {
		self.exprs.iter().all(|(_, v)| matches!(v, Value::Block(_)))
			&& (self.close.as_ref().is_none()
				|| self.close.as_ref().is_some_and(|v| matches!(v, Value::Block(_))))
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
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
		let mut f = Pretty::from(f);
		match self.bracketed() {
			true => {
				write!(
					f,
					"{}",
					&Fmt::new(
						self.exprs.iter().map(|args| {
							Fmt::new(args, |(cond, then), f| {
								if is_pretty() {
									write!(f, "IF {cond}")?;
									let indent = pretty_indent();
									write!(f, "{then}")?;
									drop(indent);
								} else {
									write!(f, "IF {cond} {then}")?;
								}
								Ok(())
							})
						}),
						if is_pretty() {
							fmt_separated_by("ELSE")
						} else {
							fmt_separated_by(" ELSE ")
						},
					),
				)?;
				if let Some(ref v) = self.close {
					if is_pretty() {
						write!(f, "ELSE")?;
						let indent = pretty_indent();
						write!(f, "{v}")?;
						drop(indent);
					} else {
						write!(f, " ELSE {v}")?;
					}
				}
				Ok(())
			}
			false => {
				write!(
					f,
					"{}",
					&Fmt::new(
						self.exprs.iter().map(|args| {
							Fmt::new(args, |(cond, then), f| {
								if is_pretty() {
									write!(f, "IF {cond} THEN")?;
									let indent = pretty_indent();
									write!(f, "{then}")?;
									drop(indent);
								} else {
									write!(f, "IF {cond} THEN {then}")?;
								}
								Ok(())
							})
						}),
						if is_pretty() {
							fmt_separated_by("ELSE")
						} else {
							fmt_separated_by(" ELSE ")
						},
					),
				)?;
				if let Some(ref v) = self.close {
					if is_pretty() {
						write!(f, "ELSE")?;
						let indent = pretty_indent();
						write!(f, "{v}")?;
						drop(indent);
					} else {
						write!(f, " ELSE {v}")?;
					}
				}
				if is_pretty() {
					f.write_str("END")?;
				} else {
					f.write_str(" END")?;
				}
				Ok(())
			}
		}
	}
}

pub fn ifelse(i: &str) -> IResult<&str, IfelseStatement> {
	alt((worded, bracketed))(i)
}

fn worded(i: &str) -> IResult<&str, IfelseStatement> {
	//
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
	//
	fn close(i: &str) -> IResult<&str, Value> {
		let (i, _) = shouldbespace(i)?;
		let (i, _) = tag_no_case("ELSE")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, then) = value(i)?;
		Ok((i, then))
	}
	//
	fn split(i: &str) -> IResult<&str, ()> {
		let (i, _) = shouldbespace(i)?;
		let (i, _) = tag_no_case("ELSE")(i)?;
		let (i, _) = shouldbespace(i)?;
		Ok((i, ()))
	}
	//
	let (i, exprs) = separated_list1(split, exprs)(i)?;
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

fn bracketed(i: &str) -> IResult<&str, IfelseStatement> {
	//
	fn exprs(i: &str) -> IResult<&str, (Value, Value)> {
		let (i, _) = tag_no_case("IF")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, cond) = value(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, then) = map(block, Value::from)(i)?;
		Ok((i, (cond, then)))
	}
	//
	fn close(i: &str) -> IResult<&str, Value> {
		let (i, _) = shouldbespace(i)?;
		let (i, _) = tag_no_case("ELSE")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, then) = map(block, Value::from)(i)?;
		Ok((i, then))
	}
	//
	fn split(i: &str) -> IResult<&str, ()> {
		let (i, _) = shouldbespace(i)?;
		let (i, _) = tag_no_case("ELSE")(i)?;
		let (i, _) = shouldbespace(i)?;
		Ok((i, ()))
	}
	//
	let (i, exprs) = separated_list1(split, exprs)(i)?;
	let (i, close) = opt(close)(i)?;
	Ok((
		i,
		IfelseStatement {
			exprs,
			close,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn ifelse_worded_statement_first() {
		let sql = "IF this THEN that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_worded_statement_close() {
		let sql = "IF this THEN that ELSE that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_worded_statement_other() {
		let sql = "IF this THEN that ELSE IF this THEN that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_worded_statement_other_close() {
		let sql = "IF this THEN that ELSE IF this THEN that ELSE that END";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_bracketed_statement_first() {
		let sql = "IF this { that }";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_bracketed_statement_close() {
		let sql = "IF this { that } ELSE { that }";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_bracketed_statement_other() {
		let sql = "IF this { that } ELSE IF this { that }";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_bracketed_statement_other_close() {
		let sql = "IF this { that } ELSE IF this { that } ELSE { that }";
		let res = ifelse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
