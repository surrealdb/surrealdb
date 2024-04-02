use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::fmt::{fmt_separated_by, is_pretty, pretty_indent, Fmt, Pretty};
use crate::sql::Value;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct IfelseStatement {
	/// The first if condition followed by a body, followed by any number of else if's
	pub exprs: Vec<(Value, Value)>,
	/// the final else body, if there is one
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
