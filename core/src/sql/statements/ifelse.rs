use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::fmt::{fmt_separated_by, is_pretty, pretty_indent, Fmt, Pretty};
use crate::sql::Value;
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
		self.close.as_ref().is_some_and(|v| v.writeable())
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
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		for (ref cond, ref then) in &self.exprs {
			let v = cond.compute(stk, ctx, opt, doc).await?;
			if v.is_truthy() {
				return then.compute_unbordered(stk, ctx, opt, doc).await;
			}
		}
		match self.close {
			Some(ref v) => v.compute_unbordered(stk, ctx, opt, doc).await,
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
							fmt_separated_by("ELSE ")
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
							fmt_separated_by("ELSE ")
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

#[cfg(test)]
mod tests {
	use crate::syn::parse;

	#[test]
	fn format_pretty() {
		let query = parse("IF 1 { 1 } ELSE IF 2 { 2 }").unwrap();
		assert_eq!(format!("{}", query), "IF 1 { 1 } ELSE IF 2 { 2 };");
		assert_eq!(format!("{:#}", query), "IF 1\n\t{ 1 }\nELSE IF 2\n\t{ 2 }\n;");
	}
}
