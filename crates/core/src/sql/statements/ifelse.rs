use crate::sql::SqlValue;
use crate::sql::fmt::{Fmt, Pretty, fmt_separated_by, is_pretty, pretty_indent};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct IfelseStatement {
	/// The first if condition followed by a body, followed by any number of else if's
	pub exprs: Vec<(SqlValue, SqlValue)>,
	/// the final else body, if there is one
	pub close: Option<SqlValue>,
}

impl IfelseStatement {
	/// Check if the statement is bracketed
	pub(crate) fn bracketed(&self) -> bool {
		self.exprs.iter().all(|(_, v)| matches!(v, SqlValue::Block(_)))
			&& (self.close.as_ref().is_none()
				|| self.close.as_ref().is_some_and(|v| matches!(v, SqlValue::Block(_))))
	}
}

impl From<IfelseStatement> for crate::expr::statements::IfelseStatement {
	fn from(v: IfelseStatement) -> Self {
		crate::expr::statements::IfelseStatement {
			exprs: v.exprs.into_iter().map(|(e1, e2)| (e1.into(), e2.into())).collect(),
			close: v.close.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::IfelseStatement> for IfelseStatement {
	fn from(v: crate::expr::statements::IfelseStatement) -> Self {
		IfelseStatement {
			exprs: v.exprs.into_iter().map(|(e1, e2)| (e1.into(), e2.into())).collect(),
			close: v.close.map(Into::into),
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
