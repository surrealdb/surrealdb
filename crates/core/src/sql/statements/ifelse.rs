use std::fmt::{self, Display, Write};

use crate::sql::Expr;
use crate::sql::fmt::{Fmt, Pretty, fmt_separated_by, is_pretty, pretty_indent};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct IfelseStatement {
	/// The first if condition followed by a body, followed by any number of
	/// else if's
	pub exprs: Vec<(Expr, Expr)>,
	/// the final else body, if there is one
	pub close: Option<Expr>,
}

impl IfelseStatement {
	/// Check if the statement is bracketed
	pub(crate) fn bracketed(&self) -> bool {
		self.exprs.iter().all(|(_, v)| matches!(v, Expr::Block(_)))
			&& self.close.as_ref().map(|v| matches!(v, Expr::Block(_))).unwrap_or(true)
	}
}

impl From<IfelseStatement> for crate::expr::statements::IfelseStatement {
	fn from(v: IfelseStatement) -> Self {
		crate::expr::statements::IfelseStatement {
			exprs: v.exprs.into_iter().map(|(a, b)| (From::from(a), From::from(b))).collect(),
			close: v.close.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::IfelseStatement> for IfelseStatement {
	fn from(v: crate::expr::statements::IfelseStatement) -> Self {
		IfelseStatement {
			exprs: v.exprs.into_iter().map(|(a, b)| (From::from(a), From::from(b))).collect(),
			close: v.close.map(Into::into),
		}
	}
}

impl Display for IfelseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		if self.bracketed() {
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
		} else {
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

#[cfg(test)]
mod tests {
	use crate::syn;

	#[test]
	fn format_pretty() {
		let query = syn::parse("IF 1 { 1 } ELSE IF 2 { 2 }").unwrap();
		assert_eq!(format!("{}", query), "IF 1 { 1 } ELSE IF 2 { 2 };");
		assert_eq!(format!("{:#}", query), "IF 1\n\t{ 1 }\nELSE IF 2\n\t{ 2 }\n;");
	}
}
