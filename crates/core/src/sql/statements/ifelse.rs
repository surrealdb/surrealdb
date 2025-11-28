use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{CoverStmtsSql, Fmt, fmt_separated_by};
use crate::sql::Expr;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct IfelseStatement {
	/// The first if condition followed by a body, followed by any number of
	/// else if's
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
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

impl ToSql for IfelseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if self.bracketed() {
			write_sql!(
				f,
				fmt,
				"{}",
				&Fmt::new(
					self.exprs.iter().map(|args| {
						Fmt::new(args, |(cond, then), f, fmt| {
							if fmt.is_pretty() {
								write_sql!(f, fmt, "IF {}", CoverStmtsSql(cond));
								f.push('\n');
								let fmt = fmt.increment();
								fmt.write_indent(f);
								write_sql!(f, fmt, "{then}");
							} else {
								write_sql!(f, fmt, "IF {} {then}", CoverStmtsSql(cond));
							}
						})
					}),
					if fmt.is_pretty() {
						fmt_separated_by("\nELSE ")
					} else {
						fmt_separated_by(" ELSE ")
					},
				),
			);
			if let Some(ref v) = self.close {
				if fmt.is_pretty() {
					f.push('\n');
					write_sql!(f, fmt, "ELSE");
					f.push('\n');
					let fmt = fmt.increment();
					fmt.write_indent(f);
					write_sql!(f, fmt, "{v}");
				} else {
					write_sql!(f, fmt, " ELSE {v}");
				}
			}
		} else {
			write_sql!(
				f,
				fmt,
				"{}",
				&Fmt::new(
					self.exprs.iter().map(|args| {
						Fmt::new(args, |(cond, then), f, fmt| {
							if fmt.is_pretty() {
								write_sql!(f, fmt, "IF {} THEN", CoverStmtsSql(cond));
								f.push('\n');
								let fmt = fmt.increment();
								fmt.write_indent(f);
								write_sql!(f, fmt, "{then}");
							} else {
								write_sql!(f, fmt, "IF {} THEN {then}", CoverStmtsSql(cond));
							}
						})
					}),
					if fmt.is_pretty() {
						fmt_separated_by("\nELSE ")
					} else {
						fmt_separated_by(" ELSE ")
					},
				),
			);
			if let Some(ref v) = self.close {
				if fmt.is_pretty() {
					f.push('\n');
					write_sql!(f, fmt, "ELSE");
					f.push('\n');
					let fmt = fmt.increment();
					fmt.write_indent(f);
					write_sql!(f, fmt, "{v}");
				} else {
					write_sql!(f, fmt, " ELSE {v}");
				}
			}
			if fmt.is_pretty() {
				write_sql!(f, fmt, "END");
			} else {
				write_sql!(f, fmt, " END");
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::syn;

	#[test]
	fn format_pretty() {
		let query = syn::parse("IF 1 { 1 } ELSE IF 2 { 2 }").unwrap();
		assert_eq!(query.to_sql(), "IF 1 { 1 } ELSE IF 2 { 2 };");
		assert_eq!(query.to_sql_pretty(), "IF 1\n\t{ 1 }\nELSE IF 2\n\t{ 2 }\n;");
	}
}
