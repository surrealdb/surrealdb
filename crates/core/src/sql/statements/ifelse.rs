use surrealdb_types::{SqlFormat, ToSql};

use crate::sql::Expr;

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

impl ToSql for IfelseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		// Always use the bracketed logic since we don't have a separate Display implementation
		for (i, (cond, then)) in self.exprs.iter().enumerate() {
			if i > 0 {
				if fmt.is_pretty() {
					f.push('\n');
					f.push_str("ELSE ");
				} else {
					f.push_str(" ELSE ");
				}
			}
			if fmt.is_pretty() && self.bracketed() {
				f.push_str("IF ");
				cond.fmt_sql(f, fmt);
				let inner_fmt = fmt.increment();
				f.push('\n');
				inner_fmt.write_indent(f);
				then.fmt_sql(f, inner_fmt);
			} else {
				f.push_str("IF ");
				cond.fmt_sql(f, fmt);
				f.push(' ');
				then.fmt_sql(f, fmt);
			}
		}
		if let Some(ref v) = self.close {
			if fmt.is_pretty() && self.bracketed() {
				f.push('\n');
				f.push_str("ELSE");
				let inner_fmt = fmt.increment();
				f.push('\n');
				inner_fmt.write_indent(f);
				v.fmt_sql(f, inner_fmt);
			} else {
				f.push_str(" ELSE ");
				v.fmt_sql(f, fmt);
			}
		}
	}
}
