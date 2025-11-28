use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::Fmt;
use crate::sql::Expr;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Block(
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub(crate) Vec<Expr>,
);

impl From<Block> for crate::expr::Block {
	fn from(v: Block) -> Self {
		crate::expr::Block(v.0.into_iter().map(Into::into).collect())
	}
}
impl From<crate::expr::Block> for Block {
	fn from(v: crate::expr::Block) -> Self {
		Block(v.0.into_iter().map(Into::into).collect())
	}
}

impl ToSql for Block {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self.0.len() {
			0 => f.push_str("{;}"),
			1 => {
				let v = &self.0[0];
				// Check if we need special formatting for statement-like expressions
				let needs_newline =
					fmt.is_pretty() && matches!(v, Expr::IfElse(_) | Expr::Foreach(_));

				f.push_str("{ ");
				v.fmt_sql(f, fmt);
				if needs_newline {
					f.push_str("\n ");
				} else {
					f.push(' ');
				}
				f.push('}');
			}
			l => {
				f.push('{');
				if l > 1 {
					f.push('\n');
				} else if !fmt.is_pretty() {
					f.push(' ');
				}
				let fmt = fmt.increment();
				if fmt.is_pretty() {
					f.push('\n');
					write_sql!(
						f,
						fmt,
						"{}",
						&Fmt::two_line_separated(
							self.0.iter().map(|args| Fmt::new(args, |v, f, fmt| write_sql!(
								f, fmt, "{};", v
							))),
						)
					);
					f.push('\n');
					// Write indent at the block's level (not the content level)
					// The content was at fmt (incremented), so block's level is one less
					if let SqlFormat::Indented(level) = fmt {
						if level > 0 {
							for _ in 0..(level - 1) {
								f.push('\t');
							}
						}
					}
					f.push('\n');
				} else {
					write_sql!(
						f,
						fmt,
						"{}",
						&Fmt::one_line_separated(
							self.0.iter().map(|args| Fmt::new(args, |v, f, fmt| write_sql!(
								f, fmt, "{};", v
							))),
						)
					);
				}
				if l > 1 && !fmt.is_pretty() {
					f.push('\n');
				} else if l == 1 && !fmt.is_pretty() {
					f.push(' ');
				}
				f.push('}')
			}
		}
	}
}
