use std::fmt::{self, Display, Formatter, Write};

use surrealdb_types::{SqlFormat, ToSql};
use crate::fmt::{Fmt, Pretty, is_pretty, pretty_indent};
use crate::sql::Expr;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Block(pub(crate) Vec<Expr>);

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

impl Display for Block {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		match self.0.len() {
			0 => f.write_str("{;}"),
			1 => {
				let v = &self.0[0];
				write!(f, "{{ {v} }}")
			}
			l => {
				f.write_char('{')?;
				if l > 1 {
					f.write_char('\n')?;
				} else if !is_pretty() {
					f.write_char(' ')?;
				}
				let indent = pretty_indent();
				if is_pretty() {
					write!(
						f,
						"{}",
						&Fmt::two_line_separated(
							self.0.iter().map(|args| Fmt::new(args, |v, f| write!(f, "{};", v))),
						)
					)?;
				} else {
					write!(
						f,
						"{}",
						&Fmt::one_line_separated(
							self.0.iter().map(|args| Fmt::new(args, |v, f| write!(f, "{};", v))),
						)
					)?;
				}
				drop(indent);
				if l > 1 {
					f.write_char('\n')?;
				} else if !is_pretty() {
					f.write_char(' ')?;
				}
				f.write_char('}')
			}
		}
	}
}

impl ToSql for Block {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self.0.len() {
			0 => f.push_str("{;}"),
			1 => {
				let start_len = f.len();
				f.push_str("{ ");
				self.0[0].fmt_sql(f, fmt);
				// Check if the statement added newlines (pretty-formatted itself)
				let content = &f[start_len..];
				if fmt.is_pretty() && content.contains('\n') {
					f.push_str("\n ");
				} else {
					f.push(' ');
				}
				f.push('}');
			}
			_ => {
				f.push('{');
				
				let inner_fmt = fmt.increment();
				if fmt.is_pretty() {
					// Pretty mode: two line separated with semicolons - \n\n before each, \n<indent>\n after all
					for expr in self.0.iter() {
						f.push('\n');
						f.push('\n');
						inner_fmt.write_indent(f);
						expr.fmt_sql(f, inner_fmt);
						f.push(';');
					}
					f.push('\n');
					fmt.write_indent(f);  // Write current level indent (one less than inner)
					f.push('\n');
				} else {
					// Single line: one line separated with semicolons
					f.push('\n');
					for (i, expr) in self.0.iter().enumerate() {
						if i > 0 {
							f.push('\n');
						}
						expr.fmt_sql(f, inner_fmt);
						f.push(';');
					}
					f.push('\n');
				}
				
				f.push('}');
			}
		}
	}
}
