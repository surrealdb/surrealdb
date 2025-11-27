use surrealdb_types::{SqlFormat, ToSql};

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
					// Pretty mode: two line separated with semicolons - \n\n before each,
					// \n<indent>\n after all
					for expr in self.0.iter() {
						f.push('\n');
						f.push('\n');
						inner_fmt.write_indent(f);
						expr.fmt_sql(f, inner_fmt);
						f.push(';');
					}
					f.push('\n');
					fmt.write_indent(f); // Write current level indent (one less than inner)
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
