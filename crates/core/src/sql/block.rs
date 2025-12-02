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
				let v = &self.0[0];
				if fmt.is_pretty() {
					// Pretty mode: use expanded format even for single element
					f.push('{');
					f.push('\n');
					f.push('\n');
					let fmt = fmt.increment();
					fmt.write_indent(f);
					v.fmt_sql(f, fmt);
					f.push('\n');
					// Write indent at the block's level
					if let SqlFormat::Indented(level) = fmt
						&& level > 0
					{
						for _ in 0..(level - 1) {
							f.push('\t');
						}
					}
					f.push('}')
				} else {
					// Non-pretty: compact format
					f.push_str("{ ");
					v.fmt_sql(f, fmt);
					f.push_str(" }");
				}
			}
			_ => {
				// Multi-element blocks
				if fmt.is_pretty() {
					f.push('{');
					f.push('\n');
					f.push('\n');
					let fmt = fmt.increment();
					for (i, v) in self.0.iter().enumerate() {
						if i > 0 {
							f.push('\n');
							f.push('\n');
						}
						fmt.write_indent(f);
						v.fmt_sql(f, fmt);
						f.push(';');
					}
					f.push('\n');
					// Write indent at the block's level (not the content level)
					// The content was at fmt (incremented), so block's level is one less
					if let SqlFormat::Indented(level) = fmt
						&& level > 0
					{
						for _ in 0..(level - 1) {
							f.push('\t');
						}
					}
					f.push('}')
				} else {
					// Non-pretty: all on one line with space separation
					f.push_str("{ ");
					for (i, v) in self.0.iter().enumerate() {
						if i > 0 {
							f.push(' ');
						}
						v.fmt_sql(f, fmt);
						f.push(';');
					}
					f.push_str(" }")
				}
			}
		}
	}
}
