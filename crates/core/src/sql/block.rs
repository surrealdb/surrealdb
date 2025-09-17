use std::fmt::{self, Display, Formatter, Write};

use crate::sql::Expr;
use crate::sql::fmt::{Fmt, Pretty, is_pretty, pretty_indent};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Block(pub Vec<Expr>);

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
		match (self.0.len(), self.0.first()) {
			(0, _) => f.write_str("{}"),
			(1, Some(v)) => {
				write!(f, "{{ {v} }}")
			}
			(l, _) => {
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
