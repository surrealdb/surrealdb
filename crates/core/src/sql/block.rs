use std::fmt::{self, Display, Formatter, Write};

use crate::fmt::{Pretty, is_pretty, pretty_indent, pretty_sequence_item};
use crate::sql::{Expr, Literal};

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

impl Display for Block {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		match self.0.len() {
			0 => f.write_str("{;}"),
			1 => {
				let v = &self.0[0];
				if let Expr::Literal(Literal::RecordId(_)) = v {
					write!(f, "{{ ({v}) }}")
				} else {
					write!(f, "{{ {v} }}")
				}
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
					for (idx, x) in self.0.iter().enumerate() {
						if idx > 0 {
							f.write_char('\n')?;
							pretty_sequence_item();
						}

						if idx == 0
							&& let Expr::Literal(Literal::RecordId(_)) = x
						{
							write!(f, "({});", x)?;
						} else {
							write!(f, "{};", x)?;
						}
					}
				} else {
					for (idx, x) in self.0.iter().enumerate() {
						if idx > 0 {
							f.write_char('\n')?;
						}

						if idx == 0
							&& let Expr::Literal(Literal::RecordId(_)) = x
						{
							write!(f, "({});", x)?;
						} else {
							write!(f, "{};", x)?;
						}
					}
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
