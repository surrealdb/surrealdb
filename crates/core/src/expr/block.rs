use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;

use reblessive::tree::Stk;
use revision::revisioned;

use super::FlowResult;
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::fmt::{Fmt, Pretty, is_pretty, pretty_indent};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Value};
use crate::val::Strand;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Block(pub Vec<Expr>);

impl Deref for Block {
	type Target = [Expr];
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Block {
	/// Check if this block does only reads.
	pub(crate) fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.read_only())
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		// Duplicate context
		let mut ctx = Some(MutableContext::new(ctx).freeze());
		// Loop over the statements
		let mut res = Value::None;
		for v in self.iter() {
			match v {
				Expr::Let(x) => res = x.compute(stk, &mut ctx, opt, doc).await?,
				v => res = stk.run(|stk| v.compute(stk, ctx.as_ref().unwrap(), opt, doc)).await?,
			}
		}
		// Return nothing
		Ok(res)
	}
}

impl Display for Block {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		match (self.len(), self.first()) {
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

impl InfoStructure for Block {
	fn structure(self) -> Value {
		// TODO: Null byte validity
		Value::Strand(Strand::new(self.to_string()).unwrap())
	}
}
