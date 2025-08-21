use std::fmt::{self, Display, Write};

use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::fmt::{Fmt, Pretty, fmt_separated_by, is_pretty, pretty_indent};
use crate::expr::{Expr, FlowResult, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct IfelseStatement {
	/// The first if condition followed by a body, followed by any number of
	/// else if's
	pub exprs: Vec<(Expr, Expr)>,
	/// the final else body, if there is one
	pub close: Option<Expr>,
}

impl IfelseStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		self.exprs.iter().all(|x| x.0.read_only() && x.1.read_only())
			&& self.close.as_ref().map(|x| x.read_only()).unwrap_or(true)
	}
	/// Check if we require a writeable transaction
	pub(crate) fn bracketed(&self) -> bool {
		self.exprs.iter().all(|(_, v)| matches!(v, Expr::Block(_)))
			&& (self.close.as_ref().is_none()
				|| self.close.as_ref().is_some_and(|v| matches!(v, Expr::Block(_))))
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		for (cond, then) in &self.exprs {
			let v = stk.run(|stk| cond.compute(stk, ctx, opt, doc)).await?;
			if v.is_truthy() {
				return stk.run(|stk| then.compute(stk, ctx, opt, doc)).await;
			}
		}
		match self.close {
			Some(ref v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			None => Ok(Value::None),
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
		let query = syn::expr("IF 1 { 1 } ELSE IF 2 { 2 }").unwrap();
		assert_eq!(format!("{}", query), "IF 1 { 1 } ELSE IF 2 { 2 }");
		assert_eq!(format!("{:#}", query), "IF 1\n\t{ 1 }\nELSE IF 2\n\t{ 2 }");
	}
}
