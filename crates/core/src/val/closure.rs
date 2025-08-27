use std::cmp::Ordering;
use std::fmt;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::revisioned;

use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Expr, FlowResultExt, Ident, Kind};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Closure {
	pub args: Vec<(Ident, Kind)>,
	pub returns: Option<Kind>,
	pub body: Expr,
}

impl PartialOrd for Closure {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}
impl Ord for Closure {
	fn cmp(&self, _: &Self) -> Ordering {
		Ordering::Equal
	}
}

impl Closure {
	pub fn read_only(&self) -> bool {
		self.body.read_only()
	}

	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		args: Vec<Value>,
	) -> Result<Value> {
		let mut ctx = MutableContext::new_isolated(ctx);
		for (i, (name, kind)) in self.args.iter().enumerate() {
			match (kind, args.get(i)) {
				(Kind::Option(_), None) => continue,
				(_, None) => {
					bail!(Error::InvalidArguments {
						name: "ANONYMOUS".to_string(),
						message: format!("Expected a value for ${name}"),
					})
				}
				(kind, Some(val)) => {
					if let Ok(val) = val.to_owned().coerce_to_kind(kind) {
						ctx.add_value(name.to_string(), val.into());
					} else {
						bail!(Error::InvalidArguments {
							name: "ANONYMOUS".to_string(),
							message: format!(
								"Expected a value of type '{kind}' for argument ${name}"
							),
						});
					}
				}
			}
		}

		let ctx = ctx.freeze();
		let result = stk.run(|stk| self.body.compute(stk, &ctx, opt, doc)).await.catch_return()?;
		if let Some(returns) = &self.returns {
			result
				.coerce_to_kind(returns)
				.map_err(|e| Error::ReturnCoerce {
					name: "ANONYMOUS".to_string(),
					error: Box::new(e),
				})
				.map_err(anyhow::Error::new)
		} else {
			Ok(result)
		}
	}
}

impl fmt::Display for Closure {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("|")?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "${name}: ")?;
			match kind {
				k @ Kind::Either(_) => write!(f, "<{k}>")?,
				k => write!(f, "{k}")?,
			}
		}
		f.write_str("|")?;
		if let Some(returns) = &self.returns {
			write!(f, " -> {returns}")?;
		}
		write!(f, " {}", self.body)
	}
}
