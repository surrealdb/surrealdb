use std::cmp::Ordering;
use std::fmt;

use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::ParameterCapturePass;
use crate::expr::{Expr, Kind, Param};
use crate::val::{Closure, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ClosureExpr {
	pub args: Vec<(Param, Kind)>,
	pub returns: Option<Kind>,
	pub body: Expr,
}

impl PartialOrd for ClosureExpr {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}
impl Ord for ClosureExpr {
	fn cmp(&self, _: &Self) -> Ordering {
		Ordering::Equal
	}
}

impl ClosureExpr {
	pub(crate) async fn compute(&self, ctx: &Context) -> Result<Value> {
		let captures = ParameterCapturePass::capture(ctx, &self.body);

		Ok(Value::Closure(Box::new(Closure {
			args: self.args.clone(),
			returns: self.returns.clone(),
			captures,
			body: self.body.clone(),
		})))
	}
}

impl fmt::Display for ClosureExpr {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("|")?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "{name}: ")?;
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
