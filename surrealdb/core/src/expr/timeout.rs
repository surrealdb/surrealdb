use std::fmt;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, Literal};
use crate::val::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Timeout(pub(crate) Expr);

impl Default for Timeout {
	fn default() -> Self {
		Self(Expr::Literal(Literal::Duration(Duration::default())))
	}
}

impl Timeout {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Duration> {
		Ok(compute_to!(stk, ctx, opt, doc, self.0 => Duration))
	}
}

impl fmt::Display for Timeout {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "TIMEOUT {}", self.0)
	}
}
