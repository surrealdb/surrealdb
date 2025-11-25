use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

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

impl ToSql for Timeout {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "TIMEOUT {}", self.0)
	}
}
