use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::parameterize::expr_to_optional_ident;
use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefaultConfig {
	pub namespace: Expr,
	pub database: Expr,
}

impl Default for DefaultConfig {
	fn default() -> Self {
		Self {
			namespace: Expr::Literal(Literal::None),
			database: Expr::Literal(Literal::None),
		}
	}
}

impl DefaultConfig {
	#[instrument(level = "trace", name = "DefaultConfig::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> anyhow::Result<crate::catalog::DefaultConfig> {
		let namespace = match &self.namespace {
			Expr::Literal(Literal::None) => None,
			x => expr_to_optional_ident(stk, ctx, opt, doc, x, "namespace").await?,
		};

		let database = match &self.database {
			Expr::Literal(Literal::None) => None,
			x => expr_to_optional_ident(stk, ctx, opt, doc, x, "database").await?,
		};

		Ok(crate::catalog::DefaultConfig {
			namespace,
			database,
		})
	}
}
