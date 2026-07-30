use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::define::config::defaults::DefaultConfig;
use crate::expr::{Expr, Literal};
use crate::legacy::expr_to_optional_ident;

#[instrument(level = "trace", name = "DefaultConfig::compute", skip_all)]
pub(crate) async fn default_config_compute(
	this: &DefaultConfig,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> anyhow::Result<crate::catalog::DefaultConfig> {
	let namespace = match &this.namespace {
		Expr::Literal(Literal::None) => None,
		x => expr_to_optional_ident(stk, ctx, opt, doc, x, "namespace").await?,
	};

	let database = match &this.database {
		Expr::Literal(Literal::None) => None,
		x => expr_to_optional_ident(stk, ctx, opt, doc, x, "database").await?,
	};

	Ok(crate::catalog::DefaultConfig {
		namespace,
		database,
	})
}
