use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::MiddlewareDefinition;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::statements::define::config::api::ApiConfig;

#[instrument(level = "trace", name = "ApiConfig::compute", skip_all)]
pub(crate) async fn api_config_compute(
	this: &ApiConfig,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<crate::catalog::ApiConfig> {
	let mut middleware = Vec::new();
	for m in this.middleware.iter() {
		let mut args = Vec::new();
		for arg in m.args.iter() {
			args.push(
				stk.run(|stk| crate::legacy::expr_compute(arg, stk, ctx, opt, doc))
					.await
					.catch_return()?,
			)
		}
		middleware.push(MiddlewareDefinition {
			name: m.name.clone(),
			args,
		});
	}

	Ok(crate::catalog::ApiConfig {
		middleware,
		permissions: this.permissions.clone(),
	})
}
