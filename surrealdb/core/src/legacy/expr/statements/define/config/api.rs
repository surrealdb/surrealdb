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

	// A PERMISSIONS clause must not perform writes (GHSA-66r2-5gwj-gxm2).
	if this.permissions.has_direct_write() {
		return Err(crate::exec::Error::PermissionClauseNotReadonly {
			kind: "config",
			name: "api".to_string(),
		}
		.into());
	}
	crate::fnc::mutability::ensure_guards_call_read_only(
		ctx,
		opt,
		"config",
		"api".to_string(),
		[&this.permissions],
	)
	.await?;

	Ok(crate::catalog::ApiConfig {
		middleware,
		permissions: this.permissions.clone(),
	})
}
