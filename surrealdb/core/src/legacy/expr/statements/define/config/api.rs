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
			let arg = stk
				.run(|stk| crate::legacy::expr_compute(arg, stk, ctx, opt, doc))
				.await
				.catch_return()?;
			// SECURITY: a stored middleware argument is rendered back recursively by
			// INFO/EXPORT, whose `MiddlewareDefinition::to_sql_middleware` converts each
			// arg to a public value and formats it. `MiddlewareDefinition` is a plain
			// revisioned encode with no depth guard of its own, so bound each argument's
			// nesting the same way a stored record or param is; without this a
			// `DEFINE API ... MIDDLEWARE m(<expr>)` whose argument nests past the cap
			// would persist and then abort the process every time it is read back.
			crate::val::check_value_depth(&arg)?;
			args.push(arg);
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
