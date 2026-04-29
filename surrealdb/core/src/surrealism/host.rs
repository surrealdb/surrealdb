use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use reblessive::TreeStack;
use surrealism_runtime::capabilities::{FunctionTargets, SurrealismCapabilities};
use surrealism_runtime::config::SurrealismConfig;
use surrealism_runtime::host::InvocationContext;
use surrealism_runtime::kv::{BTreeMapStore, KVStore};

use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::dbs::capabilities::{Capabilities, FuncTarget, NetTarget, Targets};
use crate::doc::CursorDoc;
use crate::expr::function::Function;
use crate::expr::{Expr, FlowResultExt, FunctionCall, Model};
#[cfg(feature = "http")]
use crate::http::HttpClient;
use crate::syn;
use crate::types::{PublicObject, PublicValue};
use crate::val::convert_value_to_public_value;

pub(crate) struct Host {
	// FIXME: We shouldn't be creating a tree stack here.
	// This is here so that a wasm executable can run the executor, however because it
	// creates it's own tree-stack this removes it reblessive stack protection ability.
	pub(crate) stk: TreeStack,
	pub(crate) ctx: FrozenContext,
	pub(crate) opt: Options,
	pub(crate) doc: Option<CursorDoc>,
	kv: Arc<BTreeMapStore>,
	module_name: String,
	#[cfg(feature = "http")]
	/// Surrealism modules have their own http limitations so it needs it's own client.
	http_client: Arc<HttpClient>,
}

impl Host {
	pub(crate) fn new(
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		kv: Arc<BTreeMapStore>,
		module_name: String,
		#[cfg(feature = "http")] http_client: Arc<HttpClient>,
	) -> Self {
		Self {
			stk: TreeStack::new(),
			ctx: ctx.clone(),
			opt: opt.clone(),
			doc: doc.cloned(),
			kv,
			module_name,
			http_client,
		}
	}

	/// Build a context whose capabilities are narrowed to the module's
	/// declared permissions. `validate_surrealism_capabilities` guarantees
	/// at load time that the module's requests are within the server's
	/// bounds, so here we only need to restrict downward.
	fn module_context(&self, config: &SurrealismConfig) -> Context {
		let scoped = Arc::new(module_scoped_capabilities(
			&self.ctx.get_capabilities(),
			&config.capabilities,
		));
		Context::new_child_with_capabilities(
			&self.ctx,
			scoped,
			#[cfg(feature = "http")]
			self.http_client.clone(),
		)
	}
}

/// Parse the module's `allow_net` entries as [`NetTarget`] (same strings as in config).
pub(crate) fn module_allow_net_targets(module: &SurrealismCapabilities) -> HashSet<NetTarget> {
	module
		.allow_net
		.iter()
		.filter_map(|n| match NetTarget::from_str(n) {
			Ok(t) => Some(t),
			Err(e) => {
				tracing::warn!(
					pattern = %n,
					error = %e,
					"Ignoring unparseable network target pattern"
				);
				None
			}
		})
		.collect()
}

/// Narrow the server's `Capabilities` to only what the module declares.
/// Server deny-lists are always preserved as defense in depth.
fn module_scoped_capabilities(
	server: &Capabilities,
	module: &SurrealismCapabilities,
) -> Capabilities {
	let mut caps = server.clone();

	if !module.allow_scripting {
		caps = caps.with_scripting(false);
	}

	match &module.allow_functions {
		FunctionTargets::None => {
			caps = caps.with_functions(Targets::None);
		}
		FunctionTargets::Some(patterns) => {
			let targets = patterns
				.iter()
				.filter_map(|p| match FuncTarget::from_str(p) {
					Ok(t) => Some(t),
					Err(e) => {
						tracing::warn!(
							pattern = %p,
							error = %e,
							"Ignoring unparseable function target pattern"
						);
						None
					}
				})
				.collect();
			caps = caps.with_functions(Targets::Some(targets));
		}
		FunctionTargets::All => {}
	}

	caps
}

#[async_trait]
impl InvocationContext for Host {
	async fn sql(
		&mut self,
		config: &SurrealismConfig,
		query: String,
		vars: PublicObject,
	) -> Result<PublicValue> {
		if !config.capabilities.allow_arbitrary_queries {
			bail!("Module does not have the 'allow_arbitrary_queries' capability");
		}

		let mut ctx = self.module_context(config);
		if !vars.is_empty() {
			ctx.attach_public_variables(vars.into())?;
		}
		let ctx = ctx.freeze();

		let expr: Expr = syn::expr(&query)?.into();
		let res = self
			.stk
			.enter(|stk| expr.compute(stk, &ctx, &self.opt, self.doc.as_ref()))
			.finish()
			.await
			.catch_return()?;

		convert_value_to_public_value(res)
	}

	async fn run(
		&mut self,
		config: &SurrealismConfig,
		fnc: String,
		version: Option<String>,
		args: Vec<PublicValue>,
	) -> Result<PublicValue> {
		if !config.capabilities.allow_functions.allows(&fnc) {
			bail!("Module is not allowed to call function '{fnc}'");
		}

		let segments: Vec<&str> = fnc.split("::").collect();
		let receiver = match segments.first().copied() {
			Some("silo") => {
				let org = segments
					.get(1)
					.ok_or_else(|| anyhow::anyhow!("Expected silo organisation name in '{fnc}'"))?;
				let pkg = segments
					.get(2)
					.ok_or_else(|| anyhow::anyhow!("Expected silo package name in '{fnc}'"))?;
				let version = version
					.ok_or_else(|| anyhow::anyhow!("Expected version for silo function '{fnc}'"))?;
				let (major, minor, patch) = parse_semver(&version)?;
				let sub = if segments.len() > 3 {
					Some(segments[3..].join("::"))
				} else {
					None
				};
				Function::Silo {
					org: (*org).to_string(),
					pkg: (*pkg).to_string(),
					major,
					minor,
					patch,
					sub,
				}
			}
			Some("ml") => {
				if segments.len() < 2 {
					bail!("Expected model name after 'ml::' prefix in '{fnc}'");
				}
				let name = segments[1..].join("::");
				let version = version.ok_or_else(|| {
					anyhow::anyhow!("Expected version for model function '{fnc}'")
				})?;
				Function::Model(Model {
					name,
					version,
				})
			}
			_ => {
				let f: crate::sql::function::Function = syn::function(&fnc)?;
				f.into()
			}
		};

		let expr = Expr::FunctionCall(Box::new(FunctionCall {
			receiver,
			arguments: args.into_iter().map(Expr::from_public_value).collect(),
		}));

		let ctx = self.module_context(config).freeze();
		let res = self
			.stk
			.enter(|stk| expr.compute(stk, &ctx, &self.opt, self.doc.as_ref()))
			.finish()
			.await
			.catch_return()?;

		convert_value_to_public_value(res)
	}

	fn kv(&mut self) -> Result<&dyn KVStore> {
		Ok(&*self.kv)
	}

	fn stdout(&mut self, output: &str) -> Result<()> {
		let ns = self.opt.ns().unwrap_or("?");
		let db = self.opt.db().unwrap_or("?");
		let module = &self.module_name;
		match crate::cnf::SURREALISM_LOG_LEVEL.as_str() {
			"trace" => tracing::trace!(target: "surrealism::module", module, ns, db, "{output}"),
			"info" => tracing::info!(target: "surrealism::module", module, ns, db, "{output}"),
			"warn" => tracing::warn!(target: "surrealism::module", module, ns, db, "{output}"),
			"error" => tracing::error!(target: "surrealism::module", module, ns, db, "{output}"),
			_ => tracing::debug!(target: "surrealism::module", module, ns, db, "{output}"),
		}
		Ok(())
	}

	fn stderr(&mut self, output: &str) -> Result<()> {
		let ns = self.opt.ns().unwrap_or("?");
		let db = self.opt.db().unwrap_or("?");
		let module = &self.module_name;
		tracing::warn!(target: "surrealism::module", module, ns, db, "{output}");
		Ok(())
	}

	fn stdout_callback(&self) -> Arc<dyn Fn(&str) + Send + Sync> {
		let module = self.module_name.clone();
		let ns = self.opt.ns().unwrap_or("?").to_string();
		let db = self.opt.db().unwrap_or("?").to_string();
		let level = crate::cnf::SURREALISM_LOG_LEVEL.clone();
		Arc::new(move |output| match level.as_str() {
			"trace" => {
				tracing::trace!(target: "surrealism::module", module = %module, ns = %ns, db = %db, "{output}")
			}
			"info" => {
				tracing::info!(target: "surrealism::module", module = %module, ns = %ns, db = %db, "{output}")
			}
			"warn" => {
				tracing::warn!(target: "surrealism::module", module = %module, ns = %ns, db = %db, "{output}")
			}
			"error" => {
				tracing::error!(target: "surrealism::module", module = %module, ns = %ns, db = %db, "{output}")
			}
			_ => {
				tracing::debug!(target: "surrealism::module", module = %module, ns = %ns, db = %db, "{output}")
			}
		})
	}

	fn stderr_callback(&self) -> Arc<dyn Fn(&str) + Send + Sync> {
		let module = self.module_name.clone();
		let ns = self.opt.ns().unwrap_or("?").to_string();
		let db = self.opt.db().unwrap_or("?").to_string();
		Arc::new(
			move |output| tracing::warn!(target: "surrealism::module", module = %module, ns = %ns, db = %db, "{output}"),
		)
	}
}

fn parse_semver(version: &str) -> Result<(u32, u32, u32)> {
	let v = semver::Version::parse(version)
		.map_err(|e| anyhow::anyhow!("Invalid semver '{version}': {e}"))?;
	let major = u32::try_from(v.major)
		.map_err(|_| anyhow::anyhow!("semver major component too large: {}", v.major))?;
	let minor = u32::try_from(v.minor)
		.map_err(|_| anyhow::anyhow!("semver minor component too large: {}", v.minor))?;
	let patch = u32::try_from(v.patch)
		.map_err(|_| anyhow::anyhow!("semver patch component too large: {}", v.patch))?;
	Ok((major, minor, patch))
}
