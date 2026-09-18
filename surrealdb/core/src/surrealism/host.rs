use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use reblessive::TreeStack;
use surrealism_runtime::capabilities::{FunctionTargets, SurrealismCapabilities};
use surrealism_runtime::config::SurrealismConfig;
use surrealism_runtime::host::InvocationContext;
use surrealism_runtime::{HostCallError, HostCallResult, NetDeny};

use crate::ctx::reason::Reason;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::dbs::capabilities::{Capabilities, FuncTarget, NetTarget, Targets};
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::function::Function;
use crate::expr::{Expr, FunctionCall, Model};
#[cfg(feature = "http")]
use crate::http::HttpClient;
use crate::syn;
use crate::types::{PublicObject, PublicValue};
use crate::val::convert_value_to_public_value;

pub(crate) struct Host {
	pub(crate) ctx: FrozenContext,
	pub(crate) opt: Options,
	pub(crate) doc: Option<CursorDoc>,
	module_name: String,
	#[cfg(feature = "http")]
	/// Surrealism modules have their own http limitations so it needs it's own client.
	http_client: Arc<HttpClient>,
	/// Orders the host calls one invocation holds in flight.
	///
	/// A guest may await several `transaction.sql` / `transaction.run` calls at
	/// once — that is what the async ABI is for — and they all run on the
	/// caller's single transaction. Writes cannot take that freedom: their
	/// savepoints form a stack with no handle, so a second writer entering
	/// before the first has left would unwind the wrong one and silently
	/// discard the other's writes and index deltas. `crate::exec::fan_out`
	/// states the same invariant for the streaming path.
	///
	/// So a writing call takes this exclusively and a reading one shares it.
	/// Which of the two a call is comes off the parsed expression, not off the
	/// module's `writeable` flag: that flag is the module's own claim about
	/// itself, it is not enforced, and a function declaring a plain
	/// `Transaction` can still issue a `CREATE`. Reading it from the statement
	/// keeps concurrent reads overlapping without believing the manifest.
	///
	/// This orders the calls of *one* invocation. Two invocations of a function
	/// the manifest marks non-writeable can still be evaluated side by side on
	/// one transaction, because that flag is what `access_mode` reports and
	/// what licenses core's per-row fan-out. Only refusing the write closes
	/// that, which is `Options::no_write`.
	writes: tokio::sync::RwLock<()>,
}

/// A host call's place in the ordering [`Host::writes`] imposes.
enum CallOrder<'a> {
	/// A statement that cannot write. Overlaps other readers.
	Reading(#[allow(dead_code)] tokio::sync::RwLockReadGuard<'a, ()>),
	/// A statement that may write, or a call whose body this crate cannot see.
	/// Runs alone.
	Writing(#[allow(dead_code)] tokio::sync::RwLockWriteGuard<'a, ()>),
}

impl Host {
	/// `writeable` comes from the function's entry in the baked exports
	/// manifest, where the guest declared whether it needs write access. When it
	/// is `false` the execution frame refuses data-modifying statements, so the
	/// declaration is enforced rather than merely recorded — the guest builds
	/// its query as a string, so nothing earlier can settle it.
	pub(crate) fn new(
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		writeable: bool,
		module_name: String,
		#[cfg(feature = "http")] http_client: Arc<HttpClient>,
	) -> Self {
		Self {
			ctx: Arc::clone(ctx),
			opt: if writeable {
				opt.clone()
			} else {
				opt.new_for_read_only_module()
			},
			doc: doc.cloned(),
			module_name,
			#[cfg(feature = "http")]
			http_client,
			writes: tokio::sync::RwLock::new(()),
		}
	}

	/// Take a place in the ordering, held for the rest of the call.
	///
	/// `read_only` is what the parsed statement says about itself, which
	/// over-approximates: a call to anything this crate cannot see the body of
	/// counts as writing.
	///
	/// # The invariant this rests on
	///
	/// What has to be serialised is narrower than "changes something": it is
	/// **staging a document write on the caller's transaction**, because that is
	/// what takes a savepoint, and savepoints are a stack with no handle. The
	/// only two calls to `new_save_point` are in `doc::insert` and
	/// `doc::upsert`, and both are reachable only through a data-modifying
	/// statement form or through a builtin that evaluates a nested one
	/// (`sql::method::is_writer_builtin`). `Expr::read_only` reports both as
	/// writable, so both take the exclusive lock.
	///
	/// Builtins that change state *without* a statement — `sequence::nextval`
	/// advancing a counter, `file::put` writing to a bucket, a writing
	/// `http::*` verb — are reported read-only here and share the lock. That is
	/// correct for this ordering, because they take no savepoint: a sequence
	/// allocates through the transaction without one, and the other two write
	/// outside it entirely.
	///
	/// So: **a builtin that can stage a document write must be in
	/// `is_writer_builtin`.** One added later that reaches `doc::upsert`
	/// without going through a statement would silently take the shared lock,
	/// and nothing here or in the tests would notice.
	async fn order(&self, read_only: bool) -> CallOrder<'_> {
		if read_only {
			CallOrder::Reading(self.writes.read().await)
		} else {
			CallOrder::Writing(self.writes.write().await)
		}
	}

	/// Refuse a call whose caller has already given up.
	///
	/// A host call waits: for its place in the ordering above, and then inside
	/// the datastore. By the time it runs, the query that lent the handle may
	/// have timed out or been cancelled, and the transaction it would write to
	/// is about to be rolled back. `deadline-exceeded` is a case of its own in
	/// the WIT so a guest can tell this from a datastore failure and stop
	/// issuing further calls rather than retry.
	fn check_caller_still_waiting(&self) -> HostCallResult<()> {
		match self.ctx.done(true) {
			Ok(None) => Ok(()),
			Ok(Some(Reason::Timedout(_))) => Err(HostCallError::DeadlineExceeded),
			Ok(Some(Reason::Canceled)) => Err(HostCallError::Other(
				"the query that lent this transaction handle was cancelled".to_string(),
			)),
			Err(e) => Err(HostCallError::Datastore(e.to_string())),
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
			Arc::clone(&self.http_client),
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

/// The operator's `deny_net`, in the form the Surrealism runtime evaluates.
///
/// The runtime cannot read `Capabilities`, so the rules cross as the strings
/// the operator wrote and are re-parsed there by the same parser `allow_net`
/// uses. Rendering goes through `Display`, which emits `host`, `host:port` or
/// a CIDR — every spelling that parser accepts.
///
/// This is what stops the guest's own WASI sockets from being the one way out
/// of the process that the server's deny list does not reach.
pub(crate) fn surrealism_deny_net(server: &Capabilities) -> NetDeny {
	match &server.deny_net {
		Targets::None => NetDeny::Nothing,
		Targets::All => NetDeny::Everything,
		Targets::Some(targets) => {
			let entries: Vec<String> = targets.iter().map(|t| t.to_string()).collect();
			match NetDeny::parse(&entries) {
				Ok(deny) => deny,
				Err(e) => {
					// A deny rule that failed to re-parse must not silently
					// become "deny nothing": refusing every outbound connection
					// is the only direction a lost restriction may fail in.
					tracing::error!(
						error = %e,
						"Could not parse the server deny_net rules for a Surrealism module; \
						 denying all outbound access from it"
					);
					NetDeny::Everything
				}
			}
		}
	}
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

	let net_targets = module_allow_net_targets(module);
	let network = if net_targets.is_empty() {
		Targets::None
	} else {
		Targets::Some(net_targets)
	};
	caps.with_network_targets(network)
}

#[async_trait]
impl InvocationContext for Host {
	async fn sql(
		&self,
		config: &SurrealismConfig,
		query: String,
		vars: PublicObject,
	) -> HostCallResult<PublicValue> {
		if !config.capabilities.allow_arbitrary_queries {
			return Err(HostCallError::PermissionDenied(
				"module does not have the 'allow_arbitrary_queries' capability".to_string(),
			));
		}

		let mut ctx = self.module_context(config);
		if !vars.is_empty() {
			ctx.attach_public_variables(vars.into())
				.map_err(|e| HostCallError::InvalidRequest(e.to_string()))?;
		}
		let ctx = ctx.freeze();

		let expr: Expr = syn::expr_with_capabilities(
			&query,
			&self.ctx.get_capabilities(),
			&self.ctx.config.parser,
		)
		.map_err(|e| HostCallError::InvalidRequest(e.to_string()))?
		.into();
		// Held for the rest of the call: a statement that may write runs alone
		// on the caller's transaction, a read shares it with other reads.
		let _order = self.order(expr.read_only()).await;
		self.check_caller_still_waiting()?;
		// Everything past parsing is the datastore's verdict, including a write
		// refused because this function was lent a read-only transaction.
		let res = TreeStack::new()
			.enter(|stk| {
				crate::legacy::expr_compute(&expr, stk, &ctx, &self.opt, self.doc.as_ref())
			})
			.finish()
			.await
			.catch_return()
			.map_err(|e| HostCallError::Datastore(e.to_string()))?;

		convert_value_to_public_value(res).map_err(|e| HostCallError::Other(e.to_string()))
	}

	async fn run(
		&self,
		config: &SurrealismConfig,
		fnc: String,
		version: Option<String>,
		args: Vec<PublicValue>,
	) -> HostCallResult<PublicValue> {
		if !config.capabilities.allow_functions.allows(&fnc) {
			return Err(HostCallError::PermissionDenied(format!(
				"module is not allowed to call function '{fnc}'"
			)));
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
					return Err(HostCallError::InvalidRequest(format!(
						"expected model name after 'ml::' prefix in '{fnc}'"
					)));
				}
				let name = segments[1..].join("::");
				let version = version.ok_or_else(|| {
					anyhow::anyhow!("Expected version for model function '{fnc}'")
				})?;
				Function::Model(Model {
					name: name.into(),
					version: version.into(),
				})
			}
			_ => {
				let f: crate::sql::function::Function = syn::function_with_capabilities(
					&fnc,
					&self.ctx.get_capabilities(),
					&self.ctx.config.parser,
				)?;
				f.into()
			}
		};

		let expr = Expr::FunctionCall(Box::new(FunctionCall {
			receiver,
			arguments: args.into_iter().map(Expr::from_public_value).collect(),
		}));

		let ctx = self.module_context(config).freeze();
		// Same ordering as `sql`. A `fn::` body and a module call are opaque to
		// `read_only`, so they count as writing and run alone; a pure builtin
		// overlaps.
		let _order = self.order(expr.read_only()).await;
		self.check_caller_still_waiting()?;
		let res = TreeStack::new()
			.enter(|stk| {
				crate::legacy::expr_compute(&expr, stk, &ctx, &self.opt, self.doc.as_ref())
			})
			.finish()
			.await
			.catch_return()
			.map_err(|e| HostCallError::Datastore(e.to_string()))?;

		convert_value_to_public_value(res).map_err(|e| HostCallError::Other(e.to_string()))
	}

	fn stdout_callback(&self) -> Arc<dyn Fn(&str) + Send + Sync> {
		let module = self.module_name.clone();
		let ns = self.opt.ns().unwrap_or("?").to_string();
		let db = self.opt.db().unwrap_or("?").to_string();
		let level = self.ctx.config.surrealism.surrealism_log_level.clone();
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

#[cfg(test)]
mod tests {
	use crate::expr::Expr;

	/// The classification `Host::order` runs on, pinned per statement shape.
	///
	/// The invariant it protects is on `Host::order`: everything that can stage
	/// a document write on the caller's transaction — the only thing that takes
	/// a savepoint — must land on the exclusive side. A change to
	/// `Expr::read_only` that moved one of these silently would otherwise be
	/// invisible until two guest writes unwound each other's savepoint.
	#[test]
	fn a_statement_that_can_write_takes_the_exclusive_side() {
		for query in [
			"CREATE t:1 SET n = 1",
			"UPSERT t:1 SET n = 1",
			"UPDATE t:1 SET n = 1",
			"DELETE t:1",
			"INSERT INTO t { n: 1 }",
			"RELATE a:1 -> likes -> b:1",
			"DEFINE TABLE t SCHEMALESS",
			// Evaluates a nested statement, so it can reach a savepoint
			// whatever its argument looks like: `is_writer_builtin`.
			"eval::surql('CREATE t:1')",
			// A write buried in a subquery is still a write.
			"SELECT * FROM (CREATE t:1)",
		] {
			let expr: Expr = crate::syn::expr(query).expect(query).into();
			assert!(!expr.read_only(), "{query} must run alone");
		}
	}

	#[test]
	fn a_statement_that_cannot_write_shares() {
		for query in
			["SELECT * FROM t", "RETURN 1 + 1", "SLEEP 1ms", "INFO FOR DB", "string::len('abc')"]
		{
			let expr: Expr = crate::syn::expr(query).expect(query).into();
			assert!(expr.read_only(), "{query} must be allowed to overlap");
		}
	}

	/// Builtins that change something without evaluating a statement share the
	/// lock, and that is correct here rather than an oversight: what this
	/// ordering serialises is savepoints, and none of these take one — a
	/// sequence allocates through the transaction without a savepoint, and the
	/// rest write outside it entirely.
	///
	/// Listed explicitly so that moving one of them to the exclusive side is a
	/// deliberate act with a failing test, not a quiet change of meaning.
	#[test]
	fn a_builtin_that_mutates_outside_a_statement_still_shares() {
		// `file::*` belongs here too, but its argument is a file literal, which
		// needs the `files` experimental target to parse.
		for query in ["sequence::nextval('n')", "http::post('https://example.com/', {})"] {
			let expr: Expr = crate::syn::expr(query).expect(query).into();
			assert!(expr.read_only(), "{query} takes no savepoint, so it may overlap");
		}
	}
}
