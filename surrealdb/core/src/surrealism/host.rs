use std::sync::Arc;

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use surrealism_runtime::host::InvocationContext;
use surrealism_runtime::{HostCallError, HostCallResult, NetDeny, NetPolicy};

use crate::ctx::reason::Reason;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::dbs::capabilities::{Capabilities, Targets};
use crate::doc::CursorDoc;
use crate::exec::catch_scope::CatchScope;
use crate::expr::function::Function;
use crate::expr::{Expr, FunctionCall, Model};
use crate::extension::capabilities::EffectiveCapabilities;
#[cfg(feature = "http")]
use crate::http::HttpClient;
use crate::surrealism::runner::RunnerCapabilities;
use crate::syn;
use crate::types::{PublicObject, PublicValue};
use crate::val::{Value, convert_value_to_public_value};

pub(crate) struct Host {
	pub(crate) ctx: FrozenContext,
	pub(crate) opt: Options,
	pub(crate) doc: Option<CursorDoc>,
	/// What the guest's standard output and error are recorded with, shared
	/// by both streams.
	log: Arc<GuestLog>,
	/// What this invocation may do, checked on every host call.
	capabilities: Arc<RunnerCapabilities>,
	/// Orders the host calls one invocation holds in flight.
	///
	/// A guest may await several `transaction.sql` / `transaction.run` calls at
	/// once — that is what the async ABI is for — and they all run on the
	/// caller's single transaction. Writes cannot take that freedom: their
	/// savepoints form a stack with no handle, so a second writer entering
	/// before the first has left would unwind the wrong one and silently
	/// discard the other's writes and index deltas. `crate::exec::fan_out`
	/// states the same invariant for the engine's own writes.
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
	/// A statement that cannot write, run where it opens no savepoint.
	/// Overlaps other readers.
	Reading(#[allow(dead_code)] tokio::sync::RwLockReadGuard<'a, ()>),
	/// A statement that may write, or a call whose body this crate cannot see,
	/// whatever the module was lent and whatever its transaction can take; or a
	/// statement that cannot write, of a writeable module, in a transaction that
	/// defined a sequence. Runs alone.
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
		module_name: Arc<str>,
		capabilities: Arc<RunnerCapabilities>,
	) -> Self {
		let log = Arc::new(GuestLog {
			module: module_name,
			ns: opt.ns().unwrap_or("?").into(),
			db: opt.db().unwrap_or("?").into(),
			stdout: match ctx.config.surrealism.surrealism_log_level.as_str() {
				"trace" => tracing::Level::TRACE,
				"info" => tracing::Level::INFO,
				"warn" => tracing::Level::WARN,
				"error" => tracing::Level::ERROR,
				_ => tracing::Level::DEBUG,
			},
		});
		Self {
			ctx: Arc::clone(ctx),
			opt: if writeable {
				opt.clone()
			} else {
				opt.new_for_read_only_module()
			},
			doc: doc.cloned(),
			log,
			capabilities,
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
	/// savepoints are the write path's per-record attempts, the creating
	/// attempt of an `INSERT` or `UPSERT` and the update it retries as
	/// (`exec::operators::write::plan` and the VM's lowering of it), and the
	/// scopes callers that catch a failure open around the unit they catch
	/// ([`crate::exec::catch_scope`]), this host's writing calls among them
	/// ([`Self::caught`]). All are reachable only through a data-modifying
	/// statement form or through a builtin that evaluates a nested one
	/// (`expr::method::is_writer_builtin`). `Expr::read_only` reports both as
	/// writable, so both take the exclusive lock.
	///
	/// Builtins that change state *without* a statement — `sequence::nextval`
	/// advancing a counter, `file::put` writing to a bucket, a writing
	/// `http::*` verb — are reported read-only. The last two write outside the
	/// transaction entirely, so they share the lock. A sequence draw does too,
	/// unless the transaction defined a sequence: a draw from one is
	/// provisional, and a savepoint rollback takes it back, so in such a
	/// transaction a writeable module's every call runs behind a savepoint of
	/// its own ([`crate::exec::catch_scope::CatchScope::needed`]) and holds the
	/// exclusive lock, and no call's draw is taken back by another call's
	/// rollback.
	///
	/// So: **a builtin that can stage a document write must be in
	/// `is_writer_builtin`.** One added later that reaches the write path
	/// without going through a statement would silently take the shared lock,
	/// and nothing here or in the tests would notice.
	///
	/// A statement that may write takes the exclusive lock whether or not it
	/// opens a savepoint of its own ([`Self::scoped`]): whether the module's
	/// read-only frame or its transaction refuses the write is the datastore's
	/// verdict once the call runs, so the order stays the statement's own. Only
	/// a statement that cannot write is ordered by whether it opens one.
	async fn order(&self, read_only: bool) -> CallOrder<'_> {
		if read_only && !self.scoped(true) {
			let reading = self.writes.read().await;
			// A call holding the exclusive place may have defined a sequence
			// while this one waited for its share.
			if !self.scoped(true) {
				return CallOrder::Reading(reading);
			}
		}
		CallOrder::Writing(self.writes.write().await)
	}

	/// Whether a call whose statement is `read_only` or not runs behind a
	/// savepoint of its own ([`Self::caught`]). A module lent a read-only frame
	/// cannot write, nor draw from a sequence, so its calls open none: two
	/// invocations of it may share the transaction.
	fn scoped(&self, read_only: bool) -> bool {
		self.opt.no_write.is_none() && CatchScope::needed(self.ctx.try_tx(), !read_only)
	}

	/// Evaluate `expr`, a call's statement, in `ctx`, in the place `order`
	/// took for it ([`Self::order`]).
	///
	/// The guest is handed a failed call's error and may go on, and the
	/// transaction it was lent commits whatever the guest made of it, so a call
	/// that may write, or draw from a sequence the transaction defined, runs
	/// behind a savepoint of its own, and one that fails leaves none of its
	/// writes behind ([`crate::exec::catch_scope`]). Only a call holding the
	/// exclusive place opens one, which keeps the savepoint the only one opened
	/// on the transaction until it is closed.
	async fn caught(
		&self,
		order: &CallOrder<'_>,
		read_only: bool,
		ctx: &FrozenContext,
		expr: &Expr,
	) -> Result<Value> {
		let scope = match order {
			CallOrder::Writing(_) if self.opt.no_write.is_none() => {
				Some(CatchScope::open(self.ctx.try_tx(), !read_only).await?)
			}
			CallOrder::Writing(_) | CallOrder::Reading(_) => None,
		};
		let exec = crate::exec::entry::frame(ctx, &self.opt, self.doc.as_ref()).await;
		let answered = crate::exec::entry::scalar(&exec, expr).await;
		if let Some(scope) = scope {
			scope.end(answered.is_err()).await?;
		}
		answered
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

	/// Build a context whose capabilities are narrowed to the invocation's
	/// grant, one extension invocation deeper than the caller's.
	fn module_context(&self) -> Context {
		let mut ctx = Context::new_child_with_capabilities(
			&self.ctx,
			Arc::clone(&self.capabilities.scoped),
			#[cfg(feature = "http")]
			Arc::clone(&self.capabilities.http_client),
		);
		ctx.enter_extension();
		ctx
	}
}

/// Where a guest's standard output and error go: the module, namespace and
/// database it ran for, and the level its standard output is recorded at.
/// Standard error is always recorded at `warn`.
struct GuestLog {
	module: Arc<str>,
	ns: Box<str>,
	db: Box<str>,
	stdout: tracing::Level,
}

impl GuestLog {
	fn record(&self, level: tracing::Level, output: &str) {
		let (module, ns, db) = (&*self.module, &*self.ns, &*self.db);
		match level {
			tracing::Level::TRACE => {
				tracing::trace!(target: "surrealism::module", module, ns, db, "{output}")
			}
			tracing::Level::DEBUG => {
				tracing::debug!(target: "surrealism::module", module, ns, db, "{output}")
			}
			tracing::Level::INFO => {
				tracing::info!(target: "surrealism::module", module, ns, db, "{output}")
			}
			tracing::Level::WARN => {
				tracing::warn!(target: "surrealism::module", module, ns, db, "{output}")
			}
			tracing::Level::ERROR => {
				tracing::error!(target: "surrealism::module", module, ns, db, "{output}")
			}
		}
	}
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

#[async_trait]
impl InvocationContext for Host {
	async fn sql(&self, query: String, vars: PublicObject) -> HostCallResult<PublicValue> {
		if !self.capabilities.grant.queries {
			return Err(HostCallError::PermissionDenied(
				"the extension's CAPABILITIES do not grant `queries`".to_string(),
			));
		}

		let mut ctx = self.module_context();
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
		.map_err(|e| HostCallError::InvalidRequest(e.to_string()))?;
		// Held for the rest of the call: a statement that may write runs alone
		// on the caller's transaction, a read shares it with other reads.
		let read_only = expr.read_only();
		let order = self.order(read_only).await;
		self.check_caller_still_waiting()?;
		// Everything past parsing is the datastore's verdict, including a write
		// refused because this function was lent a read-only transaction.
		// Under a driver of its own: the guest may await several calls at once,
		// and calls nested under one shared driver would run one at a time.
		let res = crate::exec::trampoline::concurrent(self.caught(&order, read_only, &ctx, &expr))
			.await
			.map_err(|e| HostCallError::Datastore(e.to_string()))?;

		convert_value_to_public_value(res).map_err(|e| HostCallError::Other(e.to_string()))
	}

	async fn run(
		&self,
		fnc: String,
		version: Option<String>,
		args: Vec<PublicValue>,
	) -> HostCallResult<PublicValue> {
		if !self.capabilities.grant.allows_function_name(&fnc) {
			return Err(HostCallError::PermissionDenied(format!(
				"the extension's CAPABILITIES do not grant the function '{fnc}'"
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
			_ => syn::function_with_capabilities(
				&fnc,
				&self.ctx.get_capabilities(),
				&self.ctx.config.parser,
			)?,
		};

		let expr = Expr::FunctionCall(Box::new(FunctionCall {
			receiver,
			arguments: args.into_iter().map(Expr::from_public_value).collect(),
		}));

		let ctx = self.module_context().freeze();
		// Same ordering as `sql`. A `fn::` body and a module call are opaque to
		// `read_only`, so they count as writing and run alone; a pure builtin
		// overlaps.
		let read_only = expr.read_only();
		let order = self.order(read_only).await;
		self.check_caller_still_waiting()?;
		// Under a driver of its own: the guest may await several calls at once,
		// and calls nested under one shared driver would run one at a time.
		let res = crate::exec::trampoline::concurrent(self.caught(&order, read_only, &ctx, &expr))
			.await
			.map_err(|e| HostCallError::Datastore(e.to_string()))?;

		convert_value_to_public_value(res).map_err(|e| HostCallError::Other(e.to_string()))
	}

	fn stdout_callback(&self) -> Arc<dyn Fn(&str) + Send + Sync> {
		let log = Arc::clone(&self.log);
		Arc::new(move |output| log.record(log.stdout, output))
	}

	fn stderr_callback(&self) -> Arc<dyn Fn(&str) + Send + Sync> {
		let log = Arc::clone(&self.log);
		Arc::new(move |output| log.record(tracing::Level::WARN, output))
	}
}

/// What a definition may do, in the form its invocations check, from its
/// `CAPABILITIES` already intersected with the server's
/// ([`effective_capabilities`](crate::extension::capabilities::effective_capabilities)).
///
/// The guest's own sockets get the granted network targets under the server's
/// deny rules; `tx.sql` and `tx.run` run under the server's capabilities
/// narrowed to the grant. The memory and execution time limits are the
/// smaller of the grant and the server's ceiling. The grant's
/// `strict_timeout: false` takes effect only when the server allows unbounded
/// execution; otherwise the invocation runs on the guarded engine, where the
/// ceiling and the query's timeout can stop it.
pub(crate) fn runner_capabilities(
	grant: EffectiveCapabilities,
	server: &Capabilities,
	config: &crate::config::RuntimeConfig,
) -> Result<RunnerCapabilities> {
	let allow_net: Vec<String> = match &grant.allow_net {
		Targets::None => Vec::new(),
		Targets::All => vec!["*".to_string()],
		Targets::Some(targets) => targets.iter().map(|t| t.to_string()).collect(),
	};
	let net = NetPolicy::parse(&allow_net, surrealism_deny_net(server))
		.context("Failed to parse the granted network targets")?;

	let scoped = server
		.clone()
		.with_scripting(grant.scripting)
		.with_functions(grant.allow_functions.clone())
		.with_network_targets(grant.allow_net.clone());

	#[cfg(feature = "http")]
	let http_client = Arc::new(
		HttpClient::new(grant.allow_net.clone(), grant.deny_net.clone(), &config.http)
			.context("Failed to create http client for the extension")?,
	);

	let ceiling_memory = config.surrealism.surrealism_max_memory;
	let ceiling_time =
		config.surrealism.surrealism_max_execution_time.map(std::time::Duration::from_millis);
	let memory = grant.memory.map(|m| usize::try_from(m).unwrap_or(usize::MAX));

	Ok(RunnerCapabilities {
		net,
		memory: min_option(ceiling_memory, memory),
		timeout: min_option(ceiling_time, grant.timeout),
		strict_timeout: grant.strict_timeout
			|| !config.surrealism.surrealism_allow_unbounded_execution,
		grant,
		scoped: Arc::new(scoped),
		#[cfg(feature = "http")]
		http_client,
	})
}

fn min_option<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
	match (a, b) {
		(Some(a), Some(b)) => Some(a.min(b)),
		(a, b) => a.or(b),
	}
}

/// Capabilities that grant nothing, for tests that need a value.
#[cfg(test)]
pub(crate) fn test_capabilities() -> RunnerCapabilities {
	RunnerCapabilities {
		net: NetPolicy::deny_all(),
		memory: None,
		timeout: None,
		strict_timeout: true,
		grant: crate::extension::capabilities::effective_capabilities(
			&Default::default(),
			&Capabilities::default(),
		)
		.expect("an empty grant"),
		scoped: Arc::new(Capabilities::default()),
		#[cfg(feature = "http")]
		http_client: Arc::new(
			HttpClient::new(Targets::None, Targets::All, &Default::default()).expect("http client"),
		),
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
	use std::str::FromStr;

	use surrealism_runtime::{NetDeny, NetRule};

	use super::surrealism_deny_net;
	use crate::dbs::Capabilities;
	use crate::dbs::capabilities::{NetTarget, Targets};
	use crate::expr::{Expr, ExtensionCapabilities};
	use crate::extension::capabilities::effective_capabilities;

	/// A server wildcard deny crosses into the runtime as the same rule. Were it
	/// to fail to re-parse, the extension would lose all network access instead,
	/// which is safe but would hide the mismatch.
	#[test]
	fn a_wildcard_deny_reaches_the_runtime_as_a_wildcard() {
		let server = Capabilities::all().without_network_targets(Targets::Some(
			[NetTarget::from_str("*.example.com:443").unwrap()].into(),
		));
		let NetDeny::Rules(rules) = surrealism_deny_net(&server) else {
			panic!("expected the deny rules to cross as rules");
		};
		assert_eq!(
			&*rules,
			[NetRule::HostSuffix {
				suffix: "example.com".to_string(),
				port: Some(443),
			}]
		);
	}

	/// `strict_timeout: false` in a definition reaches the runner only on a
	/// server that allows unbounded execution; by default the invocation stays
	/// on the guarded engine, where timeouts are enforced.
	#[test]
	fn unbounded_execution_needs_the_server_to_allow_it() {
		let server = Capabilities::default();
		let definition = ExtensionCapabilities {
			strict_timeout: false,
			..Default::default()
		};
		let runner = |allow: bool| {
			let mut config = crate::config::RuntimeConfig::default();
			config.surrealism.surrealism_allow_unbounded_execution = allow;
			let grant = effective_capabilities(&definition, &server).expect("grant");
			super::runner_capabilities(grant, &server, &config).expect("runner capabilities")
		};
		assert!(runner(false).strict_timeout);
		assert!(!runner(true).strict_timeout);

		let strict = effective_capabilities(&ExtensionCapabilities::default(), &server).unwrap();
		let mut config = crate::config::RuntimeConfig::default();
		config.surrealism.surrealism_allow_unbounded_execution = true;
		assert!(super::runner_capabilities(strict, &server, &config).unwrap().strict_timeout);
	}

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
			let expr: Expr = crate::syn::expr(query).expect(query);
			assert!(!expr.read_only(), "{query} must run alone");
		}
	}

	#[test]
	fn a_statement_that_cannot_write_shares() {
		for query in
			["SELECT * FROM t", "RETURN 1 + 1", "SLEEP 1ms", "INFO FOR DB", "string::len('abc')"]
		{
			let expr: Expr = crate::syn::expr(query).expect(query);
			assert!(expr.read_only(), "{query} must be allowed to overlap");
		}
	}

	/// The place `Host::order` gives a call: one whose statement may write
	/// runs alone whatever its module was lent and whatever its transaction can
	/// take, and one whose statement cannot write shares, unless it runs behind
	/// a savepoint of its own: a writeable module's call in a transaction that
	/// defined a sequence.
	#[tokio::test]
	async fn a_call_takes_its_place_by_its_statement_and_its_savepoint() {
		use std::sync::Arc;

		use super::{CallOrder, Host, test_capabilities};
		use crate::catalog::{DatabaseId, NamespaceId};
		use crate::ctx::Context;
		use crate::dbs::Options;
		use crate::kvs::Datastore;
		use crate::kvs::TransactionType::{Read, Write};

		let ds = Datastore::new("memory").await.unwrap();
		let opt = Options::new(&ds.config().exec);
		for (kind, label, sequence) in
			[(Write, "write", false), (Read, "read", false), (Write, "write", true)]
		{
			let txn = ds.transaction(kind).await.unwrap().enclose();
			if sequence {
				txn.sequence_defined_here(NamespaceId(1), DatabaseId(1), "sq".to_owned()).await;
			}
			let mut ctx = Context::new_test();
			ctx.set_transaction(Arc::clone(&txn));
			let ctx = ctx.freeze();
			for writeable in [true, false] {
				let host = Host::new(
					&ctx,
					&opt,
					None,
					writeable,
					"m".into(),
					Arc::new(test_capabilities()),
				);
				let case = format!("{label}, sequence {sequence}, writeable {writeable}");
				assert!(
					matches!(host.order(false).await, CallOrder::Writing(_)),
					"a call that may write runs alone: {case}"
				);
				let scoped = sequence && writeable;
				assert_eq!(
					matches!(host.order(true).await, CallOrder::Writing(_)),
					scoped,
					"a read runs alone only behind a savepoint of its own: {case}"
				);
			}
			drop(ctx);
			txn.cancel().await.unwrap();
		}
	}

	/// Builtins that change something without evaluating a statement are
	/// read-only statements: what this ordering serialises is savepoints, and
	/// the statement takes none, so the call shares the lock. A sequence draw
	/// of a writeable module still takes the exclusive side in a transaction
	/// that defined a sequence, because its call runs behind a savepoint there
	/// (`Host::scoped`), which the statement's classification does not decide
	/// (`a_call_takes_its_place_by_its_statement_and_its_savepoint`).
	///
	/// Listed explicitly so that moving one of them to the exclusive side is a
	/// deliberate act with a failing test, not a quiet change of meaning.
	#[test]
	fn a_builtin_that_mutates_outside_a_statement_is_a_read_only_statement() {
		for query in [
			"sequence::nextval('n')",
			"http::post('https://example.com/', {})",
			"file::put(f\"b:/k\", 'x')",
		] {
			let expr: Expr = crate::syn::expr(query).expect(query);
			assert!(expr.read_only(), "{query} takes no savepoint, so it may overlap");
		}
	}
}
