//! The execution backend a single test run is driven against.
//!
//! The test runner is written against the [`Executor`] trait rather than a
//! concrete [`Datastore`], so the same `run_test` flow can drive either an
//! embedded in-process datastore (the default — [`EmbeddedExecutor`]) or, behind
//! the `remote` feature, a remote SurrealDB instance reached over the API
//! (`RemoteExecutor`).
//!
//! [`EmbeddedExecutor`] is a thin wrapper that delegates to the existing
//! in-process helpers (`util::session_from_test_config`, `util::run_imports_list`,
//! the core `iam` signin/signup, and the `cleanup_environment` /
//! `check_retained_keys` free functions in the parent module), so the embedded
//! path behaves exactly as it did before the trait was introduced.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, QueryRequest, QuerySource};
use surrealdb_gql as gql;
use surrealdb_syn::error::RenderedError;
use surrealdb_syn::parser;
use surrealdb_rpc::capabilities::ExperimentalTarget;
use surrealdb_types::{Object, Value as SurValue};

use crate::cmd::util::ImportFailure;
use crate::cmd::{graphql, util};
use crate::tests::case::{Dialect, TestCase};
use crate::tests::schema::{NewPlannerStrategyConfig, TestConfig};

/// Why a remote executor cannot faithfully run a particular test.
///
/// Embedded execution supports everything, so [`EmbeddedExecutor::unsupported`]
/// always returns `None`. A remote executor returns `Some(..)` for behaviours
/// that are server-startup configuration rather than per-request state, or that
/// have no API surface — these tests are reported as skipped, not failed.
#[derive(Debug, Clone)]
#[cfg(feature = "remote")]
pub enum Unsupported {
	/// The test configures non-default capabilities, which are fixed at server
	/// startup and cannot be set per-test on a shared remote instance.
	NonDefaultCapabilities,
	/// The test requires a versioned (MVCC) datastore, which is server-startup
	/// configuration.
	Versioned,
	/// The test authenticates as a specific record (`auth = { ... rid = ... }`).
	/// A record session cannot be fabricated over the API — you can only
	/// authenticate through an access method's signin flow.
	RecordAuth,
	/// A GraphQL or GQL test running under non-owner auth. The `/graphql` and
	/// `/gql` requests are authenticated with root credentials, so the test's
	/// intended auth level can't be conveyed and a permissions check would
	/// falsely pass.
	HttpRouteAuth,
	/// The test set `[env] remote-skip = "<reason>"`: it asserts behaviour that
	/// only holds for an embedded, in-process datastore (e.g. a connection-less
	/// `$session`, the absence of a bootstrap root user, or a process-relative
	/// file path) and so can't be reproduced over the API. The carried string is
	/// the test author's reason.
	EmbeddedOnly(String),
	/// The test declares a `[env] planner-strategy` list that does not contain the
	/// strategy the endpoint runs. The strategy is server-startup configuration,
	/// so a remote run cannot select the one the test asks for.
	PlannerStrategy(NewPlannerStrategyConfig),
	/// The test's `[env] auth` is scoped to a namespace or database other than
	/// the one it runs in. A remote run renames the test's own namespace, so a
	/// scope naming a different one cannot be provisioned without writing a user
	/// into a namespace the test does not own.
	ForeignAuthScope,
}

#[cfg(feature = "remote")]
impl Unsupported {
	/// A short human-readable reason, shown in the skip report.
	pub fn reason(&self) -> std::borrow::Cow<'static, str> {
		use std::borrow::Cow;
		match self {
			Unsupported::NonDefaultCapabilities => {
				Cow::Borrowed("requires non-default capabilities (fixed at server startup)")
			}
			Unsupported::Versioned => {
				Cow::Borrowed("requires a versioned datastore (server-startup config)")
			}
			Unsupported::RecordAuth => {
				Cow::Borrowed("record-level auth cannot be fabricated over the API")
			}
			Unsupported::HttpRouteAuth => Cow::Borrowed(
				"GraphQL/GQL under non-owner auth can't convey the test's auth level over HTTP",
			),
			Unsupported::EmbeddedOnly(reason) => Cow::Owned(reason.clone()),
			Unsupported::PlannerStrategy(strategy) => Cow::Owned(format!(
				"is narrowed away from the engine the endpoint's `{strategy}` planner strategy uses (server-startup config)"
			)),
			Unsupported::ForeignAuthScope => Cow::Borrowed(
				"authenticates against a namespace/database other than the one it runs in, which a renamed remote namespace cannot reproduce",
			),
		}
	}
}

/// The outcome of executing the query under test for one dialect.
pub enum ExecOutcome {
	/// Parsing/lowering failed before the query executed; this is the test's
	/// final result.
	ParserError(RenderedError),
	/// The query executed.
	Executed {
		did_timeout: bool,
		result: Result<Vec<Result<SurValue, String>>>,
	},
}

/// The backend a single test run executes against.
///
/// Created fresh per test run. `prepare` is called first to establish the
/// namespace/database/auth context, then the body (imports, optional
/// signup/signin, the query), then `cleanup`, then (for non-`clean` tests)
/// `check_retained_keys`.
#[allow(async_fn_in_trait)]
pub trait Executor {
	/// Establish the test's namespace/database/auth context.
	async fn prepare(&mut self, config: &TestConfig) -> Result<()>;

	/// Run the test's import cases against the established context.
	async fn run_imports(&mut self, imports: &[Arc<TestCase>]) -> Result<Option<ImportFailure>>;

	/// Run the test's own signup (auth under test). The `Err` is surfaced as the
	/// test's [`crate::tests::report::TestTaskResult::SignupError`].
	async fn signup(&mut self, vars: Object) -> Result<()>;

	/// Run the test's own signin (auth under test). The `Err` is surfaced as the
	/// test's [`crate::tests::report::TestTaskResult::SigninError`].
	async fn signin(&mut self, vars: Object) -> Result<()>;

	/// Execute the query under test.
	async fn execute(&mut self, case: &TestCase, timeout: Duration) -> Result<ExecOutcome>;

	/// Clean up the namespace/database/root artefacts the test created.
	async fn cleanup(&mut self) -> Result<()>;

	/// Check for keys left in the store after cleanup. Embedded performs the raw
	/// KV scan; a remote executor has no raw KV access and returns `Ok(None)`,
	/// meaning the check is skipped.
	async fn check_retained_keys(&mut self) -> Result<Option<Vec<Vec<u8>>>>;

	/// Root-level entities the test created and did not declare it would, each
	/// described well enough for the reader to find it. A non-empty result fails
	/// the test.
	///
	/// This is what holds concurrent remote runs together: a test sharing an
	/// endpoint is isolated by its namespace alone, so anything it writes outside
	/// one survives cleanup and is visible to every test still running. Declaring
	/// `[env] remote-exclusive` buys the right to do that, at the cost of running
	/// alone. The check is the enforcement — a test cannot silently acquire the
	/// need, whether through its own body, an import, or a function it calls,
	/// because the endpoint is asked rather than the source read.
	///
	/// Embedded runs give every test a private datastore, so there is nothing to
	/// share and nothing to check; the default returns empty.
	async fn check_root_leak(&mut self) -> Result<Vec<String>> {
		Ok(Vec::new())
	}
}

/// Executes a test against an embedded, in-process [`Datastore`] — the historical
/// (and default) behaviour. Every method delegates to the existing helpers so the
/// embedded path is unchanged.
pub struct EmbeddedExecutor {
	dbs: Arc<Datastore>,
	/// The planner strategy this run's session selects. Each test runs once per
	/// strategy, so the strategy belongs to the run rather than to the test.
	planner_config: NewPlannerStrategyConfig,
	/// Built in [`EmbeddedExecutor::prepare`]; mutated by signin/signup and read
	/// by the query and cleanup.
	session: Session,
}

impl EmbeddedExecutor {
	pub fn new(dbs: Arc<Datastore>, planner_config: NewPlannerStrategyConfig) -> Self {
		EmbeddedExecutor {
			dbs,
			planner_config,
			session: Session::default(),
		}
	}
}

impl Executor for EmbeddedExecutor {
	async fn prepare(&mut self, config: &TestConfig) -> Result<()> {
		self.session = util::session_from_test_config(config, self.planner_config.into());
		Ok(())
	}

	async fn run_imports(&mut self, imports: &[Arc<TestCase>]) -> Result<Option<ImportFailure>> {
		util::run_imports_list(imports, self.session.clone(), &self.dbs).await
	}

	async fn signup(&mut self, vars: Object) -> Result<()> {
		surrealdb_core::iam::signup::signup(&self.dbs, &mut self.session, vars.into()).await?;
		Ok(())
	}

	async fn signin(&mut self, vars: Object) -> Result<()> {
		surrealdb_core::iam::signin::signin(&self.dbs, &mut self.session, vars.into()).await?;
		Ok(())
	}

	async fn execute(&mut self, case: &TestCase, timeout: Duration) -> Result<ExecOutcome> {
		let dbs = &self.dbs;
		let session = &self.session;
		match case.dialect {
			Dialect::SurrealQl => {
				let settings = parser::ParserSettings {
					files_enabled: dbs
						.get_capabilities()
						.allows_experimental(&ExperimentalTarget::Files),
					surrealism_enabled: dbs
						.get_capabilities()
						.allows_experimental(&ExperimentalTarget::Surrealism),
					..Default::default()
				};

				let source = case.source.as_bytes();
				let mut p = parser::Parser::new_with_settings(source, settings);
				let mut stack = reblessive::Stack::new();
				let query = match stack.enter(|stk| p.parse_query(stk)).finish() {
					Ok(x) => match p.assert_finished() {
						Ok(()) => x,
						Err(e) => return Ok(ExecOutcome::ParserError(e.render_on_bytes(source))),
					},
					Err(e) => return Ok(ExecOutcome::ParserError(e.render_on_bytes(source))),
				};

				let start = Instant::now();
				let result = dbs.process(query, session, None).await;
				let did_timeout = start.elapsed() > timeout;
				let result = result
					.map(|x| x.into_iter().map(|x| x.result.map_err(|e| e.to_string())).collect())
					.map_err(|e| anyhow::anyhow!(e));
				Ok(ExecOutcome::Executed {
					did_timeout,
					result,
				})
			}
			Dialect::Gql => {
				// GQL has no capability-gated syntax; the default recursion
				// limit matches `parser::ParserSettings::default()` above.
				// Lowering produces a `PreparedGqlQuery` (a `MatchPlan` embedded
				// in a logical plan), which is submitted as an already-lowered
				// `QuerySource::Plan`. It executes through the streaming engine,
				// so `.gql` cases default away from the compute-only strategy
				// (see `Dialect::Gql` default in the schema's planner-strategy
				// seam).
				let settings = gql::GqlParserSettings::default();
				let source = &case.source.as_bytes();
				let query = match gql::parse_to_plan_with_settings(&case.source, settings) {
					Ok(x) => x,
					Err(e) => {
						return Ok(ExecOutcome::ParserError(e.render_on_bytes(source)));
					}
				};

				let start = Instant::now();
				let result =
					dbs.run(QueryRequest::new(QuerySource::Plan(query.0), session)).await;
				let did_timeout = start.elapsed() > timeout;
				let result = result
					.map(|x| x.into_iter().map(|x| x.result.map_err(|e| e.to_string())).collect())
					.map_err(|e| anyhow::anyhow!(e));
				Ok(ExecOutcome::Executed {
					did_timeout,
					result,
				})
			}
			Dialect::GraphQl => {
				// Schema generation reads the catalog like the server does on a
				// cache miss; it is setup, not the query under test, so it stays
				// outside the timed window. Failures (e.g. GraphQL not configured)
				// are part of the testable surface and become the single result.
				match graphql::generate_schema(dbs, session).await {
					Ok(schema) => {
						let request = graphql::build_request(case, dbs, session)?;
						let start = Instant::now();
						let response = schema.execute(request).await;
						let did_timeout = start.elapsed() > timeout;
						Ok(ExecOutcome::Executed {
							did_timeout,
							result: Ok(vec![graphql::response_to_result(response)]),
						})
					}
					Err(e) => Ok(ExecOutcome::Executed {
						did_timeout: false,
						result: Ok(vec![Err(e)]),
					}),
				}
			}
		}
	}

	async fn cleanup(&mut self) -> Result<()> {
		super::cleanup_environment(&self.dbs, &self.session).await
	}

	async fn check_retained_keys(&mut self) -> Result<Option<Vec<Vec<u8>>>> {
		Ok(Some(super::check_retained_keys(&self.dbs).await?))
	}
}

#[cfg(feature = "remote")]
pub use remote_executor::{
	Isolation, RemoteExecutor, RootSnapshot, SweepResidue, remote_unsupported, snapshot_root,
	sweep_to_baseline,
};

#[cfg(feature = "remote")]
mod remote_executor {
	use std::sync::atomic::{AtomicU64, Ordering};
	use std::time::{Duration, Instant};

	use anyhow::{Context, Result, anyhow};
	use surrealdb::opt::auth::{Database, Namespace, Root};
	use surrealdb_expr::expr::{Explain, Expr as SurqlExpr, LogicalPlan, TopLevelExpr};
	use surrealdb_syn::error::RenderedError;
	use surrealdb_syn::parser::{self, ParserSettings};
	use surrealdb_types::{Object, ToSql, Value as SurValue};

	use super::{ExecOutcome, Executor, Unsupported};
	use crate::cmd::graphql;
	use crate::cmd::remote::{RawCredentials, RemoteConn, extract_results};
	use crate::cmd::util::ImportFailure;
	use crate::tests::case::{Dialect, TestCase};
	use crate::tests::run::CaseImports;
	use crate::tests::schema::{AuthLevel, BoolOr, NewPlannerStrategyConfig, TestAuth, TestConfig};

	/// Unique-name counter so concurrent tests on one shared remote server get
	/// isolated namespaces/databases (and provisioned users).
	static NEXT_UID: AtomicU64 = AtomicU64::new(0);

	/// A fixed password for the harness-provisioned auth users. They are created
	/// per test and removed by `cleanup` (root users explicitly, namespace/database
	/// users when their namespace is dropped).
	const PROVISIONED_PASSWORD: &str = "language_tests_remote";

	/// What the connection should authenticate as while running the query, after
	/// imports have run as root.
	enum PendingAuth {
		/// Stay root (owner-equivalent) — no provisioning or signin needed.
		Root,
		/// A root-scoped user with the given role.
		RootUser {
			user: String,
			role: &'static str,
		},
		/// A namespace-scoped user with the given role.
		NamespaceUser {
			ns: String,
			user: String,
			role: &'static str,
		},
		/// A database-scoped user with the given role.
		DatabaseUser {
			ns: String,
			db: String,
			user: String,
			role: &'static str,
		},
	}

	fn role_str(level: &AuthLevel) -> &'static str {
		match level {
			AuthLevel::Owner => "OWNER",
			AuthLevel::Editor => "EDITOR",
			AuthLevel::Viewer => "VIEWER",
		}
	}

	/// The root-level entities `INFO FOR ROOT` reports, plus the two singleton
	/// objects it carries. Everything a test can create that outlives a
	/// `REMOVE NAMESPACE`.
	///
	/// `config` and `defaults` are distinct and both belong here: `config` is the
	/// dynamic system configuration (`ALTER SYSTEM`), `defaults` is the default
	/// namespace/database a session lands on (`DEFINE CONFIG DEFAULT`).
	#[derive(Default, Clone)]
	pub struct RootSnapshot {
		namespaces: std::collections::HashSet<String>,
		users: std::collections::HashSet<String>,
		accesses: std::collections::HashSet<String>,
		config: SurValue,
		defaults: SurValue,
	}

	impl RootSnapshot {
		/// Names of entities present here but not in `base`, described well enough
		/// to act on. This is half the comparison — [`RootSnapshot::removed_since`]
		/// is the other half, and what a run destroyed matters as much as what it
		/// left.
		///
		/// `owned` is this run's name prefix where one applies, and everything
		/// carrying it belongs to the run: the namespace each concurrent test is
		/// given, and the root user the harness provisions for a root-scoped
		/// `[env] auth`. Both are live while their test runs, so neither is a leak
		/// — and neither is something to remove out from under it.
		///
		/// `None` means nothing here is the run's to keep, which is how an
		/// exclusive test's cleanup asks what it failed to remove: by then its own
		/// provisioned user is meant to be gone too. It is deliberately not spelt
		/// as an empty prefix, which every name starts with and which would
		/// silently report nothing at all.
		fn added_since(&self, base: &RootSnapshot, owned: Option<&str>) -> Vec<String> {
			let is_owned = |name: &str| owned.is_some_and(|prefix| name.starts_with(prefix));
			let mut out = Vec::new();
			for ns in self.namespaces.difference(&base.namespaces) {
				if is_owned(ns) {
					continue;
				}
				out.push(format!("NAMESPACE `{ns}`"));
			}
			for user in self.users.difference(&base.users) {
				if is_owned(user) {
					continue;
				}
				out.push(format!("USER `{user}` ON ROOT"));
			}
			for ac in self.accesses.difference(&base.accesses) {
				if is_owned(ac) {
					continue;
				}
				out.push(format!("ACCESS `{ac}` ON ROOT"));
			}
			if self.config != base.config {
				out.push("the system config (ALTER SYSTEM)".to_owned());
			}
			if self.defaults != base.defaults {
				out.push("the default namespace/database (DEFINE CONFIG DEFAULT)".to_owned());
			}
			out.sort();
			out
		}

		/// Names of entities in `base` that are gone here — root state the run
		/// destroyed rather than created.
		///
		/// The counterpart to [`RootSnapshot::added_since`], and not optional: a
		/// one-way comparison reads a removal as no difference at all, so a test
		/// that removes a root user, access or namespace the endpoint already had
		/// would pass and be swept clean without a word. `REMOVE ... IF EXISTS`
		/// does not even fail to warn of it.
		///
		/// There is no `owned` prefix here. The prefix names what this run
		/// created, and `base` is read before the run creates anything, so a
		/// prefixed name cannot appear in it.
		///
		/// These are reported, never repaired. A snapshot holds names, not
		/// definitions, and `INFO FOR ROOT` redacts a user's hash — recreating one
		/// would invent an account rather than restore it.
		fn removed_since(&self, base: &RootSnapshot) -> Vec<String> {
			let mut out = Vec::new();
			for ns in base.namespaces.difference(&self.namespaces) {
				out.push(format!("NAMESPACE `{ns}`"));
			}
			for user in base.users.difference(&self.users) {
				out.push(format!("USER `{user}` ON ROOT"));
			}
			for ac in base.accesses.difference(&self.accesses) {
				out.push(format!("ACCESS `{ac}` ON ROOT"));
			}
			out.sort();
			out
		}

		/// Whether `ns` was present when this snapshot was taken.
		///
		/// Asked of the run baseline to decide whether a namespace an exclusive
		/// test claims is the endpoint's rather than the run's: an exclusive test
		/// drops its namespace before snapshotting root, so one that was already
		/// there is destroyed outright and cannot be given back.
		pub fn has_namespace(&self, ns: &str) -> bool {
			self.namespaces.contains(ns)
		}

		/// Whether a `DEFINE CONFIG DEFAULT` was in force when this was taken.
		///
		/// `INFO FOR ROOT` carries the `defaults` key whether or not one is defined,
		/// so an absent, `NONE`, `NULL` or empty value all mean the same thing.
		fn has_defaults(&self) -> bool {
			match &self.defaults {
				SurValue::None | SurValue::Null => false,
				SurValue::Object(o) => !o.is_empty(),
				_ => true,
			}
		}
	}

	/// Put every dynamic system setting back to the value `before` recorded.
	///
	/// Best-effort per setting so one failure doesn't mask the rest. `ALTER
	/// SYSTEM DROP <KEY>` is the way back to unset; there is no removal for a
	/// setting that has a value, so it is re-issued instead.
	pub async fn restore_system_config(conn: &RemoteConn, before: &SurValue, after: &SurValue) {
		let (SurValue::Object(before), SurValue::Object(after)) = (before, after) else {
			return;
		};

		let keys: std::collections::BTreeSet<&String> = before.keys().chain(after.keys()).collect();

		for key in keys {
			let was = before.get(key.as_str()).unwrap_or(&SurValue::None);
			let now = after.get(key.as_str()).unwrap_or(&SurValue::None);
			if was == now {
				continue;
			}

			let statement = if matches!(was, SurValue::None) {
				format!("ALTER SYSTEM DROP {key}")
			} else {
				format!("ALTER SYSTEM {key} {}", was.to_sql())
			};
			let _ = conn.db.query(statement).await;
		}
	}

	/// What a sweep could not put back.
	#[derive(Default)]
	pub struct SweepResidue {
		/// Created during the run and still on the endpoint.
		pub remaining: Vec<String>,
		/// On the endpoint before the run and gone now. Reported only: see
		/// [`RootSnapshot::removed_since`] for why it cannot be undone.
		pub destroyed: Vec<String>,
	}

	impl SweepResidue {
		pub fn is_empty(&self) -> bool {
			self.remaining.is_empty() && self.destroyed.is_empty()
		}
	}

	/// Return the endpoint to `baseline`, and report whatever survived the
	/// attempt.
	///
	/// Every job has finished by the time this runs, so root is meant to look
	/// exactly as it did before the run: each test's `cleanup` removes what it
	/// created and fails the test when it cannot. This is the backstop for the
	/// paths that never reach a `cleanup` at all — a panicking test, or a
	/// connection lost mid-test — which nothing else reports, because the
	/// concurrent leak check excuses this run's own prefix so that a live test's
	/// namespace and user are not read as another test's leak.
	///
	/// Diffing the whole of root is what makes the backstop cover both isolations.
	/// A concurrent test can only leave its prefixed namespace and root user, but
	/// an exclusive test is allowed to write root state and so can leave a
	/// namespace, user or access under any name at all, plus a changed system
	/// config. The diff reaches as far as `INFO FOR ROOT` enumerates.
	///
	/// It runs both ways. A test may just as well *remove* something the endpoint
	/// already had, which no cleanup can restore and which a one-way comparison
	/// cannot even see; that is the single place in a run where such a removal is
	/// reported, so it is reported rather than repaired.
	///
	/// Call once, after every job has finished. Removals are best-effort and the
	/// endpoint is re-read afterwards, because what matters is what is still there
	/// rather than which statement reported success.
	pub async fn sweep_to_baseline(
		conn: &RemoteConn,
		baseline: &RootSnapshot,
	) -> Result<SweepResidue> {
		conn.signin_root().await?;
		let after = snapshot_root(conn).await?;

		// Nothing is the run's to keep now that it is over, so the whole diff is
		// leaked state — including the prefixed entities a live test would have been
		// excused.
		let found = after.added_since(baseline, None);
		let destroyed = after.removed_since(baseline);
		if found.is_empty() {
			return Ok(SweepResidue {
				remaining: Vec::new(),
				destroyed,
			});
		}
		println!(" Removing {} item(s) this run left at root:", found.len());
		for item in &found {
			println!("   - {item}");
		}

		for ns in after.namespaces.difference(&baseline.namespaces) {
			let _ = conn.db.query(format!("REMOVE NAMESPACE IF EXISTS `{ns}`")).await;
		}
		for user in after.users.difference(&baseline.users) {
			let _ = conn.db.query(format!("REMOVE USER IF EXISTS `{user}` ON ROOT")).await;
		}
		for ac in after.accesses.difference(&baseline.accesses) {
			let _ = conn.db.query(format!("REMOVE ACCESS IF EXISTS `{ac}` ON ROOT")).await;
		}
		// `DEFINE CONFIG DEFAULT` is the one root config `INFO FOR ROOT` reports.
		// Removing it is the way back only when the run began without one: a
		// baseline that had one cannot be rebuilt from a snapshot, and removing it
		// would destroy state no test touched, so that case is left to the residue
		// below to report instead.
		if after.defaults != baseline.defaults && !baseline.has_defaults() {
			let _ = conn.db.query("REMOVE CONFIG IF EXISTS DEFAULT").await;
		}
		// Dynamic system settings are not entities to be removed: `INFO FOR ROOT`
		// reports a fixed set of keys whose value is NONE while unset, so one a test
		// changed and did not put back is restored by key.
		restore_system_config(conn, &baseline.config, &after.config).await;

		// Read the endpoint back rather than trust the removals above. Destruction
		// is taken from the reading before them: the removals only ever subtract,
		// so re-deriving it afterwards would blame this sweep for the namespaces it
		// has just correctly taken away.
		Ok(SweepResidue {
			remaining: snapshot_root(conn).await?.added_since(baseline, None),
			destroyed,
		})
	}

	/// Read the root-level state of an endpoint. The caller must already be
	/// authenticated as root.
	pub async fn snapshot_root(conn: &RemoteConn) -> Result<RootSnapshot> {
		let info = conn.db.query("INFO FOR ROOT").await?.take::<SurValue>(0)?;
		let SurValue::Object(info) = info else {
			return Ok(RootSnapshot::default());
		};
		let names = |field: &str| -> std::collections::HashSet<String> {
			match info.get(field) {
				Some(SurValue::Object(o)) => o.keys().cloned().collect(),
				_ => std::collections::HashSet::new(),
			}
		};
		Ok(RootSnapshot {
			namespaces: names("namespaces"),
			users: names("users"),
			accesses: names("accesses"),
			config: info.get("config").cloned().unwrap_or_default(),
			defaults: info.get("defaults").cloned().unwrap_or_default(),
		})
	}

	/// How a test is kept from colliding with the others sharing one endpoint.
	///
	/// The corpus names its namespace `test` almost everywhere, so faithfulness
	/// and concurrency pull against each other: two tests running at once cannot
	/// both have `test`, and a handful of tests assert on the name or on state
	/// that has no namespace to live in. Each test therefore declares which it
	/// needs, and `remote_isolation` turns that into one of these.
	pub enum Isolation {
		/// The test runs alongside others, in a namespace named for this run and
		/// this test alone, which `cleanup` drops whole. Anything it writes
		/// outside that namespace is a defect rather than something to clean up,
		/// and [`Executor::check_root_leak`] reports it.
		Concurrent {
			/// The namespace this test owns, `<run prefix>_<n>`.
			ns: String,
			/// Where `run prefix` ends, so a leak check can tell this run's
			/// namespaces from one a test created itself.
			run_prefix: String,
			/// Root state from before the run started. Tests are forbidden from
			/// changing it, so one reading serves them all.
			baseline: std::sync::Arc<RootSnapshot>,
			/// How many tests may be in flight at once.
			///
			/// A test's `[env] timeout` is a budget for its query, but remote runs
			/// can only measure the round trip, which includes waiting behind the
			/// other tests sharing the server. The budget is multiplied by this so
			/// it stays a statement about the query rather than about how many jobs
			/// the run was given. An exclusive test has the endpoint to itself and
			/// needs no such allowance.
			jobs: u32,
		},
		/// The test runs alone, in the namespace its config names, and may write
		/// root-level state — which `cleanup` removes by diffing a snapshot taken
		/// in `prepare`, sound only because nothing else is running. The only
		/// option for a test that declares no namespace, since it has nowhere but
		/// root to write.
		Exclusive {
			/// This run's name prefix. The namespace keeps its configured name
			/// here, but a provisioned root user still takes the prefix, so that
			/// one rule holds across both isolations: everything the harness
			/// creates outside a test's namespace carries it, and so says plainly
			/// which run left it if a run is interrupted.
			run_prefix: String,
		},
	}

	/// Runs a test against a remote SurrealDB instance over the API.
	///
	/// SurrealQL goes through the SDK (`db.query`); GraphQL/GQL are POSTed to the
	/// `/graphql` / `/gql` HTTP routes. Each test runs in its own namespace,
	/// created in `prepare` and dropped in `cleanup`; [`Isolation`] decides
	/// whether that namespace is named for the test or for the run.
	pub struct RemoteExecutor<'a> {
		conn: &'a RemoteConn,
		/// Counter for uniquely naming harness-provisioned auth users, so they
		/// can't clash with a user the test defines itself.
		uid: u64,
		/// How this test is kept from colliding with concurrent ones.
		isolation: Isolation,
		/// The namespace the test runs in: its configured name when exclusive,
		/// this run's unique name when concurrent. `None` when the test declares
		/// no namespace, which only an exclusive test may do.
		ns: Option<String>,
		/// The test's database — its real configured name.
		db: Option<String>,
		/// The namespace and database names the test's *config* gives, which its
		/// `[env] signin`/`signup` credentials name literally. Equal to `ns`/`db`
		/// for an exclusive test; see [`RemoteExecutor::redirect_credentials`].
		configured_ns: Option<String>,
		configured_db: Option<String>,
		/// Whether EXPLAIN-volatile attrs should be stripped client-side to match
		/// the embedded harness's deterministic EXPLAIN output.
		redact_explain: bool,
		/// The principal to authenticate as for the query (applied after imports).
		pending_auth: PendingAuth,
		/// Set when the test's own `[env].signin`/`signup` ran, so the per-test
		/// auth level is not re-applied over it.
		under_test_authed: bool,
		/// Root state captured in `prepare`, for an exclusive test only — a
		/// concurrent one diffs against the run baseline instead.
		///
		/// `None` until `prepare` has taken it. Cleanup must not diff against a
		/// default in its place: an empty snapshot says the endpoint was empty
		/// when the test started, so every namespace, user and access method on it
		/// reads as something the test created and gets removed.
		root_snapshot: Option<RootSnapshot>,
	}

	impl<'a> RemoteExecutor<'a> {
		pub fn new(conn: &'a RemoteConn, isolation: Isolation) -> Self {
			RemoteExecutor {
				conn,
				uid: 0,
				isolation,
				ns: None,
				db: None,
				configured_ns: None,
				configured_db: None,
				redact_explain: true,
				pending_auth: PendingAuth::Root,
				under_test_authed: false,
				root_snapshot: None,
			}
		}

		/// Run a statement as the current principal, returning an error if any
		/// statement in it failed.
		async fn run_checked(&self, sql: String) -> Result<()> {
			self.conn.db.query(sql).await?.check()?;
			Ok(())
		}

		/// DEFINE the auth user this test runs as (no-op for the root case).
		async fn provision_auth(&self) -> Result<()> {
			match &self.pending_auth {
				PendingAuth::Root => Ok(()),
				PendingAuth::RootUser {
					user,
					role,
				} => {
					self.run_checked(format!(
						"DEFINE USER `{user}` ON ROOT PASSWORD '{PROVISIONED_PASSWORD}' ROLES {role}"
					))
					.await
				}
				PendingAuth::NamespaceUser {
					ns,
					user,
					role,
				} => {
					self.conn.db.use_ns(ns.clone()).await?;
					self.run_checked(format!(
						"DEFINE USER `{user}` ON NAMESPACE PASSWORD '{PROVISIONED_PASSWORD}' ROLES {role}"
					))
					.await
				}
				PendingAuth::DatabaseUser {
					ns,
					db,
					user,
					role,
				} => {
					self.conn.db.use_ns(ns.clone()).await?;
					self.conn.db.use_db(db.clone()).await?;
					self.run_checked(format!(
						"DEFINE USER `{user}` ON DATABASE PASSWORD '{PROVISIONED_PASSWORD}' ROLES {role}"
					))
					.await
				}
			}
		}

		/// Authenticate as the per-test principal (called after imports). Skipped
		/// if the test ran its own signin/signup.
		async fn apply_auth(&self) -> Result<()> {
			if self.under_test_authed {
				return Ok(());
			}
			let pass = PROVISIONED_PASSWORD.to_string();
			match &self.pending_auth {
				PendingAuth::Root => Ok(()),
				PendingAuth::RootUser {
					user,
					..
				} => {
					self.conn
						.db
						.signin(Root {
							username: user.clone(),
							password: pass,
						})
						.await?;
					Ok(())
				}
				PendingAuth::NamespaceUser {
					ns,
					user,
					..
				} => {
					self.conn
						.db
						.signin(Namespace {
							namespace: ns.clone(),
							username: user.clone(),
							password: pass,
						})
						.await?;
					Ok(())
				}
				PendingAuth::DatabaseUser {
					ns,
					db,
					user,
					..
				} => {
					self.conn
						.db
						.signin(Database {
							namespace: ns.clone(),
							database: db.clone(),
							username: user.clone(),
							password: pass,
						})
						.await?;
					Ok(())
				}
			}
		}

		/// The wall-clock a test's query is allowed, from the budget its `[env]
		/// timeout` declares.
		///
		/// Remote execution can only time the whole round trip, so under
		/// concurrency the measurement includes queueing behind the other tests
		/// sharing the server; `Isolation::Concurrent`'s `jobs` is the allowance
		/// for that.
		fn timeout_budget(&self, declared: Duration) -> Duration {
			match &self.isolation {
				Isolation::Concurrent {
					jobs,
					..
				} => declared.saturating_mul(*jobs),
				Isolation::Exclusive {
					..
				} => declared,
			}
		}

		/// Point a test's own signin/signup credentials at the namespace and
		/// database it is actually running in.
		///
		/// `[env] signin = { ns: "test", db: "test", ac: "account", .. }` names
		/// them literally, because that is where the embedded harness puts the
		/// test. A concurrent remote run puts it somewhere else, so the literal
		/// would reach a namespace that does not exist.
		///
		/// Only a value equal to the test's own configured name is rewritten. A
		/// test signing in to some other namespace is asserting something about
		/// that namespace, and is left alone.
		fn redirect_credentials(&self, vars: Object) -> Object {
			redirect_credentials(
				vars,
				self.configured_ns.as_deref(),
				self.ns.as_deref(),
				self.configured_db.as_deref(),
				self.db.as_deref(),
			)
		}
	}

	/// See [`RemoteExecutor::redirect_credentials`]; free-standing so it can be
	/// tested without a connection.
	fn redirect_credentials(
		mut vars: Object,
		configured_ns: Option<&str>,
		actual_ns: Option<&str>,
		configured_db: Option<&str>,
		actual_db: Option<&str>,
	) -> Object {
		const NS_KEYS: &[&str] = &["ns", "NS", "namespace"];
		const DB_KEYS: &[&str] = &["db", "DB", "database"];
		for (keys, configured, actual) in
			[(NS_KEYS, configured_ns, actual_ns), (DB_KEYS, configured_db, actual_db)]
		{
			let (Some(configured), Some(actual)) = (configured, actual) else {
				continue;
			};
			for key in keys {
				if let Some(SurValue::String(v)) = vars.get_mut(*key)
					&& v.as_str() == configured
				{
					*v = actual.to_owned();
				}
			}
		}
		vars
	}

	/// Parses a test's SurrealQL source into the logical plan
	/// [`is_explain_analyze`] walks, rendering a syntax error against the source
	/// so it reads as the test's result rather than as a runner error.
	fn parse_to_plan(source: &str, settings: ParserSettings) -> Result<LogicalPlan, RenderedError> {
		let bytes = source.as_bytes();
		let mut p = parser::Parser::new_with_settings(bytes, settings);
		let mut stack = reblessive::Stack::new();
		let ast =
			stack.enter(|stk| p.parse_query(stk)).finish().map_err(|e| e.render_on_bytes(bytes))?;
		p.assert_finished().map_err(|e| e.render_on_bytes(bytes))?;
		Ok(ast.into())
	}

	/// Whether a test can be faithfully reproduced over a remote `--endpoint`.
	/// Standalone so the scheduler can skip a test without acquiring a connection.
	pub fn remote_unsupported(
		case: &CaseImports,
		strategy: Option<NewPlannerStrategyConfig>,
	) -> Option<Unsupported> {
		let config = &case.test.config.parsed;
		let env = &config.env;
		// An explicit opt-out always wins: the test author has declared this test
		// can't be faithfully reproduced over the API and given a reason.
		if let Some(reason) = &env.remote_skip {
			return Some(Unsupported::EmbeddedOnly(reason.clone()));
		}
		if !matches!(env.capabilities, BoolOr::Bool(true)) {
			return Some(Unsupported::NonDefaultCapabilities);
		}
		// Which planner answers a read-only statement is server-startup
		// configuration, so a remote run has the one strategy the endpoint holds
		// rather than the axis an embedded run walks. A test narrowed away from
		// that engine was written to be graded under the other one — typically
		// because it pins a plan only that one produces — so it is reported here
		// rather than graded under an engine it did not ask for.
		//
		// Judged by engine rather than by name: `all-ro` and `best-effort-ro`
		// plan an answerable statement identically, so a test declaring either is
		// gradeable under either. See
		// [`NewPlannerStrategyConfig::is_streaming`].
		if let Some(strategy) = strategy
			&& !env.planner_strategy.iter().any(|x| x.is_streaming() == strategy.is_streaming())
		{
			return Some(Unsupported::PlannerStrategy(strategy));
		}
		if env.versioned {
			return Some(Unsupported::Versioned);
		}
		if matches!(env.auth, Some(TestAuth::Record { .. })) {
			return Some(Unsupported::RecordAuth);
		}
		// A namespace- or database-scoped `[env] auth` names the scope the
		// identity is created in. The embedded harness honours that name; a remote
		// run cannot, because the test's own namespace is renamed and a scope
		// naming any other one would put a user somewhere the test does not own.
		//
		// Where the declared scope *is* the test's own, the provisioning follows
		// the rename and is faithful. Where it differs, the test is asking to be
		// graded against privileges in one namespace while querying another —
		// running it against the query namespace instead would let a statement
		// that should be denied succeed, which is a pass the test did not earn.
		let scope = match &env.auth {
			Some(TestAuth::Namespace {
				namespace,
				..
			}) => Some((Some(namespace.as_str()), None)),
			Some(TestAuth::Database {
				namespace,
				database,
				..
			}) => Some((Some(namespace.as_str()), Some(database.as_str()))),
			_ => None,
		};
		if let Some((ns, db)) = scope
			&& (ns.is_some_and(|ns| Some(ns) != env.namespace())
				|| db.is_some_and(|db| Some(db) != env.database()))
		{
			return Some(Unsupported::ForeignAuthScope);
		}
		// The `/graphql` and `/gql` routes are reached with their own HTTP request,
		// authenticated as root; neither the SDK connection's signed-in principal
		// nor the harness-provisioned per-test user reaches them. A test's
		// non-owner auth (or auth under test) therefore can't be conveyed, and a
		// permissions check would falsely pass. Owner-at-root / no-auth map
		// cleanly to root, so only those run.
		if matches!(case.test.dialect, Dialect::GraphQl | Dialect::Gql) {
			let non_owner_auth = !matches!(
				env.auth,
				None | Some(TestAuth::Root {
					level: AuthLevel::Owner
				})
			);
			if non_owner_auth || env.signin.is_some() || env.signup.is_some() {
				return Some(Unsupported::HttpRouteAuth);
			}
		}
		None
	}

	impl Executor for RemoteExecutor<'_> {
		async fn prepare(&mut self, config: &TestConfig) -> Result<()> {
			let env = &config.env;
			self.uid = NEXT_UID.fetch_add(1, Ordering::Relaxed);
			self.redact_explain = env.redact_volatile_explain_attrs.unwrap_or(true);
			self.under_test_authed = false;

			// An exclusive test keeps the namespace name its config gives (mirroring
			// the embedded harness), so a test asserting on the name, or defining a
			// ns/db/user/access by it, behaves identically; nothing else is running,
			// so the name is free. A concurrent test cannot have it — the corpus
			// names almost every namespace `test` — and takes this run's unique one.
			// The database keeps its configured name either way: it is scoped to the
			// namespace, so it never collides.
			self.ns = match &self.isolation {
				Isolation::Concurrent {
					ns,
					..
				} => env.namespace().map(|_| ns.clone()),
				Isolation::Exclusive {
					..
				} => env.namespace().map(str::to_owned),
			};
			self.db = env.database().map(str::to_owned);
			self.configured_ns = env.namespace().map(str::to_owned);
			self.configured_db = env.database().map(str::to_owned);

			self.conn.signin_root().await?;

			if let Isolation::Exclusive {
				..
			} = self.isolation
			{
				// `cleanup` is best-effort per statement, so a previous test's
				// namespace may still be here. Drop it *before* the snapshot below:
				// captured as pre-existing it would never be cleaned, and the bare
				// `DEFINE NAMESPACE` further down errors against an existing name —
				// which would fail not just this test but every later one sharing it.
				if let Some(ns) = self.ns.as_deref() {
					self.run_checked(format!("REMOVE NAMESPACE IF EXISTS `{ns}`")).await?;
				}

				// Snapshot root state *before* creating anything, so cleanup removes
				// exactly what this test adds and leaves pre-existing entities alone.
				// A concurrent test needs neither: its namespace name is unique, so
				// nothing is there to drop, and it is forbidden from writing at root
				// at all — which `check_root_leak` verifies against the run baseline.
				self.root_snapshot = Some(snapshot_root(self.conn).await?);
			}

			if let Some(ns) = self.ns.clone() {
				self.run_checked(format!("DEFINE NAMESPACE `{ns}`")).await?;
				self.conn.db.use_ns(ns).await?;
			}
			if let Some(db) = self.db.clone() {
				self.run_checked(format!("DEFINE DATABASE `{db}`")).await?;
				self.conn.db.use_db(db).await?;
			}

			// Decide (and provision) the principal the query runs as. Owner at root
			// (and the no-auth default) is root itself; every other level needs a
			// provisioned user so the test sees the right privileges.
			// A root-scoped one is created outside the test's namespace and stays
			// there for as long as the test runs, so it is named from this run's
			// prefix: that is what tells a concurrent test's leak check it belongs
			// to the run rather than being something a test left behind.
			let user = match &self.isolation {
				Isolation::Concurrent {
					run_prefix,
					..
				} => format!("{run_prefix}_user_{}", self.uid),
				Isolation::Exclusive {
					run_prefix,
				} => format!("{run_prefix}_user_{}", self.uid),
			};
			self.pending_auth = match &env.auth {
				None => PendingAuth::Root,
				Some(TestAuth::Root {
					level: AuthLevel::Owner,
				}) => PendingAuth::Root,
				Some(TestAuth::Root {
					level,
				}) => PendingAuth::RootUser {
					user,
					role: role_str(level),
				},
				Some(TestAuth::Namespace {
					level,
					..
				}) => PendingAuth::NamespaceUser {
					ns: self.ns.clone().unwrap_or_default(),
					user,
					role: role_str(level),
				},
				Some(TestAuth::Database {
					level,
					..
				}) => PendingAuth::DatabaseUser {
					ns: self.ns.clone().unwrap_or_default(),
					db: self.db.clone().unwrap_or_default(),
					user,
					role: role_str(level),
				},
				// Record auth is filtered out by `unsupported`.
				Some(TestAuth::Record {
					..
				}) => PendingAuth::Root,
			};
			self.provision_auth().await?;
			Ok(())
		}

		async fn run_imports(
			&mut self,
			imports: &[std::sync::Arc<TestCase>],
		) -> Result<Option<ImportFailure>> {
			// Imports run as root (owner) in the per-test namespace/database.
			for import in imports {
				match self.conn.db.query(import.source.clone()).await {
					Err(e) => {
						return Ok(Some(ImportFailure {
							path: import.origin.path.clone(),
							message: format!("Failed to run import: `{e}`"),
						}));
					}
					Ok(mut resp) => {
						let errors = resp.take_errors();
						if let Some((_, e)) = errors.into_iter().min_by_key(|(i, _)| *i) {
							return Ok(Some(ImportFailure {
								path: import.origin.path.clone(),
								message: format!("Failed to run import: `{e}`"),
							}));
						}
					}
				}
			}
			// Downgrade to the per-test principal now that imports are done.
			self.apply_auth().await?;
			Ok(None)
		}

		async fn signup(&mut self, vars: Object) -> Result<()> {
			self.conn.db.signup(RawCredentials(self.redirect_credentials(vars))).await?;
			self.under_test_authed = true;
			Ok(())
		}

		async fn signin(&mut self, vars: Object) -> Result<()> {
			self.conn.db.signin(RawCredentials(self.redirect_credentials(vars))).await?;
			self.under_test_authed = true;
			Ok(())
		}

		async fn execute(&mut self, case: &TestCase, timeout: Duration) -> Result<ExecOutcome> {
			match case.dialect {
				Dialect::SurrealQl => {
					// Parse client-side first so parse failures surface as a
					// ParserError (matching the embedded result type), then send the
					// source to the server to execute.
					//
					// We can't read the remote server's capabilities from here, so we
					// assume the Files/Surrealism experimental syntax is enabled. This
					// parse exists only to reproduce the ParserError *result type*; the
					// server's actual capabilities still govern execution. The only
					// divergence is a test using that experimental syntax against an
					// endpoint where it's disabled: it parses OK here (no ParserError)
					// and instead fails server-side — differing from the embedded
					// ParserError. That's part of the known remote-parity surface
					// (such tests are best run against an endpoint with matching
					// capabilities).
					let settings = ParserSettings {
						files_enabled: true,
						surrealism_enabled: true,
						..Default::default()
					};
					// The same parse also says which statements are EXPLAIN ANALYZE,
					// which is what bounds the redaction below to the results that
					// carry server metrics.
					//
					// What it must not do is decide the outcome by itself. The endpoint
					// can be a different build, whose grammar accepts syntax this one
					// rejects; answering from here would report a parse error for a
					// query the endpoint runs, and would let a test expecting a parse
					// error pass on this build's verdict rather than the endpoint's. So
					// the endpoint is asked first and this error is kept only to
					// classify a refusal the endpoint also made. Sending a query that
					// cannot parse costs a round trip and runs nothing: the server
					// parses a query whole before executing any of it.
					let (analyzed, parse_error): (Vec<bool>, _) =
						match parse_to_plan(&case.source, settings) {
							Ok(plan) => {
								(plan.expressions.iter().map(is_explain_analyze).collect(), None)
							}
							Err(e) => (Vec::new(), Some(e)),
						};

					let start = Instant::now();
					let res = self.conn.db.query(case.source.clone()).await;
					let did_timeout = start.elapsed() > self.timeout_budget(timeout);
					let result = match res {
						Ok(resp) => {
							let mut values = extract_results(resp);
							if self.redact_explain {
								// One result per statement, so the flags line up by
								// index. A statement whose flag is missing is left
								// alone: redacting a value the server never generated
								// would rewrite the test's own data.
								for (idx, slot) in values.iter_mut().enumerate() {
									if analyzed.get(idx).copied().unwrap_or(false)
										&& let Ok(value) = slot
									{
										redact_volatile_explain(value);
									}
								}
							}
							Ok(values)
						}
						// The endpoint refused it. If this build's parser refused it
						// too, the refusal is a parse error and that is the result
						// shape the test is graded against.
						Err(e) => match parse_error {
							Some(rendered) => return Ok(ExecOutcome::ParserError(rendered)),
							None => Err(anyhow!(e)),
						},
					};
					Ok(ExecOutcome::Executed {
						did_timeout,
						result,
					})
				}
				Dialect::Gql => {
					let settings = surrealdb_gql::GqlParserSettings::default();
					let source = &case.source.as_bytes();
					// Parsed for the error shape alone, and held rather than acted on:
					// the endpoint may run a different build whose GQL grammar accepts
					// what this one rejects, and answering from here would grade such a
					// case without ever asking it. Same reasoning as the SurrealQL arm
					// above.
					let parse_error =
						surrealdb_gql::parse_to_plan_with_settings(&case.source, settings)
							.err()
							.map(|e| e.render_on_bytes(source));
					let (Some(ns), Some(db)) = (self.ns.as_deref(), self.db.as_deref()) else {
						return Ok(ExecOutcome::Executed {
							did_timeout: false,
							result: Err(anyhow!("GQL requires a namespace/database")),
						});
					};
					let start = Instant::now();
					let res = self.conn.gql(ns, db, &case.source).await?;
					let did_timeout = start.elapsed() > self.timeout_budget(timeout);
					let result = match res {
						crate::cmd::remote::GqlOutcome::Ran(values) => Ok(values),
						// The route declined it. If this build's parser declined it
						// too, that is a parse error on both sides and the shape the
						// test is graded against; otherwise the rejection stands as
						// the single result, which is what names a missing route or a
						// disabled capability.
						crate::cmd::remote::GqlOutcome::Rejected(msg) => match parse_error {
							Some(rendered) => return Ok(ExecOutcome::ParserError(rendered)),
							None => Ok(vec![Err(msg)]),
						},
					};
					Ok(ExecOutcome::Executed {
						did_timeout,
						result,
					})
				}
				Dialect::GraphQl => {
					let (Some(ns), Some(db)) = (self.ns.as_deref(), self.db.as_deref()) else {
						return Ok(ExecOutcome::Executed {
							did_timeout: false,
							result: Err(anyhow!("GraphQL requires a namespace/database")),
						});
					};
					let variables = graphql::request_variables(case)
						.map(|v| serde_json::to_value(v).unwrap_or(serde_json::Value::Null))
						.unwrap_or(serde_json::Value::Null);
					let mut body = serde_json::json!({
						"query": graphql::case_source(case),
						"variables": variables,
					});
					if let Some(op) = case.config.parsed.graphql.operation.as_ref() {
						body["operationName"] = serde_json::Value::String(op.clone());
					}
					let start = Instant::now();
					let res = self.conn.graphql(ns, db, body).await;
					let did_timeout = start.elapsed() > self.timeout_budget(timeout);
					let result = match res {
						Ok(response) => Ok(vec![graphql::response_to_result(response)]),
						Err(e) => Err(anyhow!(e)),
					};
					Ok(ExecOutcome::Executed {
						did_timeout,
						result,
					})
				}
			}
		}

		async fn cleanup(&mut self) -> Result<()> {
			// Re-establish root first: the test may have signed in as another
			// principal, and everything below needs owner rights.
			self.conn.signin_root().await?;

			// Removing a namespace cascades its databases, data, users and access
			// methods, so for a concurrent test — which is confined to one — that
			// single statement is the whole cleanup. Best-effort: a failure here
			// leaves a uniquely named namespace behind, which costs storage on the
			// endpoint but cannot collide with or be seen by any later test.
			if let Isolation::Concurrent {
				..
			} = self.isolation
			{
				if let Some(ns) = self.ns.as_deref() {
					let _ = self.conn.db.query(format!("REMOVE NAMESPACE IF EXISTS `{ns}`")).await;
				}
				// A root-scoped principal is the one thing the harness provisions
				// outside the test's namespace, so the namespace drop above does not
				// take it. Namespace- and database-scoped users live inside and go
				// with it.
				//
				// Checked, not best-effort, and checked at the statement level as
				// well as the transport: the account holds a known fixed password and
				// outlives the namespace. The leak check that runs next skips every
				// name carrying the run prefix, which this one does by construction —
				// that is what stops a live test's own user being read as another
				// test's leak — so nothing until the end-of-run sweep would notice a
				// discarded failure, and by then it can only be reported against the
				// run rather than against the test that caused it.
				if let PendingAuth::RootUser {
					user,
					..
				} = &self.pending_auth
				{
					self.run_checked(format!("REMOVE USER IF EXISTS `{user}` ON ROOT"))
						.await
						.with_context(|| {
							format!("removing the root user provisioned for this test (`{user}`)")
						})?;
				}
				return Ok(());
			}

			// An exclusive test may have written anywhere, so remove every
			// root-level entity that appeared while it ran (diff against the
			// prepare snapshot). This returns the shared endpoint to its pre-test
			// state — the serial analogue of the embedded harness's per-test fresh
			// datastore. Best-effort per statement so one failed removal doesn't
			// mask the rest.
			//
			// Without a snapshot there is nothing to diff against, and `prepare`
			// creates nothing before taking it, so there is nothing to undo either.
			let Some(before) = self.root_snapshot.clone() else {
				return Ok(());
			};
			let after = snapshot_root(self.conn).await?;
			for ns in after.namespaces.difference(&before.namespaces) {
				let _ = self.conn.db.query(format!("REMOVE NAMESPACE IF EXISTS `{ns}`")).await;
			}
			for user in after.users.difference(&before.users) {
				let _ = self.conn.db.query(format!("REMOVE USER IF EXISTS `{user}` ON ROOT")).await;
			}
			for ac in after.accesses.difference(&before.accesses) {
				let _ = self.conn.db.query(format!("REMOVE ACCESS IF EXISTS `{ac}` ON ROOT")).await;
			}
			// `GRAPHQL` and `API` aren't enumerated by `INFO FOR ROOT`, so a test's
			// are dropped unconditionally (idempotent). `DEFAULT` is enumerated, and
			// so is dropped only when the test actually changed it: doing it blindly
			// would destroy one the endpoint already had, which is the harness
			// committing the very fault `RootSnapshot::removed_since` reports.
			let _ = self
				.conn
				.db
				.query("REMOVE CONFIG IF EXISTS GRAPHQL; REMOVE CONFIG IF EXISTS API")
				.await;
			if after.defaults != before.defaults && !before.has_defaults() {
				let _ = self.conn.db.query("REMOVE CONFIG IF EXISTS DEFAULT").await;
			}
			// Dynamic system settings are not entities to be removed: `INFO FOR ROOT`
			// reports a fixed set of keys whose value is NONE while unset, so one a
			// test changed and did not put back is restored by key. A test that
			// failed before its own compensating `ALTER SYSTEM DROP` would otherwise
			// leave the setting in force for every test that follows.
			restore_system_config(self.conn, &before.config, &after.config).await;

			// Read the endpoint back rather than trust the removals above: what
			// matters is what is still there. Anything left fails the test, because
			// an exclusive test has no leak check to catch it later and the next
			// test's own snapshot would record the residue as pre-existing. The
			// root user provisioned for a root-scoped `[env] auth` is the costly
			// case — it holds a known fixed password — but a root user, access or
			// namespace the test defined is just as much a dirty endpoint.
			let after_cleanup = snapshot_root(self.conn).await?;
			let residue = after_cleanup.added_since(&before, None);
			// A test is as free to remove root state as to add it, and removal is
			// the worse of the two: nothing can put it back. This is the only check
			// that sees it per test — the concurrent leak check deliberately does
			// not — so it reports it here, where exactly one test can be blamed.
			let destroyed = after_cleanup.removed_since(&before);
			if !residue.is_empty() || !destroyed.is_empty() {
				let mut parts = Vec::new();
				if !residue.is_empty() {
					parts.push(format!("still present: {}", residue.join(", ")));
				}
				if !destroyed.is_empty() {
					parts.push(format!(
						"removed by the test and not restorable: {}",
						destroyed.join(", ")
					));
				}
				anyhow::bail!(
					"cleanup could not return the endpoint to its pre-test state; {}",
					parts.join("; ")
				);
			}
			Ok(())
		}

		async fn check_retained_keys(&mut self) -> Result<Option<Vec<Vec<u8>>>> {
			// No raw KV access over the API; the server's own GC handles tombstones.
			Ok(None)
		}

		async fn check_root_leak(&mut self) -> Result<Vec<String>> {
			// A concurrent test's isolation is its namespace, so anything it leaves
			// outside one is both uncleaned and visible to every test still running.
			// Compare against the baseline taken before the run began: tests are
			// forbidden from changing root state, so it must still hold. This runs
			// after `cleanup`, so the test's own namespace is already gone; the
			// others' are excluded by their shared prefix.
			let Isolation::Concurrent {
				run_prefix,
				baseline,
				..
			} = &self.isolation
			else {
				return Ok(Vec::new());
			};
			self.conn.signin_root().await?;
			let after = snapshot_root(self.conn).await?;
			let added = after.added_since(baseline, Some(run_prefix));

			// Put the endpoint back to the baseline. Without this one offender is
			// reported by every test that runs after it, since they all diff against
			// the same reading — and the stray entity would stay visible to them too,
			// turning one declaration bug into a run-wide failure.
			for ns in after.namespaces.difference(&baseline.namespaces) {
				if ns.starts_with(run_prefix) {
					continue;
				}
				let _ = self.conn.db.query(format!("REMOVE NAMESPACE IF EXISTS `{ns}`")).await;
			}
			for user in after.users.difference(&baseline.users) {
				if user.starts_with(run_prefix) {
					continue;
				}
				let _ = self.conn.db.query(format!("REMOVE USER IF EXISTS `{user}` ON ROOT")).await;
			}
			for ac in after.accesses.difference(&baseline.accesses) {
				if ac.starts_with(run_prefix) {
					continue;
				}
				let _ = self.conn.db.query(format!("REMOVE ACCESS IF EXISTS `{ac}` ON ROOT")).await;
			}
			if after.defaults != baseline.defaults {
				let _ = self.conn.db.query("REMOVE CONFIG IF EXISTS DEFAULT").await;
			}
			restore_system_config(self.conn, &baseline.config, &after.config).await;

			Ok(added)
		}
	}

	/// The metrics an EXPLAIN ANALYZE keeps when the server redacts: the row count,
	/// and the flag saying a bitmap branch declined (which makes its `rows: 0`
	/// distinguishable from a genuinely empty candidate set). Everything else the
	/// server emits is progress- or timing-dependent.
	const DETERMINISTIC_METRICS: &[&str] = &["output_rows", "dropped"];

	/// Recursively strip the volatile EXPLAIN-ANALYZE metrics that a session with
	/// `redact_volatile_explain_attrs` set never receives, so remote EXPLAIN output
	/// matches the deterministic output the embedded harness asserts against.
	///
	/// That flag lives on the server's `Session` and there is no way to set it on a
	/// request over the API, so the redaction is reproduced client-side here.
	///
	/// EXPLAIN ANALYZE has two output shapes, both handled:
	/// - JSON (`FORMAT JSON`): a `metrics` object per operator, reduced to
	///   [`DETERMINISTIC_METRICS`].
	/// - Text (the default): an inline block per operator, `{rows: 3, batches: 1, elapsed:
	///   8.29µs}`, reduced to `{rows: 3}` — see [`redact_explain_metrics_text`].
	///
	/// The JSON half **retains** the deterministic keys rather than naming the
	/// volatile ones. The server emits eleven volatile counters and gains more over
	/// time; an allow-list cannot fall behind that, and a deny-list already had —
	/// `records_fetched`, which the server renders even at zero, reached every
	/// assertion untouched. Retaining also confines the rewrite to a `metrics`
	/// object, so a test whose own data happens to use one of these names is left
	/// alone.
	/// Whether a parsed top-level expression runs its statement under
	/// `EXPLAIN ANALYZE`.
	///
	/// Only the analyzing form carries the volatile metrics the redaction strips.
	/// A plain `EXPLAIN` renders the plan without them and is excluded, as is
	/// every ordinary statement — whose results are the test's own data and must
	/// be returned untouched.
	///
	/// Decided on the parsed expression, so only a statement that really runs an
	/// EXPLAIN qualifies. Two spellings reach the same place and both count:
	/// - `EXPLAIN ANALYZE [FORMAT ...] <statement>`, an [`Expr::Explain`] whose `analyze` is set;
	/// - `<statement> EXPLAIN FULL`, the trailing clause the four data statements carry, whose flag
	///   distinguishes `FULL` from a plain `EXPLAIN`.
	///
	/// Matched anywhere in the statement, not just at its root: an EXPLAIN is
	/// routinely nested, as in `RETURN (EXPLAIN ANALYZE SELECT ...)`, and its
	/// metrics then surface inside the enclosing statement's result. The visitor
	/// is what reaches those.
	///
	/// Reading the text instead would mark a statement that merely *mentions* the
	/// spelling — in a string literal, say — as analyzed, and rewrite a `metrics`
	/// object that is the test's own data. The embedded executor redacts nothing,
	/// so that divergence would be the remote path inventing a difference.
	///
	/// Under-matching is the failure this cannot detect itself: a new way to
	/// spell an EXPLAIN would silently stop being redacted. The EXPLAIN language
	/// tests are the guard, failing loudly against an endpoint, as they are for
	/// [`redact_explain_metrics_text`].
	fn is_explain_analyze(expr: &TopLevelExpr) -> bool {
		use surrealdb_expr::expr::visit::{Visit, Visitor};

		#[derive(Default)]
		struct FindExplain {
			found: bool,
		}

		impl Visitor for FindExplain {
			type Error = std::convert::Infallible;

			fn visit_expr(&mut self, expr: &SurqlExpr) -> Result<(), Self::Error> {
				let explained = match expr {
					SurqlExpr::Explain {
						analyze,
						..
					} => *analyze,
					// `EXPLAIN` alone renders no metrics; only `EXPLAIN FULL`
					// does, which is what the flag carries.
					SurqlExpr::Select(s) => matches!(s.explain, Some(Explain(true))),
					SurqlExpr::Update(s) => matches!(s.explain, Some(Explain(true))),
					SurqlExpr::Upsert(s) => matches!(s.explain, Some(Explain(true))),
					SurqlExpr::Delete(s) => matches!(s.explain, Some(Explain(true))),
					_ => false,
				};
				self.found |= explained;
				// Keep walking regardless: a nested EXPLAIN counts too, and the
				// visitor has no early exit.
				expr.visit(self)
			}
		}

		// Only an expression can carry one; the other top-level forms are
		// transaction control and session statements.
		let TopLevelExpr::Expr(expr) = expr else {
			return false;
		};
		let mut find = FindExplain::default();
		let Ok(()) = find.visit_expr(expr);
		find.found
	}

	fn redact_volatile_explain(value: &mut SurValue) {
		match value {
			SurValue::Object(obj) => {
				if let Some(SurValue::Object(metrics)) = obj.get_mut("metrics") {
					metrics.retain(|k, _| DETERMINISTIC_METRICS.contains(&k.as_str()));
				}
				for (_, v) in obj.iter_mut() {
					redact_volatile_explain(v);
				}
			}
			SurValue::Array(arr) => {
				for v in arr.iter_mut() {
					redact_volatile_explain(v);
				}
			}
			SurValue::String(s) if s.contains(", batches: ") => {
				*s = redact_explain_metrics_text(s);
			}
			_ => {}
		}
	}

	/// The one deterministic metric the server renders *after* the volatile tail of
	/// a text-format block, and so the one thing the span below must put back.
	const TEXT_DROPPED_SUFFIX: &str = ", dropped: true";

	/// Strips the volatile portion of each inline metrics block from a text-format
	/// EXPLAIN ANALYZE plan, leaving what the server renders when redacting:
	/// `rows: N`, plus `, dropped: true` where the block carried it.
	///
	/// This hand-mirrors the server's `format_metrics_text` rather than calling a
	/// shared helper (none is exposed across the crate boundary), so it is coupled
	/// to that exact text format:
	/// `{rows: N, batches: M, elapsed: T<volatile counters>[, dropped: true]}`.
	/// `dropped` is emitted last, *after* the volatile counters, so it sits inside
	/// the span being cut and has to be re-emitted — dropping it would silently
	/// erase the flag that tells a declined bitmap branch from an empty one.
	///
	/// If the server's format changes (different separator/key order, or a `}`
	/// inside a metric value) this would strip the wrong span and the result would
	/// look like a real test mismatch rather than a redaction bug. The matching
	/// EXPLAIN language-tests are the guard: they'd fail loudly here if the format
	/// drifts, signalling this needs updating.
	fn redact_explain_metrics_text(s: &str) -> String {
		let mut out = String::with_capacity(s.len());
		let mut rest = s;
		while let Some(pos) = rest.find(", batches: ") {
			out.push_str(&rest[..pos]);
			// Everything from `, batches:` up to the closing `}` is volatile,
			// except a trailing `dropped`; resume at the `}` so the rest of the
			// plan is preserved.
			match rest[pos..].find('}') {
				Some(end) => {
					let cut = &rest[pos..pos + end];
					if cut.ends_with(TEXT_DROPPED_SUFFIX) {
						out.push_str(TEXT_DROPPED_SUFFIX);
					}
					rest = &rest[pos + end..];
				}
				None => {
					rest = "";
					break;
				}
			}
		}
		out.push_str(rest);
		out
	}

	#[cfg(test)]
	mod tests {
		use surrealdb_types::{Array, Object, Value as SurValue};

		use super::{
			RootSnapshot, is_explain_analyze, redact_explain_metrics_text, redact_volatile_explain,
			redirect_credentials,
		};

		/// Parse `src` and report, per top-level statement, whether the redaction
		/// treats it as carrying server metrics.
		fn analyzed(src: &str) -> Vec<bool> {
			let plan = super::parse_to_plan(src, Default::default()).unwrap();
			plan.expressions.iter().map(is_explain_analyze).collect()
		}

		#[test]
		fn both_spellings_of_explain_analyze_are_recognised() {
			// Leading, and the trailing form that rewrites to it.
			assert_eq!(analyzed("EXPLAIN ANALYZE SELECT * FROM t"), [true]);
			assert_eq!(analyzed("SELECT * FROM t EXPLAIN FULL"), [true]);
			assert_eq!(analyzed("SELECT * FROM t WHERE a > 1 EXPLAIN FULL"), [true]);
			assert_eq!(analyzed("UPDATE t SET a = 1 EXPLAIN FULL"), [true]);
			assert_eq!(analyzed("DELETE t EXPLAIN FULL"), [true]);
		}

		#[test]
		fn merely_naming_an_explain_is_not_running_one() {
			// The statement's own data, which happens to spell the thing the
			// detector looks for. Reading the rendered text would mark these
			// analyzed and then rewrite the `metrics` object below — data the
			// embedded executor returns untouched, so the remote path would be
			// inventing a difference rather than hiding a volatile one.
			assert_eq!(
				analyzed(
					"RETURN { note: 'EXPLAIN ANALYZE', metrics: { elapsed_ns: 7, label: 'x' } }"
				),
				[false]
			);
			assert_eq!(analyzed("RETURN 'SELECT * FROM t EXPLAIN FULL'"), [false]);
			assert_eq!(analyzed("CREATE doc SET body = 'EXPLAIN ANALYZE SELECT 1'"), [false]);
			// A field or alias may be spelled that way too.
			assert_eq!(analyzed("SELECT 1 AS `EXPLAIN ANALYZE` FROM t"), [false]);
		}

		#[test]
		fn a_statement_without_metrics_is_left_alone() {
			// A plain EXPLAIN renders the plan but carries no metrics, so there is
			// nothing to redact and its result must survive untouched.
			assert_eq!(analyzed("EXPLAIN SELECT * FROM t"), [false]);
			assert_eq!(analyzed("SELECT * FROM t EXPLAIN"), [false]);
			// Ordinary statements: their results are the test's own data.
			assert_eq!(analyzed("SELECT * FROM t"), [false]);
			assert_eq!(analyzed("RETURN { metrics: { elapsed_ns: 7 } }"), [false]);
			assert_eq!(analyzed("RETURN 'Scan {rows: 3, batches: 1, elapsed: 8ms}'"), [false]);
		}

		#[test]
		fn a_nested_explain_analyze_is_recognised() {
			// The metrics surface in the enclosing statement's result, so matching
			// only at the statement's edges would miss these.
			assert_eq!(analyzed("RETURN (EXPLAIN ANALYZE SELECT * FROM t)"), [true]);
			assert_eq!(analyzed("LET $a = (SELECT * FROM t EXPLAIN FULL)"), [true]);
			assert_eq!(analyzed("{ RETURN (EXPLAIN ANALYZE SELECT * FROM t); }"), [true]);
			// Inside an object literal, beside the keys the redaction rewrites.
			assert_eq!(analyzed("RETURN { plan: (EXPLAIN ANALYZE SELECT * FROM t) }"), [true]);
		}

		#[test]
		fn the_flags_line_up_with_the_statements() {
			assert_eq!(
				analyzed("SELECT * FROM t; EXPLAIN ANALYZE SELECT * FROM t; RETURN 1"),
				[false, true, false]
			);
		}

		fn text(s: &str) -> SurValue {
			SurValue::String(s.to_string())
		}

		fn names(xs: &[&str]) -> std::collections::HashSet<String> {
			xs.iter().map(|x| (*x).to_owned()).collect()
		}

		fn creds(pairs: &[(&str, &str)]) -> Object {
			let mut o = Object::new();
			for (k, v) in pairs {
				o.insert((*k).to_owned(), text(v));
			}
			o
		}

		#[test]
		fn the_baseline_answers_whether_the_endpoint_already_held_a_namespace() {
			// What the pre-flight check asks of every exclusive test's configured
			// namespace: present in the baseline means the endpoint's own, and
			// dropping it would destroy data the run cannot give back.
			let base = RootSnapshot {
				namespaces: names(&["test", "customer_data"]),
				..RootSnapshot::default()
			};
			assert!(base.has_namespace("test"));
			assert!(base.has_namespace("customer_data"));
			assert!(!base.has_namespace("lt_a_1"));
			// Names are compared whole, not by prefix.
			assert!(!base.has_namespace("tes"));
			assert!(!base.has_namespace("test2"));
		}

		#[test]
		fn a_run_owned_namespace_or_user_is_not_a_leak() {
			let base = RootSnapshot::default();
			let after = RootSnapshot {
				namespaces: names(&["lt_a_1", "lt_a_2"]),
				users: names(&["lt_a_user_7"]),
				..RootSnapshot::default()
			};
			assert!(after.added_since(&base, Some("lt_a")).is_empty());
		}

		#[test]
		fn without_an_owned_prefix_everything_added_is_reported() {
			// An exclusive test's cleanup asks this question, to find what it failed
			// to remove — its own provisioned user included. An empty prefix would
			// answer it with silence, since every name starts with one.
			let base = RootSnapshot {
				namespaces: names(&["keep"]),
				users: names(&["root"]),
				..RootSnapshot::default()
			};
			let after = RootSnapshot {
				namespaces: names(&["keep", "lt_a_1"]),
				users: names(&["root", "lt_a_user_0"]),
				accesses: names(&["lt_a_ac"]),
				..RootSnapshot::default()
			};
			assert_eq!(
				after.added_since(&base, None),
				// Sorted, as `added_since` leaves it.
				vec![
					"ACCESS `lt_a_ac` ON ROOT".to_owned(),
					"NAMESPACE `lt_a_1`".to_owned(),
					"USER `lt_a_user_0` ON ROOT".to_owned(),
				],
			);
		}

		#[test]
		fn the_end_of_run_sweep_reports_an_exclusive_test_s_own_names() {
			// The sweep diffs against the run's baseline rather than matching the
			// run prefix, which is what lets it see state an exclusive test left
			// under a name of its own choosing after dying before its `cleanup`.
			let base = RootSnapshot::default();
			let after = RootSnapshot {
				namespaces: names(&["test", "lt_a_1"]),
				users: names(&["someone"]),
				accesses: names(&["jwt"]),
				..RootSnapshot::default()
			};
			assert_eq!(
				after.added_since(&base, None),
				vec![
					"ACCESS `jwt` ON ROOT".to_owned(),
					"NAMESPACE `lt_a_1`".to_owned(),
					"NAMESPACE `test`".to_owned(),
					"USER `someone` ON ROOT".to_owned(),
				],
			);
		}

		#[test]
		fn removing_what_the_endpoint_already_had_is_reported() {
			// The case a one-way comparison cannot see: `after \ base` is empty
			// because the entities are gone, not added.
			let base = RootSnapshot {
				namespaces: names(&["operators_ns"]),
				users: names(&["root", "operators_user"]),
				accesses: names(&["operators_ac"]),
				..RootSnapshot::default()
			};
			let after = RootSnapshot {
				users: names(&["root"]),
				..RootSnapshot::default()
			};
			assert!(after.added_since(&base, None).is_empty());
			assert_eq!(
				after.removed_since(&base),
				vec![
					"ACCESS `operators_ac` ON ROOT".to_owned(),
					"NAMESPACE `operators_ns`".to_owned(),
					"USER `operators_user` ON ROOT".to_owned(),
				],
			);
		}

		#[test]
		fn a_run_that_changed_nothing_reports_neither_half() {
			let base = RootSnapshot {
				namespaces: names(&["operators_ns"]),
				users: names(&["root"]),
				..RootSnapshot::default()
			};
			assert!(base.added_since(&base, None).is_empty());
			assert!(base.removed_since(&base).is_empty());
		}

		#[test]
		fn an_unset_default_config_reads_as_absent() {
			// The sweep removes `CONFIG DEFAULT` only when the run began without
			// one, so each spelling of "none defined" has to answer alike.
			for defaults in [SurValue::None, SurValue::Null, SurValue::Object(Object::new())] {
				let snapshot = RootSnapshot {
					defaults,
					..RootSnapshot::default()
				};
				assert!(!snapshot.has_defaults());
			}
			let defined = RootSnapshot {
				defaults: SurValue::Object(creds(&[("namespace", "ns")])),
				..RootSnapshot::default()
			};
			assert!(defined.has_defaults());
		}

		#[test]
		fn everything_outside_the_run_prefix_is_reported() {
			let base = RootSnapshot {
				namespaces: names(&["preexisting"]),
				..RootSnapshot::default()
			};
			let after = RootSnapshot {
				namespaces: names(&["preexisting", "lt_a_1", "test"]),
				users: names(&["root_secret"]),
				accesses: names(&["api"]),
				..RootSnapshot::default()
			};
			assert_eq!(
				after.added_since(&base, Some("lt_a")),
				vec![
					"ACCESS `api` ON ROOT".to_owned(),
					"NAMESPACE `test`".to_owned(),
					"USER `root_secret` ON ROOT".to_owned(),
				]
			);
		}

		#[test]
		fn a_changed_default_namespace_is_reported() {
			// `DEFINE CONFIG DEFAULT` lands under its own `INFO FOR ROOT` key, not
			// under the dynamic system config.
			let base = RootSnapshot::default();
			let after = RootSnapshot {
				defaults: text("DEFINE CONFIG DEFAULT NAMESPACE test_ns DATABASE test_db"),
				..RootSnapshot::default()
			};
			assert_eq!(
				after.added_since(&base, Some("lt_a")),
				vec!["the default namespace/database (DEFINE CONFIG DEFAULT)".to_owned()]
			);
		}

		#[test]
		fn a_changed_system_config_is_reported() {
			let base = RootSnapshot::default();
			let after = RootSnapshot {
				config: text("QUERY_TIMEOUT 200ms"),
				..RootSnapshot::default()
			};
			assert_eq!(
				after.added_since(&base, Some("lt_a")),
				vec!["the system config (ALTER SYSTEM)".to_owned()]
			);
		}

		#[test]
		fn credentials_naming_the_configured_namespace_follow_the_test() {
			let got = redirect_credentials(
				creds(&[("ns", "test"), ("db", "test"), ("ac", "account")]),
				Some("test"),
				Some("lt_a_1"),
				Some("test"),
				Some("test"),
			);
			// The namespace was renamed for this run, the database was not, and
			// the access method is none of the harness's business.
			assert_eq!(got.get("ns"), Some(&text("lt_a_1")));
			assert_eq!(got.get("db"), Some(&text("test")));
			assert_eq!(got.get("ac"), Some(&text("account")));
		}

		#[test]
		fn credentials_naming_some_other_namespace_are_left_alone() {
			let got = redirect_credentials(
				creds(&[("ns", "elsewhere")]),
				Some("test"),
				Some("lt_a_1"),
				None,
				None,
			);
			assert_eq!(got.get("ns"), Some(&text("elsewhere")));
		}

		#[test]
		fn strips_the_volatile_tail_of_one_metrics_block() {
			assert_eq!(
				redact_explain_metrics_text(
					"Scan [ctx: Db] {rows: 3, batches: 1, elapsed: 8.29\u{b5}s}"
				),
				"Scan [ctx: Db] {rows: 3}"
			);
		}

		#[test]
		fn strips_the_optional_skipped_metric_too() {
			assert_eq!(
				redact_explain_metrics_text("{rows: 3, batches: 1, elapsed: 5\u{b5}s, skipped: 2}"),
				"{rows: 3}"
			);
		}

		#[test]
		fn strips_every_block_in_a_multi_operator_plan() {
			let plan = "Limit {rows: 1, batches: 1, elapsed: 2\u{b5}s}\n    Scan {rows: 9, batches: 3, elapsed: 40\u{b5}s}";
			assert_eq!(redact_explain_metrics_text(plan), "Limit {rows: 1}\n    Scan {rows: 9}");
		}

		#[test]
		fn leaves_a_plan_without_metrics_alone() {
			let plan = "Limit [ctx: Db] [limit: 0]\n    CountScan [ctx: Db] [source: sale]";
			assert_eq!(redact_explain_metrics_text(plan), plan);
		}

		/// An unclosed block truncates at the block rather than looping forever or
		/// dropping the rest of the plan silently.
		#[test]
		fn an_unclosed_block_truncates_rather_than_looping() {
			assert_eq!(redact_explain_metrics_text("{rows: 3, batches: 1"), "{rows: 3");
		}

		/// One operator node: `{ operator, metrics: {..}, children: [..] }`.
		fn node(operator: &str, metrics: Object, children: Vec<SurValue>) -> SurValue {
			let mut obj = Object::new();
			obj.insert("operator", operator);
			obj.insert("metrics", SurValue::Object(metrics));
			if !children.is_empty() {
				obj.insert("children", Array::from_iter(children));
			}
			SurValue::Object(obj)
		}

		fn metrics(pairs: &[(&str, i64)]) -> Object {
			let mut obj = Object::new();
			for (k, v) in pairs {
				obj.insert(*k, *v);
			}
			obj
		}

		#[test]
		fn json_shape_keeps_only_the_deterministic_metrics_at_every_depth() {
			let mut value = node(
				"SelectProject",
				metrics(&[("output_rows", 3), ("output_batches", 1), ("elapsed_ns", 812)]),
				vec![node(
					"TableScan",
					metrics(&[
						("output_rows", 3),
						("output_batches", 1),
						("elapsed_ns", 40),
						("skipped_rows", 2),
						("edges_scanned", 7),
						("cache_hits", 4),
					]),
					vec![],
				)],
			);
			redact_volatile_explain(&mut value);

			let expected = node(
				"SelectProject",
				metrics(&[("output_rows", 3)]),
				vec![node("TableScan", metrics(&[("output_rows", 3)]), vec![])],
			);
			assert_eq!(value, expected);
		}

		/// The server renders this one even at zero, so a deny-list of volatile
		/// names let it through to every assertion.
		#[test]
		fn json_shape_strips_records_fetched_at_zero() {
			let mut value =
				node("TableScan", metrics(&[("output_rows", 3), ("records_fetched", 0)]), vec![]);
			redact_volatile_explain(&mut value);
			assert_eq!(value, node("TableScan", metrics(&[("output_rows", 3)]), vec![]));
		}

		/// `dropped` is deterministic: it says a bitmap branch declined, which is
		/// what distinguishes its `output_rows: 0` from an empty candidate set.
		#[test]
		fn json_shape_keeps_the_dropped_flag() {
			let mut m = metrics(&[("output_rows", 0), ("elapsed_ns", 9)]);
			m.insert("dropped", true);
			let mut value = node("BitmapBranch", m, vec![]);
			redact_volatile_explain(&mut value);

			let mut expected_m = metrics(&[("output_rows", 0)]);
			expected_m.insert("dropped", true);
			assert_eq!(value, node("BitmapBranch", expected_m, vec![]));
		}

		/// A `metrics` key is the only thing rewritten, so a test asserting on its
		/// own data is never silently edited.
		#[test]
		fn a_field_outside_metrics_sharing_a_name_is_left_alone() {
			let mut row = Object::new();
			row.insert("elapsed_ns", 7i64);
			row.insert("records_fetched", 0i64);
			let mut value = SurValue::Object(row.clone());
			redact_volatile_explain(&mut value);
			assert_eq!(value, SurValue::Object(row));
		}

		#[test]
		fn text_shape_is_redacted_through_a_nested_value() {
			let mut value = SurValue::Array(Array::from_iter([text(
				"Scan {rows: 2, batches: 1, elapsed: 3\u{b5}s}",
			)]));
			redact_volatile_explain(&mut value);
			assert_eq!(value, SurValue::Array(Array::from_iter([text("Scan {rows: 2}")])));
		}

		/// `dropped` is rendered after the volatile counters, so it falls inside the
		/// cut span and has to be put back.
		#[test]
		fn text_shape_keeps_a_trailing_dropped_flag() {
			assert_eq!(
				redact_explain_metrics_text(
					"Branch {rows: 0, batches: 1, elapsed: 2\u{b5}s, dropped: true}"
				),
				"Branch {rows: 0, dropped: true}"
			);
		}

		/// Every volatile counter between `batches` and `dropped` goes too.
		#[test]
		fn text_shape_strips_the_scan_side_counters() {
			assert_eq!(
				redact_explain_metrics_text(
					"Scan {rows: 4, batches: 2, elapsed: 9\u{b5}s, scanned: 12, cache_hits: 3, fetched: 5}"
				),
				"Scan {rows: 4}"
			);
		}
	}
}
