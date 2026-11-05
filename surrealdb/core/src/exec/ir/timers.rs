//! Per-phase timers of a compile, emitted as tracing events under the `compile-timers` feature.
//!
//! Four targets carry the report, each at `DEBUG`, the highest level the workspace's tracing
//! configuration keeps in a release build (`release_max_level_debug`):
//!
//! - `surrealdb::compile::phase` with `phase` (a name in [`COMPILE_PHASES`]) and `nanos`: the wall
//!   time of one stretch of the compile. A phase may be reported many times in one compile; the
//!   consumer sums them. The stretches are disjoint, so the sum of every phase is the time the
//!   compile spent in instrumented code. A stretch that encloses the planning of a unit (`lower`
//!   encloses the planning of every guard, event, view and function unit the program needs) reports
//!   its time net of that planning, whose own phases are reported as they run.
//! - `surrealdb::compile::request` with `nanos`: the wall time of compiling one request, from the
//!   request's first statement to its finished program. Every phase but `parse`, which runs before
//!   the request is compiled, falls inside it, so the phases' sum over this time is the share of
//!   the compile the timers account for.
//! - `surrealdb::compile::catalog_reads` with `count`: catalog reads, reported at two sites that
//!   count in different units. Each site reports once per pass, a count of zero included:
//!   - the legacy bind of a statement planned for a request's program reports, when the bind
//!     returns, whether or not it succeeded, the distinct catalog entries the transaction answered
//!     during it (tables, functions, params, databases, listings and references by name, and one
//!     for a read it cannot name), whether or not the datastore's cache served them;
//!   - the planning of a container (the request's, and each unit planned while lowering, and a
//!     subscription's) reports, once its physical verification has passed, the calls its catalog
//!     memo issued to the transaction: `2n + 1` for a preload of `n` tables (the snapshot, then
//!     each table's events and views), and one for each function, param, database definition and
//!     reference-field listing read. A read the memo answers from what it holds is not counted, and
//!     a snapshot the datastore's cache serves is counted all the same. A planning that fails
//!     before its physical verification reports nothing.
//!
//!   The consumer's sum is the two units added, not a count of distinct entries or of datastore
//!   reads: an entry both sites read is counted by each.
//! - `surrealdb::compile::egraph` with `nodes` and `iterations`: one e-graph run, reported only for
//!   a statement the e-graph ran on.
//!
//! Without the feature the macros expand to the expression they wrap (or to nothing), so the
//! binary holds no event and no clock read. A phase name that is not in [`COMPILE_PHASES`] fails
//! to compile in every configuration.

/// The phase names a compile reports, in the order a compile runs them. A cache lookup is not a
/// phase: no compile at a request's arrival consults a program cache, and the compile-cache lookup
/// of a statement served when the run reaches it is not timed.
///
/// - `parse`: statement text to syntax tree;
/// - `bind`: the legacy bind, the catalog preload, the binder, the fill and the field checks;
/// - `verify`: both phase boundaries;
/// - `logical`: the logical passes, the record folds and the loop legality;
/// - `egraph`: the e-graph's planning of each statement;
/// - `phys`: the physical conversion and its rewrites;
/// - `cse`: the physical common-subexpression pass;
/// - `lower`: lowering the planned container, units included;
/// - `finish`: putting the request's program together.
pub const COMPILE_PHASES: &[&str] =
	&["parse", "bind", "verify", "logical", "egraph", "phys", "cse", "lower", "finish"];

/// Whether `name` is one of [`COMPILE_PHASES`].
///
/// A `const fn` so that [`compile_phase!`] can check its literal when the code is compiled.
pub const fn is_phase(name: &str) -> bool {
	let name = name.as_bytes();
	let mut index = 0;
	while index < COMPILE_PHASES.len() {
		let phase = COMPILE_PHASES[index].as_bytes();
		if phase.len() == name.len() {
			let mut at = 0;
			while at < phase.len() && phase[at] == name[at] {
				at += 1;
			}
			if at == phase.len() {
				return true;
			}
		}
		index += 1;
	}
	false
}

/// Evaluates `$body`, and reports the time it took as one stretch of phase `$name`.
///
/// `$name` is a literal naming one of [`COMPILE_PHASES`]. The value of `$body` is the value of the
/// macro, and `?` and `.await` in `$body` act on the enclosing function. A body that leaves by
/// `?` reports nothing.
#[cfg(feature = "compile-timers")]
macro_rules! compile_phase {
	($name:literal, $body:expr) => {{
		const _: () = assert!(
			$crate::exec::ir::timers::is_phase($name),
			"a compile phase name must be one of COMPILE_PHASES"
		);
		let started = $crate::exec::ir::timers::start();
		let out = $body;
		$crate::exec::ir::timers::report_phase($name, started);
		out
	}};
}

/// Evaluates `$body`; without the `compile-timers` feature it reports nothing.
#[cfg(not(feature = "compile-timers"))]
macro_rules! compile_phase {
	($name:literal, $body:expr) => {{
		const _: () = assert!(
			$crate::exec::ir::timers::is_phase($name),
			"a compile phase name must be one of COMPILE_PHASES"
		);
		$body
	}};
}

/// A stretch of phase `$name` that runs until the enclosing scope ends, on every way out of it,
/// bound as `let _stretch = compile_phase_scope!("lower");`.
///
/// `$name` is a literal naming one of [`COMPILE_PHASES`]. With `excluding $nested`, a
/// [`NestedPlanning`], the stretch is reported net of the planning `$nested` timed in it. A
/// binding declared after the guard is dropped before the stretch ends, and so is part of it.
macro_rules! compile_phase_scope {
	($name:literal) => {{
		const _: () = assert!(
			$crate::exec::ir::timers::is_phase($name),
			"a compile phase name must be one of COMPILE_PHASES"
		);
		$crate::exec::ir::timers::PhaseTiming::begin($name, None)
	}};
	($name:literal, excluding $nested:expr) => {{
		const _: () = assert!(
			$crate::exec::ir::timers::is_phase($name),
			"a compile phase name must be one of COMPILE_PHASES"
		);
		$crate::exec::ir::timers::PhaseTiming::begin($name, Some(&$nested))
	}};
}

/// Reports `$count` catalog reads for the compile under way.
#[cfg(feature = "compile-timers")]
macro_rules! compile_catalog_reads {
	($count:expr) => {
		$crate::exec::ir::timers::report_catalog_reads($count)
	};
}

/// Reports nothing and does not evaluate `$count`.
#[cfg(not(feature = "compile-timers"))]
macro_rules! compile_catalog_reads {
	($count:expr) => {};
}

/// Reports one e-graph run of `$nodes` nodes over `$iterations` iterations. Only the e-graph's
/// own report calls it, which exists with the feature alone.
#[cfg(feature = "compile-timers")]
macro_rules! compile_egraph {
	($nodes:expr, $iterations:expr) => {
		$crate::exec::ir::timers::report_egraph($nodes, $iterations)
	};
}

/// Evaluates `$body`, and reports as catalog reads the entries `$txn` answered the catalog read of
/// while it ran.
///
/// The count is the number of distinct catalog entries read, as the transaction's recording of
/// catalog reads names them. Without the `compile-timers` feature it evaluates `$body` only.
#[cfg(feature = "compile-timers")]
macro_rules! compile_recorded_catalog_reads {
	($txn:expr, $body:expr) => {{
		let recording = $txn.record_catalog_reads();
		let out = $body;
		$crate::exec::ir::timers::report_catalog_reads($crate::exec::ir::timers::entries_read(
			&recording.finish(),
		));
		out
	}};
}

/// Evaluates `$body`; without the `compile-timers` feature it records nothing.
#[cfg(not(feature = "compile-timers"))]
macro_rules! compile_recorded_catalog_reads {
	($txn:expr, $body:expr) => {
		$body
	};
}

pub(crate) use compile_catalog_reads;
#[cfg(feature = "compile-timers")]
pub(crate) use compile_egraph;
pub(crate) use compile_phase;
pub(crate) use compile_phase_scope;
pub(crate) use compile_recorded_catalog_reads;

#[cfg(feature = "compile-timers")]
pub(crate) fn start() -> web_time::Instant {
	web_time::Instant::now()
}

#[cfg(feature = "compile-timers")]
pub(crate) fn report_phase(phase: &'static str, started: web_time::Instant) {
	let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
	tracing::event!(target: "surrealdb::compile::phase", tracing::Level::DEBUG, phase, nanos);
}

#[cfg(feature = "compile-timers")]
fn report_request(started: web_time::Instant) {
	let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
	tracing::event!(target: "surrealdb::compile::request", tracing::Level::DEBUG, nanos);
}

#[cfg(feature = "compile-timers")]
pub(crate) fn report_catalog_reads(count: u64) {
	tracing::event!(target: "surrealdb::compile::catalog_reads", tracing::Level::DEBUG, count);
}

#[cfg(feature = "compile-timers")]
pub(crate) fn report_egraph(nodes: u32, iterations: usize) {
	let nodes = u64::from(nodes);
	let iterations = u64::try_from(iterations).unwrap_or(u64::MAX);
	tracing::event!(
		target: "surrealdb::compile::egraph",
		tracing::Level::DEBUG,
		nodes,
		iterations
	);
}

/// The number of catalog entries `reads` names; a read of anything the recording cannot name
/// counts as one.
#[cfg(feature = "compile-timers")]
pub(crate) fn entries_read(reads: &crate::kvs::catalog_reads::CatalogReads) -> u64 {
	let named = reads.tables.len()
		+ reads.named_tables.len()
		+ reads.functions.len()
		+ reads.params.len()
		+ reads.databases.len()
		+ reads.listings.len()
		+ reads.references.len()
		+ usize::from(reads.opaque);
	u64::try_from(named).unwrap_or(u64::MAX)
}

/// A stretch of one phase that ends when the guard drops, as [`compile_phase_scope!`] begins it.
#[must_use = "the stretch is timed until the guard drops"]
pub(crate) struct PhaseTiming<'a> {
	#[cfg(feature = "compile-timers")]
	phase: &'static str,
	#[cfg(feature = "compile-timers")]
	started: web_time::Instant,
	#[cfg(feature = "compile-timers")]
	excluding: Option<&'a NestedPlanning>,
	#[cfg(not(feature = "compile-timers"))]
	_nested: std::marker::PhantomData<&'a NestedPlanning>,
}

impl<'a> PhaseTiming<'a> {
	/// Begins a stretch of `phase`, net of the planning `excluding` times while it lasts.
	pub(crate) fn begin(phase: &'static str, excluding: Option<&'a NestedPlanning>) -> Self {
		#[cfg(not(feature = "compile-timers"))]
		let _ = (phase, excluding);
		Self {
			#[cfg(feature = "compile-timers")]
			phase,
			#[cfg(feature = "compile-timers")]
			started: web_time::Instant::now(),
			#[cfg(feature = "compile-timers")]
			excluding,
			#[cfg(not(feature = "compile-timers"))]
			_nested: std::marker::PhantomData,
		}
	}
}

#[cfg(feature = "compile-timers")]
impl Drop for PhaseTiming<'_> {
	fn drop(&mut self) {
		let elapsed = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
		let nanos = elapsed.saturating_sub(self.excluding.map_or(0, NestedPlanning::nanos));
		let phase = self.phase;
		tracing::event!(target: "surrealdb::compile::phase", tracing::Level::DEBUG, phase, nanos);
	}
}

/// The compile of one request or one statement under way, as [`RequestTiming::begin`] begins it.
/// The compile's time is reported when the guard drops.
#[must_use = "the compile is timed until the guard drops"]
pub(crate) struct RequestTiming {
	#[cfg(feature = "compile-timers")]
	started: web_time::Instant,
}

impl RequestTiming {
	/// Begins timing a compile that nothing else is compiling.
	pub(crate) fn begin() -> Self {
		Self {
			#[cfg(feature = "compile-timers")]
			started: web_time::Instant::now(),
		}
	}
}

#[cfg(feature = "compile-timers")]
impl Drop for RequestTiming {
	fn drop(&mut self) {
		report_request(self.started);
	}
}

/// The wall time one compile spent planning units inside a stretch that reports net of it
/// ([`compile_phase_scope!`]).
///
/// A handle is cloned to the planner that plans the units and to the stretch it is subtracted
/// from; clones share the total. Planning is not reentrant, so the times added never overlap.
/// Without the `compile-timers` feature it holds nothing.
#[derive(Clone, Default)]
pub(crate) struct NestedPlanning {
	#[cfg(feature = "compile-timers")]
	nanos: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl NestedPlanning {
	/// Times the planning of one unit: the time until the guard drops, on every way out of the
	/// scope that holds it, is added to the total.
	pub(crate) fn timing(&self) -> NestedTiming<'_> {
		NestedTiming {
			#[cfg(feature = "compile-timers")]
			into: self,
			#[cfg(feature = "compile-timers")]
			started: web_time::Instant::now(),
			#[cfg(not(feature = "compile-timers"))]
			_nested: std::marker::PhantomData,
		}
	}

	/// The time added so far.
	#[cfg(feature = "compile-timers")]
	fn nanos(&self) -> u64 {
		self.nanos.load(std::sync::atomic::Ordering::Relaxed)
	}
}

/// The planning of one unit under way, as [`NestedPlanning::timing`] begins it.
#[must_use = "the planning is timed until the guard drops"]
pub(crate) struct NestedTiming<'a> {
	#[cfg(feature = "compile-timers")]
	into: &'a NestedPlanning,
	#[cfg(feature = "compile-timers")]
	started: web_time::Instant,
	#[cfg(not(feature = "compile-timers"))]
	_nested: std::marker::PhantomData<&'a NestedPlanning>,
}

#[cfg(feature = "compile-timers")]
impl Drop for NestedTiming<'_> {
	fn drop(&mut self) {
		let nanos = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
		self.into.nanos.fetch_add(nanos, std::sync::atomic::Ordering::Relaxed);
	}
}

/// The catalog reads a [`PlanningCatalog`](super::cache::planning::PlanningCatalog) issued to its
/// transaction.
///
/// A read the memo answers from what it holds is not counted. Without the `compile-timers`
/// feature it holds nothing.
#[derive(Default)]
pub(crate) struct ReadCount {
	#[cfg(feature = "compile-timers")]
	issued: std::sync::atomic::AtomicU64,
}

impl ReadCount {
	/// Counts `reads` more reads issued.
	pub(crate) fn add(&self, reads: u64) {
		#[cfg(feature = "compile-timers")]
		self.issued.fetch_add(reads, std::sync::atomic::Ordering::Relaxed);
		#[cfg(not(feature = "compile-timers"))]
		let _ = reads;
	}

	/// The reads counted so far.
	#[cfg(feature = "compile-timers")]
	pub(crate) fn issued(&self) -> u64 {
		self.issued.load(std::sync::atomic::Ordering::Relaxed)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn only_the_listed_names_are_phases() {
		for phase in COMPILE_PHASES {
			assert!(is_phase(phase), "{phase} is listed");
		}
		assert!(!is_phase(""));
		assert!(!is_phase("verif"));
		assert!(!is_phase("verifyy"));
		assert!(!is_phase("Verify"));
		assert!(!is_phase("map"));
	}

	#[test]
	fn the_phase_names_are_distinct() {
		for (at, phase) in COMPILE_PHASES.iter().enumerate() {
			assert!(!COMPILE_PHASES[..at].contains(phase), "{phase} is listed twice");
		}
	}
}

#[cfg(all(test, feature = "compile-timers", feature = "kv-mem"))]
mod event_tests {
	use std::collections::BTreeMap;
	use std::sync::{Arc, Mutex};

	use tracing::field::{Field, Visit};
	use tracing::{Event, Subscriber};
	use tracing_subscriber::layer::{Context, SubscriberExt};
	use tracing_subscriber::{Layer, registry};

	use super::COMPILE_PHASES;
	use crate::catalog::providers::CatalogProvider;
	use crate::dbs::Session;
	#[cfg(feature = "egraph")]
	use crate::exec::ir::egraph::{EgraphMode, ForcedPlanning};
	use crate::kvs::{Datastore, TransactionType};

	/// What the compile events of one request carried.
	#[derive(Default)]
	struct Seen {
		/// Per phase: the number of events and the nanoseconds they sum to.
		phases: BTreeMap<String, (u64, u64)>,
		/// The nanoseconds of each request compile.
		requests: Vec<u64>,
		catalog_reads: Vec<u64>,
		egraph_runs: Vec<(u64, u64)>,
	}

	#[derive(Default)]
	struct Fields {
		phase: Option<String>,
		nanos: u64,
		count: u64,
		nodes: u64,
		iterations: u64,
	}

	impl Visit for Fields {
		fn record_u64(&mut self, field: &Field, value: u64) {
			match field.name() {
				"nanos" => self.nanos = value,
				"count" => self.count = value,
				"nodes" => self.nodes = value,
				"iterations" => self.iterations = value,
				_ => {}
			}
		}

		fn record_str(&mut self, field: &Field, value: &str) {
			if field.name() == "phase" {
				self.phase = Some(value.to_owned());
			}
		}

		fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
	}

	struct Collect(Arc<Mutex<Seen>>);

	impl<S: Subscriber> Layer<S> for Collect {
		fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
			let mut fields = Fields::default();
			event.record(&mut fields);
			let mut seen = self.0.lock().expect("the collector lock");
			match event.metadata().target() {
				"surrealdb::compile::phase" => {
					let entry = seen.phases.entry(fields.phase.expect("a phase name")).or_default();
					entry.0 += 1;
					entry.1 += fields.nanos;
				}
				"surrealdb::compile::request" => seen.requests.push(fields.nanos),
				"surrealdb::compile::catalog_reads" => seen.catalog_reads.push(fields.count),
				"surrealdb::compile::egraph" => {
					seen.egraph_runs.push((fields.nodes, fields.iterations))
				}
				_ => {}
			}
		}
	}

	async fn datastore() -> Arc<Datastore> {
		let ds = Datastore::new("memory").await.expect("a memory datastore");
		let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
		tx.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
		tx.commit().await.expect("committing the namespace");
		ds
	}

	impl Seen {
		/// The nanoseconds every phase but `parse` reported, which run inside the request compile.
		fn compiled_nanos(&self) -> u64 {
			self.phases
				.iter()
				.filter(|(phase, _)| phase.as_str() != "parse")
				.map(|(_, (_, n))| n)
				.sum()
		}

		/// Asserts that the stretches reported are disjoint: the compiles took at least as long
		/// as every phase inside them summed.
		fn assert_disjoint(&self) {
			assert!(!self.requests.is_empty(), "no compile was timed");
			let (phases, requests) = (self.compiled_nanos(), self.requests.iter().sum::<u64>());
			assert!(
				phases <= requests,
				"the phases sum to {phases} ns, more than the {requests} ns the compiles took: {:?}",
				self.phases
			);
		}
	}

	/// The compile events `sql` emits when it arrives on `ds`.
	async fn events_of(ds: &Datastore, sql: &str) -> Seen {
		let seen = Arc::new(Mutex::new(Seen::default()));
		let session = Session::owner().with_ns("test").with_db("test");
		let guard = tracing::subscriber::set_default(registry().with(Collect(Arc::clone(&seen))));
		let results = ds.execute(sql, &session, None).await.expect("the request runs");
		drop(guard);
		for result in results {
			result.result.expect("the statement succeeds");
		}
		std::mem::take(&mut *seen.lock().expect("the collector lock"))
	}

	#[tokio::test]
	async fn a_compile_reports_every_phase_it_runs_and_only_listed_names() {
		let ds = datastore().await;
		let setup = events_of(&ds, "DEFINE TABLE t SCHEMALESS; CREATE t:1 SET a = 1;").await;
		assert!(!setup.phases.is_empty());
		let seen = events_of(&ds, "SELECT * FROM t WHERE a = 1 LIMIT 5;").await;
		for phase in
			["parse", "bind", "verify", "logical", "egraph", "phys", "cse", "lower", "finish"]
		{
			let (events, _) = seen.phases.get(phase).copied().unwrap_or_default();
			assert!(events > 0, "the {phase} phase reported nothing: {:?}", seen.phases);
		}
		for phase in seen.phases.keys() {
			assert!(COMPILE_PHASES.contains(&phase.as_str()), "{phase} is not a listed phase");
		}
		// Both phase boundaries verify the container, and no unit is planned to add more
		assert_eq!(seen.phases["verify"].0, 2);
		assert_eq!(seen.catalog_reads.len(), 2, "the bind and the memo each report once");
		assert!(seen.catalog_reads.iter().sum::<u64>() > 0);
		seen.assert_disjoint();
	}

	/// A write plans the units its table runs (events, field guards) while it lowers; they report
	/// their own phases, and `lower` reports net of them.
	#[tokio::test]
	async fn the_units_planned_while_lowering_are_not_counted_in_lower() {
		let ds = datastore().await;
		let setup = events_of(
			&ds,
			"DEFINE TABLE t SCHEMAFULL; DEFINE FIELD a ON t TYPE int ASSERT $value > 0; \
			 DEFINE EVENT e ON t WHEN true THEN (CREATE log SET at = time::now());",
		)
		.await;
		assert!(!setup.phases.is_empty());
		let plain = events_of(&ds, "SELECT * FROM t;").await;
		let write = events_of(&ds, "CREATE t:1 SET a = 1;").await;
		assert!(
			write.phases["verify"].0 > plain.phases["verify"].0,
			"a write's units verify their own containers: {:?} against {:?}",
			write.phases,
			plain.phases
		);
		assert!(
			write.catalog_reads.len() > plain.catalog_reads.len(),
			"a unit planned while lowering reports its reads: {:?}",
			write.catalog_reads
		);
		write.assert_disjoint();
	}

	/// A statement the run reaches after a schema change is planned then, in a compile of its own.
	#[tokio::test]
	async fn a_statement_planned_when_reached_is_timed_as_a_compile() {
		let ds = datastore().await;
		let seen = events_of(
			&ds,
			"BEGIN; DEFINE TABLE u SCHEMALESS; CREATE u:1 SET a = 1; SELECT * FROM u; \
			 SELECT * FROM u WHERE a = 1; COMMIT;",
		)
		.await;
		assert!(
			seen.requests.len() > 1,
			"a compile for the request and one for each statement served"
		);
		seen.assert_disjoint();
	}

	#[tokio::test]
	async fn an_egraph_event_is_reported_only_for_a_run() {
		let ds = datastore().await;
		let seen = events_of(&ds, "RETURN 1;").await;
		assert!(seen.egraph_runs.is_empty());
	}

	/// The e-graph is forced on for the statement, so its run is reported with what it held.
	#[cfg(feature = "egraph")]
	#[tokio::test]
	async fn an_egraph_run_reports_its_nodes_and_iterations() {
		let ds = datastore().await;
		events_of(&ds, "DEFINE TABLE t SCHEMALESS; CREATE t:1 SET a = 1;").await;
		let _forced = ForcedPlanning::new(EgraphMode::ForceOnSeeded, None);
		let seen = events_of(&ds, "SELECT * FROM t WHERE a = 1;").await;
		assert_eq!(seen.egraph_runs.len(), 1, "one run for the one statement");
		let (nodes, iterations) = seen.egraph_runs[0];
		assert!(nodes > 0, "the run held no node");
		assert!(iterations > 0, "the run made no iteration");
		assert!(seen.phases["egraph"].0 > 0);
	}
}
