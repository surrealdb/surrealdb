//! The compile cache: compiled units, served by the catalog object they were
//! compiled from.
//!
//! A unit is code compiled from one catalog object's definition: a function
//! body with its `PERMISSIONS` predicate, a table's read. The node keeps it
//! under a [`UnitKey`]: the namespace and database, by name and by id; the
//! object, by kind and name; the version the object's definition was read at
//! ([`ObjectVersion`]); and the identity of the build that compiled it
//! ([`BuildId`]). A unit is derived state, never part of the catalog: it may be
//! dropped at any time, and is compiled again from the definition.
//!
//! **Principal-generic.** Nothing that tells one caller from another is in
//! the key. A key holds one compilation per frame it is linked into
//! ([`UnitFrame`]): the class of the principal
//! ([`super::program::PrincipalClass`]), not its identity, and the permission
//! contract — whether permissions are suppressed, and whether reads are
//! checked. Every principal of a class is served the one compilation, which
//! is the code it would compile itself, and the frames inside it admit the
//! class, so a run under a principal of another class fails closed. The
//! `AUTH LIMIT` cap, the `PERMISSIONS` predicate, the capability to call a
//! function and the computation-depth budget are applied by the run, against
//! the run's principal: a unit is compiled at the depth of a call made from a
//! statement, and binds only its own arguments, whatever surrounds the call.
//!
//! **Single flight.** The first request for a key and frame compiles; a
//! request for it while that compilation runs waits for it and takes its
//! result, so concurrent requests compile once. A compilation that requests
//! another unit while it compiles never waits on a flight it does not lead:
//! it takes a unit already compiled, or compiles one itself and keeps it as a
//! flight's leader would, so two compilations that request each other's units
//! never wait on each other.
//!
//! **What else a unit depends on.** A unit's code may hold what its
//! compilation read of catalog objects other than its own — the tables its
//! reads plan against, the bodies of the functions it calls, which bundle
//! with it — and the values its binding folded in from the planning context.
//! Those are kept with it ([`CatalogDeps`], [`ContextDeps`]) and checked when
//! it is served; a unit whose dependencies no longer hold for the requesting
//! transaction is compiled again, and replaces the one kept. A compilation
//! whose dependencies cannot be named is not kept.
//!
//! What a transaction's uncommitted DDL defines is read under versions no
//! other transaction sees until it commits ([`crate::kvs::Transaction`]'s
//! catalog versions are minted afresh per write), so a unit compiled against
//! it is served to no other transaction, and to none once it rolls back.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;
use uuid::Uuid;

use super::program::{ISA_VERSION, UnitFrame};
use super::units::{CapabilitiesId, ContextDeps};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::catalog_deps::CatalogDeps;

/// The identity of the build a unit was compiled by: the instruction set
/// and the release, which name the compiler and the instructions together,
/// and the capabilities the node runs under, which compilation folds where it
/// can. Two builds, or two configurations of one node, share no unit.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BuildId {
	isa: u32,
	release: &'static str,
	capabilities: CapabilitiesId,
}

impl BuildId {
	/// This build, under `capabilities`.
	pub(crate) fn current(capabilities: Arc<crate::dbs::Capabilities>) -> Self {
		Self {
			isa: ISA_VERSION,
			release: env!("CARGO_PKG_VERSION"),
			capabilities: CapabilitiesId::new(capabilities),
		}
	}
}

/// The version an object's definition was read at: the catalog version its
/// writes stamp, or, for a definition no write of this build has stamped, a
/// hash of its content.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ObjectVersion {
	Stamped(Uuid),
	Content(u64),
	/// Code the query text carries — a closure literal — rather than the
	/// catalog: identified by the object's own key, its reads of the catalog
	/// checked as dependencies.
	Inline,
}

/// The class of the value a kept request statement reads as its source when
/// it runs (`FROM $name`), or as the target of its write (`UPDATE $name`):
/// what the statement is kept under besides its text, so a value of another
/// class is never served the compilation kept for this one. The compilation
/// reads the value when it runs and holds none of it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SourceClass {
	/// A record id of the table, whose key names one record.
	Record(crate::val::TableName),
	/// An array whose every element is such a record id, by the tables they
	/// name, sorted and each named once; empty for an empty array.
	Records(Vec<crate::val::TableName>),
	/// The table itself, which a `CREATE` generates the id of a record in.
	Table(crate::val::TableName),
}

impl SourceClass {
	/// The one table every value of the class names a record of, or is.
	pub(crate) fn table(&self) -> Option<&crate::val::TableName> {
		match self {
			SourceClass::Record(table) | SourceClass::Table(table) => Some(table),
			SourceClass::Records(tables) => match tables.as_slice() {
				[table] => Some(table),
				_ => None,
			},
		}
	}
}

/// The catalog object a unit is compiled from.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum UnitObject {
	/// A user-defined function, by its name without `fn::`: its body, its
	/// `PERMISSIONS` predicate and the expression a call inlines.
	Function(String),
	/// A table's read: decode, the `SELECT` permission, computed fields and
	/// field permissions.
	TableRead(crate::val::TableName),
	/// The field state a table's read applies: its computed fields and, for a
	/// checked frame, its field permissions, with their statements planned,
	/// for a read at the `VERSION` stamp it names, or the current state.
	FieldState(crate::val::TableName, Option<u64>),
	/// A table's `SELECT` permission, with its statements planned, for a read
	/// at the `VERSION` stamp it names, or the current state.
	SelectPermission(crate::val::TableName, Option<u64>),
	/// The unit of a closure's body, for values that capture `captured` and
	/// the row captures `rows`, lowered at re-entry depth `depth`.
	Closure {
		closure: ClosureId,
		captured: Vec<String>,
		rows: super::program::RowBits,
		depth: u32,
	},
	/// An expression reached at run time through one the program did not
	/// compile, compiled at re-entry depth `depth`, with the names `shadowed`
	/// read when it runs ([`super::serve::expr`]).
	Expr {
		expr: Arc<crate::expr::logical::LogicalExpr>,
		depth: u32,
		shadowed: Vec<String>,
	},
	/// A statement reached at run time through an expression the program did
	/// not compile — a function body, an API handler — planned at re-entry
	/// depth `depth`, with the names `shadowed` read when it runs
	/// ([`super::serve::statement`]); `binds_rows` is whether it is the value
	/// of a top-level `LET`, whose plan states that its rows arrive in the
	/// order they were bound. `sources` are the names among `shadowed` the
	/// statement reads as its source, each with the class of the value it was
	/// compiled for ([`SourceClass`]).
	Statement {
		op: Arc<crate::expr::logical::LogicalOp>,
		depth: u32,
		shadowed: Vec<String>,
		binds_rows: bool,
		sources: Vec<(String, SourceClass)>,
	},
	/// A top-level statement of a request served when it is reached, planned
	/// with the values the session's own parameters it reads hold folded in,
	/// for those values ([`super::serve::request_statement`]); `binds_rows` is
	/// whether it is the value of a top-level `LET`, whose rows the statements
	/// after it read in the order they were bound, which its plan states.
	SessionStatement {
		op: Arc<crate::expr::logical::LogicalOp>,
		session: ArgValues,
		shadowed: Vec<String>,
		binds_rows: bool,
		sources: Vec<(String, SourceClass)>,
	},
	/// A front end's plan — a GQL query `eval::gql` evaluates — compiled at
	/// re-entry depth `depth`, with the names `shadowed` read when it runs
	/// ([`super::serve::front_end`]).
	FrontEnd {
		op: Arc<crate::expr::logical::LogicalOp>,
		depth: u32,
		shadowed: Vec<String>,
	},
	/// The bundle of the stages a write that may reach any table applies to
	/// the records of `table`, or of a table the catalog does not hold where
	/// `generic`, lowered on behalf of an actor whose enforcement is `actor`
	/// where it differs from the frame's.
	StageBundle {
		set: Arc<super::units::StageSetUnit>,
		table: crate::val::TableName,
		generic: bool,
		actor: Option<bool>,
	},
	/// The order of the rows of view table `table`: the write of a view row
	/// for each action a view's maintenance settles, as the table's
	/// definitions state it ([`super::fault::view_rows`]).
	ViewRows {
		table: crate::val::TableName,
	},
	/// A call of a user-defined function, by its name without `fn::` and the
	/// number of arguments it is called with, as a late-bound statement makes
	/// it: the checks, the binding of the arguments and the body of the
	/// definition the version names ([`super::call_unit`]).
	FunctionCall(String, usize),
	/// The arm of a statement whose target is chosen when it runs, compiled
	/// for `table` and the statement shape `shape`: `shape` is
	/// [`shape::arm_key`](crate::exec::ir::cache::shape::arm_key) of the
	/// statement's `ShapeDigest`. The catalog version and the frame key it
	/// beside the object, as for every other object. Built by
	/// [`UnitObject::dynamic_target_arm`].
	// Read by the unit load of a dynamic-target arm.
	#[allow(dead_code)]
	DynamicTargetArm {
		table: crate::val::TableName,
		shape: u64,
	},
}

impl UnitObject {
	/// The arm of a dynamic-target statement compiled for `table` and the
	/// statement shape `shape`. Keyed, as every object is, by the catalog
	/// version and the frame beside it, so the cache checks it under the same
	/// `CatalogDeps` as a table read.
	#[cfg_attr(
		not(test),
		expect(
			dead_code,
			reason = "constructed by the unit load lowered for a `phys.choose` dynamic-target arm"
		)
	)]
	pub(crate) fn dynamic_target_arm(
		table: crate::val::TableName,
		shape: &crate::exec::ir::iface::premise::ShapeDigest,
	) -> Self {
		Self::DynamicTargetArm {
			table,
			shape: crate::exec::ir::cache::shape::arm_key(shape),
		}
	}
}

/// The argument values a specialised body is compiled for, as a key: a
/// SHA-256 digest of their type-exact encoding — each value's variant and,
/// for a number, its exact bits, recursively through arrays, sets, objects,
/// record ids and ranges. Two calls share a body only where their values are
/// the same values, not merely equal ones: `1`, `1.0f` and `1dec` compare
/// equal but compile to bodies that answer differently (`type::of`), as do
/// `0.0f` and `-0.0f`. The key holds the digest, not the values, so its size
/// and the cost of comparing it are fixed whatever a call brings.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ArgValues {
	digest: [u8; 32],
}

impl ArgValues {
	/// The key of `values`. Returns `None` for values whose encoding (with
	/// the count of values that leads it) is longer than `max_bytes`, and
	/// for values that have no encoding (a closure). The encoding stops at
	/// `max_bytes`. A body compiled for such values is compiled for the call
	/// alone.
	pub(crate) fn of(values: &[crate::val::Value], max_bytes: usize) -> Option<Self> {
		use std::io::Write as _;

		use sha2::Digest as _;
		/// Hashes what is written, and refuses a write past `max_bytes`,
		/// which ends the encoding there.
		struct Hashing {
			hasher: sha2::Sha256,
			len: usize,
			max_bytes: usize,
		}
		impl std::io::Write for Hashing {
			fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
				self.len = self.len.saturating_add(buf.len());
				if self.len > self.max_bytes {
					return Err(std::io::ErrorKind::FileTooLarge.into());
				}
				self.hasher.update(buf);
				Ok(buf.len())
			}
			fn flush(&mut self) -> std::io::Result<()> {
				Ok(())
			}
		}
		let mut out = Hashing {
			hasher: sha2::Sha256::new(),
			len: 0,
			max_bytes,
		};
		out.write_all(&(values.len() as u64).to_le_bytes()).ok()?;
		for value in values {
			revision::SerializeRevisioned::serialize_revisioned(value, &mut out).ok()?;
		}
		Some(Self {
			digest: out.hasher.finalize().into(),
		})
	}
}

/// Whether `a` and `b` are the same value, not merely equal ones; see
/// [`crate::val::same_value`]. A value folded into compiled code stands for
/// another only where they are the same: `1` and `1.0f` compare equal, but a
/// body that folded `1` answers `type::of` with `int`.
pub(crate) fn same_value(a: &crate::val::Value, b: &crate::val::Value) -> bool {
	crate::val::same_value(a, b)
}

/// A closure as written, as a key: hashed by its digest, and equal to another
/// only where the two are the same closure, since two closures may share a
/// digest.
#[derive(Clone, Debug)]
pub(crate) struct ClosureId {
	pub(crate) closure: Arc<crate::expr::logical::LogicalClosure>,
	pub(crate) digest: super::program::ClosureDigest,
}

impl PartialEq for ClosureId {
	fn eq(&self, other: &Self) -> bool {
		self.digest == other.digest
			&& (Arc::ptr_eq(&self.closure, &other.closure) || *self.closure == *other.closure)
	}
}

impl Eq for ClosureId {}

impl std::hash::Hash for ClosureId {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.digest.hash(state);
	}
}

/// What the node keeps a unit under; see the module.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct UnitKey {
	pub(crate) ns: String,
	pub(crate) db: String,
	pub(crate) ids: (NamespaceId, DatabaseId),
	pub(crate) object: UnitObject,
	pub(crate) version: ObjectVersion,
	pub(crate) build: BuildId,
}

/// A compiled unit and what else its compilation depended on.
struct Kept<V> {
	value: V,
	deps: Arc<CatalogDeps>,
	context: Arc<ContextDeps>,
}

/// One compilation of a key for a frame: empty while it runs, then the unit,
/// or `None` for a compilation that could not be kept.
struct Flight<V> {
	cell: tokio::sync::OnceCell<Option<Arc<Kept<V>>>>,
}

impl<V> Flight<V> {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			cell: tokio::sync::OnceCell::new(),
		})
	}
}

/// How many units one frame of a key keeps, one per state of what else
/// their compilations depended on, the latest last.
const VARIANTS: usize = 4;

/// The units one frame of a key keeps, oldest first.
type KeptUnits<V> = Arc<[Arc<Kept<V>>]>;

/// One frame of a key: the flight compiling for it now or last, and the
/// units it keeps.
struct Slot<V> {
	frame: UnitFrame,
	flight: Arc<Flight<V>>,
	kept: KeptUnits<V>,
}

impl<V> Clone for Slot<V> {
	fn clone(&self) -> Self {
		Self {
			frame: self.frame.clone(),
			flight: Arc::clone(&self.flight),
			kept: Arc::clone(&self.kept),
		}
	}
}

/// The frames of one key. Every request for the key reads them, so they are
/// published whole, and a request reads them without taking a lock; a change
/// publishes them anew under `changing`.
struct Entry<V> {
	slots: ArcSwap<Vec<Slot<V>>>,
	/// Held while the slots are changed, so no change is lost to another.
	changing: Mutex<()>,
}

impl<V> Entry<V> {
	fn new() -> Self {
		Self {
			slots: ArcSwap::from_pointee(Vec::new()),
			changing: Mutex::new(()),
		}
	}

	/// Run `f` over the slot for `frame`, made where there is none, and
	/// publish the slots it leaves.
	fn with_slot<R>(&self, frame: &UnitFrame, f: impl FnOnce(&mut Slot<V>) -> R) -> R {
		let _changing = self.changing.lock().unwrap_or_else(PoisonError::into_inner);
		let mut slots: Vec<Slot<V>> = self.slots.load().iter().cloned().collect();
		let at = match slots.iter().position(|slot| slot.frame == *frame) {
			Some(at) => at,
			None => {
				slots.push(Slot {
					frame: frame.clone(),
					flight: Flight::new(),
					kept: Arc::from(Vec::new()),
				});
				slots.len() - 1
			}
		};
		let answer = f(&mut slots[at]);
		self.slots.store(Arc::new(slots));
		answer
	}

	/// The flight for `frame`, and the units it keeps.
	fn flight(&self, frame: &UnitFrame) -> (Arc<Flight<V>>, KeptUnits<V>) {
		if let Some(slot) = self.slots.load().iter().find(|slot| slot.frame == *frame) {
			return (Arc::clone(&slot.flight), Arc::clone(&slot.kept));
		}
		self.with_slot(frame, |slot| (Arc::clone(&slot.flight), Arc::clone(&slot.kept)))
	}

	/// Replace `stale`, the flight for `frame`, with a new one, unless another
	/// request replaced it first; the flight that stands for `frame` now.
	fn replace(&self, frame: &UnitFrame, stale: &Arc<Flight<V>>) -> Arc<Flight<V>> {
		self.with_slot(frame, |slot| {
			if Arc::ptr_eq(&slot.flight, stale) {
				slot.flight = Flight::new();
			}
			Arc::clone(&slot.flight)
		})
	}

	/// Keep `unit` for `frame`, dropping the oldest past [`VARIANTS`].
	fn keep(&self, frame: &UnitFrame, unit: Arc<Kept<V>>) {
		self.with_slot(frame, |slot| {
			let from = (slot.kept.len() + 1).saturating_sub(VARIANTS);
			slot.kept = slot.kept[from..].iter().cloned().chain([unit]).collect();
		});
	}
}

/// What a compilation answers: the unit, and what its compilation depended
/// on besides its own definition, or `None` where that cannot be named and
/// the unit is not kept.
pub(crate) type Compiled<V> = (V, Option<(CatalogDeps, ContextDeps)>);

/// How the requests of a cache were served.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Stats {
	/// Served a kept unit.
	pub(crate) hits: u64,
	/// Compiled the unit and kept it.
	pub(crate) compiles: u64,
	/// Served the unit another request's flight compiled meanwhile.
	pub(crate) waits: u64,
	/// Found the kept unit's dependencies changed, and compiled again.
	pub(crate) stale: u64,
	/// Compiled a unit that could not be kept, or that a unit kept meanwhile
	/// already stood for.
	pub(crate) private: u64,
}

#[derive(Default)]
struct Counters {
	hits: AtomicU64,
	compiles: AtomicU64,
	waits: AtomicU64,
	stale: AtomicU64,
	private: AtomicU64,
}

impl Counters {
	fn add(&self, other: &Counters) {
		for (to, from) in [
			(&self.hits, &other.hits),
			(&self.compiles, &other.compiles),
			(&self.waits, &other.waits),
			(&self.stale, &other.stale),
			(&self.private, &other.private),
		] {
			to.fetch_add(from.load(Ordering::Relaxed), Ordering::Relaxed);
		}
	}

	fn stats(&self) -> Stats {
		Stats {
			hits: self.hits.load(Ordering::Relaxed),
			compiles: self.compiles.load(Ordering::Relaxed),
			waits: self.waits.load(Ordering::Relaxed),
			stale: self.stale.load(Ordering::Relaxed),
			private: self.private.load(Ordering::Relaxed),
		}
	}
}

/// The requests of every cache this process dropped, by kind of object, for
/// a measurement over many datastores ([`report`]).
static FUNCTIONS_TOTAL: Counters = Counters {
	hits: AtomicU64::new(0),
	compiles: AtomicU64::new(0),
	waits: AtomicU64::new(0),
	stale: AtomicU64::new(0),
	private: AtomicU64::new(0),
};
static TABLES_TOTAL: Counters = Counters {
	hits: AtomicU64::new(0),
	compiles: AtomicU64::new(0),
	waits: AtomicU64::new(0),
	stale: AtomicU64::new(0),
	private: AtomicU64::new(0),
};

thread_local! {
	/// How many flights this thread is compiling, nested: a request made
	/// inside one never waits on another's.
	static IN_FLIGHT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// `future`, polled as the compilation of a flight.
struct Leading<F>(std::pin::Pin<Box<F>>);

impl<F: std::future::Future> std::future::Future for Leading<F> {
	type Output = F::Output;
	fn poll(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<F::Output> {
		IN_FLIGHT.with(|depth| depth.set(depth.get() + 1));
		let polled = self.0.as_mut().poll(cx);
		IN_FLIGHT.with(|depth| depth.set(depth.get() - 1));
		polled
	}
}

/// The units of one kind of object the node keeps; see the module.
pub(crate) struct UnitCache<V> {
	entries: Option<quick_cache::sync::Cache<UnitKey, Arc<Entry<V>>>>,
	counters: Counters,
	totals: &'static Counters,
}

impl<V> Drop for UnitCache<V> {
	fn drop(&mut self) {
		self.totals.add(&self.counters);
		// With `SURREAL_COMPILE_CACHE_STATS` naming a file, each cache dropped
		// appends how its requests were served to it, for a measurement over
		// a run that makes many datastores.
		if let Some(path) = std::env::var_os("SURREAL_COMPILE_CACHE_STATS") {
			use std::io::Write as _;
			let stats = self.counters.stats();
			if stats != Stats::default()
				&& let Ok(mut file) =
					std::fs::OpenOptions::new().create(true).append(true).open(path)
			{
				let kind = if std::ptr::eq(self.totals, &FUNCTIONS_TOTAL) {
					"function"
				} else {
					"table"
				};
				let _ = writeln!(
					file,
					"{kind} hits={} compiles={} waits={} stale={} private={}",
					stats.hits, stats.compiles, stats.waits, stats.stale, stats.private
				);
			}
		}
	}
}

impl<V: Clone + Send + Sync + 'static> UnitCache<V> {
	/// A cache for at most `capacity` keys; one of `0` keeps none.
	fn new(capacity: usize, totals: &'static Counters) -> Self {
		Self {
			entries: (capacity > 0).then(|| quick_cache::sync::Cache::new(capacity)),
			counters: Counters::default(),
			totals,
		}
	}

	/// A cache of function units for at most `capacity` keys.
	pub(crate) fn functions(capacity: usize) -> Self {
		Self::new(capacity, &FUNCTIONS_TOTAL)
	}

	/// A cache of table units for at most `capacity` keys.
	pub(crate) fn tables(capacity: usize) -> Self {
		Self::new(capacity, &TABLES_TOTAL)
	}

	/// How this cache's requests were served.
	pub(crate) fn stats(&self) -> Stats {
		self.counters.stats()
	}

	/// The most keys this cache keeps; `0` for a cache that keeps none.
	pub(crate) fn capacity(&self) -> usize {
		self.entries.as_ref().map_or(0, |entries| entries.capacity() as usize)
	}

	/// The unit for `key` linked into `frame`, as the transaction `txn` and
	/// the planning context `ctx` would compile it: the one kept where it
	/// still holds for them, the one a concurrent request's flight compiles,
	/// or the one `compile` compiles now, which is kept for later requests
	/// where its dependencies can be named. An error of `compile` is the
	/// request's, and nothing is kept.
	pub(crate) async fn get_or_compile<E, Fut>(
		&self,
		key: &UnitKey,
		frame: &UnitFrame,
		txn: &crate::kvs::Transaction,
		ctx: &crate::ctx::FrozenContext,
		compile: impl FnOnce() -> Fut,
	) -> Result<V, E>
	where
		Fut: std::future::Future<Output = Result<Compiled<V>, E>>,
	{
		let Some(entries) = &self.entries else {
			return self.private(compile).await;
		};
		let entry = entries
			.get_or_insert_with(key, || Ok::<_, std::convert::Infallible>(Arc::new(Entry::new())));
		let Ok(entry) = entry;
		let (mut flight, kept) = entry.flight(frame);
		// A unit kept for another state of what else it depends on serves a
		// request for which that state holds.
		for kept in kept.iter().rev() {
			if kept.context.hold(ctx) && kept.deps.hold(txn).await {
				self.counters.hits.fetch_add(1, Ordering::Relaxed);
				return Ok(kept.value.clone());
			}
		}
		let nested = IN_FLIGHT.with(std::cell::Cell::get) > 0;
		let mut compile = Some(compile);
		// A kept unit that no longer holds is replaced once; a request that
		// finds the replacement stale too compiles for itself.
		for _ in 0..2 {
			if nested && !flight.cell.initialized() {
				// Inside another compilation: compile without waiting on the
				// flight, and keep what compiles for the requests after.
				let Some(compile) = compile.take() else {
					unreachable!("a request that has not compiled yet")
				};
				let (value, deps) = compile().await?;
				// A concurrent request may have kept a unit that holds for this one meanwhile;
				// a second copy would push out a variant kept for another state.
				let (_, kept) = entry.flight(frame);
				let mut held = false;
				for kept in kept.iter().rev() {
					if kept.context.hold(ctx) && kept.deps.hold(txn).await {
						held = true;
						break;
					}
				}
				match deps {
					Some(_) if held => {
						self.counters.private.fetch_add(1, Ordering::Relaxed);
					}
					Some((deps, context)) => {
						entry.keep(
							frame,
							Arc::new(Kept {
								value: value.clone(),
								deps: Arc::new(deps),
								context: Arc::new(context),
							}),
						);
						self.counters.compiles.fetch_add(1, Ordering::Relaxed);
					}
					None => {
						self.counters.private.fetch_add(1, Ordering::Relaxed);
					}
				}
				return Ok(value);
			}
			let ready = flight.cell.initialized();
			let mut led: Option<V> = None;
			let mut failed: Option<E> = None;
			let held = flight
				.cell
				.get_or_init(|| {
					let compile = compile.take();
					let led = &mut led;
					let failed = &mut failed;
					Leading(Box::pin(async move {
						let compile = compile?;
						match compile().await {
							Ok((value, deps)) => {
								*led = Some(value.clone());
								deps.map(|(deps, context)| {
									Arc::new(Kept {
										value,
										deps: Arc::new(deps),
										context: Arc::new(context),
									})
								})
							}
							Err(e) => {
								*failed = Some(e);
								None
							}
						}
					}))
				})
				.await
				.clone();
			if let Some(e) = failed {
				// The flight holds nothing for a compilation that failed: the
				// next request compiles again.
				entry.replace(frame, &flight);
				return Err(e);
			}
			if let Some(value) = led {
				match held {
					Some(kept) => {
						entry.keep(frame, kept);
						self.counters.compiles.fetch_add(1, Ordering::Relaxed);
					}
					None => {
						self.counters.private.fetch_add(1, Ordering::Relaxed);
					}
				}
				return Ok(value);
			}
			match held {
				// A unit already judged among those kept is not judged again.
				Some(found) if ready && kept.iter().any(|k| Arc::ptr_eq(k, &found)) => {
					self.counters.stale.fetch_add(1, Ordering::Relaxed);
					flight = entry.replace(frame, &flight);
				}
				Some(kept) => {
					if kept.context.hold(ctx) && kept.deps.hold(txn).await {
						if ready {
							self.counters.hits.fetch_add(1, Ordering::Relaxed);
						} else {
							self.counters.waits.fetch_add(1, Ordering::Relaxed);
						}
						return Ok(kept.value.clone());
					}
					self.counters.stale.fetch_add(1, Ordering::Relaxed);
					flight = entry.replace(frame, &flight);
				}
				// The compilation that led the flight could not be kept: this
				// request compiles for itself.
				None => break,
			}
		}
		match compile {
			Some(compile) => self.private(compile).await,
			None => unreachable!("a request whose compilation ran answered it"),
		}
	}

	/// The unit kept for `key` linked into `frame` that holds for the
	/// transaction `txn` and the context `ctx`, compiling nothing: what a
	/// running program loads a unit with. `None` where none is kept, which the
	/// caller serves by asking for the unit to be compiled.
	pub(crate) async fn lookup(
		&self,
		key: &UnitKey,
		frame: &UnitFrame,
		txn: &crate::kvs::Transaction,
		ctx: &crate::ctx::FrozenContext,
	) -> Option<V> {
		self.lookup_with_context(key, frame, txn, ctx).await.map(|(value, _)| value)
	}

	/// [`Self::lookup`], with the context values the unit served was lowered
	/// against, which a caller holding the unit checks again before reusing
	/// it under another context.
	pub(crate) async fn lookup_with_context(
		&self,
		key: &UnitKey,
		frame: &UnitFrame,
		txn: &crate::kvs::Transaction,
		ctx: &crate::ctx::FrozenContext,
	) -> Option<(V, Arc<ContextDeps>)> {
		let entry = self.entries.as_ref()?.get(key)?;
		let (flight, kept) = entry.flight(frame);
		// The unit the flight compiled last is asked first, where it is not
		// among those kept.
		let latest = flight
			.cell
			.get()
			.and_then(Option::as_ref)
			.filter(|latest| !kept.iter().any(|held| Arc::ptr_eq(held, latest)));
		for kept in latest.into_iter().chain(kept.iter().rev()) {
			if kept.context.hold(ctx) && kept.deps.hold(txn).await {
				self.counters.hits.fetch_add(1, Ordering::Relaxed);
				return Some((kept.value.clone(), Arc::clone(&kept.context)));
			}
		}
		None
	}

	/// `compile`'s unit, compiled for this request alone.
	async fn private<E, Fut>(&self, compile: impl FnOnce() -> Fut) -> Result<V, E>
	where
		Fut: std::future::Future<Output = Result<Compiled<V>, E>>,
	{
		let (value, _) = compile().await?;
		self.counters.private.fetch_add(1, Ordering::Relaxed);
		Ok(value)
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::{BuildId, ObjectVersion, UnitCache, UnitKey, UnitObject};
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::exec::catalog_deps::CatalogDeps;
	use crate::exec::vm::program::UnitFrame;
	use crate::exec::vm::units::ContextDeps;

	/// Argument values key a specialised body type-exact: values that compare
	/// equal across number kinds, or as `0.0` and `-0.0`, at any depth, are
	/// different keys; the same values are one key.
	#[test]
	fn argument_values_key_type_exact() {
		use crate::val::{Number, Value};
		let key = |values: Vec<Value>| super::ArgValues::of(&values, usize::MAX).expect("encodes");
		let num = |n: Number| Value::Number(n);
		let dec = |s: &str| num(Number::Decimal(s.parse().expect("decimal")));
		let distinct = [
			vec![num(Number::Int(1))],
			vec![num(Number::Float(1.0))],
			vec![dec("1")],
			vec![dec("1.0")],
			vec![num(Number::Float(0.0))],
			vec![num(Number::Float(-0.0))],
			vec![Value::from(vec![num(Number::Int(1))])],
			vec![Value::from(vec![num(Number::Float(1.0))])],
			vec![num(Number::Int(1)), num(Number::Int(2))],
			vec![num(Number::Int(2)), num(Number::Int(1))],
		];
		for (i, a) in distinct.iter().enumerate() {
			assert_eq!(key(a.clone()), key(a.clone()), "{a:?} keys itself");
			assert!(super::same_value(&Value::from(a.clone()), &Value::from(a.clone())));
			for b in &distinct[i + 1..] {
				assert_ne!(key(a.clone()), key(b.clone()), "{a:?} and {b:?}");
				assert!(
					!super::same_value(&Value::from(a.clone()), &Value::from(b.clone())),
					"{a:?} and {b:?}"
				);
			}
		}
	}

	/// Values whose encoding is longer than the bound have no key, and are
	/// encoded no further than the bound; values within it have one.
	#[test]
	fn argument_values_past_the_bound_have_no_key() {
		use crate::val::Value;
		let encoded = |values: &[Value]| {
			let mut len = std::mem::size_of::<u64>();
			for value in values {
				len += revision::to_vec(value).expect("encodes").len();
			}
			len
		};
		let small = vec![Value::from("x".repeat(16))];
		let bound = encoded(&small);
		assert!(super::ArgValues::of(&small, bound).is_some(), "values at the bound");
		assert!(super::ArgValues::of(&small, bound - 1).is_none(), "values past the bound");
		assert_eq!(
			super::ArgValues::of(&small, bound),
			super::ArgValues::of(&small, usize::MAX),
			"the bound does not change the key"
		);
		let large = vec![Value::from(vec![Value::from("x".repeat(64)); 1024])];
		assert!(super::ArgValues::of(&large, 4096).is_none());
	}

	/// Values compare the same only where their variant-specific parts are:
	/// numbers nested in a record id's key, a range's bounds or a geometry's
	/// coordinates differ by kind and by bits, though the values compare
	/// equal.
	#[test]
	fn same_value_is_type_exact_inside_record_ids_ranges_and_geometries() {
		let value = |text: &str| crate::val::Value::from(crate::syn::value(text).expect("parses"));
		for (a, b) in [
			("t:[1]", "t:[1.0f]"),
			("t:{ a: 1 }", "t:{ a: 1.0f }"),
			("t:[0.0f]", "t:[-0.0f]"),
			("1..2", "1.0f..2"),
			("1..=2", "1..=2.0f"),
			("(0.0, 1.0)", "(-0.0, 1.0)"),
			(
				"{ type: 'LineString', coordinates: [[0.0, 1.0], [2.0, 3.0]] }",
				"{ type: 'LineString', coordinates: [[-0.0, 1.0], [2.0, 3.0]] }",
			),
		] {
			let (a, b) = (value(a), value(b));
			assert!(super::same_value(&a, &a.clone()), "{a:?} is itself");
			assert!(super::same_value(&b, &b.clone()), "{b:?} is itself");
			assert!(!super::same_value(&a, &b), "{a:?} and {b:?}");
			assert!(!super::same_value(&b, &a), "{b:?} and {a:?}");
		}
		for text in ["(0.0, 1.0)", "{ type: 'LineString', coordinates: [[0.0, 1.0], [2.0, 3.0]] }"]
		{
			assert!(matches!(value(text), crate::val::Value::Geometry(_)), "{text} is a geometry");
		}
		// A record id whose key is a range of array keys.
		let ranged = |start: crate::val::Number| {
			use crate::val::{Array, RecordId, RecordIdKey, RecordIdKeyRange, Value};
			let key = |n| RecordIdKey::Array(Array(vec![Value::Number(n)]));
			Value::RecordId(RecordId {
				table: "t".into(),
				key: RecordIdKey::Range(Box::new(RecordIdKeyRange {
					start: std::ops::Bound::Included(key(start)),
					end: std::ops::Bound::Excluded(key(crate::val::Number::Int(2))),
				})),
			})
		};
		let (int, float) =
			(ranged(crate::val::Number::Int(1)), ranged(crate::val::Number::Float(1.0)));
		assert!(super::same_value(&int, &int.clone()));
		assert!(!super::same_value(&int, &float));
		assert!(matches!(value("1..2"), crate::val::Value::Range(_)));
		assert!(super::same_value(&value("t:1"), &value("t:1")));
		assert!(!super::same_value(&value("t:1"), &value("u:1")));
		assert!(!super::same_value(&value("1..2"), &value("1..=2")));
	}

	fn key(build: &BuildId, version: u64) -> UnitKey {
		UnitKey {
			ns: "ns".into(),
			db: "db".into(),
			ids: (NamespaceId(1), DatabaseId(1)),
			object: UnitObject::Function("f".into()),
			version: ObjectVersion::Content(version),
			build: build.clone(),
		}
	}

	fn frame(class_digest: u64) -> UnitFrame {
		UnitFrame {
			class_digest,
			ns: Some("ns".into()),
			db: Some("db".into()),
			perms_suppressed: false,
			checked: true,
		}
	}

	/// Concurrent requests for one key and frame compile once; a request for
	/// another frame, version or build compiles its own.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn concurrent_requests_compile_once() {
		let ds = crate::kvs::Datastore::new("memory").await.unwrap();
		let txn = ds.transaction(crate::kvs::TransactionType::Read).await.unwrap();
		let txn = Arc::new(txn);
		let ctx = crate::ctx::Context::new_test().freeze();
		let cache: Arc<UnitCache<u64>> = Arc::new(UnitCache::functions(16));
		let build = BuildId::current(Arc::new(crate::dbs::Capabilities::all()));
		let compiled = Arc::new(AtomicUsize::new(0));
		let requests = (0..32).map(|_| {
			let (cache, txn, ctx, build, compiled) = (
				Arc::clone(&cache),
				Arc::clone(&txn),
				Arc::clone(&ctx),
				build.clone(),
				Arc::clone(&compiled),
			);
			tokio::spawn(async move {
				cache
					.get_or_compile::<(), _>(
						&key(&build, 1),
						&frame(3),
						&txn,
						&ctx,
						|| async move {
							compiled.fetch_add(1, Ordering::SeqCst);
							tokio::time::sleep(std::time::Duration::from_millis(20)).await;
							Ok((7, Some((CatalogDeps::default(), ContextDeps::default()))))
						},
					)
					.await
			})
		});
		for answer in futures::future::join_all(requests).await {
			assert_eq!(answer.unwrap(), Ok(7));
		}
		assert_eq!(compiled.load(Ordering::SeqCst), 1);
		let stats = cache.stats();
		assert_eq!(stats.compiles, 1);
		assert_eq!(stats.hits + stats.waits, 31);
		// Another frame, another version and another build are other units.
		for (key, frame) in [
			(key(&build, 1), frame(5)),
			(key(&build, 2), frame(3)),
			(key(&BuildId::current(Arc::new(crate::dbs::Capabilities::all())), 1), frame(3)),
		] {
			let compiled = Arc::clone(&compiled);
			let answer = cache
				.get_or_compile::<(), _>(&key, &frame, &txn, &ctx, || async move {
					compiled.fetch_add(1, Ordering::SeqCst);
					Ok((8, Some((CatalogDeps::default(), ContextDeps::default()))))
				})
				.await;
			assert_eq!(answer, Ok(8));
		}
		assert_eq!(compiled.load(Ordering::SeqCst), 4);
		txn.cancel().await.unwrap();
	}

	/// A failed compilation keeps nothing, and a compilation that cannot be
	/// kept is compiled again by the next request.
	#[tokio::test]
	async fn failures_and_unkeepable_units_are_not_kept() {
		let ds = crate::kvs::Datastore::new("memory").await.unwrap();
		let txn = ds.transaction(crate::kvs::TransactionType::Read).await.unwrap();
		let ctx = crate::ctx::Context::new_test().freeze();
		let cache: UnitCache<u64> = UnitCache::functions(16);
		let build = BuildId::current(Arc::new(crate::dbs::Capabilities::all()));
		let failed = cache
			.get_or_compile(&key(&build, 1), &frame(3), &txn, &ctx, || async { Err("no") })
			.await;
		assert_eq!(failed, Err("no"));
		let mut compiled = 0;
		for _ in 0..2 {
			let answer = cache
				.get_or_compile::<(), _>(&key(&build, 1), &frame(3), &txn, &ctx, || {
					compiled += 1;
					async { Ok((1, None)) }
				})
				.await;
			assert_eq!(answer, Ok(1));
		}
		assert_eq!(compiled, 2);
		let kept = cache
			.get_or_compile::<(), _>(&key(&build, 2), &frame(3), &txn, &ctx, || async {
				Ok((2, Some((CatalogDeps::default(), ContextDeps::default()))))
			})
			.await;
		assert_eq!(kept, Ok(2));
		let served = cache
			.get_or_compile::<(), _>(&key(&build, 2), &frame(3), &txn, &ctx, || async {
				panic!("a kept unit was compiled again")
			})
			.await;
		assert_eq!(served, Ok(2));
		txn.cancel().await.unwrap();
	}

	/// A dynamic-target arm is keyed by its table and the arm key of its
	/// statement shape: the same table and digest are one object; another
	/// table, or a digest with another arm key, is another.
	#[test]
	fn dynamic_target_arm_keys_by_table_and_shape() {
		use crate::exec::ir::iface::premise::ShapeDigest;
		let digest = |head: u8| {
			let mut bytes = [0u8; 32];
			bytes[0] = head;
			ShapeDigest(bytes)
		};
		let arm = |table: &str, digest: &ShapeDigest| {
			UnitObject::dynamic_target_arm(table.into(), digest)
		};
		assert_eq!(arm("t", &digest(1)), arm("t", &digest(1)));
		assert_ne!(arm("t", &digest(1)), arm("u", &digest(1)));
		assert_ne!(arm("t", &digest(1)), arm("t", &digest(2)));
	}
}
