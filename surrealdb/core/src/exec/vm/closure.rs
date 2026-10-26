//! Running a closure value's body.
//!
//! A closure value (`CompiledClosure`) is a code reference and its captures:
//! the digest of the closure as written ([`ClosureDigest`]) and the values it
//! captured. It also carries the closure as written, which tells apart two
//! closures that share a digest and which the compile cache keys its body by.
//! It never carries a program.
//!
//! A unit is resolved for a closure, in order:
//!
//! 1. the units the programs running around the invocation lowered when they were planned — the
//!    body of every closure literal they make and of every closure a parameter held — which each
//!    program registers as it starts ([`with_units`]);
//! 2. the units linked into the run;
//! 3. the compile cache ([`super::compile_cache`]), by the closure compared whole, the names its
//!    values capture and the frame of the invocation, linked into the run when found and compiled
//!    on a miss ([`load_unit`]).
//!
//! Both tables are the run's [`ClosureUnits`], shared by every program that
//! runs beneath the first: the statements of a block are programs of their
//! own, and a closure one makes is invoked by another. A unit is taken only
//! when its head holds the closure — compared whole — the names its value
//! captured and this build's [`ISA_VERSION`], when its frame admits the
//! invoking frame ([`super::host::frame_admits`]), and while what its lowering
//! read of the catalog holds. A unit lowered for another class of principal,
//! database or permission contract is not found, so a value never runs a body
//! lowered for a frame other than the one it runs in. A unit's head declares
//! what its body may do beyond computing its value ([`ClosureHead::effects`]);
//! the instruction that links it refuses the invocation where the frame does
//! not grant those effects ([`grants`], [`admit`]).
//!
//! The resolution is the instruction's, not the invocation's. An instruction
//! that hands a callee a closure value — `Invoke`, a call of a builtin at the
//! positions the builtin invokes a closure from, a method — first yields
//! [`super::interp::Step::NeedUnit`] for each closure its state has not
//! linked a unit for in the instruction's frame, and the host links one
//! ([`link`]). A distinct closure is loaded from the compile cache once per
//! run and frame; every later instruction finds it in the run's table, and
//! every row after the first finds it in its state. The run's table links at
//! most [`LINKED`] units, dropping the least recently used.
//!
//! The invocation itself only finds and runs ([`run_unit`]); every
//! invocation runs a unit, and no closure body is evaluated any other way.
//! A unit stands for the `VERSION` stamp its body was lowered at, and an
//! invocation read at a stamp runs only a unit lowered at it, which the host
//! lowers on its fault for the stamp alone. Where the unit the instruction
//! linked stops standing after it was taken — what the unit read changed, or
//! the run's table dropped it — the invocation finds none. An `Invoke` hands
//! the step back and takes the instruction's link again; an invocation inside
//! a call the instruction made — a builtin invoking the value per element —
//! cannot, and links it itself ([`link`]). An invocation made outside a
//! running program — from an expression reached from outside a plan — loads
//! the unit for its frame from the compile cache before it runs it, in a
//! table of its own that the body's own invocations share. A body the host
//! does not lower for the frame fails each invocation with the reason, or
//! with the error its compilation reports, kept once per closure and frame.
//!
//! The unit is one pipeline over the invocation's row — its document root,
//! or its current value where it has no document, which the run marks so the
//! body reads it as no document — emitting the body's value, and each
//! invocation runs it over a state of its own: nothing one invocation
//! computes — a statement the body evaluates once per run, a sort, a page —
//! is seen by the next. A body invoked from a computed field in the middle
//! of computing it runs with the record being computed, as the field's own
//! expression does.

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::UnitTable;
use super::program::{ClosureDigest, ClosureHead, ISA_VERSION, Program, RowBits, UnitFrame};
use super::scalar::Held;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{DatabaseId, Error as CatalogError, NamespaceId};
use crate::exec::vm::calls::CompiledClosure;
use crate::exec::{EvalContext, ExecutionContext, FlowResult};
use crate::kvs::Transaction;
use crate::val::Value;

#[cfg(test)]
thread_local! {
	/// How many closure bodies this thread has run as units, for tests that
	/// must know an invocation ran as bytecode.
	pub(super) static BODIES_RUN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many times this thread has read a unit's [`ClosureReads`] from the
	/// catalog, for tests that must know they are read once while the
	/// transaction is unchanged.
	pub(super) static READS_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The most units a run links beyond those its programs lowered.
pub(super) const LINKED: usize = 64;

/// The arguments an invocation bound, by name.
type Bound = Vec<(surrealdb_strand::Strand, Value)>;

/// What a unit's lowering read of the catalog that a transaction's catalog
/// stamps do not count — functions, parameters and databases — by name, with
/// the state each was read in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClosureReads {
	functions: Vec<(NamespaceId, DatabaseId, String)>,
	params: Vec<(NamespaceId, DatabaseId, String)>,
	databases: Vec<(String, String)>,
	/// The state each entry above was read in, in their order: the
	/// [`state_digest`] of its definition, or `None` where it was not
	/// defined.
	states: Vec<Option<[u8; 32]>>,
}

assert_plain!(ClosureReads);

impl ClosureReads {
	/// Whether every entry is, as `txn` reads it, in the state it was read in.
	async fn hold(&self, txn: &Transaction) -> bool {
		#[cfg(test)]
		READS_READ.with(|read| read.set(read.get() + 1));
		let mut recorded = self.states.iter();
		self.visit(txn, |state| recorded.next() == Some(&state)).await == Some(true)
	}

	/// Hand `visit` the state `txn` holds each entry in, in order, while it
	/// answers `true`: whether it answered `true` throughout, or `None` where
	/// an entry cannot be read.
	async fn visit(
		&self,
		txn: &Transaction,
		mut visit: impl FnMut(Option<[u8; 32]>) -> bool,
	) -> Option<bool> {
		for (ns, db, name) in &self.functions {
			let state = match txn.get_db_function(*ns, *db, name, None).await {
				Ok(definition) => Some(state_digest(FUNCTION, &definition, |d, feed| d.hash(feed))),
				Err(e) if is_absent(&e, |e| matches!(e, CatalogError::FcNotFound { .. })) => None,
				Err(_) => return None,
			};
			if !visit(state) {
				return Some(false);
			}
		}
		for (ns, db, name) in &self.params {
			let state = match txn.get_db_param(*ns, *db, name, None).await {
				Ok(definition) => Some(state_digest(PARAM, &definition, |d, feed| {
					d.value.hash(feed);
					d.permissions.hash(feed);
				})),
				Err(e) if is_absent(&e, |e| matches!(e, CatalogError::PaNotFound { .. })) => None,
				Err(_) => return None,
			};
			if !visit(state) {
				return Some(false);
			}
		}
		for (ns, db) in &self.databases {
			let state = match txn.get_db_by_name(ns, db, None).await {
				Ok(Some(definition)) => {
					Some(state_digest(DATABASE, &definition, |d, feed| d.hash(feed)))
				}
				Ok(None) => None,
				Err(_) => return None,
			};
			if !visit(state) {
				return Some(false);
			}
		}
		Some(true)
	}
}

/// The domain tags of the definitions [`state_digest`] digests.
const FUNCTION: u8 = 1;
const PARAM: u8 = 2;
const DATABASE: u8 = 3;

/// How many definitions [`state_digest`] answers by identity.
const DIGESTED: usize = 16;

/// SHA-256 over the domain tag `tag` and what `hash` feeds of `definition`:
/// the state a unit's lowering read the definition in.
///
/// The digest is compared only with one this process took, so the `Hash`
/// stream `hash` feeds need not be stable across builds: a state taken by
/// another build reads as a change, and the unit is lowered again. The
/// catalog hands out one immutable allocation per definition for as long as
/// it stands, so the last [`DIGESTED`] allocations digested on this thread
/// are answered by identity, each held so that no other can take its
/// address.
fn state_digest<T: Send + Sync + 'static>(
	tag: u8,
	definition: &Arc<T>,
	hash: impl FnOnce(&T, &mut Feed),
) -> [u8; 32] {
	type Held = (Arc<dyn std::any::Any + Send + Sync>, u8, [u8; 32]);
	thread_local! {
		static DIGESTS: std::cell::RefCell<Vec<Held>> = const { std::cell::RefCell::new(Vec::new()) };
	}
	let address = Arc::as_ptr(definition).cast::<()>();
	DIGESTS.with(|digests| {
		let mut digests = digests.borrow_mut();
		if let Some((_, _, digest)) = digests.iter().find(|(held, held_tag, _)| {
			*held_tag == tag && Arc::as_ptr(held).cast::<()>() == address
		}) {
			return *digest;
		}
		let mut feed = Feed(<sha2::Sha256 as sha2::Digest>::new());
		sha2::Digest::update(&mut feed.0, [tag]);
		hash(definition, &mut feed);
		let digest: [u8; 32] = sha2::Digest::finalize(feed.0).into();
		if digests.len() == DIGESTED {
			digests.remove(0);
		}
		let held: Arc<dyn std::any::Any + Send + Sync> = Arc::clone(definition) as _;
		digests.push((held, tag, digest));
		digest
	})
}

/// A `Hasher` that feeds every byte it is handed to SHA-256.
struct Feed(sha2::Sha256);

impl std::hash::Hasher for Feed {
	fn write(&mut self, bytes: &[u8]) {
		sha2::Digest::update(&mut self.0, bytes);
	}

	/// Not read: the digest is the SHA-256 the bytes were fed to.
	fn finish(&self) -> u64 {
		0
	}
}

/// Whether `error` is the catalog's answer that an entry is not defined.
fn is_absent(error: &anyhow::Error, is: impl Fn(&CatalogError) -> bool) -> bool {
	error.downcast_ref::<CatalogError>().is_some_and(is)
}

/// A run's closure-body units, by the digest of the closure each stands for:
/// those its programs lowered, and those it linked from the node's units or
/// faulted for. It also holds the reads of tables the
/// run linked ([`super::dispatch::LinkedReads`]), which every program of the
/// run shares whether or not the program it is beneath reads by dispatch.
#[derive(Default)]
pub(crate) struct ClosureUnits {
	table: std::sync::RwLock<Table>,
	/// The run's clock, which orders the linked units by their last use.
	clock: AtomicU64,
	/// The reads of tables the run linked.
	reads: Arc<super::dispatch::LinkedReads>,
}

#[derive(Default)]
struct Table {
	/// The units the run's programs lowered when they were planned.
	planned: HashMap<ClosureDigest, Vec<Entry>>,
	/// The units the run linked, and the faults no unit answered.
	linked: HashMap<ClosureDigest, Vec<Entry>>,
	/// How many entries `linked` holds.
	linked_len: usize,
}

/// One entry of a run's table.
struct Entry {
	/// The unit, or the failure every invocation of a body the host does
	/// not lower for the frame reports.
	unit: Result<Arc<RunUnit>, surrealdb_types::Error>,
	/// The closure as written the entry stands for: a value's closure is
	/// compared with it by identity first, and whole where the two are not
	/// one, since two closures may share a digest.
	closure: Arc<crate::expr::logical::LogicalClosure>,
	/// The names the closure's values capture, sorted.
	captured: Vec<String>,
	/// The row captures the closure's values hold.
	rows: RowBits,
	/// The frame the entry stands for.
	frame: UnitFrame,
	/// The `VERSION` stamp the entry stands for: an invocation read at it,
	/// or at none for `None`.
	version: Option<u64>,
	/// How many table catalog versions the transaction had stamped when the
	/// entry was made: it stands only while the invoking transaction has
	/// stamped no more.
	stamps: u64,
	/// The run's clock at the entry's last use.
	used: AtomicU64,
}

/// A unit a run's table holds, with the table its nested ids index and what
/// the run's invocations of it have learned.
struct RunUnit {
	unit: Arc<Program>,
	table: UnitTable,
	/// Whether the unit's body has handed the host a step in an invocation
	/// of this run: it is then run by the host from the start, rather than
	/// attempted in place first.
	steps: AtomicBool,
	/// The transaction the unit's [`ClosureReads`] were last found to hold
	/// in, with its [`Transaction::state_changes`] count read before they
	/// were: while the count is unchanged they hold still, and are not read
	/// again.
	held_in: Mutex<Option<(Arc<Transaction>, u64)>>,
}

impl RunUnit {
	fn new(unit: &Arc<Program>, table: &UnitTable) -> Arc<Self> {
		Arc::new(Self {
			unit: Arc::clone(unit),
			table: Arc::clone(table),
			steps: AtomicBool::new(false),
			held_in: Mutex::new(None),
		})
	}

	/// Whether `reads`, this unit's, hold in `txn`: read again only once the
	/// transaction has changed since they last held in it.
	async fn reads_hold(&self, reads: &ClosureReads, txn: &Arc<Transaction>) -> bool {
		let changes = txn.state_changes();
		let unchanged = self
			.held_in
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.as_ref()
			.is_some_and(|(held, at)| Arc::ptr_eq(held, txn) && *at == changes);
		if unchanged {
			return true;
		}
		if !reads.hold(txn).await {
			return false;
		}
		*self.held_in.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
			Some((Arc::clone(txn), changes));
		true
	}
}

impl Entry {
	/// The entry for `unit`, a closure body standing while the transaction
	/// has stamped `stamps` versions; `None` when it is no closure body, or
	/// one lowered to an instruction set other than this build's.
	fn of(unit: &Arc<Program>, table: &UnitTable, stamps: u64) -> Option<Self> {
		let head = unit.unit.as_ref()?;
		let closure = head.closure.as_ref().filter(|closure| closure.isa == ISA_VERSION)?;
		Some(Self {
			closure: Arc::clone(&closure.closure),
			captured: closure.captured.clone(),
			rows: closure.rows,
			frame: head.frame.clone(),
			version: closure.version,
			stamps,
			unit: Ok(RunUnit::new(unit, table)),
			used: AtomicU64::new(0),
		})
	}
}

/// What a run's table holds for an invocation.
enum Found {
	/// A unit, and whether the run linked it rather than lowered it.
	Unit(Arc<RunUnit>, bool),
	/// A fault no unit answered, with the failure it reports.
	Failed(surrealdb_types::Error),
}

impl std::fmt::Debug for ClosureUnits {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("ClosureUnits").finish_non_exhaustive()
	}
}

/// How many pairs of closures [`same_closure`] answers from a comparison it
/// made before.
const COMPARED: usize = 16;

/// Whether `a` and `b` are one closure as written: by identity, or compared
/// whole. The values of one literal share its closure, so identity answers
/// nearly every invocation; the last [`COMPARED`] pairs of distinct closures
/// compared whole on this thread are answered from that comparison, each
/// held so that no other can take its address.
fn same_closure(
	a: &Arc<crate::expr::logical::LogicalClosure>,
	b: &Arc<crate::expr::logical::LogicalClosure>,
) -> bool {
	type Pair = (
		Arc<crate::expr::logical::LogicalClosure>,
		Arc<crate::expr::logical::LogicalClosure>,
		bool,
	);
	thread_local! {
		static PAIRS: std::cell::RefCell<Vec<Pair>> = const { std::cell::RefCell::new(Vec::new()) };
	}
	if Arc::ptr_eq(a, b) {
		return true;
	}
	PAIRS.with(|pairs| {
		let mut pairs = pairs.borrow_mut();
		if let Some((_, _, same)) = pairs.iter().find(|(x, y, _)| {
			(Arc::ptr_eq(x, a) && Arc::ptr_eq(y, b)) || (Arc::ptr_eq(x, b) && Arc::ptr_eq(y, a))
		}) {
			return *same;
		}
		let same = **a == **b;
		if pairs.len() == COMPARED {
			pairs.remove(0);
		}
		pairs.push((Arc::clone(a), Arc::clone(b), same));
		same
	})
}

/// Drop the least recently used entry of `held`, where `used` reads an
/// entry's last use: whether it dropped one.
fn drop_oldest<T>(held: &mut HashMap<ClosureDigest, Vec<T>>, used: impl Fn(&T) -> u64) -> bool {
	let oldest = held
		.iter()
		.flat_map(|(digest, entries)| {
			entries.iter().enumerate().map(|(at, e)| (used(e), *digest, at))
		})
		.min();
	let Some((_, digest, at)) = oldest else {
		return false;
	};
	let Some(entries) = held.get_mut(&digest) else {
		return false;
	};
	entries.remove(at);
	if entries.is_empty() {
		held.remove(&digest);
	}
	true
}

impl ClosureUnits {
	/// The reads of tables the run linked.
	pub(super) fn linked_reads(&self) -> &Arc<super::dispatch::LinkedReads> {
		&self.reads
	}

	/// Register the closure units of `table`, if not registered already.
	fn register(&self, table: &UnitTable) {
		let mut held = self.table.write().unwrap_or_else(std::sync::PoisonError::into_inner);
		for unit in table.iter() {
			let Some(closure) = unit.unit.as_ref().and_then(|head| head.closure.as_ref()) else {
				continue;
			};
			let entries = held.planned.entry(closure.digest).or_default();
			let registered = entries.iter().any(|entry| {
				entry.unit.as_ref().is_ok_and(|registered| Arc::ptr_eq(&registered.unit, unit))
			});
			if !registered && let Some(entry) = Entry::of(unit, table, closure.stamps) {
				entries.push(entry);
			}
		}
	}

	/// What the run's table holds for `closure` invoked in `exec` while the
	/// transaction has stamped `stamps` versions, passing over the units its
	/// programs lowered when `linked_only`.
	fn find(
		&self,
		closure: &CompiledClosure,
		exec: &ExecutionContext,
		stamps: u64,
		linked_only: bool,
	) -> Option<Found> {
		let captures = &closure.captures().0;
		let logical = closure.logical();
		let version = exec.version_stamp();
		let rows = closure.rows();
		let stands = |entry: &Entry| {
			entry.stamps == stamps
				&& entry.version == version
				&& entry.captured.len() == captures.len()
				&& entry.captured.iter().zip(captures.keys()).all(|(a, b)| a == b.as_str())
				&& entry.rows == rows
				&& super::host::frame_admits_as(
					&entry.frame,
					exec,
					exec.root().skip_fetch_perms,
					|frame| super::host::class_digest(exec, frame),
				) && same_closure(&entry.closure, logical)
		};
		let held = self.table.read().unwrap_or_else(std::sync::PoisonError::into_inner);
		let digest = closure.digest();
		if !linked_only
			&& let Some(entry) =
				held.planned.get(&digest).and_then(|entries| entries.iter().find(|e| stands(e)))
			&& let Ok(unit) = &entry.unit
		{
			return Some(Found::Unit(Arc::clone(unit), false));
		}
		let entry = held.linked.get(&digest)?.iter().find(|e| stands(e))?;
		entry.used.store(self.clock.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
		Some(match &entry.unit {
			Ok(unit) => Found::Unit(Arc::clone(unit), true),
			Err(failure) => Found::Failed(failure.clone()),
		})
	}

	/// Link `entry` for the closure `digest` names into the run, dropping
	/// the least recently used linked entry when the run holds [`LINKED`].
	fn link(&self, digest: ClosureDigest, entry: Entry) {
		let mut held = self.table.write().unwrap_or_else(std::sync::PoisonError::into_inner);
		if held.linked_len >= LINKED
			&& drop_oldest(&mut held.linked, |e| e.used.load(Ordering::Relaxed))
		{
			held.linked_len -= 1;
		}
		entry.used.store(self.clock.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
		held.linked.entry(digest).or_default().push(entry);
		held.linked_len += 1;
	}

	/// Drop linked unit `unit` of the closure `digest` names, which does not
	/// stand for it.
	fn unlink(&self, digest: ClosureDigest, unit: &Arc<Program>) {
		let mut held = self.table.write().unwrap_or_else(std::sync::PoisonError::into_inner);
		let Some(entries) = held.linked.get_mut(&digest) else {
			return;
		};
		let before = entries.len();
		entries.retain(|entry| {
			!entry.unit.as_ref().is_ok_and(|linked| Arc::ptr_eq(&linked.unit, unit))
		});
		let dropped = before - entries.len();
		if entries.is_empty() {
			held.linked.remove(&digest);
		}
		held.linked_len -= dropped;
	}

	/// How many entries the run has linked.
	#[cfg(test)]
	pub(super) fn linked_len(&self) -> usize {
		self.table.read().unwrap_or_else(std::sync::PoisonError::into_inner).linked_len
	}
}

/// The context `program` runs in, when the run around it has no table of
/// closure-body units yet: one holding the units it lowered, which every
/// program beneath it shares. `None` for a program whose context carries the
/// run's table already, into which its units are registered.
pub(super) fn with_units(program: &Program, ctx: &ExecutionContext) -> Option<ExecutionContext> {
	let holds = program
		.units
		.iter()
		.any(|unit| unit.unit.as_ref().is_some_and(|head| head.closure.is_some()));
	match ctx.vm_closures() {
		Some(registry) => {
			if holds {
				registry.register(&program.units);
			}
			None
		}
		None => {
			let registry = Arc::new(ClosureUnits::default());
			if holds {
				registry.register(&program.units);
			}
			Some(ctx.with_vm_closures(registry))
		}
	}
}

/// The frame the host lowers a body for an invocation in `exec` for: the
/// frame a statement planned there is lowered for.
pub(super) fn frame_of(exec: &ExecutionContext) -> UnitFrame {
	let root = exec.root();
	let (ns, db) = names_of(exec);
	UnitFrame {
		class_digest: UnitFrame::digest(
			exec.ctx().auth_enabled(),
			Some(root.auth.as_ref()),
			ns.as_deref(),
			db.as_deref(),
		),
		ns,
		db,
		perms_suppressed: root.perms_suppressed || root.skip_fetch_perms,
		checked: true,
	}
}

/// The namespace and database `exec` runs in, where it runs in one.
pub(super) fn names_of(exec: &ExecutionContext) -> (Option<String>, Option<String>) {
	match exec.database() {
		Ok(db) => (Some(db.ns_name().to_owned()), Some(db.db_name().to_owned())),
		Err(_) => (None, None),
	}
}

/// The re-entry depth a body planned for an invocation in `exec` starts at:
/// what the frame has already spent of its budget.
pub(super) fn depth_of(exec: &ExecutionContext) -> u32 {
	let budget = exec.ctx().config.exec.max_computation_depth;
	exec.options().map_or(0, |options| budget.saturating_sub(options.dive))
}

/// What an invocation of a closure value found in the run's table.
pub(crate) enum Invoked {
	/// The unit ran, answering the body's value, or the invocation failed.
	Ran(FlowResult<Value>),
	/// The run's table holds no unit standing for the closure in the
	/// invoking frame: the instruction invoking it linked none, or what the
	/// linked one was lowered against has changed since. Handed back with the
	/// arguments, for the caller to take the instruction's fault or to link
	/// the unit itself.
	Unlinked(Bound),
}

/// The closure value `value` holds, where it holds one the engine invokes
/// by unit.
pub(super) fn compiled_of(value: &Value) -> Option<&CompiledClosure> {
	let Value::Closure(closure) = value else {
		return None;
	};
	let crate::val::Closure::Builtin(obj) = closure.as_ref() else {
		return None;
	};
	obj.as_any().downcast_ref()
}

/// The digest of the closure `value` holds, where it holds a closure value
/// the engine invokes by unit.
pub(super) fn digest_of(value: &Value) -> Option<ClosureDigest> {
	compiled_of(value).map(CompiledClosure::digest)
}

/// The value of `closure`'s body invoked from `ctx` with the arguments
/// `bound`, as the unit the run's table holds for its digest.
///
/// Inside a running program it resolves and runs only: a unit the
/// instruction invoking the closure did not link is [`Invoked::Unlinked`],
/// never compiled here. Outside one it loads the unit for the invoking frame
/// first ([`run_outside`]). Boxed, since a body that invokes a closure runs
/// this again beneath it.
pub(crate) fn run_unit<'a>(
	closure: &'a CompiledClosure,
	ctx: &'a EvalContext<'a>,
	bound: Bound,
) -> crate::exec::BoxFut<'a, Invoked> {
	Box::pin(find_and_run(closure, ctx, bound))
}

/// [`run_unit`].
async fn find_and_run(closure: &CompiledClosure, ctx: &EvalContext<'_>, bound: Bound) -> Invoked {
	let waived = waived(ctx);
	let exec = waived.as_ref().unwrap_or(ctx.exec_ctx);
	let Some(txn) = exec.ctx().try_tx() else {
		return Invoked::Ran(Err(no_transaction()));
	};
	let Some(registry) = exec.vm_closures() else {
		return Invoked::Ran(run_outside(closure, ctx, txn, bound).await);
	};
	let unit = match held(registry, closure, exec, txn).await {
		Some(Found::Unit(unit, _)) => unit,
		Some(Found::Failed(failure)) => return Invoked::Ran(Err(reported(failure))),
		None => return Invoked::Unlinked(bound),
	};
	Invoked::Ran(run_held(&unit, closure, ctx, bound, None).await)
}

/// The value of `closure`'s body invoked from `ctx` with the arguments
/// `bound`, outside a running program: the unit is loaded for the invoking
/// frame from the compile cache, compiled on a miss, into a table of the
/// invocation's own, which the invocations the body makes share. The unit
/// runs on a frame stack of its own where no statement's is polling
/// ([`crate::exec::trampoline`]), so a body that invokes its own closure
/// nests no deeper natively than the depth budget allows.
async fn run_outside(
	closure: &CompiledClosure,
	ctx: &EvalContext<'_>,
	txn: &Arc<Transaction>,
	bound: Bound,
) -> FlowResult<Value> {
	use futures::StreamExt;
	let waived = waived(ctx);
	let exec = waived.as_ref().unwrap_or(ctx.exec_ctx);
	let registry = Arc::new(ClosureUnits::default());
	load_unit(&registry, closure, exec, txn).await?;
	let unit = match registry.find(closure, exec, txn.catalog_stamps(), true) {
		Some(Found::Unit(unit, _)) => unit,
		Some(Found::Failed(failure)) => return Err(reported(failure)),
		None => return Err(internal("a closure unit loaded for an invocation was not linked")),
	};
	let run = Box::pin(run_held(&unit, closure, ctx, bound, Some(registry)));
	let mut driven = crate::exec::trampoline::Trampolined::root(futures::stream::once(run));
	match driven.next().await {
		Some(ran) => ran,
		None => Err(internal("a closure invocation that answered nothing")),
	}
}

/// Run `unit`, which a run's table holds for `closure`, for an invocation
/// from `ctx` with the arguments `bound`, in a frame holding `registry`
/// where the invocation brings a table of its own.
async fn run_held(
	unit: &RunUnit,
	closure: &CompiledClosure,
	ctx: &EvalContext<'_>,
	bound: Bound,
	registry: Option<Arc<ClosureUnits>>,
) -> FlowResult<Value> {
	let Some(head) = unit.unit.unit.as_ref().and_then(|head| head.closure.as_ref()) else {
		return Err(internal("a closure unit with no closure head"));
	};
	run_body(unit, head, closure, ctx, bound, registry).await
}

/// The frame of an invocation from `ctx` whose reads waive the checks its
/// frame applies — a permission predicate's, a gate's — with the waiver, so
/// the unit is found for, and lowered with, permissions suppressed; `None`
/// where the frame already states it.
fn waived(ctx: &EvalContext<'_>) -> Option<ExecutionContext> {
	(ctx.skip_fetch_perms && !ctx.exec_ctx.root().skip_fetch_perms)
		.then(|| ctx.exec_ctx.clone().with_skip_fetch_perms(true))
}

/// The failure a closure invoked with no transaction reports: its body is
/// lowered over one.
fn no_transaction() -> crate::expr::ControlFlow {
	crate::expr::ControlFlow::Err(anyhow::Error::new(crate::exec::Error::InvalidStatement(
		"a closure is invoked in a transaction, and none is open".to_owned(),
	)))
}

/// `failure`, a body's failure the run's table keeps, as an invocation
/// reports it.
fn reported(failure: surrealdb_types::Error) -> crate::expr::ControlFlow {
	crate::expr::ControlFlow::Err(anyhow::Error::new(failure))
}

/// An internal error for `reason`.
fn internal(reason: &str) -> crate::expr::ControlFlow {
	crate::expr::ControlFlow::Err(anyhow::Error::new(crate::err::Error::Engine(
		crate::err::EngineError::Internal(reason.to_owned()),
	)))
}

/// What the run's table `registry` holds standing for `closure` invoked in
/// `exec` over `txn`: a unit whose lowering's reads of the catalog still
/// hold, the run's record of the failure a body the host lowers none of for
/// the frame reports, or nothing. A linked unit whose reads do not hold is
/// dropped.
async fn held(
	registry: &ClosureUnits,
	closure: &CompiledClosure,
	exec: &ExecutionContext,
	txn: &Arc<Transaction>,
) -> Option<Found> {
	let stamps = txn.catalog_stamps();
	let mut linked_only = false;
	loop {
		let found = registry.find(closure, exec, stamps, linked_only)?;
		let Found::Unit(unit, linked) = &found else {
			return Some(found);
		};
		// What the lowering read beyond the table versions the stamps count
		// must be as it was for this invocation.
		let reads = unit
			.unit
			.unit
			.as_ref()
			.and_then(|head| head.closure.as_ref())
			.and_then(|head| head.reads.as_ref());
		// The run-time seal ([`crate::exec::compiles`]) wraps this check:
		// it confirms what the kept unit was lowered against.
		let holds = match reads {
			Some(reads) => crate::exec::compiles::seam(unit.reads_hold(reads, txn)).await,
			None => true,
		};
		match reads {
			Some(_) if !holds => {
				if *linked {
					registry.unlink(closure.digest(), &unit.unit);
				}
				linked_only = true;
			}
			_ => return Some(found),
		}
	}
}

/// Link the unit an invocation of `closure` from `ctx` runs into the run, as
/// the instruction invoking it does before it runs
/// ([`super::interp::Step::NeedUnit`]), then refuse the invocation where the
/// frame does not grant what the body may do ([`admit`]).
///
/// Nothing is linked where the run's table holds a unit standing for the
/// invocation, or the failure of a body the host lowers none of. Otherwise
/// the unit is loaded from the compile cache by the closure, the invoking
/// frame and its `VERSION` stamp ([`load_unit`]): one kept that stands for
/// the transaction, or, on a miss, one the host compiles from the closure as
/// written, the one kind of compile a running program causes. Outside a
/// running program nothing is linked: the invocation loads its unit itself.
///
/// Taken by the instruction handing the value over, before the call it makes,
/// and by an invocation inside that call whose linked unit stopped standing
/// after the instruction was taken, which the call cannot hand back to the
/// instruction.
pub(crate) async fn link(closure: &CompiledClosure, ctx: &EvalContext<'_>) -> FlowResult<()> {
	let waived = waived(ctx);
	let exec = waived.as_ref().unwrap_or(ctx.exec_ctx);
	if let (Some(registry), Some(txn)) = (exec.vm_closures(), exec.ctx().try_tx()) {
		// The run-time seal ([`crate::exec::compiles`]) wraps this check:
		// it confirms what a held unit was lowered against.
		let found = crate::exec::compiles::seam(held(registry, closure, exec, txn)).await;
		if found.is_none() {
			load_unit(registry, closure, exec, txn).await?;
		}
	}
	admit(closure, ctx).await
}

/// Load the unit of `closure`'s body for an invocation in `exec` over `txn`
/// from the compile cache, compiling it on a miss, and link it into the run's
/// table `registry`; a body the host lowers no unit of is linked with the
/// failure it reports, so the run asks once per closure and frame. The cache
/// keys a unit by the closure as written, compared whole, so two closures
/// that share a digest never share a unit. A unit lowered at a `VERSION`
/// stamp is compiled for the run alone ([`super::fault::serve_unit`]).
async fn load_unit(
	registry: &Arc<ClosureUnits>,
	closure: &CompiledClosure,
	exec: &ExecutionContext,
	txn: &Arc<Transaction>,
) -> FlowResult<()> {
	let stamps = txn.catalog_stamps();
	let captured = captured(closure);
	let frame = frame_of(exec);
	let served = super::fault::serve_unit(closure, &captured, &frame, exec, txn).await?;
	// A unit kept from another transaction, what it read found to hold for
	// this one, stands for this transaction's stamps.
	let entry = match served {
		Ok(served) => Entry::of(&served.unit, &served.table, stamps).map(|mut entry| {
			entry.closure = Arc::clone(closure.logical());
			entry
		}),
		Err(failure) => Some(Entry {
			unit: Err(failure),
			closure: Arc::clone(closure.logical()),
			captured,
			rows: closure.rows(),
			frame,
			version: exec.version_stamp(),
			stamps,
			used: AtomicU64::new(0),
		}),
	};
	match entry {
		Some(entry) => registry.link(closure.digest(), entry),
		None => return Err(internal("a closure unit served for another instruction set")),
	}
	Ok(())
}

/// Whether the frame of `exec` grants every effect in `effects`: the effects
/// a unit's head declares ([`ClosureHead::effects`]), or those of a closure
/// as written whose unit the run's table does not hold.
///
/// A speculative run ([`super::speculate`]) grants only what
/// [`super::speculate::admits`] admits: what its real run answers alike and
/// what acts on nothing outside the transaction. Every other frame grants
/// every effect here. A frame that refuses writes, a read-only transaction and
/// a capability the frame lacks each refuse an effect where the body reaches
/// it, with that effect's own error: a body that holds a write it does not
/// reach runs to its value, so refusing the write when the unit is linked
/// would refuse what the language allows.
pub(super) fn grants(exec: &ExecutionContext, effects: crate::expr::logical::EffectSet) -> bool {
	match exec.ctx().try_tx() {
		Some(txn) if txn.speculating() => super::speculate::admits(effects),
		_ => true,
	}
}

/// Refuse the invocation of `closure` from `ctx` that the instruction handing
/// it over links, where its frame does not grant what the body may do
/// ([`grants`]): the effects the head of the unit the run links declares, or
/// those of the closure as written where the run holds no unit of it. Taken
/// once per closure and frame of a state, with the link.
pub(super) async fn admit(closure: &CompiledClosure, ctx: &EvalContext<'_>) -> FlowResult<()> {
	let waived = waived(ctx);
	let exec = waived.as_ref().unwrap_or(ctx.exec_ctx);
	let Some(txn) = exec.ctx().try_tx() else {
		return Ok(());
	};
	if !txn.speculating() {
		return Ok(());
	}
	let linked = match exec.vm_closures() {
		Some(registry) => match held(registry, closure, exec, txn).await {
			Some(Found::Unit(unit, _)) => unit
				.unit
				.unit
				.as_ref()
				.and_then(|head| head.closure.as_ref())
				.map(|head| head.effects),
			_ => None,
		},
		_ => None,
	};
	let effects = match linked {
		Some(effects) => effects,
		None => {
			crate::exec::logical::effects_of_body(&closure.logical().body, exec.function_registry())
		}
	};
	match grants(exec, effects) {
		true => Ok(()),
		false => Err(super::speculate::real_run_only()),
	}
}

/// The names `closure`'s value captures, sorted.
fn captured(closure: &CompiledClosure) -> Vec<String> {
	closure.captures().0.keys().map(|name| name.as_str().to_owned()).collect()
}

/// Run the unit `held` holds, which `head` stands for, for an invocation from
/// `ctx` with the arguments `bound`, in a frame holding `registry` where the
/// invocation brings a table of its own.
///
/// The unit's row is the invocation's document root, and where it has none
/// its current value, which the body then reads as no document
/// ([`super::interp::State::undocumented`]). Where that document is a KNN
/// query vector's row being scanned, the body runs over no row in the
/// caller's context, which refuses a read of that row when the body makes
/// one: a body that only hands the row on, to a statement or a call, runs.
async fn run_body(
	held: &RunUnit,
	head: &ClosureHead,
	closure: &CompiledClosure,
	ctx: &EvalContext<'_>,
	bound: Bound,
	registry: Option<Arc<ClosureUnits>>,
) -> FlowResult<Value> {
	if let Some(forbidden) = closure.captured_forbidden_row() {
		return Err(forbidden.error());
	}
	let (program, units) = (&held.unit, &held.table);
	if head.rows != closure.rows() {
		return Err(internal("a closure unit run for a value holding other row captures"));
	}
	let access = ctx.document();
	let forbidden = matches!(access, crate::exec::context::RowAccess::Forbidden(_));
	let undocumented = !ctx.root_document().is_present() && ctx.value_in_scope().is_present();
	// From an element filter's body the row is the element and the document root stays the row
	// the filter is applied over, which a bare field and an index or method argument read apart.
	let element_row = ctx.in_element() && ctx.root_document().is_present();
	let root = match element_row {
		true => ctx.root_document().within_context().cloned(),
		false => None,
	};
	let row = match element_row {
		true => ctx.value_in_scope().within_context(),
		false => access.within_context(),
	};
	let has_row = row.is_some();
	let captures = &closure.captures().0;
	let mut slots = Vec::with_capacity(head.slots.len());
	let args = bound.len();
	for name in &head.slots[args..] {
		slots.push(Held::Owned(captures.get(name.as_str()).cloned().unwrap_or(Value::None)));
	}
	#[cfg(test)]
	BODIES_RUN.with(|run| run.set(run.get() + 1));
	// An invocation computing a record, or whose reads waive the checks its
	// frame applies, runs in a frame of its own, which carries the record and
	// the waiver.
	if head.frameless
		&& ctx.computing_record.is_none()
		&& (!ctx.skip_fetch_perms || ctx.exec_ctx.root().skip_fetch_perms)
	{
		// Nothing the unit runs reads the frame or re-enters, so it runs in
		// place in the caller's frame, over the row where it is; the call
		// still spends the depth it would.
		let exec = ctx.exec_ctx;
		if closure.dives() && exec.options().is_some_and(|options| options.dive < 1) {
			return Err(crate::exec::Error::ComputationDepthExceeded.into());
		}
		slots.splice(0..0, bound.into_iter().map(|(_, value)| Held::Owned(value)));
		let row = row.map_or(Cow::Owned(Value::None), Cow::Borrowed);
		let state = |slots: Vec<Held<'static>>| -> FlowResult<_> {
			let mut state = super::host::unit_state(program, exec)?;
			state.units = Some(Arc::clone(units));
			state.permissions_bypassed = ctx.skip_fetch_perms;
			state.rowless = !has_row;
			state.undocumented = undocumented;
			state.root.clone_from(&root);
			state.slots = slots;
			state.keep_slots_now(exec)?;
			Ok(state)
		};
		let pipeline = &program.pipelines[0];
		// A body that answers without the host runs synchronously, over a
		// state without the frames only the host's steps read; one that
		// hands the host a step is continued by the host where it stopped,
		// on that state where it can be.
		// A body run over a forbidden row is run by the host, whose run of it
		// refuses a read of that row.
		let (row, start, mut state) = if forbidden || held.steps.load(Ordering::Relaxed) {
			(row, 0, state(slots)?)
		} else {
			let mut inline = state(slots)?;
			match super::host::run_unit_inline(program, pipeline, row, &mut inline) {
				Ok(_) => return Ok(inline.output.pop().unwrap_or(Value::None)),
				Err(unfinished) => {
					if !unfinished.yielded {
						held.steps.store(true, Ordering::Relaxed);
					}
					match unfinished.resume {
						Some(start) => (unfinished.row, start, inline),
						None => {
							let slots = std::mem::take(&mut inline.slots);
							(unfinished.row, 0, state(slots)?)
						}
					}
				}
			}
		};
		super::host::host_frames(program, &mut state, exec);
		return super::host::out_of_line(|| {
			super::host::run_cow_row_from(program, pipeline, row, &mut state, exec, start)
		})
		.await
		.map(|_| state.output.pop().unwrap_or(Value::None));
	}
	let row = row.cloned().unwrap_or(Value::None);
	slots.splice(0..0, bound.iter().map(|(_, value)| Held::Owned(value.clone())));
	// The frame holds the unit's table, which the host code it runs — a
	// function's guard, a nested statement — finds the units it calls in.
	let running = closure.frame(ctx, bound, registry)?.with_vm_units(Arc::clone(units));
	let mut state = super::host::statement_state(program, &running)?;
	state.units = Some(Arc::clone(units));
	state.permissions_bypassed = ctx.skip_fetch_perms;
	state.rowless = !has_row;
	state.undocumented = undocumented;
	state.root = root;
	state.slots = slots;
	state.keep_slots(&running).await?;
	let program = Arc::clone(program);
	// Run on the statement's frame stack, so a body that invokes its own
	// closure nests no deeper natively than the depth budget allows.
	crate::exec::trampoline::nest(async move {
		let pipeline = &program.pipelines[0];
		super::host::run_value_row(&program, pipeline, row, &mut state, &running)
			.await
			.map(|_| state.output.pop().unwrap_or(Value::None))
	})
	.await
}

#[cfg(test)]
mod tests {
	use super::*;

	/// An entry for a fault no unit answered, for the closure `n` names.
	fn declined(n: usize) -> (ClosureDigest, Entry) {
		let mut digest = [0u8; 32];
		digest[..8].copy_from_slice(&(n as u64).to_le_bytes());
		let crate::expr::logical::LogicalExpr::Closure(closure) =
			crate::exec::operators::test_util::parse_expr("|| 1")
		else {
			panic!("a closure");
		};
		let entry = Entry {
			unit: Err(surrealdb_types::Error::internal("declined".to_owned())),
			closure: Arc::new(*closure),
			captured: Vec::new(),
			rows: RowBits::default(),
			frame: UnitFrame {
				class_digest: 0,
				ns: None,
				db: None,
				perms_suppressed: false,
				checked: true,
			},
			version: None,
			stamps: 0,
			used: AtomicU64::new(0),
		};
		(ClosureDigest(digest), entry)
	}

	/// Whether `units` has linked an entry for the closure `n` names.
	fn holds(units: &ClosureUnits, n: usize) -> bool {
		let (digest, _) = declined(n);
		units
			.table
			.read()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.linked
			.contains_key(&digest)
	}

	#[test]
	fn a_state_digest_is_sha256_and_tells_definitions_apart() {
		let one = Arc::new("DEFINE FUNCTION fn::v() { RETURN 1; }".to_owned());
		let two = Arc::new("DEFINE FUNCTION fn::v() { RETURN 2; }".to_owned());
		let again = Arc::new((*one).clone());
		let hash_text = |text: &String, feed: &mut Feed| text.hash(feed);
		let digest = state_digest(FUNCTION, &one, hash_text);
		let mut feed = Feed(<sha2::Sha256 as sha2::Digest>::new());
		sha2::Digest::update(&mut feed.0, [FUNCTION]);
		one.hash(&mut feed);
		let expected: [u8; 32] = sha2::Digest::finalize(feed.0).into();
		assert_eq!(digest, expected, "SHA-256 over the tag and the definition");
		assert_ne!(digest, state_digest(FUNCTION, &two, hash_text), "another definition");
		assert_eq!(digest, state_digest(FUNCTION, &again, hash_text), "an equal definition");
		assert_ne!(digest, state_digest(PARAM, &again, hash_text), "another kind of definition");
		// An allocation digested before is answered by identity.
		assert_eq!(state_digest(FUNCTION, &one, |_, _| {}), digest);
	}

	#[test]
	fn a_run_links_at_most_its_bound_dropping_the_least_recently_used() {
		let units = ClosureUnits::default();
		for n in 0..LINKED {
			let (digest, entry) = declined(n);
			units.link(digest, entry);
		}
		assert_eq!(units.linked_len(), LINKED);
		// The first entry is used again, so the second is the oldest.
		{
			let held = units.table.read().unwrap_or_else(std::sync::PoisonError::into_inner);
			let (digest, _) = declined(0);
			held.linked[&digest][0]
				.used
				.store(units.clock.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
		}
		let (digest, entry) = declined(LINKED);
		units.link(digest, entry);
		assert_eq!(units.linked_len(), LINKED, "the run links past its bound");
		assert!(holds(&units, 0), "the entry used last was dropped");
		assert!(!holds(&units, 1), "the least recently used entry was kept");
		assert!(holds(&units, LINKED));
	}
}
