//! Pooled WASM engines with optional epoch-based timeout enforcement.
//!
//! An [`Engines`] holds up to two engine flavours, created on first use:
//!
//! - **Guarded**: `epoch_interruption(true)`. Compiled WASM includes epoch checks at every loop
//!   back-edge and function call. A background ticker thread increments the epoch so that
//!   `Store::set_epoch_deadline` can enforce wall-clock timeouts. Adds ~10% overhead on typical
//!   code, up to ~2x on tight numerical loops.
//!
//! - **Fast**: No epoch interruption. WASM runs at full native speed. Timeouts cannot be enforced —
//!   the module must be trusted to complete within a reasonable time.
//!
//! The guarded engine allocates instances from wasmtime's pooling allocator,
//! with copy-on-write memory images. Every invocation instantiates afresh into
//! a pre-reserved slot, and the slot is reset to the module's image when the
//! store is dropped, so instantiation costs no `mmap` and no copy of the
//! initial heap. The fast engine, which only an operator who allows unbounded
//! execution ever creates, allocates on demand (still copy-on-write), so it
//! reserves no second pool.
//!
//! A slot reserves address space for one linear memory: wasmtime's default
//! reservation of 4 GiB plus its guard region on 64-bit hosts, whatever
//! [`EngineSettings::max_memory_bytes`] says, so that bounds checks can be
//! elided. The default 64 slots therefore reserve about 256 GiB of virtual
//! address space, none of it resident until used. A host that caps virtual
//! memory (`RLIMIT_AS`, `ulimit -v`) must allow that much, or fewer slots. The slot count is
//! [`EngineSettings::max_concurrent_invocations`], and [`Engines::acquire_slot`] hands out one
//! permit per slot so an invocation waits for a slot instead of failing inside the allocator.
//! Top-level invocations may hold all but [`EngineSettings::nested_reserve`]
//! slots between them, so that many slots, at least one, are free for nested
//! invocations whatever the query load. That is all the reserve guarantees:
//! when more nested invocations run at once than it holds, the rest find no
//! slot and fail at once with `NestedInvocationNoSlot`, rather than wait for
//! a slot their own callers hold.
//!
//! Engines are shared process-wide per [`EngineSettings`]: two runners with
//! the same settings share one pool and one slot count, since the slots are the
//! same address-space reservations. The pool is released when the last
//! [`Engines`] handle is dropped.
//!
//! On the guarded engine a store's epoch deadline is one tick away at all
//! times, and [`yield_until_deadline`] decides what each tick does: before the
//! invocation's deadline the guest yields to the async executor, at it the
//! guest traps. A guest that computes without calling the host therefore
//! hands its worker thread back every [`EPOCH_TICK_MS`], so busy invocations
//! cannot starve the tasks sharing their workers, including a slot wait's
//! timeout. The fast engine has no epoch checks: a guest on it keeps its
//! worker until it returns.
//!
//! Both engines use Cranelift. Winch cannot compile the component model's async
//! ABI, which `invoke` and the `transaction` methods depend on.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use surrealism_types::err::{SurrealismError, SurrealismResult};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use wasmtime::*;

/// Interval between epoch ticks (guarded engine only). Shorter = finer
/// timeout granularity, more thread wakeups.
pub const EPOCH_TICK_MS: u64 = 10;

/// Upper bounds on what one component instance may allocate from the pool.
///
/// The pool's totals are these multiplied by the slot count, so every slot
/// can hold a component of this shape. A component that needs more fails to
/// instantiate with the allocator's own error.
///
/// A `wasm32-wasip2` component holds one memory and a handful of core
/// instances (the module and the canonical-ABI shims); the instance margin
/// covers components composed from several modules. Memories are what a slot
/// reserves address space for, so a component with a second one fails to
/// instantiate rather than double every slot's reservation.
const CORE_INSTANCES_PER_COMPONENT: u32 = 16;
const MEMORIES_PER_COMPONENT: u32 = 1;
const TABLES_PER_COMPONENT: u32 = 8;
/// Fiber stacks one invocation may hold: the call into `invoke` and the
/// guest tasks the component model's async ABI starts beside it.
const STACKS_PER_INVOCATION: u32 = 8;

/// The largest linear memory a slot can hold when the server sets no ceiling:
/// the whole 32-bit address space.
const DEFAULT_MAX_MEMORY: usize = 1 << 32;

/// Process-wide configuration of a pool of engines.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EngineSettings {
	/// How many invocations may run at once. This is the pooling allocator's
	/// slot count, so it bounds memory reservations as well as concurrency.
	pub max_concurrent_invocations: u32,
	/// The largest linear memory any one slot can hold, in bytes. `None` is
	/// the whole 32-bit address space.
	pub max_memory_bytes: Option<usize>,
}

impl Default for EngineSettings {
	fn default() -> Self {
		Self {
			max_concurrent_invocations: 64,
			max_memory_bytes: None,
		}
	}
}

impl EngineSettings {
	/// The slot count, never below one.
	pub fn slots(&self) -> u32 {
		self.max_concurrent_invocations.max(1)
	}

	/// Slots only nested invocations may take: a quarter of the slots, at
	/// least one, and none when there is a single slot, which a top-level
	/// invocation must be able to take.
	pub fn nested_reserve(&self) -> u32 {
		match self.slots() {
			1 => 0,
			slots => (slots / 4).max(1),
		}
	}

	/// The largest memory a slot holds.
	pub fn slot_memory(&self) -> usize {
		self.max_memory_bytes.map_or(DEFAULT_MAX_MEMORY, |m| m.min(DEFAULT_MAX_MEMORY))
	}
}

fn base_config(settings: &EngineSettings, pooled: bool) -> Config {
	let mut cfg = Config::new();
	cfg.parallel_compilation(true);
	// The component model's async ABI. `invoke` is lifted async so the guest can
	// hold several host calls in flight, and `transaction` methods are lowered
	// async so each one becomes a subtask the host resolves independently.
	// `concurrency_support` is what `LinkerInstance::func_wrap_concurrent`
	// checks; it defaults on with the `component-model-async` cargo feature, but
	// is set explicitly here so the requirement is visible at the call site.
	//
	// Cranelift is required in every profile: Winch rejects this feature with
	// "the wasm_cm_async feature is not supported on this compiler
	// configuration".
	cfg.wasm_component_model_async(true);
	cfg.concurrency_support(true);
	#[cfg(not(debug_assertions))]
	{
		cfg.cranelift_opt_level(OptLevel::Speed);
	}

	// A slot maps the module's initial memory copy-on-write rather than copying
	// it, so clean pages are shared by every instance of the module.
	cfg.memory_init_cow(true);
	if !pooled {
		return cfg;
	}

	let slots = settings.slots();
	let mut pool = PoolingAllocationConfig::new();
	pool.total_component_instances(slots)
		.max_core_instances_per_component(CORE_INSTANCES_PER_COMPONENT)
		.max_memories_per_component(MEMORIES_PER_COMPONENT)
		.max_tables_per_component(TABLES_PER_COMPONENT)
		.total_core_instances(slots.saturating_mul(CORE_INSTANCES_PER_COMPONENT))
		.total_memories(slots.saturating_mul(MEMORIES_PER_COMPONENT))
		.total_tables(slots.saturating_mul(TABLES_PER_COMPONENT))
		.total_stacks(slots.saturating_mul(STACKS_PER_INVOCATION))
		.max_memory_size(settings.slot_memory())
		// A returned slot keeps this much of its memory and table resident and
		// resets it with `memset`; the rest is returned to the image with
		// `madvise`. Small hot regions are cheaper to clear than to fault back.
		.linear_memory_keep_resident(64 << 10)
		.table_keep_resident(4 << 10);
	cfg.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));
	cfg
}

/// Make `store`, of the guarded engine, yield to the async executor on every
/// epoch tick, and trap with [`Trap::Interrupt`] once `epoch_counter` reaches
/// the deadline `deadline` reads from the store's data. A deadline of `None`
/// never traps.
///
/// The deadline is an absolute value of the shadow counter
/// ([`Engines::epoch_counter`]); [`deadline_after`] computes one.
pub fn yield_until_deadline<T: Send + 'static>(
	store: &mut Store<T>,
	epoch_counter: Arc<AtomicU64>,
	deadline: fn(&T) -> Option<u64>,
) {
	store.set_epoch_deadline(1);
	store.epoch_deadline_callback(move |ctx| match deadline(ctx.data()) {
		Some(at) if epoch_counter.load(Ordering::Acquire) >= at => Err(Trap::Interrupt.into()),
		_ => Ok(UpdateDeadline::Yield(1)),
	});
}

/// The deadline [`yield_until_deadline`] traps at, `timeout` from now, in
/// whole ticks and never less than one.
pub fn deadline_after(epoch_counter: &AtomicU64, timeout: Duration) -> u64 {
	let ticks = (timeout.as_millis() as u64 / EPOCH_TICK_MS).max(1);
	epoch_counter.load(Ordering::Acquire).saturating_add(ticks)
}

/// One pool of engines and the slots that bound its concurrent invocations.
pub struct Engines {
	settings: EngineSettings,
	fast: OnceLock<Engine>,
	guarded: OnceLock<Engine>,
	/// Held while an engine is created, so it is created once.
	creating: parking_lot::Mutex<()>,
	/// Shadow of the guarded engine's epoch, advanced by the ticker just
	/// before the engine's own. Used to compute deadlines that cannot wrap.
	epoch_counter: Arc<AtomicU64>,
	/// Every slot. Every invocation holds one of these.
	slots: Arc<Semaphore>,
	/// The slots top-level invocations may hold between them: all but
	/// [`EngineSettings::nested_reserve`]. A top-level invocation holds one of
	/// these beside its slot.
	top_level: Arc<Semaphore>,
}

/// One invocation's claim on a slot, returned when dropped.
#[derive(Debug)]
pub struct SlotPermit {
	_slot: OwnedSemaphorePermit,
	_top_level: Option<OwnedSemaphorePermit>,
}

impl std::fmt::Debug for Engines {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Engines")
			.field("settings", &self.settings)
			.field("free_slots", &self.slots.available_permits())
			.finish_non_exhaustive()
	}
}

/// Pools in use, by settings. Entries are weak, so a pool lives exactly as long
/// as some runner holds it.
static REGISTRY: parking_lot::Mutex<Vec<(EngineSettings, Weak<Engines>)>> =
	parking_lot::Mutex::new(Vec::new());

/// The pool for `settings`, shared with every other holder of the same
/// settings in this process.
pub fn engines(settings: EngineSettings) -> Arc<Engines> {
	let mut registry = REGISTRY.lock();
	registry.retain(|(_, weak)| weak.strong_count() > 0);
	if let Some(engines) =
		registry.iter().find(|(s, _)| *s == settings).and_then(|(_, weak)| weak.upgrade())
	{
		return engines;
	}
	let engines = Arc::new(Engines {
		settings,
		fast: OnceLock::new(),
		guarded: OnceLock::new(),
		creating: parking_lot::Mutex::new(()),
		epoch_counter: Arc::new(AtomicU64::new(0)),
		slots: Arc::new(Semaphore::new(settings.slots() as usize)),
		top_level: Arc::new(Semaphore::new(
			(settings.slots() - settings.nested_reserve()) as usize,
		)),
	});
	registry.push((settings, Arc::downgrade(&engines)));
	engines
}

/// How long an invocation may wait for a slot.
#[derive(Clone, Copy, Debug)]
pub enum SlotWait {
	/// Wait up to this long, then fail.
	Bounded(Duration),
	/// Fail at once if no slot is free.
	///
	/// For an invocation nested inside another: the outer one holds its slot
	/// while the inner one runs, so if every slot were held by an outer call
	/// the inner calls would wait for each other for ever. A nested invocation
	/// may take a reserved slot, which no top-level invocation holds.
	Never,
}

impl Engines {
	pub fn settings(&self) -> &EngineSettings {
		&self.settings
	}

	/// The engine of one flavour, created on first use.
	///
	/// Creating an engine reserves the pool's address space, which can fail
	/// when the host refuses the reservation. Concurrent first callers wait
	/// for one creation, so the space is reserved once. The guarded engine is
	/// installed only once its epoch ticker runs: without one, no timeout
	/// would ever fire.
	pub fn engine(&self, guarded: bool) -> SurrealismResult<Engine> {
		let cell = if guarded {
			&self.guarded
		} else {
			&self.fast
		};
		if let Some(engine) = cell.get() {
			return Ok(engine.clone());
		}
		let _creating = self.creating.lock();
		if let Some(engine) = cell.get() {
			return Ok(engine.clone());
		}
		let mut cfg = base_config(&self.settings, guarded);
		if guarded {
			cfg.epoch_interruption(true);
		}
		let engine = Engine::new(&cfg).map_err(SurrealismError::Compilation)?;
		if guarded {
			self.start_ticker(&engine)?;
		}
		Ok(cell.get_or_init(|| engine).clone())
	}

	/// Advance the guarded engine's epoch every [`EPOCH_TICK_MS`] until the
	/// engine is dropped.
	fn start_ticker(&self, engine: &Engine) -> SurrealismResult<()> {
		let weak = engine.weak();
		let counter = Arc::clone(&self.epoch_counter);
		std::thread::Builder::new()
			.name("surrealism-epoch-ticker".into())
			.spawn(move || {
				loop {
					std::thread::sleep(Duration::from_millis(EPOCH_TICK_MS));
					let Some(engine) = weak.upgrade() else {
						return;
					};
					counter.fetch_add(1, Ordering::Release);
					engine.increment_epoch();
				}
			})
			.map(|_| ())
			.map_err(|e| {
				SurrealismError::Other(anyhow::anyhow!(
					"Failed to start the Surrealism epoch ticker, so timeouts could not be \
					 enforced: {e}"
				))
			})
	}

	/// Shadow of the guarded engine's epoch counter. Always at least the
	/// engine's own epoch, because the ticker advances it first.
	pub fn epoch_counter(&self) -> &Arc<AtomicU64> {
		&self.epoch_counter
	}

	/// The total number of slots.
	pub fn slot_count(&self) -> usize {
		self.settings.slots() as usize
	}

	/// Take one slot for an invocation. The permit returns it on drop.
	///
	/// A top-level invocation ([`SlotWait::Bounded`]) first takes one of the
	/// slots top-level invocations may share, then a slot, both within its
	/// wait. A nested one ([`SlotWait::Never`]) takes any free slot, reserved
	/// or not, or fails at once.
	pub async fn acquire_slot(&self, wait: SlotWait) -> SurrealismResult<SlotPermit> {
		match wait {
			SlotWait::Never => {
				let slot = Arc::clone(&self.slots).try_acquire_owned().map_err(|_| {
					tracing::warn!(
						slots = self.slot_count(),
						reserved = self.settings.nested_reserve(),
						"Refused a nested Surrealism invocation: no invocation slot is free"
					);
					SurrealismError::NestedInvocationNoSlot {
						slots: self.slot_count(),
					}
				})?;
				Ok(SlotPermit {
					_slot: slot,
					_top_level: None,
				})
			}
			SlotWait::Bounded(wait) => {
				let acquire = async {
					let top_level = Arc::clone(&self.top_level).acquire_owned().await?;
					let slot = Arc::clone(&self.slots).acquire_owned().await?;
					Ok::<_, tokio::sync::AcquireError>(SlotPermit {
						_slot: slot,
						_top_level: Some(top_level),
					})
				};
				match tokio::time::timeout(wait, acquire).await {
					Ok(Ok(permit)) => Ok(permit),
					Ok(Err(_)) => Err(SurrealismError::Other(anyhow::anyhow!(
						"Surrealism invocation slots closed"
					))),
					Err(_) => Err(SurrealismError::NoInvocationSlot {
						waited: wait,
						slots: self.slot_count(),
					}),
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use wasmtime::*;
	use web_time::Instant;

	use super::{EngineSettings, SlotWait, deadline_after, engines, yield_until_deadline};

	/// Top-level invocations cannot take the reserved slots, so a nested
	/// invocation still finds one when queries hold every slot they may.
	#[tokio::test(flavor = "current_thread")]
	async fn top_level_load_leaves_slots_for_nested_calls() {
		let settings = EngineSettings {
			max_concurrent_invocations: 8,
			max_memory_bytes: Some(1 << 20),
		};
		assert_eq!(settings.nested_reserve(), 2);
		let engines = engines(settings);
		let wait = SlotWait::Bounded(Duration::from_millis(10));

		let mut held = Vec::new();
		for _ in 0..6 {
			held.push(engines.acquire_slot(wait).await.expect("a top-level slot"));
		}
		assert!(engines.acquire_slot(wait).await.is_err(), "a 7th top-level call must wait");
		held.push(engines.acquire_slot(SlotWait::Never).await.expect("a reserved slot"));
		held.push(engines.acquire_slot(SlotWait::Never).await.expect("a reserved slot"));
		assert!(engines.acquire_slot(SlotWait::Never).await.is_err(), "every slot is held");

		// A nested invocation may take any slot, reserved or not.
		drop(held);
		let mut nested = Vec::new();
		for _ in 0..8 {
			nested.push(engines.acquire_slot(SlotWait::Never).await.expect("any free slot"));
		}
	}

	#[test]
	fn a_single_slot_is_not_reserved() {
		let settings = EngineSettings {
			max_concurrent_invocations: 1,
			max_memory_bytes: None,
		};
		assert_eq!(settings.nested_reserve(), 0);
	}

	/// A guest spinning without host calls hands its thread back on every
	/// tick: on a single-threaded runtime a timer beside it still fires on
	/// time, and the guest traps at its own deadline.
	#[tokio::test(flavor = "current_thread")]
	async fn a_spinning_guest_yields_until_its_deadline() {
		let engines = engines(EngineSettings {
			max_concurrent_invocations: 1,
			max_memory_bytes: Some(1 << 20),
		});
		let engine = engines.engine(true).expect("engine");
		let module =
			Module::new(&engine, r#"(module (func (export "spin") (loop br 0)))"#).expect("module");
		let counter = std::sync::Arc::clone(engines.epoch_counter());
		let deadline = deadline_after(&counter, Duration::from_millis(500));
		let mut store = Store::new(&engine, Some(deadline));
		yield_until_deadline(&mut store, counter, |d: &Option<u64>| *d);
		let instance =
			Linker::new(&engine).instantiate_async(&mut store, &module).await.expect("instantiate");
		let spin = instance.get_typed_func::<(), ()>(&mut store, "spin").expect("spin");

		let start = Instant::now();
		let timer = async {
			tokio::time::sleep(Duration::from_millis(20)).await;
			start.elapsed()
		};
		let (spun, timer) = tokio::join!(spin.call_async(&mut store, ()), timer);

		let err = spun.expect_err("the spin must trap");
		assert_eq!(err.downcast_ref::<Trap>(), Some(&Trap::Interrupt), "{err:?}");
		assert!(timer < Duration::from_millis(250), "the timer waited {timer:?}");
		assert!(start.elapsed() >= Duration::from_millis(400), "trapped early");
	}
}
