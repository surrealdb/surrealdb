//! Shared WASM engines with optional epoch-based timeout enforcement.
//!
//! Two engines are maintained:
//!
//! - **Guarded**: `epoch_interruption(true)`. Compiled WASM includes epoch checks at every loop
//!   back-edge and function call. A background ticker thread increments the epoch so that
//!   `Store::set_epoch_deadline` can enforce wall-clock timeouts. Adds ~10% overhead on typical
//!   code, up to ~2x on tight numerical loops.
//!
//! - **Fast**: No epoch interruption. WASM runs at full native speed. Timeouts cannot be enforced —
//!   the module must be trusted to complete within a reasonable time.
//!
//! Modules opt in via `strict_timeout` in `surrealism.toml` (default `true`
//! → guarded engine). Compute-heavy trusted modules can set it to `false`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use wasmtime::*;

/// Interval between epoch ticks (guarded engine only). Shorter = finer
/// timeout granularity, more thread wakeups.
pub const EPOCH_TICK_MS: u64 = 10;

static SHARED: OnceLock<SharedEngines> = OnceLock::new();

/// Holds both engine variants and the epoch ticker for the guarded engine.
///
/// The ticker thread runs for the process lifetime; we intentionally do not stop it on shutdown
/// (the static [`OnceLock`] never drops). Epoch advancement must continue until exit so in-flight
/// guarded stores keep meaningful deadlines.
struct SharedEngines {
	fast: Engine,
	guarded: Engine,
	epoch_counter: Arc<AtomicU64>,
}

fn base_config() -> Config {
	let mut cfg = Config::new();
	cfg.parallel_compilation(true);
	#[cfg(debug_assertions)]
	{
		cfg.strategy(Strategy::Winch);
	}
	#[cfg(not(debug_assertions))]
	{
		cfg.cranelift_opt_level(OptLevel::Speed);
	}
	cfg
}

impl SharedEngines {
	fn new() -> Self {
		let fast = Engine::new(&base_config()).expect("failed to create fast wasmtime Engine");

		let mut guarded_cfg = base_config();
		guarded_cfg.epoch_interruption(true);
		let guarded = Engine::new(&guarded_cfg).expect("failed to create guarded wasmtime Engine");

		let epoch_counter = Arc::new(AtomicU64::new(0));

		let counter = epoch_counter.clone();
		let engine_clone = guarded.clone();
		std::thread::Builder::new()
			.name("surrealism-epoch-ticker".into())
			.spawn(move || {
				loop {
					std::thread::sleep(Duration::from_millis(EPOCH_TICK_MS));
					counter.fetch_add(1, Ordering::Release);
					engine_clone.increment_epoch();
				}
			})
			.expect("failed to spawn epoch ticker thread");

		Self {
			fast,
			guarded,
			epoch_counter,
		}
	}
}

/// Lightweight handle to a shared engine. The `guarded` flag records which
/// engine variant this handle refers to.
pub struct EngineHandle {
	engine: Engine,
	epoch_counter: Arc<AtomicU64>,
	guarded: bool,
}

impl EngineHandle {
	pub fn engine(&self) -> &Engine {
		&self.engine
	}

	/// Shadow of the guarded engine's epoch counter. Used to compute safe
	/// deltas for `Store::set_epoch_deadline`. Returns a zero-valued counter
	/// for fast engine handles (deadline calls are no-ops anyway).
	pub fn epoch_counter(&self) -> &Arc<AtomicU64> {
		&self.epoch_counter
	}

	/// Whether this handle uses the guarded (epoch-enabled) engine.
	pub fn is_guarded(&self) -> bool {
		self.guarded
	}
}

/// Obtain a handle to the shared engine.
///
/// - `guarded = true`: epoch-enabled engine, timeout enforcement, higher overhead.
/// - `guarded = false`: fast engine, no timeout enforcement, zero overhead.
pub fn shared_engine(guarded: bool) -> EngineHandle {
	let shared = SHARED.get_or_init(SharedEngines::new);
	let engine = if guarded {
		shared.guarded.clone()
	} else {
		shared.fast.clone()
	};
	EngineHandle {
		engine,
		epoch_counter: shared.epoch_counter.clone(),
		guarded,
	}
}
