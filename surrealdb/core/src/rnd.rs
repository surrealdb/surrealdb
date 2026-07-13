//! Optional deterministic RNG for reproducible data generation.
//!
//! By default this is inert: [`with_rng`] hands out the per-thread RNG with no
//! shared state and no locking, so the normal production path is unchanged.
//! Setting the `SURREAL_RAND_SEED` environment variable, or calling [`reseed`],
//! switches the data-generating `rand::*` functions and `|record:N|` id
//! generation onto a single seeded RNG, so the same statements produce the same
//! data on every run.
//!
//! TEST AND BENCHMARK USE ONLY. When active, every value drawn through
//! [`with_rng`] in the process comes from one shared, predictable stream — never
//! enable it on a shared or multi-tenant deployment, where it would make record
//! ids and `rand::*` values predictable across tenants. Security-sensitive
//! randomness (auth challenge codes, access secrets, JWT ids) is generated
//! through separate code paths that do not route through here and is unaffected.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};

use rand::SeedableRng;
use rand::rngs::StdRng;

/// Whether deterministic mode is active. Read on every [`with_rng`] call, so the
/// default (inactive) path can skip touching the shared RNG entirely.
static ACTIVE: AtomicBool = AtomicBool::new(false);

/// The shared seeded RNG. Only locked when [`ACTIVE`] is set. The seed comes from
/// [`surrealdb_cnf::RAND_SEED`] (read once from `SURREAL_RAND_SEED`); its absence
/// leaves [`ACTIVE`] unset and this RNG untouched unless [`reseed`] is called.
static SEEDED: LazyLock<Mutex<StdRng>> = LazyLock::new(|| match *surrealdb_cnf::RAND_SEED {
	Some(seed) => {
		ACTIVE.store(true, Ordering::Relaxed);
		Mutex::new(StdRng::seed_from_u64(seed))
	}
	None => Mutex::new(StdRng::seed_from_u64(0)),
});

/// Run `f` with the active RNG: the seeded RNG when deterministic mode is on,
/// otherwise the per-thread RNG. The inactive path takes no lock.
///
/// `#[inline]` lets the compiler inline the inactive branch into hot callers
/// (notably [`crate::val::RecordIdKey::rand`], used for every default record id)
/// and devirtualize the `&mut dyn RngCore` back to the concrete per-thread RNG,
/// so the default path keeps its monomorphized draw with no dynamic dispatch.
#[inline]
pub fn with_rng<T>(f: impl FnOnce(&mut dyn rand::RngCore) -> T) -> T {
	// Force the one-time seed read so `ACTIVE` reflects `SURREAL_RAND_SEED`.
	LazyLock::force(&SEEDED);
	if ACTIVE.load(Ordering::Relaxed) {
		// A poisoned lock only means a panic happened mid-draw; the RNG state is
		// still usable, so recover the guard rather than propagating the panic.
		return f(&mut *SEEDED.lock().unwrap_or_else(|e| e.into_inner()));
	}
	f(&mut rand::rng())
}

/// Reset the seeded RNG to `seed` and activate deterministic mode. The benchmark
/// harness calls this before each dataset build so generated data is identical
/// on every rebuild and independent of benchmark order. TEST AND BENCH ONLY.
pub fn reseed(seed: u64) {
	*SEEDED.lock().unwrap_or_else(|e| e.into_inner()) = StdRng::seed_from_u64(seed);
	ACTIVE.store(true, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
	use rand::Rng;

	use super::*;

	// `#[serial]` keeps this off the same thread as other serial tests; the
	// `Restore` guard below handles the rest. Activating deterministic mode flips
	// process-global state that every `rand::*` draw and record-id generation
	// reads, so this test must not run concurrently with code that draws random
	// data, and must hand that state back even if an assertion panics.
	#[test]
	#[serial_test::serial]
	fn reseed_makes_output_deterministic() {
		// Restore deterministic mode to its prior state on every exit — including
		// an assertion panic — so a failure here can't leave the shared test
		// process drawing every record id and `rand::*` value from the seeded RNG.
		struct Restore(bool);
		impl Drop for Restore {
			fn drop(&mut self) {
				ACTIVE.store(self.0, Ordering::Relaxed);
			}
		}
		let _restore = Restore(ACTIVE.load(Ordering::Relaxed));

		// Two equal reseeds must yield identical sequences across every kind of
		// draw the data generators use.
		fn draw() -> (Vec<u64>, Vec<f64>, String) {
			let ints = (0..16).map(|_| with_rng(|r| r.random_range(0u64..1_000_000))).collect();
			let floats = (0..16).map(|_| with_rng(|r| r.random::<f64>())).collect();
			let s: String =
				with_rng(|r| (0..64).map(|_| r.random_range(b'a'..=b'z') as char).collect());
			(ints, floats, s)
		}

		reseed(42);
		let first = draw();
		reseed(42);
		let second = draw();
		assert_eq!(first, second);

		// A different seed must produce a different sequence.
		reseed(43);
		let third = draw();
		assert_ne!(first, third);
	}
}
