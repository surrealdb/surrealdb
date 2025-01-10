//! System time based versionstamp.
//! This module provides a kind of Hybrid Logical Clock (HLC) based on system time.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
#[cfg(not(target_family = "wasm"))]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(target_family = "wasm")]
use wasmtimer::std::{SystemTime, UNIX_EPOCH};

use super::{u16_u64_to_versionstamp, u64_to_versionstamp, u64_u16_to_versionstamp, Versionstamp};

// A versionstamp oracle is a source of truth for the current versionstamp of the database.
// There are several kinds of versionstamp oracles, each provides a different versionstamp
// generation strategy, varying in the trade-off between the monotonicity of the versionstamps
// and the performance of the versionstamp generation.
#[allow(unused)]
#[non_exhaustive]
pub enum Oracle {
	// SysTimeCounter versionstamp oracle is a HLC based on system time in seconds as the physical time and the
	// in-memory counter that resets every second as the logical time.
	//
	// This is supposed to be only used for one-node installation and is not suitable for multi-node
	// installation of the database.
	//
	// There are two cases this oracle can be not monotonic.
	//
	// First, it can be not monotonic if the system time is not monotonic,
	// because this module does nothing to prevent the system clock from going backwards,
	// and there's no additional mechanism to detect and handle the clock going backwards.
	//
	// Second, it can be not monotonic if the database is restarted within a second,
	// because the in-memory counter resets to 0 on restart and the physical time uses the glanularity
	// of seconds.
	//
	// Do also note that the produced versionstamp's monotonicity needs to be guaranteed
	// by the caller of the oracle too.
	// For example, if it is going to be used as the commit versionstamp of a transaction,
	// the caller needs to ensure that the commit versionstamp is always increasing.
	// Otherwise, the transaction might be committed with a versionstamp that is smaller than
	// the versionstamp of a previous transaction.
	// This is because the versionstamp oracle does not know the context of the versionstamp
	// and cannot guarantee the monotonicity of the versionstamp by itself.
	// Implementation-wise, this can imply that the database needs to ensure that the only one
	// transaction is executing and committing at a time.
	SysTimeCounter(SysTimeCounter),
	// EpochCounter versionstamp oracle provides a vector clock that uses the "epoch" as the first logical time
	// and the in-memory counter for the second logical time.
	//
	// The epoch is supposed to be persisted in the underlying KVS and increased by one on each database restart.
	// The in-memory counter resets on each database restart and increased by one on each now() call.
	//
	// EpochCounter is designed to be used instead of the SysTimeCounter when the runtime environment
	// does not provide a monotonic system clock, and the database is running in a single-node mode.
	EpochCounter(EpochCounter),
}

impl Oracle {
	#[allow(unused)]
	pub fn systime_counter() -> Self {
		Oracle::SysTimeCounter(SysTimeCounter {
			state: Mutex::new((0, 0)),
			stale: (0, 0),
		})
	}

	#[allow(unused)]
	pub fn epoch_counter() -> Self {
		Oracle::EpochCounter(EpochCounter {
			epoch: 0,
			counter: AtomicU64::new(0),
		})
	}

	#[allow(unused)]
	pub fn now(&mut self) -> Versionstamp {
		match self {
			Oracle::SysTimeCounter(sys) => sys.now(),
			Oracle::EpochCounter(epoch) => epoch.now(),
		}
	}
}

#[non_exhaustive]
pub struct SysTimeCounter {
	// The first element is the saved physical time of the last versionstamp.
	// The second element is the in-memory counter that resets every second.
	state: Mutex<(u64, u16)>,

	stale: (u64, u16),
}

impl SysTimeCounter {
	pub fn now(&mut self) -> Versionstamp {
		// Increment the counter and get the current number as the logical time
		// only if the current physical time is the same as the last physical time.
		// Otherwise, reset the counter to 0 and get the current number as the logical time.
		// This is to ensure that the logical time is always increasing.
		let state = self.state.lock();
		if let Ok(mut state) = state {
			let (last_physical_time, counter) = *state;
			let current_physical_time = secs_since_unix_epoch();
			let current_logical_time = if last_physical_time == current_physical_time {
				counter
			} else {
				state.1 = 0;
				0
			};
			state.0 = current_physical_time;
			state.1 += 1;
			self.stale = (current_physical_time, current_logical_time);
			u64_u16_to_versionstamp(current_physical_time, current_logical_time)
		} else {
			u64_u16_to_versionstamp(self.stale.0, self.stale.1)
		}
	}
}

// EpochCounter versionstamp oracle providers a vector clock uses the "epoch" as the first time
// and the in-memory counter for the second logical time.
// The epoch is supposed to be persisted in the underlying KVS and increased by one on each database restart.
// The in-memory counter resets on each database restart and increased by one on each now() call.
// TODO: Refer to a paper that describes this concept and use the correct terminology.
#[non_exhaustive]
pub struct EpochCounter {
	// epoch is the first half of the versionstamp that persists
	// until the database restarts.
	// This needs to be persisted externally so that the epoch increases by one
	// on each database restart.
	epoch: u16,

	// state is the in-memory counter increases by one on each now() call.
	counter: AtomicU64,
}

impl EpochCounter {
	#[allow(unused)]
	pub fn now(&mut self) -> Versionstamp {
		let counter = self.counter.fetch_add(1, Ordering::SeqCst);
		u16_u64_to_versionstamp(self.epoch, counter)
	}
}

#[allow(unused)]
fn now() -> Versionstamp {
	let secs = secs_since_unix_epoch();
	u64_to_versionstamp(secs)
}

#[allow(unused)]
// Returns the number of seconds since the Unix Epoch (January 1st, 1970 at UTC).
fn secs_since_unix_epoch() -> u64 {
	match SystemTime::now().duration_since(UNIX_EPOCH) {
		Ok(duration) => duration.as_secs(),
		Err(error) => panic!("Clock may have gone backwards: {:?}", error.duration()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::vs::to_u128_be;

	#[test]
	fn systime_counter() {
		let mut o = Oracle::systime_counter();
		let a = to_u128_be(o.now());
		let b = to_u128_be(o.now());
		assert!(a < b, "a = {}, b = {}", a, b);
	}

	#[test]
	fn epoch_counter() {
		let mut o1 = Oracle::epoch_counter();
		let a = to_u128_be(o1.now());
		let b = to_u128_be(o1.now());
		assert!(a < b, "a = {}, b = {}", a, b);
		let mut o2 = Oracle::EpochCounter(EpochCounter {
			epoch: 1,
			counter: AtomicU64::new(0),
		});
		let c = to_u128_be(o2.now());
		assert!(b < c, "b = {}, c = {}", b, c);
	}
}
