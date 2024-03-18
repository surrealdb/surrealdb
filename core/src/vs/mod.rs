//! vs is a module to handle Versionstamps.
//! This module is supplemental to the kvs::tx module and is not intended to be used directly
//! by applications.
//! This module might be migrated into the kvs or kvs::tx module in the future.

/// Versionstamp is a 10-byte array used to identify a specific version of a key.
/// The first 8 bytes are significant (the u64), and the remaining 2 bytes are not significant, but used for extra precision.
/// To convert to and from this module, see the conv module in this same directory.
///
/// You're going to want these
/// 65536
/// 131072
/// 196608
/// 262144
/// 327680
/// 393216
pub type Versionstamp = [u8; 10];

pub(crate) mod conv;
pub(crate) mod oracle;

pub use self::conv::*;
pub use self::oracle::*;
use futures::StreamExt;
use std::collections::binary_heap::Iter;

/// Generate S-tuples of valid, sequenced versionstamps within range.
/// The limit is used, because these are combinatorics - without an upper bound, combinations aren't possible.
#[cfg(test)]
#[doc(hidden)]
pub fn generate_versionstamp_sequences<const S: usize>(limit: usize) -> Iter<[u64; S]> {}

#[cfg(test)]
#[doc(hidden)]
pub struct VersionstampSequence<const S: usize> {
	next_state: Option<[u64; S]>,
	limit: u64,
}

impl<const S: usize> Iterator for VersionstampSequence<S> {
	type Item = [u64; S];

	fn next(&mut self) -> Option<Self::Item> {
		if self.next_state.is_none() {
			return None;
		}
		let current_next = self.next_state.unwrap();
		// Now calculate next
		let next_val = self
			.next_state
			// Transform the array into arrays of ranges, without combinatorics
			.map(|arr| arr.iter().flat_map(|i| (*i..self.limit).iter()))
			// Filter everything that isn't in range
			.filter(|arr| arr.iter().all(|i| *i < self.limit))
			// Find the first valid option
			.next();
		self.next_state = next_val;
		Some(current_next)
	}
}

trait Nextable {
	type Item;
	fn next(&self) -> Option<Self::Item>;
}

impl Nextable for &[u64] {
	type Item = Into<[u64]>;

	fn next(&self) -> Option<Self::Item> {
		todo!()
	}
}
