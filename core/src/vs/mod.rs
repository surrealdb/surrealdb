//! vs is a module to handle Versionstamps.
//! This module is supplemental to the kvs::tx module and is not intended to be used directly
//! by applications.
//! This module might be migrated into the kvs or kvs::tx module in the future.

/// Versionstamp is a 10-byte array used to identify a specific version of a key.
/// The first 8 bytes are significant (the u64), and the remaining 2 bytes are not significant, but used for extra precision.
/// To convert to and from this module, see the conv module in this same directory.
///
pub type Versionstamp = [u8; 10];

pub(crate) mod conv;
pub(crate) mod oracle;

pub use self::conv::*;
pub use self::oracle::*;
use futures::StreamExt;
use std::collections::binary_heap::Iter;

/// Generate S-tuples of valid, sequenced versionstamps within range.
/// The limit is used, because these are combinatorics - without an upper bound, combinations aren't possible.
#[doc(hidden)]
pub fn generate_versionstamp_sequences(start: Versionstamp, limit: usize) -> VersionstampSequence {
	VersionstampSequence {
		next_state: Some(start),
		iterated: 0,
		limit,
	}
}

#[doc(hidden)]
pub struct VersionstampSequence {
	next_state: Option<Versionstamp>,
	iterated: usize,
	limit: usize,
}

#[doc(hidden)]
impl Iterator for VersionstampSequence {
	type Item = Versionstamp;

	fn next(&mut self) -> Option<Self::Item> {
		if self.next_state.is_none() {
			return None;
		}
		let returned_state = self.next_state.unwrap();
		// Now calculate next
		let mut next_state = self.next_state.unwrap();
		let index_to_increase =
			next_state.iter().enumerate().rev().skip(2).find(|(_, &x)| x < 255u8).take();
		if index_to_increase.is_none() {
			self.next_state = None;
			return Some(returned_state);
		}
		let (index_to_increase, _) = index_to_increase.unwrap();
		next_state[index_to_increase] += 1;
		for i in index_to_increase + 1..returned_state.len() - 2 {
			next_state[i] = 0;
		}
		self.iterated += 1;
		if self.iterated >= self.limit {
			self.next_state = None;
		} else {
			self.next_state = Some(next_state);
		}
		Some(returned_state)
	}
}

#[cfg(test)]
mod test {
	use crate::vs::Versionstamp;

	#[test]
	pub fn generate_one_vs() {
		let vs = super::generate_versionstamp_sequences([0; 10], 1).collect::<Vec<_>>();
		assert_eq!(vs.len(), 1, "Should be 1, but was {:?}", vs);
		assert_eq!(vs[0], [0; 10]);
	}

	#[test]
	pub fn generate_two_vs() {
		let limit = 2;
		let vs = super::generate_versionstamp_sequences([0, 0, 0, 0, 0, 0, 0, 1, 0, 0], limit)
			.flat_map(|vs| {
				let skip_because_first_is_equal = 1;
				let adjusted_limit = limit + skip_because_first_is_equal;
				super::generate_versionstamp_sequences(vs, adjusted_limit)
					.skip(skip_because_first_is_equal)
					.map(move |vs2| (vs, vs2))
			});
		let versionstamps = vs.collect::<Vec<(Versionstamp, Versionstamp)>>();

		assert_eq!(
			versionstamps.len(),
			4,
			"We expect the combinations to be 2x2 matrix, but was {:?}",
			versionstamps
		);

		let acceptable_values = [65536, 131072, 196608, 262144, 327680, 393216];
		for (first, second) in versionstamps {
			assert!(first < second, "First: {:?}, Second: {:?}", first, second);
		}
	}
}
