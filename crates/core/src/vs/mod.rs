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

pub use self::conv::*;

/// Generate S-tuples of valid, sequenced versionstamps within range.
/// The limit is used, because these are combinatorics - without an upper bound, combinations aren't possible.
pub fn generate_versionstamp_sequences(start: Versionstamp) -> VersionstampSequence {
	VersionstampSequence {
		next_state: Some(start),
	}
}

#[non_exhaustive]
pub struct VersionstampSequence {
	next_state: Option<Versionstamp>,
}

impl Iterator for VersionstampSequence {
	type Item = Versionstamp;

	fn next(&mut self) -> Option<Self::Item> {
		self.next_state?;
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
		for next_state_byte in
			next_state.iter_mut().take(returned_state.len() - 2).skip(index_to_increase + 1)
		{
			*next_state_byte = 0;
		}
		self.next_state = Some(next_state);
		Some(returned_state)
	}
}

#[cfg(test)]
mod test {
	use crate::vs::{to_u128_be, Versionstamp};

	#[test]
	pub fn generate_one_vs() {
		let vs = super::generate_versionstamp_sequences([0; 10]).take(1).collect::<Vec<_>>();
		assert_eq!(vs.len(), 1, "Should be 1, but was {:?}", vs);
		assert_eq!(vs[0], [0; 10]);
	}

	#[test]
	pub fn generate_two_vs_in_sequence() {
		let vs =
			super::generate_versionstamp_sequences([0, 0, 0, 0, 0, 0, 0, 1, 0, 0]).flat_map(|vs| {
				let skip_because_first_is_equal = 1;
				super::generate_versionstamp_sequences(vs)
					.skip(skip_because_first_is_equal)
					.map(move |vs2| (vs, vs2))
			});
		let versionstamps = vs.take(4).collect::<Vec<(Versionstamp, Versionstamp)>>();

		assert_eq!(
			versionstamps.len(),
			4,
			"We expect the combinations to be 2x2 matrix, but was {:?}",
			versionstamps
		);

		let acceptable_values = [65536u128, 131072, 196608, 262144, 327680, 393216];
		for (first, second) in versionstamps {
			assert!(first < second, "First: {:?}, Second: {:?}", first, second);
			let first = to_u128_be(first);
			let second = to_u128_be(second);
			assert!(acceptable_values.contains(&first));
			assert!(acceptable_values.contains(&second));
		}
	}

	#[test]
	pub fn iteration_stops_past_end() {
		let mut iter = super::generate_versionstamp_sequences([255; 10]);
		assert!(iter.next().is_some());
		assert!(iter.next().is_none());
	}
}
