use std::cmp::min;

pub trait Levenshtein {
	/// Retrieve the levenshtein distance of this &str compared to another &str
	fn levenshtein(&self, other: &str) -> i64;
}

impl Levenshtein for str {
	/// Retrieve the levenshtein distance of this &str compared to another &str
	///
	/// The implementation is loosely based on the implementation found in the
	/// [strsim](https://crates.io/crates/strsim) crate (MIT).
	fn levenshtein(&self, other: &str) -> i64 {
		let self_length = self.chars().count() as i64;
		let other_length = other.chars().count() as i64;

		if self_length == 0 {
			return other_length;
		}
		if other_length == 0 {
			return self_length;
		}

		let mut distance: i64 = other_length;
		let mut cost_cache: Vec<i64> = (1..=other_length).collect();

		for (i, self_char) in self.chars().enumerate() {
			let mut cost: i64 = i as i64;
			distance = cost + 1;
			for (j, other_char) in other.chars().enumerate() {
				let substitution_cost = cost + i64::from(self_char != other_char);
				cost = cost_cache[j];

				let deletion_cost = cost + 1;
				let insertion_cost = distance + 1;

				distance = min(insertion_cost, min(substitution_cost, deletion_cost));

				cost_cache[j] = distance;
			}
		}

		return distance;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// Levenshtein distance test cases from strsim, too.

	#[test]
	fn levenshtein_empty() {
		assert_eq!(0, "".levenshtein(""));
	}

	#[test]
	fn levenshtein_same() {
		assert_eq!(0, "levenshtein".levenshtein("levenshtein"));
	}

	#[test]
	fn levenshtein_diff_short() {
		assert_eq!(3, "kitten".levenshtein("sitting"));
	}

	#[test]
	fn levenshtein_diff_with_space() {
		assert_eq!(5, "hello, world".levenshtein("bye, world"));
	}

	#[test]
	fn levenshtein_diff_multibyte() {
		assert_eq!(3, "öঙ香".levenshtein("abc"));
		assert_eq!(3, "abc".levenshtein("öঙ香"));
	}

	#[test]
	fn levenshtein_diff_longer() {
		let a = "The quick brown fox jumped over the angry dog.";
		let b = "Lorem ipsum dolor sit amet, dicta latine an eam.";
		assert_eq!(37, a.levenshtein(b));
	}

	#[test]
	fn levenshtein_first_empty() {
		assert_eq!(7, "".levenshtein("sitting"));
	}

	#[test]
	fn levenshtein_second_empty() {
		assert_eq!(6, "kitten".levenshtein(""));
	}
}
