use std::cmp::min;

pub trait StringDistance {
	/// Retrieve the levenshtein distance of this &str compared to another &str
	fn levenshtein(&self, other: &str) -> i64;

	/// Retrieve the hamming distance of this &str compared to another &str
	/// Returns an Err if the lengths of this &str and the other &str are not equal
	fn hamming(&self, other: &str) -> Result<i64, ()>;
}

impl StringDistance for str {
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

	fn hamming(&self, other: &str) -> Result<i64, ()> {
		if self.chars().count() != other.chars().count() {
			return Err(());
		}

		Ok(self.chars().zip(other.chars()).map(|(a, b)| (a != b) as i64).sum::<i64>())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// Levenshtein & Hamming distance test cases from the strsim crate

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

	fn assert_hamming_dist(dist: i64, str1: &str, str2: &str) {
		assert_eq!(Ok(dist), str1.hamming(str2));
	}

	#[test]
	fn hamming_empty() {
		assert_hamming_dist(0, "", "")
	}

	#[test]
	fn hamming_same() {
		assert_hamming_dist(0, "hamming", "hamming")
	}

	#[test]
	fn hamming_diff() {
		assert_hamming_dist(3, "hamming", "hammers")
	}

	#[test]
	fn hamming_diff_multibyte() {
		assert_hamming_dist(2, "hamming", "h香mmüng");
	}

	#[test]
	fn hamming_unequal_length() {
		assert_eq!(Err(()), "ham".hamming("hamming"));
	}

	#[test]
	fn hamming_names() {
		assert_hamming_dist(14, "Friedrich Nietzs", "Jean-Paul Sartre")
	}
}
