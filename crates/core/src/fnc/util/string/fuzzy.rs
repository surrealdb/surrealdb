use fuzzy_matcher::skim::SkimMatcherV2;
use fuzzy_matcher::FuzzyMatcher;
use std::sync::LazyLock;

static MATCHER: LazyLock<SkimMatcherV2> = LazyLock::new(|| SkimMatcherV2::default().ignore_case());

pub trait Fuzzy {
	/// Retrieve the fuzzy similarity score of this &str compared to another &str
	fn fuzzy_match(&self, other: &str) -> bool;
	/// Check if this &str matches another &str using a fuzzy algorithm
	fn fuzzy_score(&self, other: &str) -> i64;
}

impl Fuzzy for str {
	/// Retrieve the fuzzy similarity score of this &str compared to another &str
	fn fuzzy_match(&self, other: &str) -> bool {
		MATCHER.fuzzy_match(self, other).is_some()
	}
	/// Check if this &str matches another &str using a fuzzy algorithm
	fn fuzzy_score(&self, other: &str) -> i64 {
		MATCHER.fuzzy_match(self, other).unwrap_or(0)
	}
}
