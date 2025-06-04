use crate::expr::{Id, Thing, escape::EscapeIdent};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Mock";

/// Mock is used to generate mock records, typically for testing purposes.
///
/// It can be used to generate a fixed number of records or a range of records.
///
/// Example:
/// ```sql
/// // Generate 5 random records in the `test` table.
/// CREATE |test:5|;
/// // Generate a range of records with IDs 1 to 5 in the `test` table.
/// CREATE |test:1..5|;
/// ```
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Mock")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Mock {
	Count(String, u64),
	Range(String, u64, u64),
	// Add new variants here
}

impl IntoIterator for Mock {
	type Item = Thing;
	type IntoIter = MockIterator;
	fn into_iter(self) -> Self::IntoIter {
		match self {
			Mock::Count(tb, c) => MockIterator {
				table_name: tb,
				current: 1,
				end: c,
				random: true,
			},
			Mock::Range(tb, b, e) => MockIterator {
				table_name: tb,
				current: b,
				end: e,
				random: false,
			},
		}
	}
}

impl fmt::Display for Mock {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Mock::Count(tb, c) => {
				write!(f, "|{}:{}|", EscapeIdent(tb), c)
			}
			Mock::Range(tb, b, e) => {
				write!(f, "|{}:{}..{}|", EscapeIdent(tb), b, e)
			}
		}
	}
}

/// Iterator for generating mock data.
#[non_exhaustive]
pub struct MockIterator {
	/// The name of the table to generate data for.
	table_name: String,
	/// The current ID being generated.
	current: u64,
	/// The end ID for the range of IDs to generate.
	end: u64,
	/// Whether to generate random IDs or sequential IDs.
	/// If true, IDs will be generated randomly.
	/// If false, IDs will be generated sequentially from `current` to `end`.
	random: bool,
}

impl Iterator for MockIterator {
	type Item = Thing;
	fn next(&mut self) -> Option<Thing> {
		// Range is inclusive, so we need to check if current is greater than end.
		if self.current > self.end {
			return None;
		}

		let id = if self.random {
			Id::rand()
		} else {
			Id::from(self.current)
		};

		self.current += 1;

		Some(Thing {
			tb: self.table_name.clone(),
			id,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::Thing;
	use rstest::rstest;

	#[test]
	fn test_mock_count() {
		let mock = Mock::Count("test".to_string(), 5);

		let results: Vec<Thing> = mock.into_iter().collect();
		assert_eq!(results.len(), 5);
	}

	#[rstest]
	#[case(1, 5, vec![
		Thing::new("test", 1),
		Thing::new("test", 2),
		Thing::new("test", 3),
		Thing::new("test", 4),
		Thing::new("test", 5),
	])]
	#[case(0, 5, vec![
		Thing::new("test", 0),
		Thing::new("test", 1),
		Thing::new("test", 2),
		Thing::new("test", 3),
		Thing::new("test", 4),
		Thing::new("test", 5),
	])]
	#[case(1, 1, vec![
		Thing::new("test", 1),
	])]
	fn test_mock_range(#[case] start: u64, #[case] end: u64, #[case] expected: Vec<Thing>) {
		let mock = Mock::Range("test".to_string(), start, end);

		let results: Vec<Thing> = mock.into_iter().collect();
		assert_eq!(results, expected);
	}
}
