//! Engine features a test can depend on, and the run's declaration of which it is without.
//!
//! A test names the features it cannot pass without in `[test] requires`. A `.graphql` file
//! requires `graphql` without saying so, because its dialect is the feature. A run that names a
//! feature in `--without` reports every test that requires it as skipped, with the feature as the
//! reason. Nothing else is skipped on its account: the test is not failed, filtered out of the
//! counts, or run to see what happens.
//!
//! The set is closed. An unknown name in a test or on the command line is an error rather than a
//! tag that quietly matches nothing, which is the failure a typo would otherwise be.

use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use clap::ValueEnum;
use clap::builder::PossibleValue;
use serde::{Deserialize, Serialize};

use crate::tests::case::Dialect;
use crate::tests::run::CaseImports;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Feature {
	/// Embedded JavaScript functions (`function() { .. }`), and the Surrealism modules that
	/// sit beside them.
	Scripting,
	/// The GraphQL endpoint and the `.graphql` dialect.
	GraphQl,
	/// `DEFINE INDEX .. DISKANN`.
	DiskAnn,
	/// `CHANGEFEED` on a table or database and `SHOW CHANGES`.
	ChangeFeed,
	/// The regex value type: a `/../` literal, `<regex>` casts and the `~` operators' right-hand
	/// side.
	Regex,
}

impl Feature {
	pub const ALL: [Feature; 5] = [
		Feature::Scripting,
		Feature::GraphQl,
		Feature::DiskAnn,
		Feature::ChangeFeed,
		Feature::Regex,
	];

	pub fn name(self) -> &'static str {
		match self {
			Feature::Scripting => "scripting",
			Feature::GraphQl => "graphql",
			Feature::DiskAnn => "diskann",
			Feature::ChangeFeed => "changefeed",
			Feature::Regex => "regex",
		}
	}

	fn about(self) -> &'static str {
		match self {
			Feature::Scripting => "embedded JavaScript functions and Surrealism modules",
			Feature::GraphQl => "the GraphQL endpoint and `.graphql` tests",
			Feature::DiskAnn => "DISKANN indexes",
			Feature::ChangeFeed => "change feeds and SHOW CHANGES",
			Feature::Regex => "the regex value type",
		}
	}
}

impl Display for Feature {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.write_str(self.name())
	}
}

impl FromStr for Feature {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Feature::ALL.into_iter().find(|f| f.name() == s).ok_or_else(|| {
			let known = Feature::ALL.map(Feature::name).join(", ");
			format!("unknown feature `{s}`, expected one of: {known}")
		})
	}
}

impl ValueEnum for Feature {
	fn value_variants<'a>() -> &'a [Self] {
		&Feature::ALL
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		Some(PossibleValue::new(self.name()).help(self.about()))
	}
}

impl<'de> Deserialize<'de> for Feature {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let s = String::deserialize(deserializer)?;
		s.parse().map_err(<D::Error as serde::de::Error>::custom)
	}
}

impl Serialize for Feature {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_str(self.name())
	}
}

/// Every feature the test, or a file it imports, depends on.
///
/// Imports count because they run as the test: a script that creates a function body the engine
/// cannot run takes the importing test down with it.
pub fn required(case: &CaseImports) -> BTreeSet<Feature> {
	let mut features = BTreeSet::new();
	for test in std::iter::once(&case.test).chain(case.imports.iter()) {
		features.extend(test.config.parsed.test.requires.iter().copied());
		if test.dialect == Dialect::GraphQl {
			features.insert(Feature::GraphQl);
		}
	}
	features
}

/// The reason a run without `without` cannot run the test, or `None` when it can.
///
/// The reason names every missing feature, so a test needing two is counted once in the skip
/// breakdown and the reader sees both.
pub fn unavailable(case: &CaseImports, without: &BTreeSet<Feature>) -> Option<String> {
	if without.is_empty() {
		return None;
	}
	let missing: Vec<&str> =
		required(case).into_iter().filter(|f| without.contains(f)).map(Feature::name).collect();
	if missing.is_empty() {
		return None;
	}
	Some(format!("requires {}, which this run is without", missing.join(" and ")))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn every_feature_round_trips_through_its_name() {
		for feature in Feature::ALL {
			assert_eq!(feature.name().parse::<Feature>(), Ok(feature));
		}
	}

	#[test]
	fn an_unknown_name_is_refused_with_the_known_ones() {
		let err = "scriptng".parse::<Feature>().unwrap_err();
		assert!(err.contains("scriptng"), "{err}");
		for feature in Feature::ALL {
			assert!(err.contains(feature.name()), "{err}");
		}
	}
}
