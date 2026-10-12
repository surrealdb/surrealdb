use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use clap::builder::{EnumValueParser, PossibleValue};
use clap::{ArgMatches, Command, ValueEnum, arg, command, value_parser};
use rust_decimal::serde::str;
use semver::Version;
use surrealdb_rpc::capabilities::NewPlannerStrategy;
use serde::{Deserialize, Serialize};
#[cfg(feature = "bench")]
use surrealdb_types::SurrealValue;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ResultsMode {
	Default,
	Accept,
	Overwrite,
}

impl ValueEnum for ResultsMode {
	fn value_variants<'a>() -> &'a [Self] {
		&[ResultsMode::Default, ResultsMode::Accept, ResultsMode::Overwrite]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		match self {
			ResultsMode::Default => {
				Some(PossibleValue::new("default").help("Do not change any tests"))
			}
			ResultsMode::Accept => Some(PossibleValue::new("accept").help(
				"Write the results of tests which do not have results specified as the expected results",
			)),
			ResultsMode::Overwrite => Some(PossibleValue::new("overwrite").help(
				"Overwrite the results of tests which do not have results and those that failed",
			)),
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Backend {
	Memory,
	RocksDb,
	SurrealKv,
	TikV,
}

impl Backend {
	/// Whether the provisioner can open this backend with versioning enabled, and
	/// so whether a test declaring `env.versioned` can run against it. The others
	/// are opened without it and reject a versioned statement rather than
	/// answering it.
	///
	/// Matched exhaustively on purpose: a new backend has to state which side it
	/// is on, because the wrong default silently either skips coverage or feeds
	/// tests to a store that cannot serve them.
	pub fn supports_versioning(self) -> bool {
		match self {
			Backend::RocksDb | Backend::SurrealKv => true,
			Backend::Memory | Backend::TikV => false,
		}
	}
}

#[cfg(feature = "bench")]
impl SurrealValue for Backend {
	fn kind_of() -> surrealdb_types::Kind {
		surrealdb_types::Kind::String
	}

	fn into_value(self) -> surrealdb_types::Value {
		match self {
			Backend::Memory => "Memory".into_value(),
			Backend::RocksDb => "RocksDb".into_value(),
			Backend::SurrealKv => "SurrealKv".into_value(),
			Backend::TikV => "TikV".into_value(),
		}
	}

	fn from_value(value: surrealdb_types::Value) -> Result<Self, surrealdb_types::Error>
	where
		Self: Sized,
	{
		let str = String::from_value(value)?;
		match str.as_str() {
			"Memory" => Ok(Backend::Memory),
			"RocksDb" => Ok(Backend::RocksDb),
			"SurrealKv" => Ok(Backend::SurrealKv),
			"TikV" => Ok(Backend::TikV),
			_ => {
				Err(surrealdb_types::Error::serialization("Invalid backend type".to_string(), None))
			}
		}
	}
}

impl ValueEnum for Backend {
	fn value_variants<'a>() -> &'a [Self] {
		&[Backend::Memory, Backend::RocksDb, Backend::SurrealKv, Backend::TikV]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		match self {
			Backend::Memory => Some(PossibleValue::new("memory").alias("mem")),
			Backend::RocksDb => Some(PossibleValue::new("rocksdb")),
			Backend::SurrealKv => Some(PossibleValue::new("surrealkv")),
			Backend::TikV => Some(PossibleValue::new("tikv")),
		}
	}
}

impl Display for Backend {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::Memory => f.write_str("mem"),
			Self::RocksDb => f.write_str("rocksdb"),
			Self::SurrealKv => f.write_str("surrealkv"),
			Self::TikV => f.write_str("tikv"),
		}
	}
}

impl<'de> Deserialize<'de> for Backend {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let s = String::deserialize(deserializer)?;
		match s.as_str() {
			"mem" => Ok(Backend::Memory),
			"rocksdb" => Ok(Backend::RocksDb),
			"surrealkv" => Ok(Backend::SurrealKv),
			"tikv" => Ok(Backend::TikV),
			_ => Err(<D::Error as serde::de::Error>::custom(format_args!("Invalid backend kind"))),
		}
	}
}

impl Serialize for Backend {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		match self {
			Backend::Memory => serializer.serialize_str("mem"),
			Backend::RocksDb => serializer.serialize_str("rocksdb"),
			Backend::SurrealKv => serializer.serialize_str("surrealkv"),
			Backend::TikV => serializer.serialize_str("tikv"),
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum UpgradeBackend {
	RocksDb,
	SurrealKv,
}

impl ValueEnum for UpgradeBackend {
	fn value_variants<'a>() -> &'a [Self] {
		&[UpgradeBackend::RocksDb, UpgradeBackend::SurrealKv]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		match self {
			UpgradeBackend::RocksDb => Some(PossibleValue::new("rocksdb")),
			UpgradeBackend::SurrealKv => Some(PossibleValue::new("surrealkv")),
		}
	}
}

#[derive(Clone, Eq, PartialEq, Debug, PartialOrd, Ord, Hash)]
pub enum DsVersion {
	Version(Version),
	Path(String),
}

impl fmt::Display for DsVersion {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			DsVersion::Version(version) => version.fmt(f),
			DsVersion::Path(p) => p.fmt(f),
		}
	}
}

impl DsVersion {
	fn from_str(s: &str) -> Result<Self, semver::Error> {
		if let Ok(x) = Version::parse(s) {
			return Ok(DsVersion::Version(x));
		}
		Ok(DsVersion::Path(s.to_string()))
	}
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ColorMode {
	Always,
	Never,
	Auto,
}

impl ValueEnum for ColorMode {
	fn value_variants<'a>() -> &'a [Self] {
		&[ColorMode::Always, ColorMode::Never, ColorMode::Auto]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		match self {
			ColorMode::Always => Some(PossibleValue::new("always")),
			ColorMode::Never => Some(PossibleValue::new("never")),
			ColorMode::Auto => Some(PossibleValue::new("auto")),
		}
	}
}

/// The planner strategy a remote endpoint was started with.
///
/// Spelled the way the server's own `--planner-strategy` flag spells it, so the
/// value is copied across rather than translated. The strategy is server-startup
/// configuration: a run over the SDK cannot select one per statement, so the
/// runner has to be told which one the server holds.
fn planner_strategy(value: &str) -> Result<NewPlannerStrategy, String> {
	NewPlannerStrategy::from_str(value)
}

pub fn parse() -> ArgMatches {
	let cmd = command!()
		.arg(arg!(--color <COLOR> "Set if the output should be colored").value_parser(EnumValueParser::<ColorMode>::new()).default_value("auto"))
		.subcommand(
			Command::new("test").alias("run")
			.about("Run surrealdb tests")
			.arg(arg!([filter] "Filter the tests by their path"))
			.arg(arg!(--path <PATH> "The path to tests directory").default_value("./tests"))
			.arg(
				arg!(-j --jobs <JOBS> "The number of test running in parallel, defaults to available parallism")
				.value_parser(value_parser!(u32).range(1..))
			).arg(
			arg!(--results <RESULT_MODE> "How to handle results of tests").value_parser(EnumValueParser::<ResultsMode>::new()).default_value("default").alias("failure")
			)
			.arg(
				arg!(--backend <BACKEND> "Specify the storage backend to use for the tests")
				.value_parser(EnumValueParser::<Backend>::new()).default_value("mem")
			)
			.arg(
				arg!(--"no-wip" "Skips tests marked work-in-progress")
			)
			.arg(
				arg!(--"no-results" "Skips tests that have defined results, usefull when adding new tests.")
			)
			.arg(
				arg!(--endpoint <URL> "Run the suite against a remote SurrealDB instance over ws:// or http:// (requires building with `--features remote`) instead of an embedded datastore")
			)
			.arg(
				arg!(--username <USER> "Root username to sign in to the --endpoint server").default_value("root").requires("endpoint")
			)
			.arg(
				arg!(--password <PASSWORD> "Root password to sign in to the --endpoint server").default_value("root").requires("endpoint")
			)
			.arg(
				arg!(--"planner-strategy" <STRATEGY> "The planner strategy the --endpoint server was started with, spelled as its own `--planner-strategy` flag spells it: 'best-effort' (the server's default), 'compute-only' or 'all-read-only'")
					.default_value("best-effort")
					.requires("endpoint")
					.value_parser(planner_strategy)
			),
	)
		.subcommand(
			Command::new("upgrade")
			.about("Run surrealdb upgrade tests")
			.arg(arg!([filter] "Filter the tests by their path"))
			.arg(arg!(--path <PATH> "The path to the tests directory").default_value("./tests"))
			.arg(
				arg!(-j --jobs <JOBS> "The number of test running in parallel, defaults to available parallism")
				.value_parser(value_parser!(u32).range(1..))
			)
			.arg(
				arg!(--results <RESULT_MODE> "How to handle results of tests").value_parser(EnumValueParser::<ResultsMode>::new()).default_value("default")
			)
			.arg(
				arg!(--backend <BACKEND> "Specify the storage backend to use for the upgrade test")
				.value_parser(EnumValueParser::<UpgradeBackend>::new()).default_value("surrealkv")
			)
			.arg(
				arg!(-f --from <VERSIONS> "The version to upgrade from. This can be either a version number or a path to the surrealdb codebase.").value_delimiter(',').value_parser(DsVersion::from_str)
				.required_unless_present("chain").conflicts_with("chain")
			)
			.arg(
				arg!(-t --to <VERSIONS> "The version to upgrade to. This can be either a version number or a path to the surrealdb codebase.").value_delimiter(',').value_parser(DsVersion::from_str)
				.required_unless_present("chain").conflicts_with("chain")
			)
			.arg(
				arg!(--chain <VERSIONS> "Walk one datastore through every listed version in order, instead of testing an isolated pair. Selects tests declaring `[test.chain]`.")
				.value_delimiter(',').value_parser(DsVersion::from_str)
			)
			.arg(
				arg!(--"allow-download" "Skip the confirmation for downloading binaries from github")
			)
			.arg(
				arg!(--"keep-files" "Don't cleanup the files generated by the tests")
			)
			.arg(
				arg!(--"no-wip" "Skips tests marked work-in-progress")
			)
			.arg(
				arg!(--"no-results" "Skips tests that have defined results, usefull when adding new tests.")
			)
			)
			.subcommand(
				Command::new("list")
				.about("List surrealdb tests")
				.arg(arg!([filter] "Filter the test by their path"))
				.arg(
					arg!(--path <PATH> "Set the path to tests directory").default_value("./tests"),
				),
			);

	#[cfg(feature = "bench")]
	let cmd = cmd.subcommand(crate::cmd::bench::cmd());

	cmd.subcommand_required(true).get_matches()
}
