use clap::builder::{EnumValueParser, PossibleValue};
use clap::{ArgMatches, Command, ValueEnum, arg, command, value_parser};
use semver::Version;
use std::fmt;
use std::fmt::{Display, Formatter};

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

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Backend {
	Memory,
	RocksDb,
	SurrealKv,
	TikV,
}

impl ValueEnum for Backend {
	fn value_variants<'a>() -> &'a [Self] {
		&[Backend::Memory, Backend::RocksDb, Backend::SurrealKv, Backend::TikV]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		match self {
			Backend::Memory => Some(PossibleValue::new("memory").alias("mem")),
			Backend::RocksDb => Some(PossibleValue::new("rocksdb")),
			Backend::SurrealKv => Some(PossibleValue::new("surrealkv").alias("file")),
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
			UpgradeBackend::SurrealKv => Some(PossibleValue::new("surrealkv").alias("file")),
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

/*
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LogLevel {
	Trace,
	Debug,
	Info,
	Warn,
	Error,
}

impl ValueEnum for LogLevel {
	fn value_variants<'a>() -> &'a [Self] {
		&[LogLevel::Trace, LogLevel::Debug, LogLevel::Info, LogLevel::Warn, LogLevel::Error]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		match self {
			LogLevel::Trace => Some(PossibleValue::new("trace")),
			LogLevel::Debug => Some(PossibleValue::new("debug")),
			LogLevel::Info => Some(PossibleValue::new("info")),
			LogLevel::Warn => Some(PossibleValue::new("warn")),
			LogLevel::Error => Some(PossibleValue::new("error")),
		}
	}
}*/

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
				arg!(-f --from <VERSIONS> "The version to upgrade from. This can be either a version number or a path to the surrealdb codebase.").required(true).value_delimiter(',').value_parser(DsVersion::from_str)
			)
			.arg(
				arg!(-t --to <VERSIONS> "The version to upgrade to. This can be either a version number or a path to the surrealdb codebase.").required(true).value_delimiter(',').value_parser(DsVersion::from_str)
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

	cmd.subcommand_required(true).get_matches()
}
