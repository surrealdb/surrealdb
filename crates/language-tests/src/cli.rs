use std::fmt;

use clap::{
	ArgMatches, Command, ValueEnum, arg,
	builder::{EnumValueParser, PossibleValue},
	command, value_parser,
};
use semver::Version;

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
	Foundation,
}

impl ValueEnum for Backend {
	fn value_variants<'a>() -> &'a [Self] {
		&[Backend::Memory, Backend::RocksDb, Backend::SurrealKv, Backend::Foundation]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		match self {
			Backend::Memory => Some(PossibleValue::new("memory").alias("mem")),
			Backend::RocksDb => Some(PossibleValue::new("rocksdb")),
			Backend::SurrealKv => Some(PossibleValue::new("surrealkv").alias("file")),
			Backend::Foundation => Some(PossibleValue::new("foundation")),
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum UpgradeBackend {
	RocksDb,
	SurrealKv,
	Foundation,
}

impl ValueEnum for UpgradeBackend {
	fn value_variants<'a>() -> &'a [Self] {
		&[UpgradeBackend::RocksDb, UpgradeBackend::SurrealKv, UpgradeBackend::Foundation]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		match self {
			UpgradeBackend::RocksDb => Some(PossibleValue::new("rocksdb")),
			UpgradeBackend::SurrealKv => Some(PossibleValue::new("surrealkv").alias("file")),
			UpgradeBackend::Foundation => Some(PossibleValue::new("foundationdb")),
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
