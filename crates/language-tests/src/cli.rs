use clap::{
	arg,
	builder::{EnumValueParser, PossibleValue},
	command, value_parser, ArgMatches, Command, Subcommand, ValueEnum,
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum FailureMode {
	Fail,
	Accept,
	Overwrite,
}

impl ValueEnum for FailureMode {
	fn value_variants<'a>() -> &'a [Self] {
		&[FailureMode::Fail, FailureMode::Accept, FailureMode::Overwrite]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		match self {
			FailureMode::Fail => Some(PossibleValue::new("fail")),
			FailureMode::Accept => Some(PossibleValue::new("accept")),
			FailureMode::Overwrite => Some(PossibleValue::new("overwrite")),
		}
	}
}

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
}

pub fn parse() -> ArgMatches {
	let mut cmd = command!()
		.arg(arg!(--log "Set the log level for the test suite itself").value_parser(EnumValueParser::<LogLevel>::new()).default_value("info"))
        .subcommand(
            Command::new("run")
                .about("Run surrealdb tests")
				.arg(arg!(--test-log "Set the log level for the tests").value_parser(EnumValueParser::<LogLevel>::new()).default_value("info"))
				.arg(arg!(--no-capture-log "Disable the capturing of test logs"))
                .arg(arg!([filter] "Filter the test by their path"))
                .arg(arg!(--path <PATH> "The path to tests directory").default_value("./tests"))
                .arg(
                    arg!(-j --jobs <JOBS> "The number of test running in parallel, defaults to available parallism")
                        .value_parser(value_parser!(u32).range(1..))
                ).arg(
                    arg!(--failure <FAILURE> "How to handle failure of tests").value_parser(EnumValueParser::<FailureMode>::new()).default_value("fail")
                ),
        )
        .subcommand(
            Command::new("list")
                .about("List surrealdb tests")
                .arg(arg!([filter] "Filter the test by their path"))
                .arg(
                    arg!(--path <PATH> "Set the path to tests directory").default_value("./tests"),
                ),
        );

	#[cfg(feature = "fuzzing")]
	{
		cmd = cmd.subcommand(
			Command::new("fuzz")
				.about("Command for handling fuzzing input")
				.subcommand(
					Command::new("fmt")
						.about("Debug format the query from a reproduction file")
						.arg(arg!(<INPUT> "The input file")),
				)
				.subcommand(
					Command::new("export")
						.about("Debug format the query from a reproduction file")
						.arg(arg!(<INPUT> "The input file")),
				)
				.subcommand_required(true),
		);
	}

	cmd.subcommand_required(true).get_matches()
}
