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
        &[
            FailureMode::Fail,
            FailureMode::Accept,
            FailureMode::Overwrite,
        ]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            FailureMode::Fail => Some(PossibleValue::new("fail")),
            FailureMode::Accept => Some(PossibleValue::new("accept")),
            FailureMode::Overwrite => Some(PossibleValue::new("overwrite")),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Backends {
    #[cfg(feature = "backend-mem")]
    Mem,
    #[cfg(feature = "backend-surrealkv")]
    SurrealKv,
    #[cfg(feature = "backend-rocksdb")]
    RocksDb,
    #[cfg(feature = "backend-foundation-7_1")]
    Foundation7_1,
    #[cfg(feature = "backend-foundation-7_3")]
    Foundation7_3,
    #[cfg(feature = "backend-tikv")]
    TiKv,
    #[cfg(feature = "backend-client-ws")]
    ClientWs,
    #[cfg(feature = "backend-client-http")]
    ClientHttp,
    All,
}

impl ValueEnum for Backends {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            #[cfg(feature = "backend-mem")]
            Self::Mem,
            #[cfg(feature = "backend-surrealkv")]
            Self::SurrealKv,
            #[cfg(feature = "backend-rocksdb")]
            Self::RocksDb,
            #[cfg(feature = "backend-foundation-7_1")]
            Self::Foundation7_1,
            #[cfg(feature = "backend-foundation-7_3")]
            Self::Foundation7_3,
            #[cfg(feature = "backend-tikv")]
            Self::TiKv,
            #[cfg(feature = "backend-client-ws")]
            Self::ClientWs,
            #[cfg(feature = "backend-client-http")]
            Self::ClientHttp,
            Self::All,
        ]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            #[cfg(feature = "backend-mem")]
            Self::Mem => Some(PossibleValue::new("mem")),
            #[cfg(feature = "backend-surrealkv")]
            Self::SurrealKv => Some(PossibleValue::new("skv")),
            #[cfg(feature = "backend-rocksdb")]
            Self::RocksDb => Some(PossibleValue::new("rocksb")),
            #[cfg(feature = "backend-foundation-7_1")]
            Self::Foundation7_1 => Some(PossibleValue::new("fdb7.1")),
            #[cfg(feature = "backend-foundation-7_3")]
            Self::Foundation7_3 => Some(PossibleValue::new("fdb7.3")),
            #[cfg(feature = "backend-tikv")]
            Self::TiKv => Some(PossibleValue::new("tikv")),
            #[cfg(feature = "backend-client-ws")]
            Self::ClientWs => Some(PossibleValue::new("ws")),
            #[cfg(feature = "backend-client-http")]
            Self::ClientHttp => Some(PossibleValue::new("http")),
            Self::All => Some(PossibleValue::new("all")),
        }
    }
}

pub fn parse() -> ArgMatches {
    let mut cmd = command!()
        .subcommand(
            Command::new("run")
                .about("Run surrealdb tests")
                .arg(arg!([filter] "Filter the test by their path"))
                .arg(arg!(--path <PATH> "The path to tests directory").default_value("./tests"))
                .arg(
                    arg!(-j --jobs <JOBS> "The number of test running in parallel, defaults to available parallism")
                        .value_parser(value_parser!(u32).range(1..))
                ).arg(
                    arg!(--failure <FAILURE> "How to handle failure of tests").value_parser(EnumValueParser::<FailureMode>::new()).default_value("fail")
                ).arg(
                    arg!(--backend <BACKEND> "The backend to run the tests on").value_parser(EnumValueParser::<Backends>::new()).value_delimiter(',')
                ).arg(
                    arg!(--standalone <BACKEND> "The standalone surrealdb executable to run client tests against").value_parser(value_parser!(String))
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
