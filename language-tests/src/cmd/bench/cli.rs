use clap::{Command, arg, builder::EnumValueParser};

use crate::cli::Backend;

pub fn cmd() -> Command {
	Command::new("bench")
		.about("Commands related to the surrealdb language benchmark suite")
		.arg(arg!(--"store-path" <PATH> "Set the path to save the result of the benchmark to").default_value("./bench_results").global(true))
		.arg(arg!(--"store-url" <URL> "Set the websocket url to save the result of the benchmark to").global(true))
		.arg(arg!(--"store-db" <DATABASE> "Set the database in which the results are stored").default_value("main").global(true))
		.arg(arg!(--"store-ns" <NAMESPACE> "Set the namespace in which the results are stored").default_value("main").global(true))
		.arg(arg!(--"store-user" <USER> "Set the username to login to the benchmark result datastore").default_value("viewer").env("LANG_BENCH_USER").global(true))
		.arg(arg!(--"store-password" <PASSWORD> "Set the password to login to the benchmark result datastore").default_value("viewer").env("LANG_BENCH_PASSWORD").global(true))
		.subcommand(Command::new("run").about("Run the surrealdb benchmarking suite")
			.arg(
				arg!(--backend <BACKEND> "Specify the storage backend to use for the upgrade test")
					.value_parser(EnumValueParser::<Backend>::new()).default_value("mem")
			)
			.arg(
				arg!(--"ds-cache" <DIR> "Specify where to store the dataset cache").default_value("./ds_cache")
			)
            .arg(arg!(--path <PATH> "The path to tests directory").default_value("./tests"))
			.arg(arg!(-s --save "Save the result to the comparison datastore"))
		)
		.subcommand_required(true)
}
