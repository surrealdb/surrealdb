use super::log;
use crate::cnf::LOGO;
use crate::err::Error;
use crate::lsp;

pub fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Set the default log level
	match matches.get_one::<String>("log").map(String::as_str) {
		Some("warn") => log::init(0),
		Some("info") => log::init(1),
		Some("debug") => log::init(2),
		Some("trace") => log::init(3),
		Some("full") => log::init(4),
		_ => unreachable!(),
	};

  	lsp::main();

	// All ok
	Ok(())
}
