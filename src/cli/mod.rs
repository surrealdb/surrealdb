mod backup;
mod export;
mod import;
mod log;
mod start;
mod version;

use clap::{App, AppSettings, Arg, SubCommand};

fn auth_valid(v: String) -> Result<(), String> {
	if v.contains(":") {
		return Ok(());
	}
	Err(String::from(
		"\
		Provide a valid user:pass value separated by a colon, \
		or use the --auth-user and --auth-pass flags\
	",
	))
}

fn file_valid(v: String) -> Result<(), String> {
	if v.len() > 0 {
		return Ok(());
	}
	Err(String::from(
		"\
		Provide a valid path to a SQL file\
	",
	))
}

fn conn_valid(v: String) -> Result<(), String> {
	if v.starts_with("https://") {
		return Ok(());
	}
	if v.starts_with("http://") {
		return Ok(());
	}
	Err(String::from(
		"\
		Provide a valid database connection string\
	",
	))
}

fn from_valid(v: String) -> Result<(), String> {
	if v.starts_with("https://") {
		return Ok(());
	}
	if v.starts_with("http://") {
		return Ok(());
	}
	if v.ends_with(".db") {
		return Ok(());
	}
	Err(String::from(
		"\
		Provide a valid database connection string, \
		or specify the path to a database file\
	",
	))
}

fn into_valid(v: String) -> Result<(), String> {
	if v.starts_with("https://") {
		return Ok(());
	}
	if v.starts_with("http://") {
		return Ok(());
	}
	if v.ends_with(".db") {
		return Ok(());
	}
	Err(String::from(
		"\
		Provide a valid database connection string, \
		or specify the path to a database file\
	",
	))
}

fn key_valid(v: String) -> Result<(), String> {
	match v.len() {
		16 => Ok(()),
		24 => Ok(()),
		32 => Ok(()),
		_ => Err(String::from(
			"\
			For AES-128 encryption use a 16 bit key, \
			for AES-192 encryption use a 24 bit key, \
			and for AES-256 encryption use a 32 bit key\
		",
		)),
	}
}

pub fn init() {
	let setup = App::new("SurrealDB command-line interface and server")
		.setting(AppSettings::DisableVersion)
		.setting(AppSettings::ArgRequiredElseHelp)
		.setting(AppSettings::VersionlessSubcommands)
		.arg(
			Arg::with_name("verbose")
				.short("v")
				.long("verbose")
				.multiple(true)
				.help("Specify the log output verbosity"),
		);

	let setup = setup.subcommand(
		SubCommand::with_name("start")
			.display_order(1)
			.about("Start the database server")
			.arg(
				Arg::with_name("path")
					.index(1)
					.required(true)
					.default_value("memory")
					.help("Database path used for storing data"),
			)
			.arg(
				Arg::with_name("auth")
					.short("a")
					.long("auth")
					.empty_values(false)
					.validator(auth_valid)
					.default_value("root:root")
					.help("Master database authentication details"),
			)
			.arg(
				Arg::with_name("auth-user")
					.short("u")
					.long("auth-user")
					.empty_values(false)
					.default_value("root")
					.help("The master username for the database"),
			)
			.arg(
				Arg::with_name("auth-pass")
					.short("p")
					.long("auth-pass")
					.empty_values(false)
					.default_value("root")
					.help("The master password for the database"),
			)
			.arg(
				Arg::with_name("auth-addr")
					.long("auth-addr")
					.multiple(true)
					.empty_values(false)
					.number_of_values(1)
					.default_value("127.0.0.1/32")
					.help("The allowed networks for master authentication"),
			)
			.arg(
				Arg::with_name("bind")
					.short("b")
					.long("bind")
					.empty_values(false)
					.default_value("0.0.0.0:3000")
					.help("The hostname or ip address to listen for connections on"),
			)
			.arg(
				Arg::with_name("key")
					.short("k")
					.long("key")
					.takes_value(true)
					.empty_values(false)
					.validator(key_valid)
					.help("Encryption key to use for on-disk encryption"),
			)
			.arg(
				Arg::with_name("kvs-ca")
					.long("kvs-ca")
					.takes_value(true)
					.empty_values(false)
					.help("Path to the CA file used when connecting to the remote KV store"),
			)
			.arg(
				Arg::with_name("kvs-crt")
					.long("kvs-crt")
					.takes_value(true)
					.empty_values(false)
					.help(
						"Path to the certificate file used when connecting to the remote KV store",
					),
			)
			.arg(
				Arg::with_name("kvs-key")
					.long("kvs-key")
					.takes_value(true)
					.empty_values(false)
					.help(
						"Path to the private key file used when connecting to the remote KV store",
					),
			)
			.arg(
				Arg::with_name("web-crt")
					.long("web-crt")
					.takes_value(true)
					.empty_values(false)
					.help("Path to the certificate file for encrypted client connections"),
			)
			.arg(
				Arg::with_name("web-key")
					.long("web-key")
					.takes_value(true)
					.empty_values(false)
					.help("Path to the private key file for encrypted client connections"),
			),
	);

	let setup = setup.subcommand(
		SubCommand::with_name("backup")
			.display_order(2)
			.about("Backup data to or from an existing database")
			.arg(
				Arg::with_name("from")
					.index(1)
					.required(true)
					.validator(from_valid)
					.help("Path to the remote database or file from which to export"),
			)
			.arg(
				Arg::with_name("into")
					.index(2)
					.required(true)
					.validator(into_valid)
					.help("Path to the remote database or file into which to import"),
			)
			.arg(
				Arg::with_name("user")
					.short("u")
					.long("user")
					.empty_values(false)
					.default_value("root")
					.help("Database authentication username to use when connecting"),
			)
			.arg(
				Arg::with_name("pass")
					.short("p")
					.long("pass")
					.empty_values(false)
					.default_value("root")
					.help("Database authentication password to use when connecting"),
			),
	);

	let setup = setup.subcommand(
		SubCommand::with_name("import")
			.display_order(3)
			.about("Import a SQL script into an existing database")
			.arg(
				Arg::with_name("file")
					.index(1)
					.required(true)
					.validator(file_valid)
					.help("Path to the sql file to import"),
			)
			.arg(
				Arg::with_name("ns")
					.long("ns")
					.required(true)
					.empty_values(false)
					.help("The namespace to import the data into"),
			)
			.arg(
				Arg::with_name("db")
					.long("db")
					.required(true)
					.empty_values(false)
					.help("The database to import the data into"),
			)
			.arg(
				Arg::with_name("conn")
					.short("c")
					.long("conn")
					.empty_values(false)
					.validator(conn_valid)
					.default_value("https://surreal.io")
					.help("Remote database server url to connect to"),
			)
			.arg(
				Arg::with_name("user")
					.short("u")
					.long("user")
					.empty_values(false)
					.default_value("root")
					.help("Database authentication username to use when connecting"),
			)
			.arg(
				Arg::with_name("pass")
					.short("p")
					.long("pass")
					.empty_values(false)
					.default_value("root")
					.help("Database authentication password to use when connecting"),
			),
	);

	let setup = setup.subcommand(
		SubCommand::with_name("export")
			.display_order(4)
			.about("Export an existing database into a SQL script")
			.arg(
				Arg::with_name("file")
					.index(1)
					.required(true)
					.validator(file_valid)
					.help("Path to the sql file to export"),
			)
			.arg(
				Arg::with_name("ns")
					.long("ns")
					.required(true)
					.empty_values(false)
					.help("The namespace to export the data from"),
			)
			.arg(
				Arg::with_name("db")
					.long("db")
					.required(true)
					.empty_values(false)
					.help("The database to export the data from"),
			)
			.arg(
				Arg::with_name("conn")
					.short("c")
					.long("conn")
					.empty_values(false)
					.validator(conn_valid)
					.default_value("https://surreal.io")
					.help("Remote database server url to connect to"),
			)
			.arg(
				Arg::with_name("user")
					.short("u")
					.long("user")
					.empty_values(false)
					.default_value("root")
					.help("Database authentication username to use when connecting"),
			)
			.arg(
				Arg::with_name("pass")
					.short("p")
					.long("pass")
					.empty_values(false)
					.default_value("root")
					.help("Database authentication password to use when connecting"),
			),
	);

	let setup = setup.subcommand(
		SubCommand::with_name("version")
			.display_order(5)
			.about("Output the command-line tool version information"),
	);

	let matches = setup.get_matches();

	let verbose = matches.occurrences_of("verbose") as usize;

	log::init(verbose);

	let output = match matches.subcommand() {
		("start", Some(m)) => start::init(m),
		("backup", Some(m)) => backup::init(m),
		("import", Some(m)) => import::init(m),
		("export", Some(m)) => export::init(m),
		("version", Some(m)) => version::init(m),
		_ => Ok(()),
	};

	match output {
		Err(e) => {
			error!("{}", e);
			return ();
		}
		Ok(_) => {}
	};
}
