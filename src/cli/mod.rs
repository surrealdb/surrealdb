mod backup;
mod config;
mod export;
mod import;
mod log;
mod sql;
mod start;
mod version;

pub use config::CF;

use crate::cnf::LOGO;
use clap::{Arg, Command};
use std::process::ExitCode;

pub const LOG: &str = "surrealdb::cli";

const INFO: &str = "
To get started using SurrealDB, and for guides on connecting to and building applications
on top of SurrealDB, check out the SurrealDB documentation (https://surrealdb.com/docs).

If you have questions or ideas, join the SurrealDB community (https://surrealdb.com/community).

If you find a bug, submit an issue on Github (https://github.com/surrealdb/surrealdb/issues).

We would love it if you could star the repository (https://github.com/surrealdb/surrealdb).

----------
";

fn split_endpoint(v: &str) -> (&str, &str) {
	match v {
		"memory" => ("mem", ""),
		v => match v.split_once("://") {
			Some(parts) => parts,
			None => v.split_once(':').unwrap_or_default(),
		},
	}
}

fn file_valid(v: &str) -> Result<(), String> {
	match v {
		v if !v.is_empty() => Ok(()),
		_ => Err(String::from(
			"\
			Provide a valid path to a SQL file\
		",
		)),
	}
}

fn path_valid(v: &str) -> Result<(), String> {
	match v {
		"memory" => Ok(()),
		v if v.starts_with("file:") => Ok(()),
		v if v.starts_with("rocksdb:") => Ok(()),
		v if v.starts_with("tikv:") => Ok(()),
		v if v.starts_with("fdb:") => Ok(()),
		_ => Err(String::from(
			"\
			Provide a valid database path parameter\
		",
		)),
	}
}

fn conn_valid(v: &str) -> Result<(), String> {
	let scheme = split_endpoint(v).0;
	match scheme {
		"http" | "https" | "ws" | "wss" | "fdb" | "mem" | "rocksdb" | "file" | "tikv" => Ok(()),
		_ => Err(String::from(
			"\
			Provide a valid database connection string\
		",
		)),
	}
}

fn from_valid(v: &str) -> Result<(), String> {
	match v {
		v if v.ends_with(".db") => Ok(()),
		v if v.starts_with("http://") => Ok(()),
		v if v.starts_with("https://") => Ok(()),
		_ => Err(String::from(
			"\
			Provide a valid database connection string, \
			or specify the path to a database file\
		",
		)),
	}
}

fn into_valid(v: &str) -> Result<(), String> {
	match v {
		v if v.ends_with(".db") => Ok(()),
		v if v.starts_with("http://") => Ok(()),
		v if v.starts_with("https://") => Ok(()),
		_ => Err(String::from(
			"\
			Provide a valid database connection string, \
			or specify the path to a database file\
		",
		)),
	}
}

fn key_valid(v: &str) -> Result<(), String> {
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

pub fn init() -> ExitCode {
	let setup = Command::new("SurrealDB command-line interface and server")
		.about(INFO)
		.before_help(LOGO)
		.disable_version_flag(true)
		.arg_required_else_help(true);

	let setup = setup.subcommand(
		Command::new("start")
			.display_order(1)
			.about("Start the database server")
			.arg(
				Arg::new("path")
					.index(1)
					.env("DB_PATH")
					.required(false)
					.validator(path_valid)
					.default_value("memory")
					.help("Database path used for storing data"),
			)
			.arg(
				Arg::new("user")
					.short('u')
					.env("USER")
					.long("user")
					.forbid_empty_values(true)
					.default_value("root")
					.help("The master username for the database"),
			)
			.arg(
				Arg::new("pass")
					.short('p')
					.env("PASS")
					.long("pass")
					.takes_value(true)
					.forbid_empty_values(true)
					.help("The master password for the database"),
			)
			.arg(
				Arg::new("addr")
					.env("ADDR")
					.long("addr")
					.number_of_values(1)
					.forbid_empty_values(true)
					.multiple_occurrences(true)
					.default_value("127.0.0.1/32")
					.help("The allowed networks for master authentication"),
			)
			.arg(
				Arg::new("bind")
					.short('b')
					.env("BIND")
					.long("bind")
					.forbid_empty_values(true)
					.default_value("0.0.0.0:8000")
					.help("The hostname or ip address to listen for connections on"),
			)
			.arg(
				Arg::new("key")
					.short('k')
					.env("KEY")
					.long("key")
					.takes_value(true)
					.forbid_empty_values(true)
					.validator(key_valid)
					.help("Encryption key to use for on-disk encryption"),
			)
			.arg(
				Arg::new("kvs-ca")
					.env("KVS_CA")
					.long("kvs-ca")
					.takes_value(true)
					.forbid_empty_values(true)
					.help("Path to the CA file used when connecting to the remote KV store"),
			)
			.arg(
				Arg::new("kvs-crt")
					.env("KVS_CRT")
					.long("kvs-crt")
					.takes_value(true)
					.forbid_empty_values(true)
					.help(
						"Path to the certificate file used when connecting to the remote KV store",
					),
			)
			.arg(
				Arg::new("kvs-key")
					.env("KVS_KEY")
					.long("kvs-key")
					.takes_value(true)
					.forbid_empty_values(true)
					.help(
						"Path to the private key file used when connecting to the remote KV store",
					),
			)
			.arg(
				Arg::new("web-crt")
					.env("WEB_CRT")
					.long("web-crt")
					.takes_value(true)
					.forbid_empty_values(true)
					.help("Path to the certificate file for encrypted client connections"),
			)
			.arg(
				Arg::new("web-key")
					.env("WEB_KEY")
					.long("web-key")
					.takes_value(true)
					.forbid_empty_values(true)
					.help("Path to the private key file for encrypted client connections"),
			)
			.arg(
				Arg::new("strict")
					.short('s')
					.env("STRICT")
					.long("strict")
					.required(false)
					.takes_value(false)
					.help("Whether strict mode is enabled on this database instance"),
			)
			.arg(
				Arg::new("log")
					.short('l')
					.env("LOG")
					.long("log")
					.takes_value(true)
					.default_value("info")
					.forbid_empty_values(true)
					.help("The logging level for the database server")
					.value_parser(["warn", "info", "debug", "trace", "full"]),
			)
			.arg(
				Arg::new("no-banner")
					.env("NO_BANNER")
					.long("no-banner")
					.required(false)
					.takes_value(false)
					.help("Whether to hide the startup banner"),
			),
	);

	let setup = setup.subcommand(
		Command::new("backup")
			.display_order(2)
			.about("Backup data to or from an existing database")
			.arg(
				Arg::new("from")
					.index(1)
					.required(true)
					.validator(from_valid)
					.help("Path to the remote database or file from which to export"),
			)
			.arg(
				Arg::new("into")
					.index(2)
					.required(true)
					.validator(into_valid)
					.help("Path to the remote database or file into which to import"),
			)
			.arg(
				Arg::new("user")
					.short('u')
					.long("user")
					.forbid_empty_values(true)
					.default_value("root")
					.help("Database authentication username to use when connecting"),
			)
			.arg(
				Arg::new("pass")
					.short('p')
					.long("pass")
					.forbid_empty_values(true)
					.default_value("root")
					.help("Database authentication password to use when connecting"),
			),
	);

	let setup = setup.subcommand(
		Command::new("import")
			.display_order(3)
			.about("Import a SurrealQL script into an existing database")
			.arg(
				Arg::new("file")
					.index(1)
					.required(true)
					.validator(file_valid)
					.help("Path to the sql file to import"),
			)
			.arg(
				Arg::new("ns")
					.long("ns")
					.required(true)
					.takes_value(true)
					.forbid_empty_values(true)
					.help("The namespace to import the data into"),
			)
			.arg(
				Arg::new("db")
					.long("db")
					.required(true)
					.takes_value(true)
					.forbid_empty_values(true)
					.help("The database to import the data into"),
			)
			.arg(
				Arg::new("conn")
					.short('c')
					.long("conn")
					.alias("host")
					.forbid_empty_values(true)
					.validator(conn_valid)
					.default_value("https://cloud.surrealdb.com")
					.help("Remote database server url to connect to"),
			)
			.arg(
				Arg::new("user")
					.short('u')
					.long("user")
					.forbid_empty_values(true)
					.default_value("root")
					.help("Database authentication username to use when connecting"),
			)
			.arg(
				Arg::new("pass")
					.short('p')
					.long("pass")
					.forbid_empty_values(true)
					.default_value("root")
					.help("Database authentication password to use when connecting"),
			),
	);

	let setup = setup.subcommand(
		Command::new("export")
			.display_order(4)
			.about("Export an existing database as a SurrealQL script")
			.arg(
				Arg::new("file")
					.index(1)
					.required(true)
					.validator(file_valid)
					.help("Path to the sql file to export"),
			)
			.arg(
				Arg::new("ns")
					.long("ns")
					.required(true)
					.takes_value(true)
					.forbid_empty_values(true)
					.help("The namespace to export the data from"),
			)
			.arg(
				Arg::new("db")
					.long("db")
					.required(true)
					.takes_value(true)
					.forbid_empty_values(true)
					.help("The database to export the data from"),
			)
			.arg(
				Arg::new("conn")
					.short('c')
					.long("conn")
					.alias("host")
					.forbid_empty_values(true)
					.validator(conn_valid)
					.default_value("https://cloud.surrealdb.com")
					.help("Remote database server url to connect to"),
			)
			.arg(
				Arg::new("user")
					.short('u')
					.long("user")
					.forbid_empty_values(true)
					.default_value("root")
					.help("Database authentication username to use when connecting"),
			)
			.arg(
				Arg::new("pass")
					.short('p')
					.long("pass")
					.forbid_empty_values(true)
					.default_value("root")
					.help("Database authentication password to use when connecting"),
			),
	);

	let setup = setup.subcommand(
		Command::new("version")
			.display_order(5)
			.about("Output the command-line tool version information"),
	);

	let setup = setup.subcommand(
		Command::new("sql")
			.display_order(6)
			.about("Start an SQL REPL in your terminal with pipe support")
			.arg(
				Arg::new("ns")
					.long("ns")
					.required(false)
					.takes_value(true)
					.forbid_empty_values(true)
					.help("The namespace to export the data from"),
			)
			.arg(
				Arg::new("db")
					.long("db")
					.required(false)
					.takes_value(true)
					.forbid_empty_values(true)
					.help("The database to export the data from"),
			)
			.arg(
				Arg::new("conn")
					.short('c')
					.long("conn")
					.alias("host")
					.forbid_empty_values(true)
					.validator(conn_valid)
					.default_value("wss://cloud.surrealdb.com")
					.help("Remote database server url to connect to"),
			)
			.arg(
				Arg::new("user")
					.short('u')
					.long("user")
					.forbid_empty_values(true)
					.default_value("root")
					.help("Database authentication username to use when connecting"),
			)
			.arg(
				Arg::new("pass")
					.short('p')
					.long("pass")
					.forbid_empty_values(true)
					.default_value("root")
					.help("Database authentication password to use when connecting"),
			)
			.arg(
				Arg::new("pretty")
					.long("pretty")
					.required(false)
					.takes_value(false)
					.help("Whether database responses should be pretty printed"),
			),
	);

	let matches = setup.get_matches();

	let output = match matches.subcommand() {
		Some(("sql", m)) => sql::init(m),
		Some(("start", m)) => start::init(m),
		Some(("backup", m)) => backup::init(m),
		Some(("import", m)) => import::init(m),
		Some(("export", m)) => export::init(m),
		Some(("version", m)) => version::init(m),
		_ => Ok(()),
	};

	if let Err(e) = output {
		error!(target: LOG, "{}", e);
		return ExitCode::FAILURE;
	}

	ExitCode::SUCCESS
}
