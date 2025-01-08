pub(crate) mod abstraction;
mod config;
mod export;
mod fix;
mod import;
mod isready;
mod ml;
mod sql;
mod start;
#[cfg(test)]
mod test;
mod upgrade;
mod validate;
pub(crate) mod validator;
mod version;
mod version_client;

use crate::cli::validator::parser::env_filter::CustomEnvFilter;
use crate::cli::validator::parser::env_filter::CustomEnvFilterParser;
use crate::cli::version_client::VersionClient;
#[cfg(debug_assertions)]
use crate::cnf::DEBUG_BUILD_WARNING;
use crate::cnf::{LOGO, PKG_VERSION};
use crate::env::RELEASE;
use clap::{Parser, Subcommand};
pub use config::CF;
use export::ExportCommandArguments;
use fix::FixCommandArguments;
use import::ImportCommandArguments;
use isready::IsReadyCommandArguments;
use ml::MlCommand;
use semver::Version;
use sql::SqlCommandArguments;
use start::StartCommandArguments;
use std::ops::Deref;
use std::process::ExitCode;
use std::time::Duration;
use upgrade::UpgradeCommandArguments;
use validate::ValidateCommandArguments;
use version::VersionCommandArguments;

const INFO: &str = "
To get started using SurrealDB, and for guides on connecting to and building applications
on top of SurrealDB, check out the SurrealDB documentation (https://surrealdb.com/docs).

If you have questions or ideas, join the SurrealDB community (https://surrealdb.com/community).

If you find a bug, submit an issue on GitHub (https://github.com/surrealdb/surrealdb/issues).

We would love it if you could star the repository (https://github.com/surrealdb/surrealdb).

----------
";

#[derive(Parser, Debug)]
#[command(name = "SurrealDB command-line interface and server", bin_name = "surreal")]
#[command(version = RELEASE.as_str(), about = INFO, before_help = LOGO)]
#[command(disable_version_flag = false, arg_required_else_help = true)]
struct Cli {
	#[command(subcommand)]
	command: Commands,
	#[arg(help = "The logging level for the command-line tool")]
	#[arg(env = "SURREAL_LOG", short = 'l', long = "log")]
	#[arg(global = true)]
	#[arg(default_value = "info")]
	#[arg(value_parser = CustomEnvFilterParser::new())]
	log: CustomEnvFilter,
	#[arg(help = "Whether to allow web check for client version upgrades at start")]
	#[arg(env = "SURREAL_ONLINE_VERSION_CHECK", long)]
	#[arg(default_value_t = true)]
	online_version_check: bool,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum Commands {
	#[command(about = "Start the database server")]
	Start(StartCommandArguments),
	/* Not implemented yet
	#[command(about = "Backup data to or from an existing database")]
	Backup(BackupCommandArguments),
	*/
	#[command(about = "Import a SurrealQL script into an existing database")]
	Import(ImportCommandArguments),
	#[command(about = "Export an existing database as a SurrealQL script")]
	Export(ExportCommandArguments),
	#[command(about = "Output the command-line tool and remote server version information")]
	Version(VersionCommandArguments),
	#[command(about = "Upgrade to the latest stable version")]
	Upgrade(UpgradeCommandArguments),
	#[command(about = "Start an SQL REPL in your terminal with pipe support")]
	Sql(SqlCommandArguments),
	#[command(subcommand, about = "Manage SurrealML models within an existing database")]
	Ml(MlCommand),
	#[command(
		about = "Check if the SurrealDB server is ready to accept connections",
		visible_alias = "isready"
	)]
	IsReady(IsReadyCommandArguments),
	#[command(about = "Validate SurrealQL query files")]
	Validate(ValidateCommandArguments),
	#[command(about = "Fix database storage issues")]
	Fix(FixCommandArguments),
}

pub async fn init() -> ExitCode {
	// Enables ANSI code support on Windows
	#[cfg(windows)]
	nu_ansi_term::enable_ansi_support().ok();
	// Print debug mode warning
	#[cfg(debug_assertions)]
	println!("{DEBUG_BUILD_WARNING}");
	// Start a new CPU profiler
	#[cfg(feature = "performance-profiler")]
	let guard = pprof::ProfilerGuardBuilder::default()
		.frequency(1000)
		.blocklist(&["libc", "libgcc", "pthread", "vdso"])
		.build()
		.unwrap();
	// Parse the CLI arguments
	let args = Cli::parse();
	// After parsing arguments, we check the version online
	if args.online_version_check {
		let client = version_client::new(Some(Duration::from_millis(500))).unwrap();
		if let Err(opt_version) = check_upgrade(&client, PKG_VERSION.deref()).await {
			match opt_version {
				None => {
					warn!("A new version of SurrealDB may be available.");
				}
				Some(new_version) => {
					warn!("A new version of SurrealDB is available: {}", new_version);
				}
			}
			// TODO ansi_term crate?
			warn!("You can upgrade using the {} command", "surreal upgrade");
		}
	}
	// Initialize opentelemetry and logging
	let telemetry = crate::telemetry::builder().with_log_level("info").with_filter(args.log);
	// Extract the telemetry log guards
	let (outg, errg) = telemetry.init().expect("Unable to configure logs");
	// After version warning we can run the respective command
	let output = match args.command {
		Commands::Start(args) => start::init(args).await,
		Commands::Import(args) => import::init(args).await,
		Commands::Export(args) => export::init(args).await,
		Commands::Version(args) => version::init(args).await,
		Commands::Upgrade(args) => upgrade::init(args).await,
		Commands::Sql(args) => sql::init(args).await,
		Commands::Ml(args) => ml::init(args).await,
		Commands::IsReady(args) => isready::init(args).await,
		Commands::Validate(args) => validate::init(args).await,
		Commands::Fix(args) => fix::init(args).await,
	};
	// Save the flamegraph and profile
	#[cfg(feature = "performance-profiler")]
	if let Ok(report) = guard.report().build() {
		// Import necessary traits
		use pprof::protos::Message;
		use std::io::Write;
		// Output a flamegraph
		let file = std::fs::File::create("flamegraph.svg").unwrap();
		report.flamegraph(file).unwrap();
		// Output a pprof
		let mut file = std::fs::File::create("profile.pb").unwrap();
		let profile = report.pprof().unwrap();
		let mut content = Vec::new();
		profile.encode(&mut content).unwrap();
		file.write_all(&content).unwrap();
	};
	// Error and exit the programme
	if let Err(e) = output {
		// Output any error
		error!("{}", e);
		// Drop the log guards
		drop(outg);
		drop(errg);
		// Return failure
		ExitCode::FAILURE
	} else {
		// Drop the log guards
		drop(outg);
		drop(errg);
		// Return success
		ExitCode::SUCCESS
	}
}

/// Check if there is a newer version
/// Ok = No upgrade needed
/// Err = Upgrade needed, returns the new version if it is available
async fn check_upgrade<C: VersionClient>(
	client: &C,
	pkg_version: &str,
) -> Result<(), Option<Version>> {
	if let Ok(version) = client.fetch("latest").await {
		// Request was successful, compare against current
		let old_version = upgrade::parse_version(pkg_version).unwrap();
		let new_version = upgrade::parse_version(&version).unwrap();
		if old_version < new_version {
			return Err(Some(new_version));
		}
	} else {
		// Request failed, check against date
		// TODO: We don't have an "expiry" set per-version, so this is a todo
		// It would return Err(None) if the version is too old
	}
	Ok(())
}
