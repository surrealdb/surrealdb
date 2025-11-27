pub(crate) mod auth;

use auth::CredentialsLevel;
use clap::Args;

#[derive(Args, Debug)]
pub(crate) struct AuthArguments {
	#[arg(help = "Database authentication username to use when connecting")]
	#[arg(
		env = "SURREAL_USER",
		short = 'u',
		long = "username",
		visible_alias = "user",
		requires = "password"
	)]
	pub(crate) username: Option<String>,
	#[arg(help = "Database authentication password to use when connecting")]
	#[arg(
		env = "SURREAL_PASS",
		short = 'p',
		long = "password",
		visible_alias = "pass",
		requires = "username"
	)]
	pub(crate) password: Option<String>,
	#[arg(help = "Authentication token in JWT format to use when connecting")]
	#[arg(
		env = "SURREAL_TOKEN",
		short = 't',
		long = "token",
		conflicts_with_all = ["username", "password", "auth_level"],
	)]
	pub(crate) token: Option<String>,
	#[arg(help = "Level on which the authenticating user is defined")]
	#[arg(env = "SURREAL_AUTH_LEVEL", long = "auth-level", default_value = "root", requires_all = ["username", "password"])]
	#[arg(value_parser = super::validator::parser::creds_level::CredentialsLevelParser::new())]
	pub(crate) auth_level: CredentialsLevel,
}

#[derive(Args, Debug)]
pub struct DatabaseSelectionArguments {
	#[arg(help = "The namespace selected for the operation")]
	#[arg(env = "SURREAL_NAMESPACE", long = "namespace", visible_alias = "ns")]
	pub(crate) namespace: String,
	#[arg(help = "The database selected for the operation")]
	#[arg(env = "SURREAL_DATABASE", long = "database", visible_alias = "db")]
	pub(crate) database: String,
}

#[derive(Args, Debug)]
pub struct LevelSelectionArguments {
	#[arg(help = "The selected namespace")]
	#[arg(env = "SURREAL_NAMESPACE", long = "namespace", visible_alias = "ns")]
	pub(crate) namespace: Option<String>,
	#[arg(help = "The selected database")]
	#[arg(
		env = "SURREAL_DATABASE",
		long = "database",
		visible_alias = "db",
		requires = "namespace"
	)]
	pub(crate) database: Option<String>,
}

#[derive(Args, Debug)]
pub struct DatabaseConnectionArguments {
	#[arg(help = "Remote database server url to connect to")]
	#[arg(short = 'e', long = "endpoint", visible_aliases = ["conn"])]
	#[arg(default_value = "ws://localhost:8000")]
	#[arg(value_parser = super::validator::endpoint_valid)]
	pub(crate) endpoint: String,
}

#[derive(Args, Debug)]
pub struct OptionalDatabaseConnectionArguments {
	// Endpoint w/o default value
	#[arg(help = "Remote database server url to connect to")]
	#[arg(short = 'e', long = "endpoint", visible_aliases = ["conn"])]
	#[arg(value_parser = super::validator::endpoint_valid)]
	pub(crate) endpoint: Option<String>,
}
