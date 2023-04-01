use clap::Args;

#[derive(Args, Debug)]
pub(crate) struct AuthArguments {
	#[arg(help = "Database authentication username to use when connecting")]
	#[arg(env = "SURREAL_USER", short = 'u', long = "username", visible_alias = "user")]
	#[arg(default_value = "root")]
	pub(crate) username: String,
	#[arg(help = "Database authentication password to use when connecting")]
	#[arg(short = 'p', long = "password", visible_alias = "pass")]
	#[arg(env = "SURREAL_PASS", default_value = "root")]
	pub(crate) password: String,
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
pub struct DatabaseConnectionArguments {
	#[arg(help = "Remote database server url to connect to")]
	#[arg(short = 'c', long = "connection-url", visible_aliases = ["host", "conn"])]
	#[arg(default_value = "https://cloud.surrealdb.com")]
	#[arg(value_parser = super::validator::conn_valid)]
	pub(crate) connection_url: String,
}
