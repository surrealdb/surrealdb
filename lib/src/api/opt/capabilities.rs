//! The capabilities that can be enabled for a database instance

/// Capabilities are used to limit what a user can do to the system.
///
/// Capabilities are split into 4 categories:
/// - Scripting: Whether or not the user can execute scripts
/// - Guest access: Whether or not a non-authenticated user can execute queries on the system when authentication is enabled.
/// - Functions: Whether or not the user can execute certain functions
/// - Network: Whether or not the user can access certain network addresses
///
/// Capabilities are configured globally. By default, capabilities are configured as:
/// - Scripting: false
/// - Guest access: false
/// - Functions: All functions are allowed
/// - Network: No network address is allowed nor denied, hence all network addresses are denied unless explicitly allowed
///
/// The capabilities are defined using allow/deny lists for fine-grained control.
///
/// Examples:
/// - Allow all functions: `--allow-funcs`
/// - Allow all functions except `http.*`: `--allow-funcs --deny-funcs 'http.*'`
/// - Allow all network addresses except AWS metadata endpoint: `--allow-net --deny-net='169.254.169.254'`
///
/// # Examples
///
/// Create a new instance, and allow all capabilities
#[cfg_attr(feature = "kv-rocksdb", doc = "```no_run")]
#[cfg_attr(not(feature = "kv-rocksdb"), doc = "```ignore")]
/// # use surrealdb::opt::capabilities::Capabilities;
/// # use surrealdb::opt::Config;
/// # use surrealdb::Surreal;
/// # use surrealdb::engine::local::File;
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let capabilities = Capabilities::all();
/// let config = Config::default().capabilities(capabilities);
/// let db = Surreal::new::<File>(("temp.db", config)).await?;
/// # Ok(())
/// # }
/// ```
///
/// Create a new instance, and allow certain functions
#[cfg_attr(feature = "kv-rocksdb", doc = "```no_run")]
#[cfg_attr(not(feature = "kv-rocksdb"), doc = "```ignore")]
/// # use std::str::FromStr;
/// # use surrealdb::engine::local::File;
/// # use surrealdb::opt::capabilities::Capabilities;
/// # use surrealdb::opt::capabilities::FuncTarget;
/// # use surrealdb::opt::capabilities::Targets;
/// # use surrealdb::opt::Config;
/// # use surrealdb::Surreal;
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let capabilities = Capabilities::default()
///     .with_functions(Targets::<FuncTarget>::All)
///     .without_functions(Targets::<FuncTarget>::Some(
///         [FuncTarget::from_str("http::*").unwrap()].into(),
///     ));
/// let config = Config::default().capabilities(capabilities);
/// let db = Surreal::new::<File>(("temp.db", config)).await?;
/// # Ok(())
/// # }
/// ```
pub use crate::dbs::Capabilities;

pub use crate::dbs::capabilities::FuncTarget;
pub use crate::dbs::capabilities::NetTarget;
pub use crate::dbs::capabilities::Targets;
