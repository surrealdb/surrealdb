//! The plug-in point for datastore backends: a [`BackendProvider`] claims one
//! or more connection-path schemes and constructs the matching
//! [`TransactionBuilder`] on demand.

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionBuilder;
use surrealdb_kvs::api::BoxFut;
use surrealdb_kvs::err::Result;
use tokio_util::sync::CancellationToken;

/// Everything a provider needs to construct a backend.
pub struct ConnectContext<'a> {
	/// The scheme that matched one of the provider's [`BackendProvider::schemes`],
	/// as written by the user (e.g. `mem`, `rocksdb`).
	pub scheme: &'a str,
	/// The path component after the scheme, with the query string stripped and
	/// absolute paths normalised to a single leading slash. Empty when the
	/// connection path was a bare scheme (e.g. `memory`).
	pub path: &'a str,
	/// Token for graceful shutdown of long-running backend operations.
	pub canceller: CancellationToken,
	/// Configuration, with any `?key=value` query parameters from the
	/// connection path merged in under `datastore_`-prefixed keys.
	pub config: ConfigMap,
}

/// A factory for a family of storage backends, registered into
/// [`Backends`](crate::Backends) under the schemes it claims.
///
/// Providers let external crates plug custom backends into the same
/// connection-path dispatch used by the first-party engines: implement this
/// trait, register it with [`Backends::register`](crate::Backends::register),
/// and the scheme becomes constructible and passes path validation.
pub trait BackendProvider: Send + Sync {
	/// The scheme names this provider claims, e.g. `["memory", "mem"]`.
	fn schemes(&self) -> &[&'static str];

	/// Whether a bare scheme with no `:`/`://` separator (e.g. `memory`) is a
	/// valid connection path for this provider.
	fn accepts_bare(&self) -> bool {
		false
	}

	/// Construct the backend.
	///
	/// This may perform arbitrarily heavy asynchronous startup; the returned
	/// builder must be ready for use.
	fn connect<'a>(
		&'a self,
		ctx: ConnectContext<'a>,
	) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>>;
}
