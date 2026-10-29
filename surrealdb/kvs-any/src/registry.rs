//! The backend registry: parses a datastore connection path and dispatches
//! construction to the [`BackendProvider`] that claims the scheme.

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionBuilder;
use surrealdb_kvs::err::{Error, Result};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::provider::{BackendProvider, ConnectContext};

const TARGET: &str = "surrealdb::core::kvs::ds";

/// A registry of storage backend providers, dispatching datastore connection
/// paths (e.g. `memory`, `surrealkv://path`) to the provider that
/// claims the scheme.
///
/// [`Backends::community`] returns the first-party engines enabled by `kv-*`
/// cargo features; embedders can [`register`](Backends::register) additional
/// providers on top. Providers may borrow from the caller (`'a`), so a
/// registry can be assembled on the stack around short-lived context.
pub struct Backends<'a> {
	providers: Vec<Box<dyn BackendProvider + 'a>>,
}

impl Backends<'_> {
	/// A registry with no providers. Construction and validation fail with a
	/// "no storage engine" error until providers are registered.
	pub fn empty() -> Self {
		Self {
			providers: Vec::new(),
		}
	}
}

impl<'a> Backends<'a> {
	/// Register an additional backend provider.
	///
	/// Providers are consulted in registration order; the first one claiming a
	/// scheme wins.
	pub fn register(&mut self, provider: impl BackendProvider + 'a) {
		self.providers.push(Box::new(provider));
	}

	/// Find the provider claiming the given scheme.
	fn find(&self, scheme: &str) -> Option<&dyn BackendProvider> {
		self.providers.iter().find(|p| p.schemes().contains(&scheme)).map(|p| &**p)
	}

	/// Parse a datastore connection path (e.g. `memory`, `surrealkv://path`,
	/// optionally with `?key=value` configuration parameters) and construct
	/// the matching storage backend through its registered provider.
	pub async fn new_transaction_builder(
		&self,
		path: &str,
		canceller: CancellationToken,
		config: ConfigMap,
	) -> Result<Box<dyn TransactionBuilder>> {
		if self.providers.is_empty() {
			return Err(Error::Datastore(
				"No storage engine is enabled in this build of SurrealDB".to_owned(),
			));
		}

		// Extract query parameters from the path before scheme extraction
		let (raw_path, config_string) = match path.split_once('?') {
			Some((p, q)) => (p, Some(q)),
			None => (path, None),
		};

		// Extract the scheme and path components
		let (scheme, path) = match raw_path.split_once("://").or_else(|| raw_path.split_once(':')) {
			Some((scheme, path)) => (scheme, path),
			// A bare scheme is only a valid path when its provider opts in
			// (e.g. `memory`).
			None if self.find(raw_path).is_some_and(|p| p.accepts_bare()) => (raw_path, ""),
			// Known bare-accepting first-party schemes fall through so
			// construction reports the targeted "not enabled" error below.
			None if matches!(raw_path, "memory" | "mem") => (raw_path, ""),
			// Validated already in the CLI, should never happen
			None => {
				return Err(Error::Internal("Provide a valid database path parameter".to_owned()));
			}
		};

		// The `file:` scheme has been removed. Catch it here so users with
		// legacy paths get a targeted message instead of the generic fallback
		// below.
		if scheme == "file" {
			return Err(Error::Datastore(
				"The `file://` scheme is no longer supported; use `surrealkv://` instead".into(),
			));
		}

		// Versioning is a SurrealKV configuration parameter rather than a scheme
		// of its own, so the `+versioned` spelling names no backend. Caught here
		// because the generic fallback below would report only that the datastore
		// could not be loaded, which reads as a build without the backend.
		if scheme == "surrealkv+versioned" {
			return Err(Error::Datastore(
				"The `surrealkv+versioned://` scheme is no longer supported; use `surrealkv://<path>?versioned=true` instead"
					.into(),
			));
		}

		let config = match config_string {
			Some(config_string) => config.join(path_params(scheme, config_string)),
			None => config,
		};

		let path = if path.starts_with("/") {
			// if absolute, remove all slashes except one
			let normalised = format!("/{}", path.trim_start_matches("/"));
			info!(target: TARGET, "Starting kvs store at absolute path {scheme}:{normalised}");
			normalised
		} else if path.is_empty() {
			info!(target: TARGET, "Starting kvs store in {scheme}");
			"".to_string()
		} else {
			info!(target: TARGET, "Starting kvs store at relative path {scheme}://{path}");
			path.to_string()
		};

		let Some(provider) = self.find(scheme) else {
			let schemes = self
				.providers
				.iter()
				.flat_map(|x| x.schemes())
				.map(|x| format!("`{x}`"))
				.collect::<Vec<_>>()
				.join(",");
			// The TiKV backend is built from its own repository, so a build
			// without it names where to get it rather than reporting a
			// datastore that could not be loaded.
			if scheme == "tikv" {
				return Err(Error::Datastore(
					"The `tikv://` scheme is not enabled in this build of SurrealDB. The TiKV storage backend is built from https://github.com/surrealdb/surrealdb-tikv".into(),
				));
			}
			// The datastore path is not valid
			info!(target: TARGET, "Unable to load the specified datastore {scheme}:{path}, this build supports {schemes}", );
			return Err(Error::Datastore("Unable to load the specified datastore".into()));
		};

		provider
			.connect(ConnectContext {
				scheme,
				path: &path,
				canceller,
				config,
			})
			.await
	}

	/// Validate a datastore connection path string against the registered and
	/// first-party schemes.
	///
	/// This is a cheap shape check used by the CLI before startup; scheme
	/// availability is verified at construction time, where a disabled
	/// first-party backend produces a targeted error.
	pub fn path_valid(&self, v: &str) -> Result<String> {
		if self.providers.is_empty() {
			return Err(Error::Datastore(
				"No storage engine is enabled in this build of SurrealDB".to_owned(),
			));
		}
		// Strip query parameters before validating the scheme
		let scheme_part = v.split_once('?').map(|(s, _)| s).unwrap_or(v);
		// A registered provider claiming the scheme validates it
		for provider in &self.providers {
			for scheme in provider.schemes() {
				let bare_valid = provider.accepts_bare() && scheme_part == *scheme;
				let prefixed = scheme_part
					.strip_prefix(scheme)
					.is_some_and(|rest| rest.starts_with(':') || rest.starts_with("://"));
				if bare_valid || prefixed {
					return Ok(v.to_string());
				}
			}
		}
		// First-party schemes stay valid even when their feature is compiled
		// out, so construction can report the targeted "not enabled" error.
		// The removed `file:` and `surrealkv+versioned:` schemes, and the `tikv:` scheme of the
		// backend built from its own repository, are accepted here for the same reason:
		// construction names their replacement.
		let (scheme, bare) =
			match scheme_part.split_once("://").or_else(|| scheme_part.split_once(':')) {
				Some((scheme, _)) => (scheme, false),
				None => (scheme_part, true),
			};
		let known = matches!(scheme, "file" | "surrealkv+versioned" | "tikv")
			|| self.providers.iter().flat_map(|x| x.schemes()).any(|x| *x == scheme);
		let bare_ok = !bare || matches!(scheme, "memory" | "mem");
		if known && bare_ok {
			return Ok(v.to_string());
		}
		Err(Error::Datastore("Provide a valid database path parameter".to_owned()))
	}
}

/// The configuration a `?key=value` query on a datastore path carries.
///
/// Each parameter is entered twice: as `datastore_<key>`, the settings every
/// backend shares (`versioned`, `retention`, `sync`), and as `<scheme>_<key>`,
/// the backend's own settings, which the server reads from
/// `SURREAL_<SCHEME>_<KEY>`. So `surrealkv://data?block_cache_capacity=…`
/// sets `surrealkv_block_cache_capacity`. Joined over the caller's
/// configuration, a parameter on the path wins over the same key set
/// elsewhere.
///
/// The result reaches only the storage backend. The datastore's own runtime
/// configuration is read from the caller's map, so a path parameter cannot
/// change it.
fn path_params(scheme: &str, query: &str) -> ConfigMap {
	let params = ConfigMap::from_config_string(query);
	params
		.clone()
		.map_keys(|x| format!("datastore_{x}"))
		.join(params.map_keys(|x| format!("{scheme}_{x}")))
}

#[cfg(test)]
mod tests {
	use surrealdb_kvs::TransactionBuilder;
	use surrealdb_kvs::api::BoxFut;
	use surrealdb_kvs::err::Result;

	use super::Backends;
	use crate::provider::{BackendProvider, ConnectContext};

	/// A provider claiming an external scheme, mimicking how an embedder
	/// (e.g. the enterprise distributed store) plugs into the registry.
	struct ExternalProvider;

	impl BackendProvider for ExternalProvider {
		fn schemes(&self) -> &[&'static str] {
			&["ds-mem"]
		}

		fn accepts_bare(&self) -> bool {
			true
		}

		fn connect<'a>(
			&'a self,
			_ctx: ConnectContext<'a>,
		) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
			unimplemented!("not constructed in these tests")
		}
	}

	#[test]
	fn empty_registry_rejects_all_paths() {
		let backends = Backends::empty();
		assert!(backends.path_valid("memory").is_err());
		assert!(backends.path_valid("surrealkv:/tmp/db").is_err());
	}

	#[test]
	fn registered_provider_schemes_validate() {
		let mut backends = Backends::empty();
		backends.register(ExternalProvider);
		// Bare and prefixed forms of the provider's scheme
		assert!(backends.path_valid("ds-mem").is_ok());
		assert!(backends.path_valid("ds-mem:").is_ok());
		assert!(backends.path_valid("ds-mem://.").is_ok());
		assert!(backends.path_valid("ds-mem?opt=1").is_ok());
	}

	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn unknown_scheme_reports_targeted_errors() {
		use surrealdb_cnf::ConfigMap;
		use tokio_util::sync::CancellationToken;

		let backends = Backends::community();
		// A first-party scheme whose feature may be disabled reports the
		// targeted message; an unknown scheme reports the generic one.
		let Err(err) = backends
			.new_transaction_builder("unknown:path", CancellationToken::new(), ConfigMap::empty())
			.await
		else {
			panic!("expected an error for an unknown scheme")
		};
		assert!(err.to_string().contains("Unable to load the specified datastore"));
		let Err(err) = backends
			.new_transaction_builder("file:/tmp/db", CancellationToken::new(), ConfigMap::empty())
			.await
		else {
			panic!("expected an error for the legacy `file:` scheme")
		};
		assert!(err.to_string().contains("no longer supported"));
	}

	/// The removed `surrealkv+versioned:` scheme names its replacement rather
	/// than reporting a datastore that could not be loaded, which reads as a
	/// build without the SurrealKV backend.
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn versioned_surrealkv_scheme_names_its_replacement() {
		use surrealdb_cnf::ConfigMap;
		use tokio_util::sync::CancellationToken;

		let backends = Backends::community();
		// Validation accepts it so construction is reached, and construction is
		// where the replacement is named.
		assert!(backends.path_valid("surrealkv+versioned://data").is_ok());
		let Err(err) = backends
			.new_transaction_builder(
				"surrealkv+versioned://data",
				CancellationToken::new(),
				ConfigMap::empty(),
			)
			.await
		else {
			panic!("expected an error for the removed `surrealkv+versioned:` scheme")
		};
		assert!(err.to_string().contains("versioned=true"), "got: {err}");
	}

	/// A build without the TiKV backend names where it is built from rather than
	/// reporting a datastore that could not be loaded.
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn tikv_scheme_names_its_repository() {
		use surrealdb_cnf::ConfigMap;
		use tokio_util::sync::CancellationToken;

		let backends = Backends::community();
		assert!(backends.path_valid("tikv://127.0.0.1:2379").is_ok());
		let Err(err) = backends
			.new_transaction_builder(
				"tikv://127.0.0.1:2379",
				CancellationToken::new(),
				ConfigMap::empty(),
			)
			.await
		else {
			panic!("expected an error for the `tikv:` scheme")
		};
		assert!(err.to_string().contains("surrealdb-tikv"), "got: {err}");
	}

	/// A path parameter reaches the backend under the shared `datastore_` key
	/// and under its own scheme's key, and wins over the caller's value for
	/// the same key.
	#[tokio::test]
	async fn path_params_reach_the_backend_under_both_spellings() {
		use std::sync::Mutex;

		use surrealdb_cnf::ConfigMap;
		use surrealdb_kvs::err::Error;
		use tokio_util::sync::CancellationToken;

		struct Recording<'a>(&'a Mutex<Option<ConfigMap>>);

		impl BackendProvider for Recording<'_> {
			fn schemes(&self) -> &[&'static str] {
				&["rec"]
			}

			fn connect<'a>(
				&'a self,
				ctx: ConnectContext<'a>,
			) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
				*self.0.lock().unwrap() = Some(ctx.config);
				Box::pin(async { Err(Error::Datastore("recorded".into())) })
			}
		}

		let seen = Mutex::new(None);
		let mut backends = Backends::empty();
		backends.register(Recording(&seen));
		let caller = ConfigMap::empty()
			.with_key_value("rec_block_cache_capacity", "1")
			.with_key_value("rec_max_memtable_size", "7");
		let _ = backends
			.new_transaction_builder(
				"rec://data?block_cache_capacity=2&versioned=true",
				CancellationToken::new(),
				caller,
			)
			.await;
		let config = seen.lock().unwrap().take().expect("the provider was reached");

		let mut capacity = 0u64;
		let mut memtable = 0u64;
		let mut shared = 0u64;
		let mut versioned = false;
		config
			.parse_key("rec_block_cache_capacity", &mut capacity)
			.parse_key("rec_max_memtable_size", &mut memtable)
			.parse_key("datastore_block_cache_capacity", &mut shared)
			.parse_key("datastore_versioned", &mut versioned);
		assert_eq!(capacity, 2, "the path wins over the caller's value");
		assert_eq!(memtable, 7, "a key the path does not set is kept");
		assert_eq!(shared, 2);
		assert!(versioned);
	}
}
