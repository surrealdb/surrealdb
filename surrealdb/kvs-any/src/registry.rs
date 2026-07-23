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
/// paths (e.g. `memory`, `rocksdb://path`, `tikv://...`) to the provider that
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

	/// Parse a datastore connection path (e.g. `memory`, `rocksdb://path`,
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

		let config = if let Some(config_string) = config_string {
			config.join(
				ConfigMap::from_config_string(config_string).map_keys(|x| format!("datastore_{x}")),
			)
		} else {
			config
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
				"The `file://` scheme is no longer supported; use `rocksdb://` or `surrealkv://` instead"
					.into(),
			));
		}

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
			// Keep the targeted error for first-party schemes whose backend
			// feature is not enabled in this build.
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
		// The legacy `file:` scheme is accepted here for the same reason.
		let (scheme, bare) =
			match scheme_part.split_once("://").or_else(|| scheme_part.split_once(':')) {
				Some((scheme, _)) => (scheme, false),
				None => (scheme_part, true),
			};
		let known = scheme == "file"
			|| self.providers.iter().flat_map(|x| x.schemes()).any(|x| *x == scheme);
		let bare_ok = !bare || matches!(scheme, "memory" | "mem");
		if known && bare_ok {
			return Ok(v.to_string());
		}
		Err(Error::Datastore("Provide a valid database path parameter".to_owned()))
	}
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
		assert!(backends.path_valid("rocksdb:/tmp/db").is_err());
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
}
