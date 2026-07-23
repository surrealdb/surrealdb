//! # SurrealDB KVS test suite
//!
//! The shared behaviour test suite for SurrealDB key-value store backends.
//! Every backend implementing [`surrealdb_kvs::TransactionBuilder`] and
//! [`surrealdb_kvs::Transactable`] — first-party or external — runs the same
//! set of contract tests through this crate.
//!
//! ## Consuming the suite
//!
//! Add a test target with `harness = false` whose `main` registers one
//! [`TestBackend`] per backend under test and hands them to [`run`]:
//!
//! ```ignore
//! fn main() -> std::process::ExitCode {
//!     let mut backends = Vec::new();
//!     backends.push(TestBackend::new("mem", || async {
//!         TestDs::from_builder(my_builder().await)
//!     }));
//!     surrealdb_kvs_test::run(backends)
//! }
//! ```
//!
//! [`run`] executes every registered test against every backend through a
//! libtest-CLI-compatible harness (filtering, `--list`, `--exact`, and
//! per-test reporting all work under both `cargo test` and `cargo nextest`).
//! Tests are named `{backend}::{module}::{test}`.
//!
//! On targets without threads or a blockable executor — i.e. wasm, where
//! backends like IndexedDB only make progress when control returns to the
//! JavaScript event loop — the libtest harness above is unavailable. Such
//! consumers run the suite through [`run_all`] instead: a single async entry
//! point awaited on the caller's executor (e.g. inside one
//! `#[wasm_bindgen_test]`), reporting per-test progress through a logging
//! callback.
//!
//! ## Writing tests
//!
//! A test is a private `async fn` taking `&TestBackend`, registered with
//! [`kvs_test!`]. Tests run against every backend by default; a test that
//! exercises backend-specific behaviour opts in or out by backend name.
//! Names are an open vocabulary: a name that no consumer registers simply
//! never matches, so tests may reference backends that live in other
//! repositories (e.g. `surrealds`).
//!
//! ```ignore
//! async fn same_key_conflict(b: &TestBackend) { /* ... */ }
//! kvs_test!(same_key_conflict, except = [tikv]);
//! ```
// Test-support crate: unwrap/expect are the assertion style in behaviour tests.
#![allow(clippy::unwrap_used)]

use std::any::Any;
use std::future::Future;
#[cfg(not(target_family = "wasm"))]
use std::process::ExitCode;
#[cfg(not(target_family = "wasm"))]
use std::sync::{Arc, Mutex};

#[cfg(not(target_family = "wasm"))]
use libtest_mimic::{Arguments, Trial};
use surrealdb_kvs::api::BoxFut;
use surrealdb_kvs::err::Result;
use surrealdb_kvs::{Transactable, TransactionBuilder, TransactionType};

pub mod builder_surface;
pub mod defaults;
pub mod edges;
pub mod lifecycle;
pub mod multi;
pub mod raw;
pub mod savepoint;
pub mod snapshot;
pub mod timestamp;
pub mod versioned;

/// A backend datastore under test, wrapping the boxed [`TransactionBuilder`]
/// the backend produced.
pub struct TestDs {
	builder: Box<dyn TransactionBuilder>,
	/// Keeps backend-scoped resources alive for the duration of the test,
	/// e.g. the temporary directory of an on-disk store or an in-process
	/// test cluster.
	_guard: Option<Box<dyn Any + Send + Sync>>,
}

impl TestDs {
	/// Wrap a backend's transaction builder.
	pub fn from_builder(builder: Box<dyn TransactionBuilder>) -> Self {
		Self {
			builder,
			_guard: None,
		}
	}

	/// Wrap a backend's transaction builder, keeping `guard` alive for the
	/// lifetime of the datastore.
	pub fn from_builder_with_guard(
		builder: Box<dyn TransactionBuilder>,
		guard: impl Any + Send + Sync,
	) -> Self {
		Self {
			builder,
			_guard: Some(Box::new(guard)),
		}
	}

	/// Start a new transaction on the underlying backend.
	pub async fn transaction(&self, write: TransactionType) -> Result<Box<dyn Transactable>> {
		let (tx, _) = self.builder.new_transaction(write).await?;
		Ok(tx)
	}

	/// Start a new transaction, also returning the backend's "local" flag.
	pub async fn transaction_with_locality(
		&self,
		write: TransactionType,
	) -> Result<(Box<dyn Transactable>, bool)> {
		self.builder.new_transaction(write).await
	}

	/// Access the underlying transaction builder (metrics, shutdown,
	/// extensions).
	pub fn builder(&self) -> &dyn TransactionBuilder {
		&*self.builder
	}
}

/// A backend registered by the consumer: a name, a factory producing a fresh
/// [`TestDs`] per test, and scheduling options.
pub struct TestBackend {
	name: &'static str,
	serial: bool,
	factory: Box<dyn Fn() -> BoxFut<'static, TestDs> + Send + Sync>,
}

impl TestBackend {
	/// Register a backend under `name` with a factory producing a fresh
	/// datastore per test.
	///
	/// The name is what tests' `only`/`except` lists match against, and
	/// prefixes the generated test names (`{name}::{module}::{test}`).
	pub fn new<F, Fut>(name: &'static str, factory: F) -> Self
	where
		F: Fn() -> Fut + Send + Sync + 'static,
		Fut: Future<Output = TestDs> + Send + 'static,
	{
		Self {
			name,
			serial: false,
			factory: Box::new(move || Box::pin(factory())),
		}
	}

	/// Run this backend's tests one at a time.
	///
	/// For backends whose datastores share external state between tests
	/// (e.g. a single TiKV cluster wiped by the factory). Under `cargo
	/// nextest` prefer a serial test group; this flag additionally protects
	/// plain `cargo test` runs.
	pub fn serial(mut self) -> Self {
		self.serial = true;
		self
	}

	/// The name this backend was registered under.
	pub fn name(&self) -> &'static str {
		self.name
	}

	/// Construct a fresh datastore for one test.
	pub async fn create_ds(&self) -> TestDs {
		(self.factory)().await
	}
}

/// The boxed future a registered test returns.
pub type TestFut<'a> = BoxFut<'a, ()>;

/// A single registered behaviour test.
pub struct KvsTest {
	/// `module_path!()` of the registration site; the crate-name prefix is
	/// stripped for display.
	pub module: &'static str,
	/// Backend names this test is restricted to. Empty = all backends.
	pub only: &'static [&'static str],
	/// Backend names this test does not apply to.
	pub except: &'static [&'static str],
	/// The test entry point.
	pub run: for<'a> fn(&'a TestBackend) -> TestFut<'a>,
}

// Every test registered via [`kvs_test!`], across all linked crates.
inventory::collect!(KvsTest);

/// Whether a test applies to the backend with the given name.
pub fn applies(backend: &str, test: &KvsTest) -> bool {
	if !test.only.is_empty() && !test.only.contains(&backend) {
		return false;
	}
	!test.except.contains(&backend)
}

/// Register an `async fn(&TestBackend)` in the same module as a behaviour
/// test.
///
/// ```ignore
/// async fn get(b: &TestBackend) { /* ... */ }
/// kvs_test!(get);
///
/// // Restricted to, or excluded from, specific backends by name:
/// kvs_test!(same_key_lww, only = [tikv]);
/// kvs_test!(same_key_conflict, except = [tikv, surrealds]);
/// ```
#[macro_export]
macro_rules! kvs_test {
	($name:ident) => {
		$crate::kvs_test!(@register $name, only: [], except: []);
	};
	($name:ident, only = [$($only:ident),* $(,)?]) => {
		$crate::kvs_test!(@register $name, only: [$($only)*], except: []);
	};
	($name:ident, except = [$($except:ident),* $(,)?]) => {
		$crate::kvs_test!(@register $name, only: [], except: [$($except)*]);
	};
	(@register $name:ident, only: [$($only:ident)*], except: [$($except:ident)*]) => {
		// A module named after the test function (separate namespaces), so
		// `module_path!()` inside it yields the full display path of the test.
		mod $name {
			fn run(b: &$crate::TestBackend) -> $crate::TestFut<'_> {
				::std::boxed::Box::pin(super::$name(b))
			}

			::inventory::submit!{
				$crate::KvsTest {
					module: ::core::module_path!(),
					only: &[$(::core::stringify!($only)),*],
					except: &[$(::core::stringify!($except)),*],
					run,
				}
			}
		}
	};
}

/// Run every registered test against every given backend, reporting through
/// a libtest-compatible harness. Call from the `main` of a `harness = false`
/// test target and return the exit code.
#[cfg(not(target_family = "wasm"))]
pub fn run(backends: Vec<TestBackend>) -> ExitCode {
	let args = Arguments::from_args();

	let runtime = Arc::new(
		tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()
			.expect("failed to build the tokio runtime"),
	);

	let mut trials =
		Vec::with_capacity(backends.len() * inventory::iter::<KvsTest>.into_iter().count());
	for backend in backends {
		let backend = Arc::new(backend);

		// One lock per serial backend: its tests still interleave with other
		// backends' tests, but never with each other.
		let serial_lock = backend.serial.then(|| Arc::new(Mutex::new(())));

		for test in inventory::iter::<KvsTest> {
			// The registration module path minus the defining crate's name.
			let path = test.module.split_once("::").map(|(_, rest)| rest).unwrap_or(test.module);
			let name = format!("{}::{}", backend.name, path);
			let runnable = applies(backend.name, test);

			let backend = Arc::clone(&backend);
			let runtime = Arc::clone(&runtime);

			let serial_lock = serial_lock.as_ref().map(Arc::clone);
			trials.push(
				Trial::test(name, move || {
					// A panicking test poisons the lock; later tests should
					// still run, so ignore the poison.
					let _serial =
						serial_lock.as_ref().map(|l| l.lock().unwrap_or_else(|e| e.into_inner()));
					runtime.block_on((test.run)(&backend));
					Ok(())
				})
				.with_ignored_flag(!runnable),
			);
		}
	}

	libtest_mimic::run(&args, trials).exit_code()
}

// The wasm linker collects module constructors (including `inventory`
// registrations) into this synthesized function but only calls it on its
// own under "command-style linkage", which the wasm-bindgen-test harness
// does not have — without an explicit call the test registry stays empty.
#[cfg(target_family = "wasm")]
unsafe extern "C" {
	fn __wasm_call_ctors();
}

/// Run every registered test against every given backend sequentially,
/// awaiting each on the caller's executor.
///
/// This is the entry point for targets where [`run`] is unavailable, i.e.
/// wasm: call it from a single `#[wasm_bindgen_test]` so the tests are driven
/// by the JavaScript event loop. Per-test progress lines go through `log`. A
/// failing test panics, ending the whole run (wasm has no unwinding); the
/// test is identified by the `running` line logged immediately before it.
///
/// Returns the number of tests executed, so callers can assert the
/// registry was populated (`inventory` collection relies on
/// life-before-main constructors, which a runner could silently skip).
pub async fn run_all(backends: &[TestBackend], log: impl Fn(&str)) -> usize {
	// Populate the `inventory` registry. Constructors are idempotent, but
	// only run them once in case other, non-idempotent constructors are
	// linked in.
	#[cfg(target_family = "wasm")]
	{
		static CTORS: std::sync::Once = std::sync::Once::new();
		CTORS.call_once(|| unsafe { __wasm_call_ctors() });
	}

	let mut executed = 0;
	for backend in backends {
		for test in inventory::iter::<KvsTest> {
			// The registration module path minus the defining crate's name.
			let path = test.module.split_once("::").map(|(_, rest)| rest).unwrap_or(test.module);
			let name = format!("{}::{}", backend.name, path);
			if !applies(backend.name, test) {
				log(&format!("skipped {name}"));
				continue;
			}
			log(&format!("running {name}"));
			(test.run)(backend).await;
			log(&format!("ok      {name}"));
			executed += 1;
		}
	}
	executed
}
