use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use surrealdb_core::channel;
use surrealdb_cnf::ConfigMap;
use surrealdb_core::dbs::Capabilities;
use surrealdb_core::dbs::capabilities::Targets;
use surrealdb_core::kvs::Datastore;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::cli::Backend;
use crate::cmd::util;
use crate::tests::schema::{BoolOr, TestEnv};
use crate::util::{get_timestamp, xorshift};

struct CreateInfo {
	id_gen: AtomicUsize,
	backend: Backend,
	dir: Option<String>,
}

impl CreateInfo {
	pub async fn new(backend: Backend) -> Result<Self> {
		if let Backend::Memory = backend {
			return Ok(CreateInfo {
				id_gen: AtomicUsize::new(0),
				backend,
				dir: None,
			});
		}
		let temp_dir = std::env::temp_dir();
		let mut state = get_timestamp();

		let rand = xorshift(&mut state);
		let mut dir = temp_dir.join(format!("surreal_lang_tests_{rand}"));

		while tokio::fs::metadata(&dir).await.is_ok() {
			let rand = xorshift(&mut state);
			dir = temp_dir.join(format!("surreal_lang_tests_{rand}"));
		}

		tokio::fs::create_dir(&dir).await?;

		println!(" Using '{}' as temporary directory for datastores", dir.display());

		Ok(CreateInfo {
			id_gen: AtomicUsize::new(0),
			backend,
			dir: Some(dir.to_str().unwrap().to_string()),
		})
	}

	pub async fn produce_ds(&self, versioned: bool, cap: Capabilities) -> Result<Ds> {
		let mut path = None;

		let allows_live = cap.allows_live_query_notifications();

		// Enable a temporary directory so TEMPFILES queries route to the
		// ExternalSort operator. For backends that already provision a
		// per-run scratch dir we reuse it (cleaned up in `shutdown`); the
		// Memory backend has none, so fall back to the system temp dir —
		// `ext_sort` namespaces each invocation under its own `TempDir`
		// which cleans up on drop, so no leak there either.
		let sort_temp_dir = match self.dir.as_deref() {
			Some(d) => std::path::PathBuf::from(d),
			None => std::env::temp_dir(),
		};
		// File access (e.g. `DEFINE ANALYZER ... mapper('<path>')`) is denied by
		// default unless `file_allowlist` is configured. Tests that read mapper
		// data files reference them under the `tests` tree, so allow it.
		//
		// `"../tests"` is resolved relative to the test runner's CWD and
		// canonicalized at parse time, which assumes the harness is invoked as
		// `cd language-tests && cargo run run` (the documented entrypoint). If
		// the harness is ever run from a different CWD, `../tests` fails to
		// canonicalize, `extract_allowed_paths` drops it (with only a `warn!`
		// log, which the test runner does not surface by default), and the
		// now-empty allowlist denies *all* `mapper()` access — every mapper test
		// would then fail with `File access denied`. This also sets a single
		// global allowlist shared by every language test.
		let builder = Datastore::builder()
			.with_capabilities(cap)
			.with_auth(true)
			.with_temporary_directory(Some(sort_temp_dir))
			.with_config(ConfigMap::empty().with_key_value("file_allowlist", "../tests"));

		let builder = if allows_live {
			let (send, _) = channel::bounded(15_000);
			builder.with_notify(send)
		} else {
			builder
		};

		let ds = match self.backend {
			Backend::Memory => {
				if versioned {
					builder.build_with_path("mem://?versioned=true&retention=1h").await?
				} else {
					builder.build_with_path("mem://").await?
				}
			}
			Backend::RocksDb => {
				let p = self.produce_path();
				let ds = if versioned {
					builder
						.build_with_path(&format!("rocksdb://{p}?versioned=true&retention=1h"))
						.await?
				} else {
					builder.build_with_path(&format!("rocksdb://{p}")).await?
				};
				path = Some(p);
				ds
			}
			Backend::SurrealKv => {
				let p = self.produce_path();
				let ds = if versioned {
					builder
						.build_with_path(&format!("surrealkv://{p}?versioned=true&retention=1h"))
						.await?
				} else {
					builder.build_with_path(&format!("surrealkv://{p}")).await?
				};
				path = Some(p);
				ds
			}
			Backend::TikV => {
				let p = "127.0.0.1:2379";
				let ds = builder.build_with_path(&format!("tikv://{p}")).await?;
				// Every TiKV datastore in a run aliases the one physical cluster, so a
				// freshly built handle still sees whatever earlier tests left behind.
				// Physically drop the entire keyspace out-of-transaction; `bootstrap`
				// below then re-seeds the node keys. See `reset_storage`.
				ds.unsafe_destroy_range((vec![0u8]..vec![0xffu8]).into()).await?;
				ds
			}
		};

		ds.bootstrap().await?;

		Ok(Ds {
			store: Arc::new(ds),
			path,
		})
	}

	/// Reset a reused datastore's storage between tests.
	///
	/// Only TiKV needs this. Every TiKV datastore in a run aliases the one
	/// physical cluster, so the reused base datastore can observe keys left
	/// behind by `clean`/create-path tests — whose post-run cleanup the harness
	/// does not verify — and the next base test then fails the retained-key
	/// check on that foreign data. Embedded backends give each datastore its
	/// own directory, so there is nothing to reset, hence the no-op.
	///
	/// We physically drop the keys via `unsafe_destroy_range` rather than a
	/// transactional `delr`: the wipe runs serially (`--jobs 1`) with no open
	/// transaction, so the MVCC-bypass hazards do not apply, and it avoids both
	/// the `delr` key cap and the tombstone build-up of wiping on every test.
	///
	/// Crucially we wipe *around* the node/bootstrap keys rather than the whole
	/// keyspace: `/!ic` (index-compaction queue), `/!nd` (cluster membership)
	/// and `/!nh` / `/!ni` (namespace-ID generator) live contiguously in
	/// `[/!ic, /!ns)`, and nothing a test creates falls in that band (tests
	/// create `/!ac`, `/!cg`, `/!eq` below it and `/!ns`, `/!tl`, `/!us` plus
	/// the `/*` data keys at or above it). Preserving that block keeps this
	/// long-lived datastore registered, so we skip the per-test `bootstrap`
	/// (three node-registry transactions) that a full wipe would force —
	/// these are the same prefixes the retained-key check whitelists.
	async fn reset_storage(&self, ds: &Datastore) -> Result<()> {
		if let Backend::TikV = self.backend {
			ds.unsafe_destroy_range((vec![0u8]..b"/!ic".to_vec()).into()).await?;
			ds.unsafe_destroy_range((b"/!ns".as_slice()..[0xffu8].as_slice()).into()).await?;
		}
		Ok(())
	}

	fn produce_path(&self) -> String {
		let path = self.dir.as_ref().unwrap();

		let id = self.id_gen.fetch_add(1, Ordering::AcqRel);

		let path = Path::new(path).join(format!("store_{id}"));
		path.to_str().unwrap().to_owned()
	}
}

pub struct Ds {
	/// The store itself. Held in an `Arc` because GraphQL schema generation
	/// (and the resolvers it produces) capture a clone of the datastore.
	store: Arc<Datastore>,
	/// The path where you can find the store, none if the store is in-memory
	path: Option<String>,
}

#[must_use]
pub struct Provisioner {
	send: Sender<Ds>,
	recv: Receiver<Ds>,
	grade_send: Sender<Box<Datastore>>,
	grade_recv: Receiver<Box<Datastore>>,
	create_info: Arc<CreateInfo>,
}

enum PermitInner {
	Reuse {
		ds: Ds,
		channel: Sender<Ds>,
	},
	Create {
		versioned: bool,
		capabilities: Box<Capabilities>,
	},
}

pub enum CanReuse {
	Reusable,
	Reset,
}

#[must_use]
pub struct Permit {
	info: Arc<CreateInfo>,
	grade_send: Sender<Box<Datastore>>,
	grade_ds: Box<Datastore>,
	inner: PermitInner,
}

impl Permit {
	pub async fn with<F: AsyncFnOnce(&Arc<Datastore>, &Box<Datastore>) -> (CanReuse, R), R>(
		self,
		f: F,
	) -> Result<R> {
		let mut sender = None;

		let store = match self.inner {
			PermitInner::Reuse {
				ds,
				channel,
			} => {
				sender = Some(channel);
				// The create path wipes at build time; the reused base datastore is
				// never rebuilt, so reset it here to clear any cruft a prior test
				// (e.g. a `clean` test defining extra namespaces) left on the shared
				// physical cluster before handing it to this test.
				self.info.reset_storage(ds.store.as_ref()).await?;
				ds
			}
			PermitInner::Create {
				versioned,
				capabilities,
			} => self.info.produce_ds(versioned, *capabilities).await?,
		};

		let (can_reuse, res) = f(&store.store, &self.grade_ds).await;

		if let CanReuse::Reset = can_reuse {
			if let Err(e) = self.grade_ds.shutdown().await {
				println!("Failed to shutdown panicking datastore: {e}");
			}
			let new_ds = create_grade_ds().await;
			self.grade_send
				.try_send(new_ds)
				.expect("Too many datastores entered into datastore channel");
		} else {
			self.grade_send
				.try_send(self.grade_ds)
				.expect("Too many datastores entered into datastore channel");
		}

		if let Some(sender) = sender {
			if let CanReuse::Reset = can_reuse {
				// Shutdown the panicking datastore to release resources
				if let Err(e) = store.store.shutdown().await {
					println!("Failed to shutdown panicking datastore: {e}");
				}
				// We need to send back a new datastore otherwise the que might get exhausted and
				// jobs will get stuck forever waiting for a new task.
				let ds = match self.info.produce_ds(false, Capabilities::all()).await {
					Ok(x) => x,
					Err(e) => {
						println!(
							"Failed to create a new datastore to replace panicking datastore: {e}"
						);
						return Ok(res);
					}
				};
				sender.try_send(ds).expect("Too many datastores entered into datastore channel");
			} else {
				sender.try_send(store).expect("Too many datastores entered into datastore channel");
			}
			return Ok(res);
		}

		// Shutdown the datastore before removing its directory to ensure all file descriptors
		// are closed This is critical for RocksDB which can have many open file handles
		if let Err(e) = store.store.shutdown().await {
			println!("Failed to shutdown datastore before cleanup: {e}");
		}

		if let Some(remove_path) = &store.path {
			// Remove the directory synchronously to ensure cleanup completes before next test
			// This prevents file descriptor exhaustion on backends like RocksDB
			if let Err(e) = tokio::fs::remove_dir_all(&remove_path).await {
				println!("Failed to remove temporary directory {remove_path}: {e}");
			}
		}
		Ok(res)
	}
}

async fn create_grade_ds() -> Box<Datastore> {
	let ds = Datastore::builder()
		.with_capabilities(
			Capabilities::none()
				.with_functions(Targets::All)
				.without_functions(Targets::None)
				.with_scripting(true),
		)
		.with_query_timeout(None)
		.build_with_path("memory")
		.await
		.expect("datastore to build successfully");

	ds.bootstrap().await.unwrap();

	let mut session = surrealdb_core::dbs::Session::default();
	ds.process_use(None, &mut session, Some("match".to_string()), Some("match".to_string()))
		.await
		.unwrap();

	Box::new(ds)
}

impl Provisioner {
	pub async fn new(num_jobs: usize, backend: Backend) -> Result<Self> {
		let info = CreateInfo::new(backend).await?;

		let (send, recv) = mpsc::channel(num_jobs);
		for _ in 0..num_jobs {
			let ds =
				info.produce_ds(false, Capabilities::all().with_experimental(Targets::All)).await?;
			send.try_send(ds).unwrap();
		}
		let (grade_send, grade_recv) = mpsc::channel(num_jobs);
		for _ in 0..num_jobs {
			let ds = create_grade_ds().await;
			grade_send.try_send(ds).unwrap();
		}

		Ok(Provisioner {
			send,
			recv,
			grade_send,
			grade_recv,
			create_info: Arc::new(info),
		})
	}

	pub async fn obtain(&mut self, env: &TestEnv) -> Permit {
		let grade_ds = self.grade_recv.recv().await.expect("Datastore channel closed early");
		if is_base_environment(env) {
			let ds = self.recv.recv().await.expect("Datastore channel closed early");
			Permit {
				info: self.create_info.clone(),
				grade_send: self.grade_send.clone(),
				grade_ds,
				inner: PermitInner::Reuse {
					ds,
					channel: self.send.clone(),
				},
			}
		} else {
			let capabilities = match &env.capabilities {
				BoolOr::Bool(true) => Capabilities::all().with_experimental(Targets::All),
				BoolOr::Bool(false) => Capabilities::none(),
				BoolOr::Value(x) => util::core_capabilities_from_test_config(x),
			};
			Permit {
				info: self.create_info.clone(),
				grade_send: self.grade_send.clone(),
				grade_ds,
				inner: PermitInner::Create {
					versioned: env.versioned,
					capabilities: Box::new(capabilities),
				},
			}
		}
	}

	pub async fn shutdown(mut self) -> Result<()> {
		mem::drop(self.send);
		mem::drop(self.grade_send);
		while let Some(datastore) = self.recv.recv().await {
			// Best-effort shutdown - ignore errors since datastores may have been
			// cleared by other tests, especially with shared datastore instances
			if let Err(e) = datastore.store.shutdown().await {
				println!("Warning: Datastore shutdown error: {e}");
			}
		}

		if let Some(dir) = self.create_info.dir.as_ref() {
			// Best-effort cleanup - ignore errors since datastores may have been
			// cleared by other tests, especially with shared datastore instances
			if let Err(e) = tokio::fs::remove_dir_all(dir).await {
				println!("Failed to clean up temporary dir: {e}");
			}
		}

		while let Some(datastore) = self.grade_recv.recv().await {
			// Best-effort shutdown - ignore errors since datastores may have been
			// cleared by other tests, especially with shared datastore instances
			if let Err(e) = datastore.shutdown().await {
				println!("Warning: Datastore shutdown error: {e}");
			}
		}

		Ok(())
	}
}

fn is_base_environment(env: &TestEnv) -> bool {
	!env.clean && !env.versioned && matches!(env.capabilities, BoolOr::Bool(true))
}
