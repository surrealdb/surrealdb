use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use surrealdb_core::channel;
use surrealdb_core::dbs::Capabilities;
use surrealdb_core::dbs::capabilities::Targets;
use surrealdb_core::kvs::{Datastore, LockType, TransactionType};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::cli::Backend;
use crate::tests::schema::{BoolOr, Capabilities as TestCapabilities, SchemaTarget, TestEnv};
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

		let builder = Datastore::builder().with_capabilities(cap).with_auth(true);

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
					builder.build_with_path(&format!("rocksdb://{p}?versioned=true&retention=1h")).await?
				}else{
					builder.build_with_path(&format!("rocksdb://{p}")).await?
				};
				path = Some(p);
				ds
			}
			Backend::SurrealKv => {
				let p = self.produce_path();
				let ds = if versioned {
					builder.build_with_path(&format!("surrealkv://{p}?versioned=true&retention=1h")).await?
				} else {
					builder.build_with_path(&format!("surrealkv://{p}")).await?
				};
				path = Some(p);
				ds
			}
			Backend::TikV => {
				let p = "127.0.0.1:2379";
				let ds = builder.build_with_path(&format!("tikv://{p}")).await?;
				let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
				tx.delr(vec![0u8]..vec![0xffu8]).await?;
				tx.commit().await?;
				ds
			}
		};

		ds.bootstrap().await?;

		Ok(Ds {
			store: Box::new(ds),
			path,
		})
	}

	fn produce_path(&self) -> String {
		let path = self.dir.as_ref().unwrap();

		let id = self.id_gen.fetch_add(1, Ordering::AcqRel);

		let path = Path::new(path).join(format!("store_{id}"));
		path.to_str().unwrap().to_owned()
	}
}

pub struct Ds {
	/// The store itself
	store: Box<Datastore>,
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
	pub async fn with<F: AsyncFnOnce(&mut Box<Datastore>, &Box<Datastore>) -> (CanReuse, R), R>(
		self,
		f: F,
	) -> Result<R> {
		let mut sender = None;

		let mut store = match self.inner {
			PermitInner::Reuse {
				ds,
				channel,
			} => {
				sender = Some(channel);
				ds
			}
			PermitInner::Create {
				versioned,
				capabilities,
			} => self.info.produce_ds(versioned, *capabilities).await?,
		};

		let (can_reuse, res) = f(&mut store.store, &self.grade_ds).await;

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
				BoolOr::Value(x) => core_capabilities_from_test_config(x),
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

/// Creates the right core capabilities from a test config.
pub fn core_capabilities_from_test_config(cap: &TestCapabilities) -> Capabilities {
	/// Returns Targets::All if there is no value and none_on_missing is false,
	/// Returns Targets::None if there is no value and none_on_missing is true ensuring the default
	/// behaviour is to allow everything.
	///
	/// If there is a value it will return Targets::All on the value true, Targets::None on the
	/// value false, and otherwise the returns the specified values.
	fn extract_targets<T>(v: &BoolOr<Vec<SchemaTarget<T>>>) -> Targets<T>
	where
		T: Eq + std::hash::Hash + Ord + Clone,
	{
		match v {
			BoolOr::Bool(true) => Targets::All,
			BoolOr::Bool(false) => Targets::None,
			BoolOr::Value(x) => Targets::Some(x.iter().map(|x| x.0.clone()).collect()),
		}
	}

	Capabilities::none()
		.with_scripting(cap.scripting)
		.with_guest_access(cap.quest_access)
		.with_live_query_notifications(cap.live_query_notifications)
		.with_functions(extract_targets(&cap.allow_functions))
		.without_functions(extract_targets(&cap.deny_functions))
		.with_network_targets(extract_targets(&cap.allow_net))
		.without_network_targets(extract_targets(&cap.deny_net))
		.with_rpc_methods(extract_targets(&cap.allow_rpc))
		.without_rpc_methods(extract_targets(&cap.deny_rpc))
		.with_http_routes(extract_targets(&cap.allow_http))
		.without_http_routes(extract_targets(&cap.deny_http))
		.with_experimental(extract_targets(&cap.allow_experimental))
		.without_experimental(extract_targets(&cap.deny_experimental))
}
