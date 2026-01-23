use std::any::Any;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use anyhow::{Context, Result};
use futures::FutureExt as _;
use surrealdb_core::dbs::Capabilities;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::kvs::LockType;
use surrealdb_core::kvs::TransactionType;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::cli::Backend;

struct CreateInfo {
	id_gen: AtomicUsize,
	backend: Backend,
	dir: Option<String>,
}

fn xorshift(state: &mut u32) -> u32 {
	let mut x = *state;
	x ^= x << 13;
	x ^= x >> 17;
	x ^= x << 5;
	*state = x;
	x
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
		let time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
		let time = time.as_secs() ^ time.subsec_nanos() as u64;
		let mut state = (time >> 32) as u32 ^ time as u32;

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

	pub async fn produce_ds(&self) -> Result<(Datastore, Option<String>)> {
		let mut path = None;
		let ds = match self.backend {
			Backend::Memory => Datastore::new("memory").await?,
			Backend::RocksDb => {
				let p = self.produce_path();
				let ds = Datastore::new(&format!("rocksdb://{p}")).await?;
				path = Some(p);
				ds
			}
			Backend::SurrealKv => {
				let p = self.produce_path();
				let ds = Datastore::new(&format!("surrealkv://{p}")).await?;
				path = Some(p);
				ds
			}
			Backend::TikV => {
				let p = "127.0.0.1:2379";
				let ds = Datastore::new(&format!("tikv://{p}")).await?;
				let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
				tx.delr(vec![0u8]..vec![0xffu8]).await?;
				tx.commit().await?;
				ds
			}
		};

		let ds =
			ds.with_capabilities(Capabilities::all()).with_notifications().with_auth_enabled(true);

		ds.bootstrap().await?;

		Ok((ds, path))
	}

	fn produce_path(&self) -> String {
		let path = self.dir.as_ref().unwrap();

		let id = self.id_gen.fetch_add(1, Ordering::AcqRel);

		let path = Path::new(path).join(format!("store_{id}"));
		path.to_str().unwrap().to_owned()
	}
}

#[must_use]
pub struct Provisioner {
	send: Sender<Datastore>,
	recv: Receiver<Datastore>,
	create_info: Arc<CreateInfo>,
}

pub enum PermitError {
	Panic(Box<dyn Any + Send + 'static>),
	Other(anyhow::Error),
}

enum PermitInner {
	Reuse {
		ds: Datastore,
		channel: Sender<Datastore>,
	},
	Create {
		info: Arc<CreateInfo>,
	},
}

async fn create_base_datastore() -> Result<Datastore> {
	let db = Datastore::new("memory")
		.await?
		.with_capabilities(Capabilities::all())
		.with_notifications()
		.with_auth_enabled(true);

	db.bootstrap().await?;

	Ok(db)
}

#[must_use]
pub struct Permit {
	inner: PermitInner,
}

impl Permit {
	pub async fn with<U: FnOnce(Datastore) -> Datastore, F: AsyncFnOnce(&mut Datastore) -> R, R>(
		self,
		u: U,
		f: F,
	) -> Result<R, PermitError> {
		let mut sender = None;
		let mut remove_path = None;
		let store = match self.inner {
			PermitInner::Reuse {
				ds,
				channel,
			} => {
				sender = Some(channel);
				ds
			}
			PermitInner::Create {
				info,
			} => {
				let (ds, path) = info.produce_ds().await.map_err(PermitError::Other)?;
				remove_path = path;
				ds
			}
		};

		let mut store = u(store);
		let fut = f(&mut store);
		let res = AssertUnwindSafe(fut).catch_unwind().await.map_err(PermitError::Panic);

		if let Some(sender) = sender {
			if res.is_err() {
				// Shutdown the panicking datastore to release resources
				if let Err(e) = store.shutdown().await {
					println!("Failed to shutdown panicking datastore: {e}");
				}
				let new_ds = match create_base_datastore().await {
					Ok(x) => x,
					Err(e) => {
						println!(
							"Failed to create a new datastore to replace panicking datastore: {e}"
						);
						return res;
					}
				};
				sender
					.try_send(new_ds)
					.expect("Too many datastores entered into datastore channel");
			} else {
				sender.try_send(store).expect("Too many datastores entered into datastore channel");
			}
		} else if remove_path.is_some() {
			// Shutdown the datastore before removing its directory to ensure all file descriptors are closed
			// This is critical for RocksDB which can have many open file handles
			if let Err(e) = store.shutdown().await {
				println!("Failed to shutdown datastore before cleanup: {e}");
			}
		}

		if let Some(remove_path) = remove_path {
			// Remove the directory synchronously to ensure cleanup completes before next test
			// This prevents file descriptor exhaustion on backends like RocksDB
			if let Err(e) = tokio::fs::remove_dir_all(&remove_path).await {
				println!("Failed to remove temporary directory {remove_path}: {e}");
			}
		}
		res
	}
}

impl Provisioner {
	pub async fn new(num_jobs: usize, backend: Backend) -> Result<Self> {
		let info = CreateInfo::new(backend).await?;

		let (send, recv) = mpsc::channel(num_jobs);
		for _ in 0..num_jobs {
			let (db, _) = info.produce_ds().await?;
			send.try_send(db).unwrap();
		}

		Ok(Provisioner {
			send,
			recv,
			create_info: Arc::new(info),
		})
	}

	pub async fn obtain(&mut self) -> Permit {
		let ds = self.recv.recv().await.expect("Datastore channel closed early");
		Permit {
			inner: PermitInner::Reuse {
				ds,
				channel: self.send.clone(),
			},
		}
	}

	pub fn create(&mut self) -> Permit {
		Permit {
			inner: PermitInner::Create {
				info: self.create_info.clone(),
			},
		}
	}

	pub async fn shutdown(mut self) -> Result<()> {
		mem::drop(self.send);
		while let Some(datastore) = self.recv.recv().await {
			// Best-effort shutdown - ignore errors since datastores may have been
			// cleared by other tests, especially with shared datastore instances
			if let Err(e) = datastore.shutdown().await {
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

		Ok(())
	}
}
