#[cfg(not(target_family = "wasm"))]
use std::any::Any;
use std::mem;
#[cfg(not(target_family = "wasm"))]
use std::panic::AssertUnwindSafe;
#[cfg(not(target_family = "wasm"))]
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
#[cfg(not(target_family = "wasm"))]
use futures::FutureExt as _;
use surrealdb_core::dbs::Capabilities;
use surrealdb_core::kvs::{Datastore, LockType, TransactionType};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::cli::Backend;

struct CreateInfo {
	id_gen: AtomicUsize,
	backend: Backend,
	#[cfg(not(target_family = "wasm"))]
	dir: Option<String>,
}

#[cfg(not(target_family = "wasm"))]
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
		#[cfg(not(target_family = "wasm"))]
		{
			if matches!(backend, Backend::Memory | Backend::IndxDb) {
				return Ok(CreateInfo {
					id_gen: AtomicUsize::new(0),
					backend,
					dir: None,
				});
			}
			let temp_dir = std::env::temp_dir();
			let time = web_time::SystemTime::now()
				.duration_since(web_time::SystemTime::UNIX_EPOCH)
				.unwrap();
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

		#[cfg(target_family = "wasm")]
		{
			Ok(CreateInfo {
				id_gen: AtomicUsize::new(0),
				backend,
			})
		}
	}

	pub async fn produce_ds(&self, versioned: bool) -> Result<(Datastore, Option<String>)> {
		#[allow(unused_mut)]
		let mut path = None;
		let ds = match self.backend {
			Backend::Memory => {
				if versioned {
					Datastore::new("mem://?versioned=true").await?
				} else {
					Datastore::new("mem://").await?
				}
			}
			#[cfg(not(target_family = "wasm"))]
			Backend::RocksDb => {
				let p = self.produce_path();
				let ds = Datastore::new(&format!("rocksdb://{p}")).await?;
				path = Some(p);
				ds
			}
			#[cfg(not(target_family = "wasm"))]
			Backend::SurrealKv => {
				let p = self.produce_path();
				let ds = if versioned {
					Datastore::new(&format!("surrealkv://{p}?versioned=true")).await?
				} else {
					Datastore::new(&format!("surrealkv://{p}")).await?
				};
				path = Some(p);
				ds
			}
			#[cfg(not(target_family = "wasm"))]
			Backend::TikV => {
				let p = "127.0.0.1:2379";
				let ds = Datastore::new(&format!("tikv://{p}")).await?;
				let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
				tx.delr(vec![0u8]..vec![0xffu8]).await?;
				tx.commit().await?;
				ds
			}
			Backend::IndxDb => {
				let id = self.id_gen.fetch_add(1, Ordering::AcqRel);
				Datastore::new(&format!("indxdb://surreal_test_{id}")).await?
			}
			#[cfg(target_family = "wasm")]
			_ => anyhow::bail!("Backend {} is not supported on WASM", self.backend),
		};

		let ds =
			ds.with_capabilities(Capabilities::all()).with_notifications().with_auth_enabled(true);

		ds.bootstrap().await?;

		Ok((ds, path))
	}

	#[cfg(not(target_family = "wasm"))]
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
	#[cfg(not(target_family = "wasm"))]
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
		versioned: bool,
	},
}

#[cfg_attr(target_family = "wasm", allow(dead_code))]
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
	#[cfg(not(target_family = "wasm"))]
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
				versioned,
			} => {
				let (ds, path) = info.produce_ds(versioned).await.map_err(PermitError::Other)?;
				remove_path = path;
				ds
			}
		};

		let mut store = u(store);
		let fut = f(&mut store);
		let res = AssertUnwindSafe(fut).catch_unwind().await.map_err(PermitError::Panic);

		if let Some(sender) = sender {
			if res.is_err() {
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
			if let Err(e) = store.shutdown().await {
				println!("Failed to shutdown datastore before cleanup: {e}");
			}
		}

		if let Some(remove_path) = remove_path {
			if let Err(e) = tokio::fs::remove_dir_all(&remove_path).await {
				println!("Failed to remove temporary directory {remove_path}: {e}");
			}
		}
		res
	}

	#[cfg(target_family = "wasm")]
	pub async fn with<U: FnOnce(Datastore) -> Datastore, F: AsyncFnOnce(&mut Datastore) -> R, R>(
		self,
		u: U,
		f: F,
	) -> Result<R, PermitError> {
		let store = match self.inner {
			PermitInner::Reuse {
				ds,
				channel,
			} => {
				let mut store = u(ds);
				let res = f(&mut store).await;
				channel
					.try_send(store)
					.expect("Too many datastores entered into datastore channel");
				return Ok(res);
			}
			PermitInner::Create {
				info,
				versioned,
			} => {
				let (ds, _) = info.produce_ds(versioned).await.map_err(PermitError::Other)?;
				ds
			}
		};

		let mut store = u(store);
		let res = f(&mut store).await;
		Ok(res)
	}
}

impl Provisioner {
	pub async fn new(num_jobs: usize, backend: Backend) -> Result<Self> {
		let info = CreateInfo::new(backend).await?;

		let (send, recv) = mpsc::channel(num_jobs);
		for _ in 0..num_jobs {
			let (db, _) = info.produce_ds(false).await?;
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

	pub fn create(&mut self, versioned: bool) -> Permit {
		Permit {
			inner: PermitInner::Create {
				info: self.create_info.clone(),
				versioned,
			},
		}
	}

	pub async fn shutdown(mut self) -> Result<()> {
		mem::drop(self.send);
		while let Some(datastore) = self.recv.recv().await {
			if let Err(e) = datastore.shutdown().await {
				println!("Warning: Datastore shutdown error: {e}");
			}
		}

		#[cfg(not(target_family = "wasm"))]
		if let Some(dir) = self.create_info.dir.as_ref() {
			if let Err(e) = tokio::fs::remove_dir_all(dir).await {
				println!("Failed to clean up temporary dir: {e}");
			}
		}

		Ok(())
	}
}
