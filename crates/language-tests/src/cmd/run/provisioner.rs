use std::{
	any::Any,
	mem,
	panic::AssertUnwindSafe,
	path::Path,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::SystemTime,
};

use anyhow::{Context, Result};
use futures::FutureExt as _;
use surrealdb_core::{dbs::Capabilities, kvs::Datastore};
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
			Backend::Foundation => {
				let p = self.produce_path();
				let ds = Datastore::new(&format!("fdb://{p}")).await?;
				path = Some(p);
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
				let new_ds = match create_base_datastore().await {
					Ok(x) => x,
					Err(e) => {
						println!(
							"Failed to create a new datastore to replaced panicking datastore: {e}"
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
		}

		if let Some(remove_path) = remove_path {
			tokio::spawn(tokio::fs::remove_dir_all(remove_path));
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
		while let Some(x) = self.recv.recv().await {
			x.shutdown().await.context("Datastore failed to shutdown properly")?;
		}

		if let Some(dir) = self.create_info.dir.as_ref() {
			tokio::fs::remove_dir_all(dir).await.context("Failed to clean up temporary dir")?;
		}

		Ok(())
	}
}
