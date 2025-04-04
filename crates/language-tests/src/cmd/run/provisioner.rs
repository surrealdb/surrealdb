use std::{any::Any, mem, panic::AssertUnwindSafe, sync::Arc};

use anyhow::{Context, Result};
use futures::FutureExt as _;
use surrealdb_core::{dbs::Capabilities, kvs::Datastore};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{cli::Backend, temp_dir::TempDir};

struct CreateInfo {
	backend: Backend,
	dir: Option<TempDir>,
}

impl CreateInfo {
	pub async fn new(backend: Backend) -> Result<Self> {
		if let Backend::Memory = backend {
			return Ok(CreateInfo {
				backend,
				dir: None,
			});
		}

		let dir = TempDir::new("surreal_test")
			.await
			.context("Failed to create a temporary directory for test databases")?;

		println!(" Using '{}' as temporary directory for datastores", dir.path().display());

		Ok(CreateInfo {
			backend,
			dir: Some(dir),
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
		self.dir
			.as_ref()
			.unwrap()
			.sub_dir_path()
			.to_str()
			.expect("temporary sub directory path should be valid utf-8")
			.to_owned()
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
			dir.cleanup().await.context("Failed to clean up temporary directory for datastores")?;
		}

		Ok(())
	}
}
