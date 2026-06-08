//! Functionality for storing benchmarking data,

use std::pin::Pin;

use crate::{cli::Backend, cmd::bench::stats::MeasurementData};

use anyhow::Result;
use clap::ArgMatches;

mod local;
mod remote;

static SCHEMA: &str = include_str!("./schema.surql");

pub struct StoreConfig<'a> {
	path: &'a String,
	url: Option<&'a String>,
	user: &'a String,
	password: &'a String,
	ns: &'a String,
	db: &'a String,
}

impl<'a> StoreConfig<'a> {
	pub fn from_matches(matches: &'a ArgMatches) -> Self {
		let path = matches.get_one::<String>("store-path").unwrap();
		let url = matches.get_one::<String>("store-url");
		let user = matches.get_one::<String>("store-user").unwrap();
		let password = matches.get_one::<String>("store-password").unwrap();
		let ns = matches.get_one::<String>("store-ns").unwrap();
		let db = matches.get_one::<String>("store-db").unwrap();
		Self {
			path,
			url,
			user,
			password,
			ns,
			db,
		}
	}
}

pub struct BenchMarkRun {
	pub path: String,
	pub backend: Backend,
	pub measurement: MeasurementData,
}

trait BenchDataStore: Send + Sync {
	fn add(&mut self, run: BenchMarkRun) -> impl Future<Output = Result<()>> + Send;

	fn fetch_latest<'a>(
		&'a mut self,
		path: &'a str,
		backend: Backend,
	) -> impl Future<Output = Result<Option<MeasurementData>>> + 'a + Send;

	fn close<'a>(&'a mut self) -> impl Future<Output = Result<()>> + 'a + Send {
		async { Ok(()) }
	}
}

type BoxFuture<'a, R> = Pin<Box<dyn Future<Output = R> + 'a + Send>>;

pub trait DynBenchDataStore {
	fn add<'a>(&'a mut self, run: BenchMarkRun) -> BoxFuture<'a, Result<()>>;

	fn fetch_latest<'a>(
		&'a mut self,
		path: &'a str,
		backend: Backend,
	) -> BoxFuture<'a, Result<Option<MeasurementData>>>;

	fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<()>>;
}

impl<T: BenchDataStore> DynBenchDataStore for T {
	fn add<'a>(&'a mut self, run: BenchMarkRun) -> BoxFuture<'a, Result<()>> {
		Box::pin(<T as BenchDataStore>::add(self, run))
	}

	fn fetch_latest<'a>(
		&'a mut self,
		path: &'a str,
		backend: Backend,
	) -> BoxFuture<'a, Result<Option<MeasurementData>>> {
		Box::pin(<T as BenchDataStore>::fetch_latest(self, path, backend))
	}

	fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<()>> {
		Box::pin(<T as BenchDataStore>::close(self))
	}
}

pub async fn get_store(cfg: &StoreConfig<'_>) -> Result<Box<dyn DynBenchDataStore>> {
	#[cfg(feature = "bench-remote-store")]
	if let Some(url) = cfg.url {
		return remote::RemoteStore::new(url, cfg)
			.await
			.map(|x| Box::new(x) as Box<dyn DynBenchDataStore>);
	}

	#[cfg(not(feature = "bench-remote-store"))]
	if cfg.url.is_some() {
		anyhow::bail!(
			"Can not open a remote datastore without the `bench-remote-store` feature being enabled"
		)
	}

	return local::LocalStore::new(cfg.path)
		.await
		.map(|x| Box::new(x) as Box<dyn DynBenchDataStore>);
}
