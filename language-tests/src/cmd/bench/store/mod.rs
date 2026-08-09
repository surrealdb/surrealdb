//! Functionality for storing benchmarking data,

use std::pin::Pin;

use anyhow::Result;
use clap::ArgMatches;

use surrealdb_types::{Datetime, SurrealValue, Value};

use crate::cli::Backend;
use crate::cmd::bench::stats::MeasurementData;

mod local;
mod remote;

static SCHEMA: &str = include_str!("./schema.surql");

pub struct StoreConfig<'a> {
	path: &'a String,
	url: Option<&'a String>,
	#[cfg_attr(not(feature = "bench-remote-store"), allow(dead_code))]
	user: &'a String,
	#[cfg_attr(not(feature = "bench-remote-store"), allow(dead_code))]
	password: &'a String,
	#[cfg_attr(not(feature = "bench-remote-store"), allow(dead_code))]
	ns: &'a String,
	#[cfg_attr(not(feature = "bench-remote-store"), allow(dead_code))]
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
	/// The commit this measurement was taken at. The `measurement` table has
	/// always declared this field, but nothing ever wrote it — so a stored
	/// baseline could not say which code it came from.
	pub commit: Option<String>,
	/// Rows the benched statement returned. Compared against the baseline's to
	/// catch a bench whose *workload* changed rather than its speed.
	pub rows: Option<i64>,
}

/// A stored measurement together with the provenance needed to judge whether
/// comparing against it means anything.
///
/// The comparison used to be handed a bare [`MeasurementData`], which carries
/// statistics and nothing else — so a baseline row from a different commit, a
/// different week, or a differently-shaped dataset was indistinguishable from a
/// current one, and a stale row read as a regression in whatever it was
/// compared against.
pub struct Baseline {
	pub measurement: MeasurementData,
	pub commit: Option<String>,
	pub datetime: Option<Datetime>,
	pub rows: Option<i64>,
}

/// The object a measurement is stored as: the sample statistics with the
/// provenance fields alongside them.
///
/// Assembled here rather than in the query so both stores write the same shape,
/// and because an object-merge builtin is not available to lean on.
fn measurement_content(
	measurement: MeasurementData,
	commit: Option<String>,
	rows: Option<i64>,
) -> Value {
	let mut object = measurement.into_value().into_object().expect("a measurement is an object");
	object.insert("commit", commit);
	object.insert("rows", rows);
	Value::Object(object)
}

/// Reads the provenance fields off a `fn::last_measurement` row.
///
/// Done by hand rather than through a derive because the row also carries the
/// statistics and the computed `path`/`backend`/`datetime` fields, and the
/// statistics are deserialized separately into [`MeasurementData`].
fn baseline_meta(value: &Value) -> (Option<String>, Option<Datetime>, Option<i64>) {
	let Some(obj) = value.as_object() else {
		return (None, None, None);
	};
	let commit = obj.get("commit").and_then(|v| v.as_string()).cloned();
	let datetime = obj.get("datetime").and_then(|v| v.as_datetime()).cloned();
	let rows = obj.get("rows").and_then(|v| v.as_int()).copied();
	(commit, datetime, rows)
}

trait BenchDataStore: Send + Sync {
	fn add(&mut self, run: BenchMarkRun) -> impl Future<Output = Result<()>> + Send;

	fn fetch_latest<'a>(
		&'a mut self,
		path: &'a str,
		backend: Backend,
	) -> impl Future<Output = Result<Option<Baseline>>> + 'a + Send;

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
	) -> BoxFuture<'a, Result<Option<Baseline>>>;

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
	) -> BoxFuture<'a, Result<Option<Baseline>>> {
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
