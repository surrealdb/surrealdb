use anyhow::{Context, Result};
use surrealdb_core::{dbs::Capabilities, kvs::Datastore};
use surrealdb_types::{SurrealValue, Variables};

use crate::{
	cli::Backend,
	cmd::bench::{stats::MeasurementData, store::BenchDataStore},
};

use super::BenchMarkRun;

pub struct LocalStore {
	ds: Datastore,
}

impl LocalStore {
	pub async fn new(path: &str) -> Result<Self> {
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_auth(false)
			.build_with_path(&format!("surrealkv://{path}"))
			.await
			.context("Could not open benchmark comparison datastore")?;

		Self::bootstrap(&ds).await?;

		Ok(LocalStore {
			ds,
		})
	}

	async fn bootstrap(ds: &Datastore) -> Result<()> {
		ds.get_version().await.context("Could not setup db version")?;

		ds.bootstrap().await.context("Failed to bootstrap db")?;

		let mut session = surrealdb_core::dbs::Session::owner();
		ds.process_use(None, &mut session, Some("bench".to_string()), Some("bench".to_string()))
			.await
			.context("Failed to setup namespace and database")?;

		ds.execute(super::SCHEMA, &session, None)
			.await
			.and_then(|x| {
				if let Some(x) = x.into_iter().find_map(|x| x.result.err()) {
					Err(x)
				} else {
					Ok(())
				}
			})
			.context("Failed to initialize database schema")?;

		Ok(())
	}
}

impl BenchDataStore for LocalStore {
	async fn add(&mut self, run: BenchMarkRun) -> Result<()> {
		let add_query = r#"
			CREATE measurement:[$path,$backend,time::now()] CONTENT $value
		"#;

		let mut vars = Variables::new();
		vars.insert("value", run.measurement.into_value());
		vars.insert("path", run.path.into_value());
		vars.insert("backend", run.backend.into_value());
		let session = surrealdb_core::dbs::Session::owner().with_ns("bench").with_db("bench");
		self.ds
			.execute(add_query, &session, Some(vars))
			.await
			.and_then(|x| {
				if let Some(x) = x.into_iter().find_map(|x| x.result.err()) {
					Err(x)
				} else {
					Ok(())
				}
			})
			.context("Could not add measurement to the store")?;
		Ok(())
	}

	async fn fetch_latest(
		&mut self,
		path: &str,
		backend: Backend,
	) -> Result<Option<MeasurementData>> {
		let add_query = r#"
			fn::last_measurement($path,$backend)
		"#;
		let mut vars = Variables::new();
		vars.insert("path", path.into_value());
		vars.insert("backend", backend.into_value());
		let session = surrealdb_core::dbs::Session::owner().with_ns("bench").with_db("bench");
		let mut res = self
			.ds
			.execute(add_query, &session, Some(vars))
			.await
			.context("Could not fetch last measurement")?;

		assert_eq!(res.len(), 1);

		let res = res.pop().unwrap().result.context("Could not fetch last measurement")?;

		Option::<MeasurementData>::from_value(res)
			.context("Could not convert data to the right type")
	}

	async fn close(&mut self) -> Result<()> {
		self.ds.shutdown().await
	}
}
