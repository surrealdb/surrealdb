use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_channel::Receiver;
use futures::{Stream, StreamExt};
use semver::Version;

use crate::Surreal;
use crate::api::conn::{Command, MlExportConfig};
use crate::api::method::BoxFuture;
use crate::api::{Connection, Error, ExtraFeatures, Result};
use crate::core::kvs::export::{Config as DbExportConfig, TableConfig};
use crate::method::{ExportConfig as Config, Model, OnceLockExt};

/// A database export future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Export<'r, C: Connection, R, T = ()> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) target: R,
	pub(super) ml_config: Option<MlExportConfig>,
	pub(super) db_config: Option<DbExportConfig>,
	pub(super) response: PhantomData<R>,
	pub(super) export_type: PhantomData<T>,
}

impl<'r, C, R> Export<'r, C, R>
where
	C: Connection,
{
	/// Export machine learning model
	pub fn ml(self, name: &str, version: Version) -> Export<'r, C, R, Model> {
		Export {
			client: self.client,
			target: self.target,
			ml_config: Some(MlExportConfig {
				name: name.to_owned(),
				version: version.to_string(),
			}),
			db_config: self.db_config,
			response: self.response,
			export_type: PhantomData,
		}
	}

	/// Configure the export options
	pub fn with_config(self) -> Export<'r, C, R, Config> {
		Export {
			client: self.client,
			target: self.target,
			ml_config: self.ml_config,
			// Use default configuration options
			db_config: Some(Default::default()),
			response: self.response,
			export_type: PhantomData,
		}
	}
}

impl<C, R> Export<'_, C, R, Config>
where
	C: Connection,
{
	/// Whether to export users from the database
	pub fn users(mut self, users: bool) -> Self {
		if let Some(cfg) = self.db_config.as_mut() {
			cfg.users = users;
		}
		self
	}

	/// Whether to export accesses from the database
	pub fn accesses(mut self, accesses: bool) -> Self {
		if let Some(cfg) = self.db_config.as_mut() {
			cfg.accesses = accesses;
		}
		self
	}

	/// Whether to export params from the database
	pub fn params(mut self, params: bool) -> Self {
		if let Some(cfg) = self.db_config.as_mut() {
			cfg.params = params;
		}
		self
	}

	/// Whether to export functions from the database
	pub fn functions(mut self, functions: bool) -> Self {
		if let Some(cfg) = self.db_config.as_mut() {
			cfg.functions = functions;
		}
		self
	}

	/// Whether to export analyzers from the database
	pub fn analyzers(mut self, analyzers: bool) -> Self {
		if let Some(cfg) = self.db_config.as_mut() {
			cfg.analyzers = analyzers;
		}
		self
	}

	/// Whether to export all versions of data from the database
	pub fn versions(mut self, versions: bool) -> Self {
		if let Some(cfg) = self.db_config.as_mut() {
			cfg.versions = versions;
		}
		self
	}

	/// Whether to export tables or which ones from the database
	///
	/// We can pass a `bool` to export all tables or none at all:
	/// ```
	/// # let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
	/// # let target = ();
	/// db.export(target).with_config().tables(true);
	/// db.export(target).with_config().tables(false);
	/// ```
	///
	/// Or we can pass a `Vec<String>` to specify a list of tables to export:
	/// ```
	/// # let db = surrealdb::Surreal::<surrealdb::engine::any::Any>::init();
	/// # let target = ();
	/// db.export(target).with_config().tables(vec!["users"]);
	/// ```
	pub fn tables(mut self, tables: impl Into<TableConfig>) -> Self {
		if let Some(cfg) = self.db_config.as_mut() {
			cfg.tables = tables.into();
		}
		self
	}

	/// Whether to export records from the database
	pub fn records(mut self, records: bool) -> Self {
		if let Some(cfg) = self.db_config.as_mut() {
			cfg.records = records;
		}
		self
	}
}

impl<C, R, T> Export<'_, C, R, T>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Export<'static, C, R, T> {
		Export {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client, T> IntoFuture for Export<'r, Client, PathBuf, T>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			if !router.features.contains(&ExtraFeatures::Backup) {
				return Err(Error::BackupsNotSupported.into());
			}

			if let Some(config) = self.ml_config {
				return router
					.execute_unit(Command::ExportMl {
						path: self.target,
						config,
					})
					.await;
			}

			router
				.execute_unit(Command::ExportFile {
					path: self.target,
					config: self.db_config,
				})
				.await
		})
	}
}

impl<'r, Client, T> IntoFuture for Export<'r, Client, (), T>
where
	Client: Connection,
{
	type Output = Result<Backup>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let router = self.client.inner.router.extract()?;
			if !router.features.contains(&ExtraFeatures::Backup) {
				return Err(Error::BackupsNotSupported.into());
			}
			let (tx, rx) = crate::channel::bounded(1);
			let rx = Box::pin(rx);

			if let Some(config) = self.ml_config {
				router
					.execute_unit(Command::ExportBytesMl {
						bytes: tx,
						config,
					})
					.await?;
				return Ok(Backup {
					rx,
				});
			}

			router
				.execute_unit(Command::ExportBytes {
					bytes: tx,
					config: self.db_config,
				})
				.await?;

			Ok(Backup {
				rx,
			})
		})
	}
}

/// A stream of exported data
#[derive(Debug, Clone)]
#[must_use = "streams do nothing unless you poll them"]
pub struct Backup {
	rx: Pin<Box<Receiver<Result<Vec<u8>>>>>,
}

impl Stream for Backup {
	type Item = Result<Vec<u8>>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.as_mut().rx.poll_next_unpin(cx)
	}
}
