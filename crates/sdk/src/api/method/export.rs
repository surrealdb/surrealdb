use crate::Surreal;
use crate::api::Error;
use crate::api::ExtraFeatures;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::conn::MlExportConfig;
use crate::api::method::BoxFuture;
use crate::method::ExportConfig as Config;
use crate::method::Model;

use async_channel::Receiver;
use futures::Stream;
use futures::StreamExt;
use semver::Version;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use surrealdb_core::kvs::export::{Config as DbExportConfig, TableConfig};
use surrealdb_protocol::proto::rpc::v1::{ExportSqlRequest, export_sql_request};
use surrealdb_protocol::proto::v1::NullValue;
use tokio::io::AsyncWriteExt;

/// A database export future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Export<R, T = ()> {
	pub(super) client: Surreal,
	pub(super) target: R,
	pub(super) export_request: ExportSqlRequest,
	pub(super) response: PhantomData<R>,
	pub(super) export_type: PhantomData<T>,
}

impl<R> Export<R> {
	/// Configure the export options
	pub fn with_config(self) -> Export<R, Config> {
		Export {
			client: self.client,
			target: self.target,
			export_request: ExportSqlRequest::default(),
			response: self.response,
			export_type: PhantomData,
		}
	}
}

impl<R> Export<R, Config> {
	/// Whether to export users from the database
	pub fn users(mut self, users: bool) -> Self {
		self.export_request.users = users;
		self
	}

	/// Whether to export accesses from the database
	pub fn accesses(mut self, accesses: bool) -> Self {
		self.export_request.accesses = accesses;
		self
	}

	/// Whether to export params from the database
	pub fn params(mut self, params: bool) -> Self {
		self.export_request.params = params;
		self
	}

	/// Whether to export functions from the database
	pub fn functions(mut self, functions: bool) -> Self {
		self.export_request.functions = functions;
		self
	}

	/// Whether to export analyzers from the database
	pub fn analyzers(mut self, analyzers: bool) -> Self {
		self.export_request.analyzers = analyzers;
		self
	}

	/// Whether to export all versions of data from the database
	pub fn versions(mut self, versions: bool) -> Self {
		self.export_request.versions = versions;
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
	pub fn tables(mut self, tables: impl Into<export_sql_request::Tables>) -> Self {
		self.export_request.tables = Some(tables.into());
		self
	}

	/// Whether to export records from the database
	pub fn records(mut self, records: bool) -> Self {
		self.export_request.records = records;
		self
	}
}

impl<R, T> Export<R, T> {}

impl<T> IntoFuture for Export<PathBuf, T> {
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(mut self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();

			let resp = client.export_sql(self.export_request).await?;

			let mut stream = resp.into_inner();

			// Open the file
			let mut file = tokio::fs::File::create(self.target).await?;

			while let Some(resp) = stream.next().await {
				let resp = resp?;

				file.write_all(resp.statement.as_bytes()).await?;
			}

			Ok(())
		})
	}
}

impl<T> IntoFuture for Export<(), T> {
	type Output = Result<Backup>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();

			let resp = client.export_sql(self.export_request).await?;

			let mut stream = resp.into_inner();

			let (tx, rx) = crate::channel::bounded(1);
			let rx = Box::pin(rx);

			tokio::spawn(async move {
				while let Some(resp) = stream.next().await {
					let resp = match resp {
						Ok(resp) => resp,
						Err(err) => {
							tx.send(Err(anyhow::anyhow!("Error exporting data: {err:?}")))
								.await
								.unwrap();
							return;
						}
					};

					tx.send(Ok(resp.statement.as_bytes().to_vec())).await.unwrap();
				}
			});

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
