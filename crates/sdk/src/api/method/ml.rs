use std::{marker::PhantomData, path::PathBuf};

use crate::{Surreal, method::Backup, opt::IntoExportDestination};
use anyhow::Result;
use futures::{StreamExt, future::BoxFuture};
use surrealdb_protocol::proto::rpc::v1::ExportMlModelRequest;
use tokio::io::AsyncWriteExt;

/// A database export future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct MlModel {
	pub(super) client: Surreal,
	pub(super) name: String,
	pub(super) version: semver::Version,
}

impl MlModel {
	pub fn export<R, RT>(self, target: impl IntoExportDestination<R>) -> MlExport<R, RT> {
		MlExport {
			client: self.client,
			target: target.into_export_destination(),
			export_request: ExportMlModelRequest {
				name: self.name,
				version: self.version.to_string(),
			},
			response: PhantomData,
			export_type: PhantomData,
		}
	}
}

pub struct MlExport<R = (), RT = ()> {
	pub(super) client: Surreal,
	pub(super) target: R,
	pub(super) export_request: ExportMlModelRequest,
	pub(super) response: PhantomData<R>,
	pub(super) export_type: PhantomData<RT>,
}

impl IntoFuture for MlExport<PathBuf, ()> {
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();

			let resp = client.export_ml_model(self.export_request).await?;

			let mut stream = resp.into_inner();

			// Open the file
			let mut file = tokio::fs::File::create(self.target).await?;

			while let Some(resp) = stream.next().await {
				let resp = resp?;

				file.write_all(&resp.model).await?;
			}

			Ok(())
		})
	}
}

impl IntoFuture for MlExport<(), Backup> {
	type Output = Result<Backup>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();

			let resp = client.export_ml_model(self.export_request).await?;

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

					tx.send(Ok(resp.model.to_vec())).await.unwrap();
				}
			});

			Ok(Backup {
				rx,
			})
		})
	}
}
