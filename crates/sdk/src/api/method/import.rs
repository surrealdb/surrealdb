use crate::Surreal;
use crate::api::Result;
use crate::api::method::BoxFuture;
use crate::method::Model;

use std::future::IntoFuture;
use std::marker::PhantomData;
use std::path::PathBuf;
use surrealdb_protocol::proto::rpc::v1::ImportSqlRequest;
use tokio::io::AsyncBufReadExt;

/// An database import future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Import<T = ()> {
	pub(super) client: Surreal,
	pub(super) file: PathBuf,
	pub(super) is_ml: bool,
	pub(super) import_type: PhantomData<T>,
}

impl Import {
	/// Import machine learning model
	pub fn ml(self) -> Import<Model> {
		Import {
			client: self.client,
			file: self.file,
			is_ml: true,
			import_type: PhantomData,
		}
	}
}

impl<T> IntoFuture for Import<T> {
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();

			let file = tokio::fs::File::open(self.file).await?;

			let txn_id = uuid::Uuid::new_v4();

			let outbound = async_stream::stream! {
				let reader = tokio::io::BufReader::new(file);
				let mut lines = reader.lines();

				loop {
					let line = match lines.next_line().await {
						Ok(Some(line)) => line,
						Ok(None) => break,
						Err(err) => {
							tracing::error!("Error reading line: {:?}", err);
							break;
						}
					};

					if line.is_empty() {
						continue;
					}

					yield ImportSqlRequest {
						txn_id: Some(txn_id.into()),
						statement: line,
					};
				}
			};

			let _ = client.import_sql(tonic::Request::new(outbound)).await?;

			Ok(())
		})
	}
}
