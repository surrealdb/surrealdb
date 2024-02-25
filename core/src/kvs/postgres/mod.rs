#![cfg(feature = "kv-postgres")]

use crate::err::Error;

pub(crate) struct Datastore;

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<super::seaorm::Datastore, Error> {
		let db = super::seaorm::Datastore::new(path).await?;
		db.ensure_table_exists().await?;
		db.ensure_indices_exists().await?;
		Ok(db)
	}
}