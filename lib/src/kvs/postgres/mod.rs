#![cfg(feature = "kv-postgres")]

use crate::err::Error;

pub(crate) struct Datastore;

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<super::seaorm::Datastore, Error> {
		super::seaorm::Datastore::new(path)
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::tests::transaction::verify_transaction_isolation;
	use test_log::test;

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
	async fn postgres_transaction() {
		verify_transaction_isolation(
			"postgres://localhost:5432/postgres?user=postgres&password=surrealdb",
		)
		.await;
	}
}
