#![cfg(feature = "kv-postgres")]

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
