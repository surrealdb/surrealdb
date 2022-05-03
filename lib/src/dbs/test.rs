use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::kvs::Datastore;
use futures::lock::Mutex;
use std::sync::Arc;

pub async fn mock<'a>() -> (Runtime, Options, Transaction) {
	let ctx = Context::default().freeze();
	let opt = Options::default();
	let kvs = Datastore::new("memory").await.unwrap();
	let txn = kvs.transaction(true, false).await.unwrap();
	let txn = Arc::new(Mutex::new(txn));
	(ctx, opt, txn)
}
