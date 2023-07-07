use crate::ctx::Context;
use crate::dbs::{Auth, Options, Transaction};
use crate::kvs::Datastore;
use futures::lock::Mutex;
use std::sync::Arc;

pub async fn mock<'a>() -> (Context<'a>, Options, Transaction) {
	let ctx = Context::default();
	let opt = Options::default().with_auth(Arc::new(Auth::Kv));
	let kvs = Datastore::new("memory").await.unwrap();
	let txn = kvs.transaction(true, false).await.unwrap();
	let txn = Arc::new(Mutex::new(txn));
	(ctx, opt, txn)
}
