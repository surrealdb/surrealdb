use crate::ctx::Context;
use crate::dbs::Options;
use crate::kvs::Datastore;
use futures::lock::Mutex;
use std::sync::Arc;

pub async fn mock<'a>() -> (Context<'a>, Options) {
	let mut ctx = Context::default();
	let opt = Options::default();
	let kvs = Datastore::new("memory").await.unwrap();
	let txn = kvs.transaction(true, false).await.unwrap();
	let txn = Arc::new(Mutex::new(txn));
	ctx.add_transaction(Some(&txn));
	(ctx, opt)
}
