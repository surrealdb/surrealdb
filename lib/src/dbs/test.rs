use crate::ctx::Context;
use crate::dbs::{Auth, Options};
use crate::kvs::Datastore;
use futures::lock::Mutex;
use std::sync::Arc;
use uuid::Uuid;

pub async fn mock<'a>() -> (Context<'a>, Options) {
	let mut ctx = Context::default();
	let opt = Options::new(Arc::new(Uuid::new_v4()), channel::unbounded().0, Arc::new(Auth::Kv));
	let kvs = Datastore::new("memory").await.unwrap();
	let txn = kvs.transaction(true, false).await.unwrap();
	let txn = Arc::new(Mutex::new(txn));
	ctx.add_transaction(Some(&txn));
	(ctx, opt)
}
