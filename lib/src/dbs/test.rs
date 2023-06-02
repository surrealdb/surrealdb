use crate::ctx::Context;
use crate::dbs::Auth;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::kvs::Datastore;
use futures::lock::Mutex;
use std::sync::Arc;
use uuid::Uuid;

impl Default for Options {
	fn default() -> Self {
		Options {
			id: Uuid::default(),
			ns: None,
			db: None,
			dive: 0,
			live: false,
			perms: true,
			force: false,
			strict: false,
			fields: true,
			events: true,
			tables: true,
			indexes: true,
			futures: false,
			auth: Arc::new(Auth::Kv),
			sender: channel::unbounded().0,
		}
	}
}

pub async fn mock<'a>() -> (Context<'a>, Options, Transaction) {
	let ctx = Context::default();
	let opt = Options::default();
	let kvs = Datastore::new("memory").await.unwrap();
	let txn = kvs.transaction(true, false).await.unwrap();
	let txn = Arc::new(Mutex::new(txn));
	(ctx, opt, txn)
}
