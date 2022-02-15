use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use futures::lock::Mutex;
use std::sync::Arc;

pub async fn mock<'a>() -> (Runtime, Options, Transaction<'a>) {
	let ctx = Context::default().freeze();
	let opt = Options::default();
	let txn = Arc::new(Mutex::new(crate::kvs::Transaction::Mock));
	(ctx, opt, txn)
}
