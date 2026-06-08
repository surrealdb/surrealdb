use std::sync::Arc;

use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::iam::{Auth, Role};
use crate::kvs::Datastore;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;

pub async fn mock() -> (FrozenContext, Options) {
	let kvs = Datastore::new("memory").await.unwrap();
	let opt = Options::new(&kvs.config()).with_auth(Arc::new(Auth::for_root(Role::Owner)));
	let txn = kvs.transaction(Write, Optimistic).await.unwrap().enclose();
	let mut ctx = Context::new_test();
	ctx.set_transaction(txn);
	(ctx.freeze(), opt)
}
