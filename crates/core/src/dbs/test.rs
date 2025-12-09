use std::sync::Arc;

use uuid::Uuid;

use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::iam::{Auth, Role};
use crate::kvs::Datastore;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;

pub async fn mock() -> (Context, Options) {
	let opt = Options::new(Uuid::new_v4()).with_auth(Arc::new(Auth::for_root(Role::Owner)));
	let kvs = Datastore::new("memory").await.unwrap();
	let txn = kvs.transaction(Write, Optimistic).await.unwrap().enclose();
	let mut ctx = MutableContext::default();
	ctx.set_transaction(txn);
	(ctx.freeze(), opt)
}
