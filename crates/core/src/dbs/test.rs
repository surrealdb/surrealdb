use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::iam::Auth;
use crate::iam::Role;
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use std::sync::Arc;

pub async fn mock() -> (Context, Options) {
	let opt = Options::default().with_auth(Arc::new(Auth::for_root(Role::Owner)));
	let kvs = Datastore::new("memory").await.unwrap();
	let txn = kvs.transaction(Write, Optimistic).await.unwrap();
	let txn = txn.rollback_and_ignore().await.enclose();
	let mut ctx = MutableContext::default();
	ctx.set_transaction(txn);
	(ctx.freeze(), opt)
}
