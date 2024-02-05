use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::iam::Auth;
use crate::iam::Role;
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use std::sync::Arc;

pub async fn mock<'a>() -> (Context<'a>, Options, Transaction) {
	let ctx = Context::default();
	let opt = Options::default().with_auth(Arc::new(Auth::for_root(Role::Owner)));
	let kvs = Datastore::new("memory").await.unwrap();
	let txn = kvs.transaction(Write, Optimistic).await.unwrap().rollback_and_ignore().enclose();
	(ctx, opt, txn)
}
