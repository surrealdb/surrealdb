use std::sync::Arc;

use async_graphql::{dynamic::indexmap::IndexMap, Name, Value as GqlValue};
use futures::TryFutureExt;
use reblessive::Stk;
use reblessive::TreeStack;
pub(crate) trait GqlValueUtils {
	fn as_i64(&self) -> Option<i64>;
	fn as_string(&self) -> Option<String>;
	fn as_list(&self) -> Option<&Vec<GqlValue>>;
	fn as_object(&self) -> Option<&IndexMap<Name, GqlValue>>;
}

impl GqlValueUtils for GqlValue {
	fn as_i64(&self) -> Option<i64> {
		if let GqlValue::Number(n) = self {
			n.as_i64()
		} else {
			None
		}
	}

	fn as_string(&self) -> Option<String> {
		if let GqlValue::String(s) = self {
			Some(s.to_owned())
		} else {
			None
		}
	}
	fn as_list(&self) -> Option<&Vec<GqlValue>> {
		if let GqlValue::List(a) = self {
			Some(a)
		} else {
			None
		}
	}
	fn as_object(&self) -> Option<&IndexMap<Name, GqlValue>> {
		if let GqlValue::Object(o) = self {
			Some(o)
		} else {
			None
		}
	}
}

use crate::ctx::Context;
use crate::ctx::MutableContext;
use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Session;
use crate::kvs::Datastore;
use crate::kvs::LockType;
use crate::kvs::Transaction;
use crate::kvs::TransactionType;
use crate::sql::part::Part;
use crate::sql::statements::SelectStatement;
use crate::sql::Fields;
use crate::sql::Statement;
use crate::sql::{Thing, Value as SqlValue};

use super::error::GqlError;

#[derive(Clone)]
pub struct GQLTx {
	tx: Arc<Transaction>,
	kvs: Arc<Datastore>,
	opt: Options,
	ctx: Context,
}

impl GQLTx {
	pub async fn new(kvs: &Arc<Datastore>, sess: &Session) -> Result<Self, GqlError> {
		let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let mut ctx = kvs.setup_ctx()?;

		sess.context(&mut ctx);

		Ok(GQLTx {
			tx: Arc::new(tx),
			kvs: kvs.clone(),
			ctx: ctx.freeze(),
			opt: kvs.setup_options(sess),
		})
	}

	pub async fn get_record(&self, rid: Thing) -> Result<SqlValue, GqlError> {
		let mut stack = TreeStack::new();
		let part = [Part::All];
		let value = SqlValue::Thing(rid);
		stack
			.enter(|stk| value.get(stk, &self.ctx, &self.opt, None, &part))
			.finish()
			.await
			.map_err(Into::into)
	}

	pub async fn get_record_field(
		&self,
		rid: Thing,
		field: impl Into<Part>,
	) -> Result<SqlValue, GqlError> {
		let mut stack = TreeStack::new();
		let part = [field.into()];
		let value = SqlValue::Thing(rid);
		stack
			.enter(|stk| value.get(stk, &self.ctx, &self.opt, None, &part))
			.finish()
			.await
			.map_err(Into::into)
	}

	pub async fn process_stmt(&self, stmt: Statement) -> Result<SqlValue, GqlError> {
		let mut exe = Executor {
			err: false,
			kvs: &self.kvs,
			txn: Some(self.tx.clone()),
		};

		let res = exe.execute(self.ctx.clone(), self.opt.clone(), vec![stmt].into()).await?;
		debug_assert_eq!(res.len(), 1);
		let res = res
			.into_iter()
			.next()
			.ok_or(GqlError::InternalError(
				"Unreachable Result of executing a single statement had length 0".to_string(),
			))?
			.result?;
		Ok(res)
	}
}

// pub async fn get_record(
// 	kvs: &Datastore,
// 	sess: &Session,
// 	rid: &Thing,
// ) -> Result<SqlValue, GqlError> {
// 	// let stmt: Statement = Statement::Select(SelectStatement {
// 	// 	expr: Fields::all(),
// 	// 	what: vec![SqlValue::Thing(rid.clone())].into(),
// 	// 	only: true,
// 	// 	..Default::default()
// 	// });
// 	// let res = kvs.process(stmt.into(), sess, Default::default()).await?;
// 	// let res = res
// 	// 	.into_iter()
// 	// 	.next()
// 	// 	.expect("constructed query with one statement so response should have one result")
// 	// 	.result?;

// 	// Ok(res)
// 	todo!()
// }
