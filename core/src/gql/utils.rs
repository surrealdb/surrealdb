use async_graphql::{dynamic::indexmap::IndexMap, Name, Value as GqlValue};
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

use crate::dbs::Options;
use crate::dbs::Session;
use crate::kvs::Datastore;
use crate::kvs::LockType;
use crate::kvs::Transaction;
use crate::kvs::TransactionType;
use crate::sql::statements::SelectStatement;
use crate::sql::Fields;
use crate::sql::Statement;
use crate::sql::{Thing, Value as SqlValue};

use super::error::GqlError;

pub struct GQLTx {
	tx: Transaction,
	opt: Options,
}

impl GQLTx {
	pub async fn new(kvs: Datastore, sess: &Session) -> Result<Self, GqlError> {
		let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;

		Ok(GQLTx {
			tx,
			opt: kvs.setup_options(sess),
		})
	}

	pub async fn get_record(&self) -> Result<SqlValue, GqlError> {
		SqlValue::get(&self, TreeStack::new(), ctx, opt, doc, path)
	}
}

pub async fn get_record(
	kvs: &Datastore,
	sess: &Session,
	rid: &Thing,
) -> Result<SqlValue, GqlError> {
	// let stmt: Statement = Statement::Select(SelectStatement {
	// 	expr: Fields::all(),
	// 	what: vec![SqlValue::Thing(rid.clone())].into(),
	// 	only: true,
	// 	..Default::default()
	// });
	// let res = kvs.process(stmt.into(), sess, Default::default()).await?;
	// let res = res
	// 	.into_iter()
	// 	.next()
	// 	.expect("constructed query with one statement so response should have one result")
	// 	.result?;

	// Ok(res)
	todo!()
}
