use std::sync::Arc;

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

use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Session;
use crate::err::Error;
use crate::iam::Error as IamError;
use crate::kvs::Datastore;
use crate::kvs::LockType;
use crate::kvs::TransactionType;
use crate::sql;
use crate::sql::part::Part;
use crate::sql::Function;
use crate::sql::Statement;
use crate::sql::{Thing, Value as SqlValue};

use super::error::GqlError;

#[derive(Clone)]
pub struct GQLTx {
	opt: Options,
	ctx: Context,
}

impl GQLTx {
	pub async fn new(kvs: &Arc<Datastore>, sess: &Session) -> Result<Self, GqlError> {
		kvs.check_anon(sess).map_err(|_| {
			Error::IamError(IamError::NotAllowed {
				actor: "anonymous".to_string(),
				action: "process".to_string(),
				resource: "graphql".to_string(),
			})
		})?;

		let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let tx = Arc::new(tx);
		let mut ctx = kvs.setup_ctx()?;
		ctx.set_transaction(tx);

		sess.context(&mut ctx);

		Ok(GQLTx {
			ctx: ctx.freeze(),
			opt: kvs.setup_options(sess),
		})
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
		let mut stack = TreeStack::new();

		let res = stack.enter(|stk| stmt.compute(stk, &self.ctx, &self.opt, None)).finish().await?;

		Ok(res)
	}

	pub async fn run_fn(&self, name: &str, args: Vec<SqlValue>) -> Result<SqlValue, GqlError> {
		let mut stack = TreeStack::new();
		let fun = sql::Value::Function(Box::new(Function::Custom(name.to_string(), args)));

		let res = stack
			// .enter(|stk| fnc::run(stk, &self.ctx, &self.opt, None, name, args))
			.enter(|stk| fun.compute(stk, &self.ctx, &self.opt, None))
			.finish()
			.await?;

		Ok(res)
	}
}
