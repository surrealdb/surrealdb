use std::sync::Arc;

use crate::ctx::Context;
use crate::dbs::{Options, Session};
use crate::err::Error;
use crate::expr;
use crate::expr::part::Part;
use crate::expr::{FlowResultExt, Function, LogicalPlan};
use crate::iam::Error as IamError;
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::val::{RecordId, Value as SqlValue};
use anyhow::Result;

use async_graphql::dynamic::FieldValue;
use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::{Name, Value as GqlValue};
use reblessive::TreeStack;

use super::error::GqlError;

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

#[derive(Clone)]
pub struct GQLTx {
	opt: Options,
	ctx: Context,
}

impl GQLTx {
	pub async fn new(kvs: &Arc<Datastore>, sess: &Session) -> Result<Self, GqlError> {
		kvs.check_anon(sess)
			.map_err(|_| {
				Error::IamError(IamError::NotAllowed {
					actor: "anonymous".to_string(),
					action: "process".to_string(),
					resource: "graphql".to_string(),
				})
			})
			.map_err(anyhow::Error::new)?;

		let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let tx = Arc::new(tx);
		let mut ctx = kvs.setup_ctx()?;
		ctx.set_transaction(tx);

		ctx.attach_session(sess).map_err(|err| GqlError::InternalError(err.to_string()))?;

		Ok(GQLTx {
			ctx: ctx.freeze(),
			opt: kvs.setup_options(sess),
		})
	}

	pub async fn get_record_field(&self, rid: RecordId, field: Part) -> Result<SqlValue, GqlError> {
		let mut stack = TreeStack::new();
		let part = [field.into()];
		let value = SqlValue::RecordId(rid);
		stack
			.enter(|stk| value.get(stk, &self.ctx, &self.opt, None, &part))
			.finish()
			.await
			.catch_return()
			.map_err(Into::into)
	}

	pub async fn process_stmt(&self, stmt: LogicalPlan) -> Result<SqlValue, GqlError> {
		let mut stack = TreeStack::new();

		let res = stack
			.enter(|stk| stmt.compute(stk, &self.ctx, &self.opt, None))
			.finish()
			.await
			.catch_return()?;

		Ok(res)
	}

	pub async fn run_fn(&self, name: &str, args: Vec<SqlValue>) -> Result<SqlValue, GqlError> {
		let mut stack = TreeStack::new();
		let fun = expr::Expr::FunctionCall(Box::new(expr::FunctionCall {
			receiver: Function::Custom(name.to_string()),
			arguments: args,
		}));

		let res = stack
			// .enter(|stk| fnc::run(stk, &self.ctx, &self.opt, None, name, args))
			.enter(|stk| fun.compute(stk, &self.ctx, &self.opt, None))
			.finish()
			.await
			.catch_return()?;

		Ok(res)
	}
}

pub type ErasedRecord = (GQLTx, RecordId);

pub fn field_val_erase_owned(val: ErasedRecord) -> FieldValue<'static> {
	FieldValue::owned_any(val)
}
