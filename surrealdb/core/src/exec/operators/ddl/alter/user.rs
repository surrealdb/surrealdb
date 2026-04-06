use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog;
use crate::catalog::providers::UserProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers;
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream,
};
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug)]
pub struct AlterUserPlan {
	pub name: String,
	pub base: Base,
	pub if_exists: bool,
	pub hash: Option<String>,
	pub roles: AlterKind<Vec<String>>,
	pub token_duration: AlterKind<Option<Duration>>,
	pub session_duration: AlterKind<Option<Duration>>,
	pub comment: AlterKind<String>,
	pub required_context: ContextLevel,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterUserPlan {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		name: String,
		base: Base,
		if_exists: bool,
		hash: Option<String>,
		roles: AlterKind<Vec<String>>,
		token_duration: AlterKind<Option<Duration>>,
		session_duration: AlterKind<Option<Duration>>,
		comment: AlterKind<String>,
		required_context: ContextLevel,
	) -> Self {
		Self {
			name,
			base,
			if_exists,
			hash,
			roles,
			token_duration,
			session_duration,
			comment,
			required_context,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterUserPlan {
	fn name(&self) -> &'static str {
		"AlterUser"
	}

	fn required_context(&self) -> ContextLevel {
		self.required_context
	}

	fn strict_context(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadWrite
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn is_scalar(&self) -> bool {
		true
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(self.metrics.as_ref())
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let base = self.base;
		let if_exists = self.if_exists;
		let hash = self.hash.clone();
		let roles = self.roles.clone();
		let token_duration = self.token_duration.clone();
		let session_duration = self.session_duration.clone();
		let comment = self.comment.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(
					&ctx,
					name,
					base,
					if_exists,
					hash,
					roles,
					token_duration,
					session_duration,
					comment,
				)
				.await
			})
		})
	}
}

fn apply(
	user: &mut catalog::UserDefinition,
	hash: &Option<String>,
	roles: &AlterKind<Vec<String>>,
	token_duration: &AlterKind<Option<Duration>>,
	session_duration: &AlterKind<Option<Duration>>,
	comment: &AlterKind<String>,
) {
	if let Some(h) = hash {
		user.hash.clone_from(h);
	}
	match roles {
		AlterKind::Set(v) => user.roles.clone_from(v),
		AlterKind::Drop => user.roles = vec![],
		AlterKind::None => {}
	}
	match token_duration {
		AlterKind::Set(v) => user.token_duration = *v,
		AlterKind::Drop => user.token_duration = None,
		AlterKind::None => {}
	}
	match session_duration {
		AlterKind::Set(v) => user.session_duration = *v,
		AlterKind::Drop => user.session_duration = None,
		AlterKind::None => {}
	}
	match comment {
		AlterKind::Set(v) => user.comment = Some(v.clone()),
		AlterKind::Drop => user.comment = None,
		AlterKind::None => {}
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute(
	ctx: &ExecutionContext,
	name: String,
	base: Base,
	if_exists: bool,
	hash: Option<String>,
	roles: AlterKind<Vec<String>>,
	token_duration: AlterKind<Option<Duration>>,
	session_duration: AlterKind<Option<Duration>>,
	comment: AlterKind<String>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Actor, &base)?;

	let txn = ctx.txn();

	match base {
		Base::Root => {
			let mut user = match txn.get_root_user(&name).await? {
				Some(v) => v.deref().clone(),
				None => {
					if if_exists {
						return Ok(Value::None);
					}
					return Err(Error::UserRootNotFound {
						name,
					}
					.into());
				}
			};
			apply(&mut user, &hash, &roles, &token_duration, &session_duration, &comment);
			txn.put_root_user(&user).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
		Base::Ns => {
			let ns_ctx = ctx.namespace()?;
			let ns = ns_ctx.ns.namespace_id;
			let mut user = match txn.get_ns_user(ns, &name).await? {
				Some(v) => v.deref().clone(),
				None => {
					if if_exists {
						return Ok(Value::None);
					}
					return Err(Error::UserNsNotFound {
						name,
						ns: ns_ctx.ns.name.clone(),
					}
					.into());
				}
			};
			apply(&mut user, &hash, &roles, &token_duration, &session_duration, &comment);
			txn.put_ns_user(ns, &user).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
		Base::Db => {
			let db_ctx = ctx.database()?;
			let ns = db_ctx.ns_ctx.ns.namespace_id;
			let db = db_ctx.db.database_id;
			let mut user = match txn.get_db_user(ns, db, &name).await? {
				Some(v) => v.deref().clone(),
				None => {
					if if_exists {
						return Ok(Value::None);
					}
					return Err(Error::UserDbNotFound {
						name,
						ns: db_ctx.ns_ctx.ns.name.clone(),
						db: db_ctx.db.name.clone(),
					}
					.into());
				}
			};
			apply(&mut user, &hash, &roles, &token_duration, &session_duration, &comment);
			txn.put_db_user(ns, db, &user).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
	}
}
