use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog;
use crate::catalog::providers::AuthorisationProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers;
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream,
};
use crate::expr::statements::alter::AlterKind;
use crate::expr::{Base, Expr};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug)]
pub struct AlterAccessPlan {
	pub name: String,
	pub base: Base,
	pub if_exists: bool,
	pub authenticate: AlterKind<Expr>,
	pub grant_duration: AlterKind<Option<Duration>>,
	pub token_duration: AlterKind<Option<Duration>>,
	pub session_duration: AlterKind<Option<Duration>>,
	pub comment: AlterKind<String>,
	pub required_context: ContextLevel,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterAccessPlan {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		name: String,
		base: Base,
		if_exists: bool,
		authenticate: AlterKind<Expr>,
		grant_duration: AlterKind<Option<Duration>>,
		token_duration: AlterKind<Option<Duration>>,
		session_duration: AlterKind<Option<Duration>>,
		comment: AlterKind<String>,
		required_context: ContextLevel,
	) -> Self {
		Self {
			name,
			base,
			if_exists,
			authenticate,
			grant_duration,
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
impl ExecOperator for AlterAccessPlan {
	fn name(&self) -> &'static str {
		"AlterAccess"
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
		let authenticate = self.authenticate.clone();
		let grant_duration = self.grant_duration.clone();
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
					authenticate,
					grant_duration,
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
	ac: &mut catalog::AccessDefinition,
	authenticate: &AlterKind<Expr>,
	grant_duration: &AlterKind<Option<Duration>>,
	token_duration: &AlterKind<Option<Duration>>,
	session_duration: &AlterKind<Option<Duration>>,
	comment: &AlterKind<String>,
) {
	match authenticate {
		AlterKind::Set(v) => ac.authenticate = Some(v.clone()),
		AlterKind::Drop => ac.authenticate = None,
		AlterKind::None => {}
	}
	match grant_duration {
		AlterKind::Set(v) => ac.grant_duration = *v,
		AlterKind::Drop => ac.grant_duration = None,
		AlterKind::None => {}
	}
	match token_duration {
		AlterKind::Set(v) => ac.token_duration = *v,
		AlterKind::Drop => ac.token_duration = None,
		AlterKind::None => {}
	}
	match session_duration {
		AlterKind::Set(v) => ac.session_duration = *v,
		AlterKind::Drop => ac.session_duration = None,
		AlterKind::None => {}
	}
	match comment {
		AlterKind::Set(v) => ac.comment = Some(v.clone()),
		AlterKind::Drop => ac.comment = None,
		AlterKind::None => {}
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute(
	ctx: &ExecutionContext,
	name: String,
	base: Base,
	if_exists: bool,
	authenticate: AlterKind<Expr>,
	grant_duration: AlterKind<Option<Duration>>,
	token_duration: AlterKind<Option<Duration>>,
	session_duration: AlterKind<Option<Duration>>,
	comment: AlterKind<String>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Access, &base)?;

	let txn = ctx.txn();

	match base {
		Base::Root => {
			let mut ac = match txn.get_root_access(&name).await? {
				Some(v) => v.deref().clone(),
				None => {
					if if_exists {
						return Ok(Value::None);
					}
					return Err(Error::AccessRootNotFound {
						ac: name,
					}
					.into());
				}
			};
			apply(
				&mut ac,
				&authenticate,
				&grant_duration,
				&token_duration,
				&session_duration,
				&comment,
			);
			let key = crate::key::root::ac::new(&name);
			txn.set(&key, &ac, None).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
		Base::Ns => {
			let ns_ctx = ctx.namespace()?;
			let ns = ns_ctx.ns.namespace_id;
			let mut ac = match txn.get_ns_access(ns, &name).await? {
				Some(v) => v.deref().clone(),
				None => {
					if if_exists {
						return Ok(Value::None);
					}
					return Err(Error::AccessNsNotFound {
						ac: name,
						ns: ns_ctx.ns.name.clone(),
					}
					.into());
				}
			};
			apply(
				&mut ac,
				&authenticate,
				&grant_duration,
				&token_duration,
				&session_duration,
				&comment,
			);
			let key = crate::key::namespace::ac::new(ns, &name);
			txn.set(&key, &ac, None).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
		Base::Db => {
			let db_ctx = ctx.database()?;
			let ns = db_ctx.ns_ctx.ns.namespace_id;
			let db = db_ctx.db.database_id;
			let mut ac = match txn.get_db_access(ns, db, &name).await? {
				Some(v) => v.deref().clone(),
				None => {
					if if_exists {
						return Ok(Value::None);
					}
					return Err(Error::AccessDbNotFound {
						ac: name,
						ns: db_ctx.ns_ctx.ns.name.clone(),
						db: db_ctx.db.name.clone(),
					}
					.into());
				}
			};
			apply(
				&mut ac,
				&authenticate,
				&grant_duration,
				&token_duration,
				&session_duration,
				&comment,
			);
			let key = crate::key::database::ac::new(ns, db, &name);
			txn.set(&key, &ac, None).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
	}
}
