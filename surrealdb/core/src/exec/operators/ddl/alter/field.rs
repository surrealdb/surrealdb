use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use surrealdb_types::ToSql;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{self, Permission, Permissions, TableDefinition};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::reference::Reference;
use crate::expr::statements::alter::{AlterDefault, AlterKind};
use crate::expr::{Base, Expr, Idiom, Kind};
use crate::iam::{Action, ResourceKind};
use crate::val::{TableName, Value};

#[derive(Debug)]
pub struct AlterFieldPlan {
	pub name: Idiom,
	pub what: TableName,
	pub if_exists: bool,
	pub kind: AlterKind<Kind>,
	pub flexible: AlterKind<()>,
	pub readonly: AlterKind<()>,
	pub value: AlterKind<Expr>,
	pub assert: AlterKind<Expr>,
	pub default: AlterDefault,
	pub permissions: Option<Permissions>,
	pub comment: AlterKind<String>,
	pub reference: AlterKind<Reference>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterFieldPlan {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		name: Idiom,
		what: TableName,
		if_exists: bool,
		kind: AlterKind<Kind>,
		flexible: AlterKind<()>,
		readonly: AlterKind<()>,
		value: AlterKind<Expr>,
		assert: AlterKind<Expr>,
		default: AlterDefault,
		permissions: Option<Permissions>,
		comment: AlterKind<String>,
		reference: AlterKind<Reference>,
	) -> Self {
		Self {
			name,
			what,
			if_exists,
			kind,
			flexible,
			readonly,
			value,
			assert,
			default,
			permissions,
			comment,
			reference,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterFieldPlan {
	ddl_operator_common!("AlterField", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let what = self.what.clone();
		let if_exists = self.if_exists;
		let kind = self.kind.clone();
		let flexible = self.flexible.clone();
		let readonly = self.readonly.clone();
		let value = self.value.clone();
		let assert = self.assert.clone();
		let default = self.default.clone();
		let permissions = self.permissions.clone();
		let comment = self.comment.clone();
		let reference = self.reference.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(
					&ctx,
					name,
					what,
					if_exists,
					kind,
					flexible,
					readonly,
					value,
					assert,
					default,
					permissions,
					comment,
					reference,
				)
				.await
			})
		})
	}
}

fn convert_permission(perm: &Permission) -> catalog::Permission {
	match perm {
		Permission::None => catalog::Permission::None,
		Permission::Full => catalog::Permission::Full,
		Permission::Specific(expr) => catalog::Permission::Specific(expr.clone()),
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute(
	ctx: &ExecutionContext,
	name: Idiom,
	what: TableName,
	if_exists: bool,
	kind: AlterKind<Kind>,
	flexible: AlterKind<()>,
	readonly: AlterKind<()>,
	value: AlterKind<Expr>,
	assert: AlterKind<Expr>,
	default: AlterDefault,
	permissions: Option<Permissions>,
	comment: AlterKind<String>,
	reference: AlterKind<Reference>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let name_str = name.to_sql();
	let mut df = match txn.get_tb_field(ns, db, &what, &name_str).await? {
		Some(fd) => fd.deref().clone(),
		None => {
			if if_exists {
				return Ok(Value::None);
			}
			return Err(crate::err::Error::FdNotFound {
				name: name_str,
			}
			.into());
		}
	};

	match kind {
		AlterKind::Set(ref k) => df.field_kind = Some(k.clone()),
		AlterKind::Drop => df.field_kind = None,
		AlterKind::None => {}
	}

	match flexible {
		AlterKind::Set(_) => df.flexible = true,
		AlterKind::Drop => df.flexible = false,
		AlterKind::None => {}
	}

	match readonly {
		AlterKind::Set(_) => df.readonly = true,
		AlterKind::Drop => df.readonly = false,
		AlterKind::None => {}
	}

	match value {
		AlterKind::Set(ref k) => df.value = Some(k.clone()),
		AlterKind::Drop => df.value = None,
		AlterKind::None => {}
	}

	match assert {
		AlterKind::Set(ref k) => df.assert = Some(k.clone()),
		AlterKind::Drop => df.assert = None,
		AlterKind::None => {}
	}

	match default {
		AlterDefault::None => {}
		AlterDefault::Drop => df.default = catalog::DefineDefault::None,
		AlterDefault::Always(ref expr) => df.default = catalog::DefineDefault::Always(expr.clone()),
		AlterDefault::Set(ref expr) => df.default = catalog::DefineDefault::Set(expr.clone()),
	}

	if let Some(permissions) = &permissions {
		df.select_permission = convert_permission(&permissions.select);
		df.create_permission = convert_permission(&permissions.create);
		df.update_permission = convert_permission(&permissions.update);
	}

	match comment {
		AlterKind::Set(ref k) => df.comment = Some(k.clone()),
		AlterKind::Drop => df.comment = None,
		AlterKind::None => {}
	}

	match reference {
		AlterKind::Set(ref k) => df.reference = Some(k.clone()),
		AlterKind::Drop => df.reference = None,
		AlterKind::None => {}
	}

	let key = crate::key::table::fd::new(ns, db, &what, &name_str);
	txn.set(&key, &df, None).await?;

	let Some(tb) = txn.get_tb(ns, db, &what).await? else {
		return Err(crate::err::Error::TbNotFound {
			name: what,
		}
		.into());
	};
	txn.put_tb(
		ns_name,
		db_name,
		&TableDefinition {
			cache_fields_ts: Uuid::now_v7(),
			..tb.as_ref().clone()
		},
	)
	.await?;

	txn.clear_cache();
	Ok(Value::None)
}
