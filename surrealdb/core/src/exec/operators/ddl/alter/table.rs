use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Permissions, TableType};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::statements::DefineTableStatement;
use crate::expr::statements::alter::AlterKind;
use crate::expr::{Base, ChangeFeed};
use crate::iam::{Action, ResourceKind};
use crate::val::{TableName, Value};

#[derive(Debug)]
pub struct AlterTablePlan {
	pub name: TableName,
	pub if_exists: bool,
	pub schemafull: AlterKind<()>,
	pub permissions: Option<Permissions>,
	pub changefeed: AlterKind<ChangeFeed>,
	pub comment: AlterKind<String>,
	pub compact: bool,
	pub kind: Option<TableType>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterTablePlan {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		name: TableName,
		if_exists: bool,
		schemafull: AlterKind<()>,
		permissions: Option<Permissions>,
		changefeed: AlterKind<ChangeFeed>,
		comment: AlterKind<String>,
		compact: bool,
		kind: Option<TableType>,
	) -> Self {
		Self {
			name,
			if_exists,
			schemafull,
			permissions,
			changefeed,
			comment,
			compact,
			kind,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterTablePlan {
	ddl_operator_common!("AlterTable", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		let schemafull = self.schemafull.clone();
		let permissions = self.permissions.clone();
		let changefeed = self.changefeed.clone();
		let comment = self.comment.clone();
		let compact = self.compact;
		let kind = self.kind.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(
					&ctx,
					name,
					if_exists,
					schemafull,
					permissions,
					changefeed,
					comment,
					compact,
					kind,
				)
				.await
			})
		})
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute(
	ctx: &ExecutionContext,
	name: TableName,
	if_exists: bool,
	schemafull: AlterKind<()>,
	permissions: Option<Permissions>,
	changefeed: AlterKind<ChangeFeed>,
	comment: AlterKind<String>,
	compact: bool,
	kind: Option<TableType>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let mut dt = match txn.get_tb(ns, db, &name).await? {
		Some(tb) => tb.deref().clone(),
		None => {
			if if_exists {
				return Ok(Value::None);
			} else {
				return Err(crate::err::Error::TbNotFound {
					name,
				}
				.into());
			}
		}
	};

	match schemafull {
		AlterKind::Set(_) => dt.schemafull = true,
		AlterKind::Drop => dt.schemafull = false,
		AlterKind::None => {}
	}

	if let Some(permissions) = &permissions {
		dt.permissions = permissions.clone();
	}

	let mut changefeed_replaced = false;
	match changefeed {
		AlterKind::Set(x) => {
			changefeed_replaced = dt.changefeed.is_some();
			dt.changefeed = Some(x);
		}
		AlterKind::Drop => dt.changefeed = None,
		AlterKind::None => {}
	}

	match comment {
		AlterKind::Set(ref x) => dt.comment = Some(x.clone()),
		AlterKind::Drop => dt.comment = None,
		AlterKind::None => {}
	}

	if let Some(kind) = &kind {
		dt.table_type = kind.clone();
	}

	if matches!(kind, Some(TableType::Relation(_))) {
		DefineTableStatement::add_in_out_fields(&txn, ns, db, &mut dt).await?;
	}

	if changefeed_replaced {
		txn.changefeed_buffer_table_change(ns, db, &name, &dt);
	}

	if compact {
		let key = crate::key::table::all::new(ns, db, &name);
		txn.compact(Some(key)).await?;
	}

	txn.put_tb(ns_name, db_name, &dt).await?;
	txn.clear_cache();
	Ok(Value::None)
}
