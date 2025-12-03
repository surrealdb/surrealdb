use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterNamespaceStatement {
	pub compact: bool,
}

impl AlterNamespaceStatement {
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> anyhow::Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;
		// Extract ids
		let namespace_id = ctx.expect_ns_id(opt).await?;
		// Do we request compacting?
		if self.compact {
			let namespace_root = crate::key::namespace::all::new(namespace_id);
			ctx.tx().compact(Some(namespace_root)).await?;
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for AlterNamespaceStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterNamespaceStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
