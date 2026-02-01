use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::val::Value;

/// Literal value - "foo", 42, true
#[derive(Debug, Clone)]
pub struct Literal(pub(crate) Value);

#[async_trait]
impl PhysicalExpr for Literal {
	fn name(&self) -> &'static str {
		"Literal"
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		Ok(self.0.clone())
	}

	fn references_current_value(&self) -> bool {
		false
	}

	fn access_mode(&self) -> AccessMode {
		// Literals are always read-only
		AccessMode::ReadOnly
	}
}

impl ToSql for Literal {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}

/// Parameter reference - $foo
#[derive(Debug, Clone)]
pub struct Param(pub(crate) String);

#[async_trait]
impl PhysicalExpr for Param {
	fn name(&self) -> &'static str {
		"Param"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// First check block-local parameters (they shadow global params)
		if let Some(local_params) = ctx.local_params {
			if let Some(value) = local_params.get(&self.0) {
				return Ok(value.clone());
			}
		}

		// Fall back to global parameters from execution context
		ctx.exec_ctx
			.params()
			.get(self.0.as_str())
			.map(|v| (**v).clone())
			.ok_or_else(|| anyhow::anyhow!("Parameter not found: ${}", self.0))
	}

	fn references_current_value(&self) -> bool {
		false
	}

	fn access_mode(&self) -> AccessMode {
		// Parameter references are read-only
		AccessMode::ReadOnly
	}
}

impl ToSql for Param {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "${}", self.0)
	}
}
