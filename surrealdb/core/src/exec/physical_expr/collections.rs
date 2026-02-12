use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, CombineAccessModes};
use crate::expr::FlowResult;
use crate::val::Value;

/// Array literal - [1, 2, 3] or [expr1, expr2, ...]
#[derive(Debug, Clone)]
pub struct ArrayLiteral {
	pub(crate) elements: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ArrayLiteral {
	fn name(&self) -> &'static str {
		"ArrayLiteral"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		self.elements
			.iter()
			.map(|e| e.required_context())
			.max()
			.unwrap_or(crate::exec::ContextLevel::Root)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let mut values = Vec::with_capacity(self.elements.len());
		for elem in &self.elements {
			let value = elem.evaluate(ctx.clone()).await?;
			values.push(value);
		}
		Ok(Value::Array(crate::val::Array::from(values)))
	}

	fn references_current_value(&self) -> bool {
		self.elements.iter().any(|e| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		self.elements.iter().map(|e| e.access_mode()).combine_all()
	}
}

impl ToSql for ArrayLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('[');
		for (i, elem) in self.elements.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			elem.fmt_sql(f, fmt);
		}
		f.push(']');
	}
}

/// Object literal - { key1: expr1, key2: expr2, ... }
#[derive(Debug, Clone)]
pub struct ObjectLiteral {
	pub(crate) entries: Vec<(String, Arc<dyn PhysicalExpr>)>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ObjectLiteral {
	fn name(&self) -> &'static str {
		"ObjectLiteral"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		self.entries
			.iter()
			.map(|(_, e)| e.required_context())
			.max()
			.unwrap_or(crate::exec::ContextLevel::Root)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let mut map = std::collections::BTreeMap::new();
		for (key, expr) in &self.entries {
			let value = expr.evaluate(ctx.clone()).await?;
			map.insert(key.clone(), value);
		}
		Ok(Value::Object(crate::val::Object(map)))
	}

	fn references_current_value(&self) -> bool {
		self.entries.iter().any(|(_, e)| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		self.entries.iter().map(|(_, e)| e.access_mode()).combine_all()
	}
}

impl ToSql for ObjectLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('{');
		for (i, (key, expr)) in self.entries.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			write_sql!(f, fmt, "{}: {}", key, expr);
		}
		f.push('}');
	}
}

/// Set literal - <{expr1, expr2, ...}>
#[derive(Debug, Clone)]
pub struct SetLiteral {
	pub(crate) elements: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for SetLiteral {
	fn name(&self) -> &'static str {
		"SetLiteral"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		self.elements
			.iter()
			.map(|e| e.required_context())
			.max()
			.unwrap_or(crate::exec::ContextLevel::Root)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let mut set = crate::val::Set::new();
		for elem in &self.elements {
			let value = elem.evaluate(ctx.clone()).await?;
			set.insert(value);
		}
		Ok(Value::Set(set))
	}

	fn references_current_value(&self) -> bool {
		self.elements.iter().any(|e| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		self.elements.iter().map(|e| e.access_mode()).combine_all()
	}
}

impl ToSql for SetLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("<{");
		for (i, elem) in self.elements.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			elem.fmt_sql(f, fmt);
		}
		f.push_str("}>");
	}
}
