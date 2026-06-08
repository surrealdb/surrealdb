use std::sync::Arc;

use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, CombineAccessModes};
use crate::expr::FlowResult;
use crate::val::Value;

/// Array literal - [1, 2, 3] or [expr1, expr2, ...]
#[derive(Debug, Clone)]
pub struct ArrayLiteral {
	pub(crate) elements: Vec<Arc<dyn PhysicalExpr>>,
}
impl PhysicalExpr for ArrayLiteral {
	fn name(&self) -> &'static str {
		"ArrayLiteral"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		self.elements
			.iter()
			.map(|e| e.required_context())
			.max()
			.unwrap_or(crate::exec::ContextLevel::Root)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let mut values = Vec::with_capacity(self.elements.len());
			for elem in &self.elements {
				let value = elem.evaluate(ctx.clone()).await?;
				values.push(value);
			}
			Ok(Value::Array(crate::val::Array::from(values)))
		})
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
	pub(crate) entries: Vec<(Strand, Arc<dyn PhysicalExpr>)>,
}
impl PhysicalExpr for ObjectLiteral {
	fn name(&self) -> &'static str {
		"ObjectLiteral"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		self.entries
			.iter()
			.map(|(_, e)| e.required_context())
			.max()
			.unwrap_or(crate::exec::ContextLevel::Root)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let mut map = std::collections::BTreeMap::new();
			for (key, expr) in &self.entries {
				let value = expr.evaluate(ctx.clone()).await?;
				map.insert(key.clone(), value);
			}
			Ok(Value::Object(crate::val::Object::from(map)))
		})
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
			write_sql!(f, fmt, "{}: {}", key.as_str(), expr);
		}
		f.push('}');
	}
}

/// Set literal - <{expr1, expr2, ...}>
#[derive(Debug, Clone)]
pub struct SetLiteral {
	pub(crate) elements: Vec<Arc<dyn PhysicalExpr>>,
}
impl PhysicalExpr for SetLiteral {
	fn name(&self) -> &'static str {
		"SetLiteral"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		self.elements
			.iter()
			.map(|e| e.required_context())
			.max()
			.unwrap_or(crate::exec::ContextLevel::Root)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let mut set = crate::val::Set::new();
			for elem in &self.elements {
				let value = elem.evaluate(ctx.clone()).await?;
				set.insert(value);
			}
			Ok(Value::Set(set))
		})
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
