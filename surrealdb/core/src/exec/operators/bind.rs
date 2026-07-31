//! `Bind` — wrap anchor records into single-binding MATCH rows.
//!
//! `Bind` sits directly above the anchor source (an ordinary `TableScan`, which
//! already applies table/field permissions, computed fields, and any pushed
//! pre-decode filter). It maps each input value `v` to the binding row
//! `{ name: v }` — a `Value::Object` with one entry, keyed by the binding name
//! (the binding-row convention, `doc/gql/V2_DESIGN.md` §3). Downstream
//! operators (Expand / EndpointBind / joins) read and extend that object.
//!
//! `Bind` is structurally transparent: it neither reorders nor drops rows, so
//! it delegates `cardinality_hint` and `output_ordering` to its input.

// The GQL v2 MATCH operators are constructed only by the gql-gated
// planner (`Expr::Match` is `#[cfg(feature = "gql")]`), so they are dead
// code when the feature is off — suppress the lint there only, keeping
// dead-code detection active in the default (gql-on) build.
#![cfg_attr(not(feature = "gql"), allow(dead_code))]

use std::sync::Arc;

use futures::TryStreamExt;

use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, OutputOrdering, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::val::{Object, Value};

/// Wraps each input value into a single-binding row `{ name: value }`.
#[derive(Debug, Clone)]
pub struct Bind {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The binding name the input value is bound under.
	pub(crate) name: String,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Bind {
	/// Create a new `Bind` binding `input`'s rows under `name`.
	pub(crate) fn new(input: Arc<dyn ExecOperator>, name: String) -> Self {
		Self {
			input,
			name,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl ExecOperator for Bind {
	fn name(&self) -> &'static str {
		"Bind"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("binding".to_string(), self.name.clone())]
	}

	fn required_context(&self) -> ContextLevel {
		// Binding rows are only meaningful at database level; never demote
		// below the input's requirement.
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		// One row out per row in; cardinality is unchanged.
		self.input.cardinality_hint()
	}

	fn output_ordering(&self) -> OutputOrdering {
		// Rows are passed through in order, only re-wrapped.
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let name = self.name.clone();

		let bound = input_stream.map_ok(move |x| {
			let values = x
				.values
				.into_iter()
				.map(|x| {
					let mut row = Object::default();
					row.insert(name.clone(), x);
					Value::Object(row)
				})
				.collect();
			ValueBatch {
				values,
			}
		});

		Ok(monitor_stream(Box::pin(bound), "Bind", &self.metrics))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{ValuesOperator, collect, root_ctx};

	#[tokio::test]
	async fn binds_each_value_under_name() {
		let input = ValuesOperator::new(vec![Value::from(1), Value::from(2)]);
		let bind: Arc<dyn ExecOperator> = Arc::new(Bind::new(input, "a".to_string()));
		let ctx = root_ctx();

		let rows = collect(&bind, &ctx).await;
		assert_eq!(rows.len(), 2);

		let expect = |n: i64| {
			let mut o = Object::default();
			o.insert("a".to_string(), Value::from(n));
			Value::Object(o)
		};
		assert_eq!(rows[0], expect(1));
		assert_eq!(rows[1], expect(2));
	}

	#[tokio::test]
	async fn preserves_stream_order_and_empty_input() {
		// Empty input yields no rows.
		let empty: Arc<dyn ExecOperator> =
			Arc::new(Bind::new(ValuesOperator::new(Vec::new()), "x".to_string()));
		let ctx = root_ctx();
		assert!(collect(&empty, &ctx).await.is_empty());

		// A whole-record object value is wrapped, not merged.
		let mut rec = Object::default();
		rec.insert("id".to_string(), Value::from("person:1"));
		rec.insert("name".to_string(), Value::from("Tobie"));
		let input = ValuesOperator::new(vec![Value::Object(rec.clone())]);
		let bind: Arc<dyn ExecOperator> = Arc::new(Bind::new(input, "a".to_string()));
		let rows = collect(&bind, &ctx).await;
		assert_eq!(rows.len(), 1);
		let mut expect = Object::default();
		expect.insert("a".to_string(), Value::Object(rec));
		assert_eq!(rows[0], Value::Object(expect));
	}

	#[test]
	fn attrs_report_binding_name() {
		let bind = Bind::new(ValuesOperator::new(Vec::new()), "person".to_string());
		assert_eq!(bind.name(), "Bind");
		assert_eq!(bind.attrs(), vec![("binding".to_string(), "person".to_string())]);
	}
}
