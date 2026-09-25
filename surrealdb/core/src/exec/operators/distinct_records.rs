//! `DistinctRecords` — admits each record once out of a scan that can reach it
//! several times.
//!
//! An index column whose value is an array contributes one entry per element,
//! so a key range that does not pin such a column reaches one record through
//! several entries. The scan yields records rather than entries, so the repeats
//! have to be collapsed below everything that counts rows — otherwise a record
//! is returned once per element, `count()` reports entries as rows, and a
//! mutating statement applies itself once per element.
//!
//! Keyed on the record id, not the row: ids are already unique, so a whole-row
//! key would cost a document clone and a structural comparison per distinct row
//! to answer a question the id answers. [`UnionIndexScan`]'s
//! `ByIndexKeyDedup` mode and the legacy engine's `FanOutDedupe` key the same
//! way for the same reason.
//!
//! Entries for one record are not adjacent in key order — they differ in the
//! columns the range leaves open, which sort ahead of the record id — so every
//! id seen so far is kept rather than compared with the previous one. The set
//! therefore grows with the number of distinct records the scan returns.
//!
//! [`UnionIndexScan`]: super::UnionIndexScan

use std::sync::Arc;

use ahash::HashSet;
use common::future::stream::{self, Yielder};
use futures::StreamExt;

use super::scan::extract_rid;
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	OutputOrdering, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::val::RecordId;

/// Emits the first row carrying each record id, dropping later rows that carry
/// one already seen.
#[derive(Debug, Clone)]
pub struct DistinctRecords {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DistinctRecords {
	/// Create a new `DistinctRecords` over `input`.
	pub(crate) fn new(input: Arc<dyn ExecOperator>) -> Self {
		Self {
			input,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl ExecOperator for DistinctRecords {
	fn name(&self) -> &'static str {
		"DistinctRecords"
	}

	fn required_context(&self) -> ContextLevel {
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

	fn output_ordering(&self) -> OutputOrdering {
		// Dropping a repeat leaves the surviving rows in their input order, so
		// whatever ordering the input guarantees still holds.
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let mut seen: HashSet<RecordId> = HashSet::default();
			while let Some(batch_result) = input_stream.next().await {
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;
				let mut values = Vec::new();
				for value in batch.into_values() {
					// A row carrying no id cannot be a repeat of one that
					// does, so it passes rather than being dropped.
					match extract_rid(&value) {
						Some(rid) => {
							if seen.insert(rid) {
								values.push(value);
							}
						}
						None => values.push(value),
					}
				}
				if !values.is_empty() {
					yielder.emit(ValueBatch::new(values)).await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "DistinctRecords", &self.metrics))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{ValuesOperator, collect, root_ctx};
	use crate::val::{Object, Value};

	fn row(table: &str, id: i64, tag: &str) -> Value {
		let mut o = Object::default();
		o.insert("id".to_string(), Value::RecordId(RecordId::new(table.into(), id)));
		o.insert("tag".to_string(), Value::from(tag));
		Value::Object(o)
	}

	#[tokio::test]
	async fn a_repeated_record_id_is_admitted_once() {
		let input = ValuesOperator::new(vec![row("t", 1, "x"), row("t", 1, "y"), row("t", 2, "z")]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctRecords::new(input));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(
			out,
			vec![row("t", 1, "x"), row("t", 2, "z")],
			"the first row for an id survives"
		);
	}

	#[tokio::test]
	async fn ids_from_different_tables_are_different_records() {
		let input = ValuesOperator::new(vec![row("a", 1, "x"), row("b", 1, "x")]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctRecords::new(input));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(out.len(), 2, "the table is part of the identity");
	}

	#[tokio::test]
	async fn a_row_without_an_id_passes_through() {
		// Nothing identifies such a row, so it cannot be shown to repeat one.
		let input = ValuesOperator::new(vec![Value::from(1), Value::from(1), row("t", 1, "x")]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctRecords::new(input));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(out.len(), 3);
	}
}
