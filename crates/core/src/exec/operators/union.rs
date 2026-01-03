use std::sync::Arc;

use futures::stream::{self, StreamExt};

use crate::err::Error;
use crate::exec::{ContextLevel, ExecutionContext, OperatorPlan, ValueBatchStream};

/// Union operator - combines results from multiple execution plans.
///
/// Fetches from all inputs in parallel but returns results in order:
/// all results from input 0, then all from input 1, etc.
///
/// This is used for `SELECT * FROM a, b, c` which should fetch from a, b, and c
/// in parallel, but return results in order a → b → c.
#[derive(Debug, Clone)]
pub struct Union {
	pub(crate) inputs: Vec<Arc<dyn OperatorPlan>>,
}

impl OperatorPlan for Union {
	fn name(&self) -> &'static str {
		"Union"
	}

	fn required_context(&self) -> ContextLevel {
		// Union requires the maximum context level of all its inputs
		self.inputs.iter().map(|input| input.required_context()).max().unwrap_or(ContextLevel::Root)
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		self.inputs.iter().collect()
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		if self.inputs.is_empty() {
			// Empty union returns empty stream
			return Ok(Box::pin(stream::empty()));
		}

		if self.inputs.len() == 1 {
			// Single input - just delegate
			return self.inputs[0].execute(ctx);
		}

		// Execute all inputs and collect their streams
		// Each input produces a stream of batches
		let mut input_streams = Vec::with_capacity(self.inputs.len());
		for input in &self.inputs {
			input_streams.push(input.execute(ctx)?);
		}

		// Chain all streams together in order
		// This preserves the ordering: all from input 0, then all from input 1, etc.
		//
		// Note: While this chains sequentially for output ordering, the streams
		// themselves can be executing in parallel internally. For true parallel
		// execution with buffering, we'd use FuturesOrdered or similar.
		// For now, we use the simpler sequential chain approach.
		let combined = stream::iter(input_streams).flatten();

		Ok(Box::pin(combined))
	}
}

#[cfg(test)]
mod tests {
	// Tests are in exec/mod.rs as end-to-end tests
}
