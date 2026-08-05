//! The vector family, reunited.
//!
//! The arithmetic, distance and similarity functions are pure and live one
//! crate down. `distance::knn` does not: it reports the distance the KNN index
//! already computed for the document being iterated, so it needs the execution
//! context and stays here.

pub use surrealdb_runtime::vector::{
	add, angle, cross, divide, dot, magnitude, multiply, normalize, project, scale, subtract, sum,
};

pub mod similarity {
	pub use surrealdb_runtime::vector::similarity::*;
}

pub mod distance {
	use anyhow::Result;
	pub use surrealdb_runtime::vector::distance::*;

	use crate::ctx::FrozenContext;
	use crate::doc::CursorDoc;
	use crate::fnc::args::Optional;
	use crate::fnc::get_execution_context;
	use crate::idx::planner::IterationStage;
	use crate::val::Value;

	pub fn knn(
		(ctx, doc): (&FrozenContext, Option<&CursorDoc>),
		(Optional(knn_ref),): (Optional<Value>,),
	) -> Result<Value> {
		if let Some((_exe, doc, thg)) = get_execution_context(ctx, doc) {
			if let Some(ir) = &doc.ir
				&& let Some(d) = ir.dist()
			{
				return Ok(d.into());
			}
			if let Some(IterationStage::Iterate(Some(results))) = ctx.get_iteration_stage() {
				let n = if let Some(Value::Number(n)) = knn_ref {
					n.as_usize()
				} else {
					0
				};
				if let Some(d) = results.get_dist(n, thg) {
					return Ok(d.into());
				}
			}
		}
		Ok(Value::None)
	}
}
