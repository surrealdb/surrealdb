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
	use crate::val::Value;

	/// The distance the KNN index computed for the record being iterated is
	/// held by the index-driven scan, and the engine's operators
	/// answer this function there. Reached from any other caller there is no
	/// index to read from, and the answer is `NONE`.
	pub fn knn(
		(_ctx, _doc): (&FrozenContext, Option<&CursorDoc>),
		(Optional(_knn_ref),): (Optional<Value>,),
	) -> Result<Value> {
		Ok(Value::None)
	}
}
