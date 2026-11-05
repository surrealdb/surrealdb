//! The row writer of a projection: [`projection_writes`] turns the bindings a
//! projection function evaluated into the writes made at the projection's root.

use crate::exec::field_path::FieldPath;
use crate::val::Value;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// FieldSelection
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Project operator
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// evaluate_and_set_field
// ---------------------------------------------------------------------------

/// The writes a projection function's `bindings` make at a projection's
/// root: with an explicit alias, one write at `output_path` — the one
/// binding's value, or every binding's value in an array; otherwise one write
/// per binding, keyed by the binding's idiom, skipping an idiom that names no
/// field path.
pub(crate) fn projection_writes(
	bindings: Vec<(crate::expr::idiom::Idiom, Value)>,
	aliased: bool,
	output_path: &FieldPath,
	writes: &mut Vec<(FieldPath, Value)>,
) {
	if aliased {
		let value = if bindings.len() == 1 {
			bindings.into_iter().next().expect("bindings verified non-empty").1
		} else {
			Value::Array(bindings.into_iter().map(|(_, v)| v).collect::<Vec<_>>().into())
		};
		writes.push((output_path.clone(), value));
	} else {
		for (idiom, value) in bindings {
			if let Ok(path) = crate::exec::field_path_convert::field_path_from_idiom(&idiom)
				&& !path.is_empty()
			{
				writes.push((path, value));
			}
		}
	}
}

// ---------------------------------------------------------------------------
// OMIT helpers
// ---------------------------------------------------------------------------

// ============================================================================
// SelectProject - Simplified projection without expression evaluation
// ============================================================================

// ---------------------------------------------------------------------------
// SelectProject helpers
// ---------------------------------------------------------------------------
