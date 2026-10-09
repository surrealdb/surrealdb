//! Lookup part -- graph/reference traversal.

/// Direction for lookup operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupDirection {
	/// Outgoing edges: `->`
	Out,
	/// Incoming edges: `<-`
	In,
	/// Both directions: `<->`
	Both,
}

impl From<crate::expr::Dir> for LookupDirection {
	fn from(dir: crate::expr::Dir) -> Self {
		match dir {
			crate::expr::Dir::Out => LookupDirection::Out,
			crate::expr::Dir::In => LookupDirection::In,
			crate::expr::Dir::Both => LookupDirection::Both,
		}
	}
}

impl From<&crate::expr::Dir> for LookupDirection {
	fn from(dir: &crate::expr::Dir) -> Self {
		match dir {
			crate::expr::Dir::Out => LookupDirection::Out,
			crate::expr::Dir::In => LookupDirection::In,
			crate::expr::Dir::Both => LookupDirection::Both,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// =========================================================================
	// Direction
	// =========================================================================

	#[tokio::test]
	async fn the_direction_metadata_maps_from_the_parsed_direction() {
		assert_eq!(LookupDirection::from(crate::expr::Dir::Out), LookupDirection::Out);
		assert_eq!(LookupDirection::from(crate::expr::Dir::In), LookupDirection::In);
		assert_eq!(LookupDirection::from(crate::expr::Dir::Both), LookupDirection::Both);
		assert_eq!(LookupDirection::from(&crate::expr::Dir::Out), LookupDirection::Out);
		assert_eq!(LookupDirection::from(&crate::expr::Dir::In), LookupDirection::In);
		assert_eq!(LookupDirection::from(&crate::expr::Dir::Both), LookupDirection::Both);
	}

	// =========================================================================
	// Input shapes
	// =========================================================================

	// =========================================================================
	// Edge and target filtering
	// =========================================================================

	// =========================================================================
	// $parent binding for lookup conditions
	// =========================================================================

	// =========================================================================
	// FROM ONLY semantics
	// =========================================================================

	// =========================================================================
	// Permission propagation
	// =========================================================================

	// =========================================================================
	// Batch evaluation
	// =========================================================================

	// =========================================================================
	// Plan metadata the engine acts on
	// =========================================================================
}
