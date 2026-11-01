//! The effects the physical catalog alone determines.
//!
//! A physical op's effects are derived from the logical ops it stands for. The additions here are
//! those only an index or a layout can add: a unique index makes a write able to reject and to
//! fail on stored data. Nothing here derives legality or an effect the logical layer states.
//!
//! The answer is a function of the op and of [`CatalogFacts`], the catalog answers the caller
//! holds. The signature names no physical catalog. A caller that sees only the effect context
//! folds with [`CatalogFacts::UNKNOWN`]. A caller that holds the catalog's answers passes them
//! and gets the narrower addition.

#![deny(clippy::wildcard_enum_match_arm)]

use super::ops::{MatchStepAttrs, PhysOp};
use crate::exec::ir::iface::fallibility::Fallibility;
use crate::exec::ir::iface::statements::BindStepShape;
use crate::expr::logical::EffectSet;

/// What the physical catalog states about the table an op touches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CatalogFacts {
	/// Whether the table has a unique index. `None` when the catalog was not asked.
	pub(crate) unique_index: Option<bool>,
}

impl CatalogFacts {
	/// No catalog answer is known.
	pub(crate) const UNKNOWN: Self = Self {
		unique_index: None,
	};
}

/// The effects and failure classes the physical catalog adds to an op, to be unioned with the
/// ones derived from the op's logical provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CatalogAdditions {
	pub(crate) set: EffectSet,
	pub(crate) fallibility: Fallibility,
}

impl CatalogAdditions {
	/// Nothing is added.
	pub(crate) const NONE: Self = Self {
		set: EffectSet::PURE,
		fallibility: Fallibility::INFALLIBLE,
	};

	/// The addition of a write that a unique index may reject: it may reject by a catalog rule,
	/// and it fails as a function of the stored records.
	const UNIQUE_WRITE: Self = Self {
		set: EffectSet::MAY_REJECT,
		fallibility: Fallibility::DATA,
	};
}

/// What a record write adds on a table whose unique-index answer is `catalog`.
fn unique_write(catalog: CatalogFacts) -> CatalogAdditions {
	match catalog.unique_index {
		Some(false) => CatalogAdditions::NONE,
		Some(true) | None => CatalogAdditions::UNIQUE_WRITE,
	}
}

/// The effects and failure classes `catalog` adds to `op`.
///
/// A write whose table has a unique index adds `MAY_REJECT` and [`Fallibility::DATA`]. A write
/// whose catalog answer is unknown adds the same, the conservative answer, and a write on a table
/// known to have no unique index adds nothing. The insert and update stages of a `MATCH` write
/// records and answer as a write does. Every other op adds nothing. The answer states
/// effects only: it never decides legality.
pub(crate) fn additions(op: &PhysOp, catalog: CatalogFacts) -> CatalogAdditions {
	match op {
		PhysOp::Write(..) => unique_write(catalog),
		PhysOp::MatchStep(attrs) => match attrs.as_ref() {
			MatchStepAttrs::Insert(..)
			| MatchStepAttrs::Update(..)
			| MatchStepAttrs::Bind(BindStepShape::Insert {
				..
			})
			| MatchStepAttrs::Bind(BindStepShape::Update {
				..
			}) => unique_write(catalog),
			MatchStepAttrs::Read {
				..
			}
			| MatchStepAttrs::Delete {
				..
			}
			| MatchStepAttrs::Output(..)
			| MatchStepAttrs::Bind(..) => CatalogAdditions::NONE,
		},
		PhysOp::TableScan(..)
		| PhysOp::IndexScan(..)
		| PhysOp::IndexUnion(..)
		| PhysOp::Bitmap(..)
		| PhysOp::IndexLookup(..)
		| PhysOp::FullTextScan(..)
		| PhysOp::KeyRangeScan(..)
		| PhysOp::KnnScan(..)
		| PhysOp::KnnTopK(..)
		| PhysOp::Count(..)
		| PhysOp::RecordScan(..)
		| PhysOp::Vacant {
			..
		}
		| PhysOp::Enforced {
			..
		}
		| PhysOp::Values {
			..
		}
		| PhysOp::Subquery
		| PhysOp::Probe {
			..
		}
		| PhysOp::Union
		| PhysOp::Join(..)
		| PhysOp::Distinct {
			..
		}
		| PhysOp::PermissionFilter(..)
		| PhysOp::Layer(..)
		| PhysOp::Filter
		| PhysOp::Fetch
		| PhysOp::Split {
			..
		}
		| PhysOp::Sort(..)
		| PhysOp::Window(..)
		| PhysOp::Aggregate(..)
		| PhysOp::Limit {
			..
		}
		| PhysOp::Project(..)
		| PhysOp::Eval
		| PhysOp::Request {
			..
		}
		| PhysOp::Txn {
			..
		}
		| PhysOp::Stmt {
			..
		}
		| PhysOp::Statement {
			..
		}
		| PhysOp::Match {
			..
		}
		| PhysOp::DdlGuard {
			..
		}
		| PhysOp::Define(..)
		| PhysOp::Remove(..)
		| PhysOp::Alter(..)
		| PhysOp::Rebuild(..)
		| PhysOp::Info {
			..
		}
		| PhysOp::Use {
			..
		}
		| PhysOp::Option {
			..
		}
		| PhysOp::Access(..)
		| PhysOp::Kill
		| PhysOp::Live(..)
		| PhysOp::Subscriber(..)
		| PhysOp::Show(..)
		| PhysOp::WriteStage {
			..
		}
		| PhysOp::Events(..)
		| PhysOp::Timeout
		| PhysOp::VersionScope
		| PhysOp::Only
		| PhysOp::Explain {
			..
		}
		| PhysOp::Assign(..)
		| PhysOp::Unset {
			..
		}
		| PhysOp::Content {
			..
		}
		| PhysOp::Block
		| PhysOp::Let(..)
		| PhysOp::Sleep
		| PhysOp::If {
			..
		}
		| PhysOp::Loop {
			..
		}
		| PhysOp::Pipeline(..)
		| PhysOp::Choose(..)
		| PhysOp::Prefetch(..)
		| PhysOp::Materialize {
			..
		}
		| PhysOp::LoadUnit(..)
		| PhysOp::Yield
		| PhysOp::Return
		| PhysOp::Break
		| PhysOp::Next => CatalogAdditions::NONE,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::iface::statements::{OutputShape, WriteFlags, WriteForm};
	use crate::exec::ir::logical::ops::{MatchInsertAttrs, WriteAttrs};
	use crate::exec::ir::phys::ops::{PhysWriteAttrs, WriteSubjectsShape};

	fn write() -> PhysOp {
		PhysOp::Write(Box::new(PhysWriteAttrs {
			write: WriteAttrs {
				form: WriteForm::Create,
				only: false,
				output: OutputShape::After,
				with: None,
				explain: None,
				flags: WriteFlags::default(),
				body: crate::exec::ir::logical::ops::WriteBody::default(),
				gates: crate::exec::ir::logical::ops::WriteGates::default(),
				image: crate::exec::ir::logical::ops::WriteImage::default(),
				stored: Default::default(),
				derived: Default::default(),
				into_text: None,
				statement: None,
				linked: None,
			},
			subjects: WriteSubjectsShape::Targets,
		}))
	}

	#[test]
	fn a_write_without_catalog_answers_is_conservative() {
		let added = additions(&write(), CatalogFacts::UNKNOWN);
		assert!(added.set.contains(EffectSet::MAY_REJECT));
		assert!(added.fallibility.contains(Fallibility::DATA));
	}

	#[test]
	fn a_match_write_without_catalog_answers_is_conservative() {
		let insert = PhysOp::MatchStep(Box::new(MatchStepAttrs::Insert(MatchInsertAttrs {
			nodes: Box::new([]),
			edges: Box::new([]),
		})));
		assert_eq!(
			additions(&insert, CatalogFacts::UNKNOWN),
			additions(&write(), CatalogFacts::UNKNOWN)
		);
		let none = CatalogFacts {
			unique_index: Some(false),
		};
		assert_eq!(additions(&insert, none), CatalogAdditions::NONE);
		let read = PhysOp::MatchStep(Box::new(MatchStepAttrs::Read {
			optional_group: None,
			patterns: Box::new([]),
			predicates: 0,
		}));
		assert_eq!(additions(&read, CatalogFacts::UNKNOWN), CatalogAdditions::NONE);
	}

	#[test]
	fn a_write_on_a_unique_index_may_reject() {
		let facts = CatalogFacts {
			unique_index: Some(true),
		};
		assert_eq!(additions(&write(), facts), additions(&write(), CatalogFacts::UNKNOWN));
	}

	#[test]
	fn a_write_without_a_unique_index_adds_nothing() {
		let facts = CatalogFacts {
			unique_index: Some(false),
		};
		assert_eq!(additions(&write(), facts), CatalogAdditions::NONE);
	}

	#[test]
	fn a_read_adds_nothing() {
		for facts in [
			CatalogFacts::UNKNOWN,
			CatalogFacts {
				unique_index: Some(true),
			},
		] {
			assert_eq!(additions(&PhysOp::Filter, facts), CatalogAdditions::NONE);
			assert_eq!(additions(&PhysOp::Union, facts), CatalogAdditions::NONE);
		}
	}
}
