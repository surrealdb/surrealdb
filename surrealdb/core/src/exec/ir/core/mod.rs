//! The IR container and the operations on it: the arena, ops, values, blocks and regions, the
//! use-lists, the rewriter, the verifier, the text form and the pattern and conversion drivers.
//!
//! One [`Ir`] owns every entity of one program. Ids name entities and never cross containers. The
//! rewriter is the only mutation API: it keeps use-lists exact and bumps the generation of every
//! op it touches, which makes the fact cache's entries for that op and its ancestors stale.

pub(crate) mod attrs;
pub(crate) mod block;
pub(crate) mod census;
pub(crate) mod convert;
pub(crate) mod copy;
pub(crate) mod facts;
pub(crate) mod fxhash;
pub(crate) mod ir;
pub(crate) mod location;
pub(crate) mod op;
pub(crate) mod parse;
pub(crate) mod pattern;
pub(crate) mod print;
pub(crate) mod region;
pub(crate) mod rewriter;
pub(crate) mod side_map;
pub(crate) mod text;
pub(crate) mod token;
pub(crate) mod value;
pub(crate) mod value_table;
pub(crate) mod verify;
pub(crate) mod walk;

#[cfg(test)]
mod tests;

// The re-exports are the surface the rest of `exec::ir` names the container through. The allows
// cover re-exports that a given build configuration leaves unimported.
#[allow(unused_imports)]
pub(crate) use block::BlockData;
#[allow(unused_imports)]
pub(crate) use convert::{ConversionPattern, ConversionReport, ConversionSet, Converted, convert};
#[allow(unused_imports)]
pub(crate) use copy::{IdMap, RegionUnit, StatementCopy};
#[allow(unused_imports)]
pub(crate) use facts::{FactCache, LegalitySnapshotSlot, OpFacts};
#[allow(unused_imports)]
pub(crate) use ir::{Ir, IrScope, OpList, Walk, WalkOrder};
#[allow(unused_imports)]
pub(crate) use location::{LocOrigin, Location, Span};
#[allow(unused_imports)]
pub(crate) use op::{OpData, OpKind, OpTag};
#[allow(unused_imports)]
pub(crate) use parse::ParseError;
#[cfg(test)]
pub(crate) use parse::parse;
#[allow(unused_imports)]
pub(crate) use pattern::{
	GREEDY_DEFAULT, GreedyLimits, GreedyReport, Limit, Pattern, PatternSet, apply_greedy,
};
#[allow(unused_imports)]
pub(crate) use print::{AttrSyntax, AttrSyntaxes, PrintOpts, print, print_with};
#[allow(unused_imports)]
pub(crate) use region::RegionData;
#[allow(unused_imports)]
pub(crate) use rewriter::{InsertPoint, OpSpec, Provenance, Rewriter};
#[allow(unused_imports)]
pub(crate) use value::{Use, ValueData, ValueDef};
#[allow(unused_imports)]
pub(crate) use value_table::ValueTable;
#[allow(unused_imports)]
pub(crate) use verify::{VerifyAt, VerifyError, VerifyRule, verify};
#[allow(unused_imports)]
pub(crate) use walk::ControlTarget;

// The planner future that holds a container and its fact cache across an `.await` is `Send`, so
// neither type, nor the rewriter's borrow of them, may hold a `Rc`, a `RefCell` or any other
// non-`Send` handle.
const _: () = {
	const fn assert_send<T: Send>() {}
	assert_send::<Ir>();
	assert_send::<FactCache>();
	assert_send::<Rewriter<'static>>();
};
