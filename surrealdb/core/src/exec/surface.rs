//! The statement families an entry serves.
//!
//! A [`Surface`] is chosen by the entry that starts a query and stored on the root
//! [`Context`](crate::ctx::Context); every context derived from it inherits the same surface. Each
//! family a surface leaves out is an injection point that holds `None`: shared code calls through
//! the hook when it is present, and refuses when it is not. The implementation of a family lives in
//! a `static` that only the surface which serves it names, so an entry whose surface leaves the
//! family out reaches none of its code.
//!
//! The families are the schema statements and their companions (`INFO`, `LIVE`, `KILL`, `USE`,
//! `OPTION`, `ACCESS`, `SHOW`), `EXPLAIN`, and GQL `MATCH`. A surface also names the codec that
//! reads and writes the catalog types it serves.
//!
//! The refusal applies to every unit lowered or run under the context, stored code included: a
//! function body, a closure or an event that holds a statement of a family the surface leaves out
//! fails at that statement.

pub(crate) use super::ir::build::match_fold::MatchBinding;
pub(crate) use super::ir::build::stmt::SchemaBinding;
pub(crate) use super::vm::codec::Codec;
pub(crate) use super::vm::ir_lower::{ExplainLowering, SchemaLowering};
pub(crate) use super::vm::store::SchemaSteps;

/// The statement families an entry serves, as the hooks that lower and run the ones that are not
/// data statements.
pub(crate) struct Surface {
	/// Binds schema statements, `INFO`, `LIVE`, `KILL`, `USE`, `OPTION`, `ACCESS` and `SHOW`.
	/// `None` refuses each of them.
	pub(crate) schema_binding: Option<&'static (dyn SchemaBinding + Sync)>,
	/// Lowers schema statements, `INFO`, `LIVE`, `KILL`, `USE`, `OPTION`, `ACCESS` and `SHOW`.
	/// `None` refuses each of them.
	pub(crate) schema_lowering: Option<&'static (dyn SchemaLowering + Sync)>,
	/// Runs the store instructions those statements lower to. `None` refuses each of them.
	pub(crate) schema_steps: Option<&'static (dyn SchemaSteps + Sync)>,
	/// Lowers `EXPLAIN`, plain and analysed. `None` refuses it.
	pub(crate) explain_lowering: Option<&'static (dyn ExplainLowering + Sync)>,
	/// Binds a GQL `MATCH` statement. `None` refuses it.
	pub(crate) match_binding: Option<&'static (dyn MatchBinding + Sync)>,
	/// Reads and writes the catalog types the entry serves. A key of a kind the codec leaves out
	/// is refused.
	pub(crate) codec: &'static Codec,
}

/// The data statements alone.
pub(crate) static DML: Surface = Surface {
	schema_binding: None,
	schema_lowering: None,
	schema_steps: None,
	explain_lowering: None,
	match_binding: None,
	codec: &crate::exec::vm::codec::EDGE_CODEC,
};

/// No statement at all: the surface a morsel leaf runs units under
/// ([`Datastore::run_morsel`](crate::kvs::Datastore::run_morsel)). A unit holds no statement and
/// reads no catalog entry, so the leaf's codec serves no catalog kind.
pub(crate) static LEAF: Surface = Surface {
	schema_binding: None,
	schema_lowering: None,
	schema_steps: None,
	explain_lowering: None,
	match_binding: None,
	codec: &crate::exec::vm::codec::LEAF_CODEC,
};

/// Every statement family.
pub(crate) static FULL: Surface = Surface {
	schema_binding: Some(&crate::exec::ir::build::stmt::FULL_SCHEMA_BINDING),
	schema_lowering: Some(&crate::exec::vm::ir_lower::FULL_SCHEMA_LOWERING),
	schema_steps: Some(&crate::exec::vm::store::FULL_SCHEMA_STEPS),
	explain_lowering: Some(&crate::exec::vm::ir_lower::FULL_EXPLAIN_LOWERING),
	match_binding: Some(&crate::exec::ir::build::match_fold::FULL_MATCH_BINDING),
	codec: &crate::exec::vm::codec::FULL_CODEC,
};
