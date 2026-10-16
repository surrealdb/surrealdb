//! The statement surgery of a container: a deep copy of one statement, adoption of a patched copy
//! in its place, and the legality snapshot retaken over the adopted ops.
//!
//! Copy and adoption are the container's own ([`Ir::copy_statement`] and
//! [`Rewriter::adopt_statement`], which drops the facts cached for the replaced code), and the loop
//! facts the snapshot stores are the legality analysis's answers. Each call either succeeds or
//! changes nothing.

use super::decl::StmtMap;
use super::elaborate::StatementSurgery;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::loop_legality;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{OpId, RegionId};
use crate::exec::ir::iface::region_kind::RegionKind;

/// The statement surgery of a container: its copy and adopt, and loop legality retaken by the
/// legality analysis.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ContainerSurgery;

/// The surgery production planning uses.
pub(crate) static CONTAINER_SURGERY: ContainerSurgery = ContainerSurgery;

impl StatementSurgery for ContainerSurgery {
	fn copy_statement(&self, ir: &Ir, stmt: OpId) -> Result<(Ir, StmtMap), IrError> {
		let (copy, map) = ir.copy_statement(stmt)?;
		let map = StmtMap::from_id_map(&copy, map);
		Ok((copy, map))
	}

	/// Adopts `from` through the rewriter, with the container's token of the copy, which `map`
	/// must carry for `stmt`: a map this surgery did not make has none, and a token of another
	/// statement does not adopt here. Either answers [`IrError::Invariant`] and changes nothing.
	fn adopt_statement(
		&self,
		ir: &mut Ir,
		facts: &mut FactCache,
		stmt: OpId,
		from: Ir,
		map: &StmtMap,
	) -> Result<(), IrError> {
		let Some(container) = map.container.as_ref() else {
			return Err(IrError::Invariant {
				what: "statement adoption without the container's id map",
			});
		};
		if container.stmt() != stmt {
			return Err(IrError::Invariant {
				what: "statement adoption of a copy of another statement",
			});
		}
		Rewriter::new(ir, facts).adopt_statement(from, container)
	}

	/// Retakes the entries of the legality snapshot that the loops of `stmt` stand for.
	///
	/// A snapshot that was never taken stays absent. Otherwise each live loop of the statement's
	/// region is analysed, and the snapshot is the old one with the entries of the logical ids
	/// those loops stand for replaced or inserted: the ids a loop's provenance lists, or the loop's
	/// own id when it stands for itself. Every other entry is kept, among them the entries of other
	/// statements and of loops the statement no longer holds. The new snapshot is computed in full
	/// before it is stored, so a failed analysis leaves `facts` as it was.
	fn retake_legality(
		&self,
		ir: &Ir,
		facts: &mut FactCache,
		registry: &FunctionRegistry,
		stmt: OpId,
	) -> Result<(), IrError> {
		let Some(held) = facts.legality_snapshot() else {
			return Ok(());
		};
		let mut slot = held.clone();
		let region = statement_region(ir, stmt)?;
		loop_legality::snapshot_into(ir, facts, registry, region, &mut slot)?;
		facts.set_legality_snapshot(slot);
		Ok(())
	}
}

/// The `Statement(_)` region of the statement op `stmt`.
fn statement_region(ir: &Ir, stmt: OpId) -> Result<RegionId, IrError> {
	let data = ir.op(stmt).ok_or(IrError::Stale("op"))?;
	data.regions
		.iter()
		.copied()
		.find(|region| {
			ir.region(*region).is_some_and(|held| matches!(held.kind, RegionKind::Statement(_)))
		})
		.ok_or(IrError::Stale("statement region"))
}
