//! The physical layer's answers to a seeded e-graph run.
//!
//! [`PhysSeedHost`] adapts the functions this dialect owns to the e-graph's [`SeedHost`]: the facts
//! of the row sources, the conversion of a statement copy, the delivery order of a physical op, the
//! order licence of a row producer and the physical cost. It holds the planning session's catalog
//! views and plan-time folder and nothing else: no storage handle, no state that outlives the
//! session, and no cache. A memo a later answer needs lives in the host instance, which is one
//! planning session, and never in process-wide storage.

use surrealdb_strand::TableName;

use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::convert::convert_logged;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::egraph::decl::{CostHook, OrderSig, PhysTableFacts};
use crate::exec::ir::egraph::{SeedFuture, SeedHost};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::{PhysCx, SourceKey};
use crate::exec::ir::iface::conversion::PHYSICAL_PHASE;
use crate::exec::ir::iface::fold::PlanTimeFolder;
use crate::exec::ir::iface::ids::{OpId, SnapshotId, ValueId};
use crate::exec::ir::phys::access::ReadVersion;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::ir::phys::{cost, facts, order};
use crate::exec::ir::registry;
use crate::expr::With;

/// Loads the table answers a conversion asks for and the memo does not hold, reading the catalog
/// through the planning session's transaction.
pub(crate) trait AnswerLoader: Send + Sync {
	/// Reads the answers of the table reads `keys` into the memo the host answers from. A key
	/// the memo already holds is left as it is.
	fn load<'a>(&'a self, keys: &'a [SourceKey]) -> SeedFuture<'a, ()>;
}

/// The physical layer's answers to a seeded e-graph run, for one planning session: the facts of
/// the row sources, the conversion of a statement copy, the delivery order of a physical op, the
/// order licence of a row producer, and the physical cost. Holds no storage handle and no state
/// that outlives the planning session.
///
/// The facts of a source the memo holds no answers for are read through the host's
/// [`AnswerLoader`], when it has one, so the host fills what it reads itself.
pub(crate) struct PhysSeedHost<'a> {
	cx: PhysCx<'a>,
	/// Answers the plan-time folds a conversion of a statement copy requests.
	folder: &'a dyn PlanTimeFolder,
	loader: Option<&'a dyn AnswerLoader>,
}

impl<'a> PhysSeedHost<'a> {
	/// A host over the planning session's catalog views `cx` and plan-time folder `folder`.
	pub(crate) fn new(cx: PhysCx<'a>, folder: &'a dyn PlanTimeFolder) -> Self {
		Self {
			cx,
			folder,
			loader: None,
		}
	}

	/// This host loading the answers it is missing through `loader`.
	#[allow(dead_code)]
	pub(crate) fn with_loader(mut self, loader: &'a dyn AnswerLoader) -> Self {
		self.loader = Some(loader);
		self
	}
}

impl SeedHost for PhysSeedHost<'_> {
	/// The facts [`facts::table_facts`] answers. A source the memo holds no answers for is loaded
	/// once through the host's loader, and the facts are read again.
	fn table_facts<'a>(
		&'a self,
		sources: &'a [(TableName, Option<With>, SnapshotId)],
	) -> SeedFuture<'a, Vec<PhysTableFacts>> {
		Box::pin(async move {
			match (facts::table_facts(&self.cx, sources), self.loader) {
				(
					Err(IrError::NotLoaded {
						..
					}),
					Some(loader),
				) => {
					let keys: Vec<SourceKey> = sources
						.iter()
						.filter(|(_, _, snapshot)| *snapshot == SnapshotId::new(0))
						.map(|(table, with, _)| (table.clone(), with.clone(), ReadVersion::Current))
						.collect();
					loader.load(&keys).await?;
					facts::table_facts(&self.cx, sources)
				}
				(answered, _) => answered,
			}
		})
	}

	/// Converts `copy` to the physical dialect with the registered patterns, folds awaited through
	/// the host's folder, and answers the replacement log of the conversion.
	///
	/// A conversion that stops with an op outside the physical dialect, or that fails, is an error:
	/// the run drops the candidate the copy stands for. A table answer the memo does not hold is
	/// not loaded here; the facts of every source were read before a copy is converted.
	fn convert_copy<'a>(
		&'a self,
		copy: &'a mut Ir,
		facts: &'a mut FactCache,
	) -> SeedFuture<'a, Vec<(ValueId, ValueId)>> {
		Box::pin(async move {
			let mut all_ops = Vec::new();
			copy.walk(copy.top(), WalkOrder::PostOrder, &mut |op| {
				all_ops.push(op);
				Walk::Continue
			});
			let structural = structural_cx(self.cx.effects.registry, copy);
			for op in all_ops {
				let _ = effects::of_with(copy, facts, &structural, op);
				let _ = effects::of_with(copy, facts, &self.cx.effects, op);
			}
			let (report, log) = convert_logged(
				copy,
				facts,
				&registry::build().conversion,
				&self.cx,
				self.folder,
				&PHYSICAL_PHASE,
			)
			.await?;
			match report.stopped_by {
				None => Ok(log),
				Some(_) => Err(IrError::Invariant {
					what: "a statement copy stopped converting with a logical op left",
				}),
			}
		})
	}

	fn order_sig(&self, op: &PhysOp, input: OrderSig) -> OrderSig {
		order::order_sig(op, input)
	}

	fn order_change_admissible(&self, ir: &Ir, facts: &mut FactCache, source: OpId) -> bool {
		order::order_change_admissible(ir, facts, self.cx.effects.registry, source)
	}

	fn cost(&self) -> &dyn CostHook {
		&cost::PHYS_FIXED_COST
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::print::{PrintOpts, print};
	use crate::exec::ir::iface::catalog::{EffectCx, PhysicalCatalog, SemanticCatalog};
	use crate::exec::ir::iface::fold::NotYetFolder;
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::ir::phys::ops::{ReadClauses, TableScanAttrs};
	use crate::kvs::Direction;

	/// A semantic catalog that holds no definition.
	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	/// A physical catalog that holds no index and no statistics.
	struct NoPhysical;

	impl PhysicalCatalog for NoPhysical {}

	/// What a [`PhysCx`] borrows.
	struct Parts {
		registry: FunctionRegistry,
		frames: FrameTable,
	}

	impl Parts {
		fn new() -> Self {
			Self {
				registry: FunctionRegistry::with_builtins(),
				frames: FrameTable::default(),
			}
		}

		fn cx(&self) -> PhysCx<'_> {
			PhysCx {
				effects: EffectCx {
					registry: &self.registry,
					catalog: &NoCatalog,
					frames: &self.frames,
				},
				physical: &NoPhysical,
				config: crate::exec::ir::iface::catalog::default_config(),
			}
		}
	}

	fn empty_ir() -> Ir {
		Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		)
	}

	fn table_scan(direction: Direction) -> PhysOp {
		PhysOp::TableScan(Box::new(TableScanAttrs {
			probes: None,
			bounds: None,
			prefilter: None,
			table: TableName::new(Strand::new("t")),
			direction,
			topk: None,
			lightweight: false,
			id_order: true,
			read: ReadClauses::default(),
		}))
	}

	#[test]
	fn host_is_send_sync() {
		fn assert_send_sync<T: Send + Sync>() {}
		assert_send_sync::<PhysSeedHost<'static>>();
	}

	#[tokio::test]
	async fn host_table_facts_without_sources_is_empty() {
		let parts = Parts::new();
		let host = PhysSeedHost::new(parts.cx(), &NotYetFolder);
		let answered = host.table_facts(&[]).await.expect("no source is answered");
		assert!(answered.is_empty());
	}

	#[tokio::test]
	async fn host_convert_copy_of_a_converted_container_answers_no_replacement() {
		let parts = Parts::new();
		let host = PhysSeedHost::new(parts.cx(), &NotYetFolder);
		let mut copy = empty_ir();
		let mut facts = FactCache::new();
		let log =
			host.convert_copy(&mut copy, &mut facts).await.expect("nothing is left to convert");
		assert!(log.is_empty());
	}

	#[test]
	fn host_order_sig_is_order_sig() {
		let parts = Parts::new();
		let host = PhysSeedHost::new(parts.cx(), &NotYetFolder);
		for op in [table_scan(Direction::Forward), table_scan(Direction::Backward), PhysOp::Filter]
		{
			for input in [OrderSig::Unordered, OrderSig::IdAsc, OrderSig::IdDesc] {
				assert_eq!(
					host.order_sig(&op, input.clone()),
					order::order_sig(&op, input.clone())
				);
			}
		}
		assert_eq!(
			host.order_sig(&table_scan(Direction::Forward), OrderSig::Unordered),
			OrderSig::IdAsc
		);
	}

	#[test]
	fn host_cost_is_the_fixed_cost() {
		let parts = Parts::new();
		let host = PhysSeedHost::new(parts.cx(), &NotYetFolder);
		let answered: *const dyn CostHook = host.cost();
		let fixed: *const dyn CostHook = &cost::PHYS_FIXED_COST;
		assert!(std::ptr::addr_eq(answered, fixed));
	}

	/// The host's source names no storage handle and holds no cache, and a conversion request
	/// leaves the container it was asked about as it was.
	#[tokio::test]
	async fn seed_conversion_has_no_side_effects() {
		let source = include_str!("seed_host.rs");
		let production = source.split("#[cfg(test)]").next().expect("the source has a head");
		for name in [
			"Transaction",
			"Datastore",
			"kvs::",
			"txn",
			"speculation",
			"compile_cache",
			"static ",
			"OnceLock",
			"thread_local",
		] {
			assert!(!production.contains(name), "seed_host.rs names `{name}`");
		}
		let parts = Parts::new();
		let host = PhysSeedHost::new(parts.cx(), &NotYetFolder);
		let original = empty_ir();
		let before = print(&original, original.top(), PrintOpts::default());
		let mut copy = original.clone();
		let mut facts = FactCache::new();
		let _ = host.convert_copy(&mut copy, &mut facts).await;
		assert_eq!(print(&original, original.top(), PrintOpts::default()), before);
		assert_eq!(print(&copy, copy.top(), PrintOpts::default()), before);
	}

	#[tokio::test]
	async fn convert_copy_retires_logical_facts_for_replaced_ops() {
		use crate::exec::ir::core::location::{LocOrigin, Location};
		use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
		use crate::exec::ir::iface::ids::FrameId;
		use crate::exec::ir::iface::types::TypeKnowledge;
		use crate::exec::ir::logical::ops::LogOp;
		use crate::exec::ir::op_kind::OpKind;

		let parts = Parts::new();
		let host = PhysSeedHost::new(parts.cx(), &NotYetFolder);
		let mut copy = empty_ir();
		let mut facts = FactCache::new();
		{
			let mut rw = Rewriter::new(&mut copy, &mut facts);
			let top_block = rw.ir().region(rw.ir().top()).expect("top region").blocks[0];
			let _ = rw
				.create(
					InsertPoint::End(top_block),
					OpSpec {
						kind: OpKind::Logical(LogOp::UnionAll),
						operands: Default::default(),
						results: std::iter::once(TypeKnowledge::Unknown).collect(),
						frame: FrameId::new(0),
						snapshot: SnapshotId::new(0),
						loc: Location {
							stmt: 0,
							span: None,
							origin: LocOrigin::Synthesized,
						},
					},
				)
				.expect("union_all created");
		}
		// Start with a fresh FactCache, just as `prepare_copy` does before `convert_copy`.
		let mut fresh_facts = FactCache::new();
		let log = host
			.convert_copy(&mut copy, &mut fresh_facts)
			.await
			.expect("union_all converts to phys.union");
		assert_eq!(log.len(), 1);
		let top_block = copy.region(copy.top()).expect("top region").blocks[0];
		let phys_op = copy.block(top_block).expect("top block").ops[0];
		let structural = structural_cx(parts.cx().effects.registry, &copy);
		let structural_effects = effects::of_with(&copy, &mut fresh_facts, &structural, phys_op);
		assert!(
			!structural_effects.set.mutates_state(),
			"converted phys.union must inherit pure effects from retired logical.union_all"
		);
		let full_effects = effects::of_with(&copy, &mut fresh_facts, &parts.cx().effects, phys_op);
		assert!(!full_effects.set.mutates_state());
	}
}
