//! Registration for the loop optimiser: its logical and physical passes, and the order they run in.

use std::sync::OnceLock;

use super::cse::Cse;
use super::dce::Dce;
use super::driver::LoPass;
use super::fold::FoldPass;
use super::inline::InlineCalls;
use super::licm::{HoistLeadingConjuncts, Licm};
use super::loop_unroll::LoopUnroll;
use super::perm_where::{PushBelowLayer, PushBelowPermission, SpecialiseGuards};
use super::push_subquery::PushIntoSubquery;
use super::unswitch::Unswitch;
use crate::exec::ir::iface::conversion::Phase;
use crate::exec::ir::registry::{IrPass, Registry};

/// The loop optimiser's passes in the order they run.
///
/// This is the one place the order is stated, so the driver and the registry cannot disagree.
///
/// The list is built once and shared: a pass holds no state.
pub(crate) fn lo_passes() -> &'static [Box<dyn LoPass>] {
	static PASSES: OnceLock<Vec<Box<dyn LoPass>>> = OnceLock::new();
	PASSES.get_or_init(|| {
		vec![
			Box::new(SpecialiseGuards),
			Box::new(InlineCalls),
			Box::new(FoldPass),
			Box::new(Dce),
			Box::new(Cse),
			Box::new(PushIntoSubquery),
			Box::new(PushBelowLayer),
			Box::new(PushBelowPermission),
			Box::new(LoopUnroll),
			Box::new(Unswitch),
			Box::new(HoistLeadingConjuncts),
			Box::new(Licm),
			Box::new(Dce),
		]
	})
}

/// A registry entry for a pass: its name and the phase it runs in.
struct Entry {
	name: &'static str,
	phase: Phase,
}

impl IrPass for Entry {
	fn name(&self) -> &'static str {
		self.name
	}

	fn phase(&self) -> Phase {
		self.phase
	}
}

/// The name the physical expression pass registers under.
pub(crate) const PHYS_CSE: &str = "phys_cse";

/// Registers one entry per loop-optimiser pass, in the order the passes run, then the physical
/// expression pass, so [`Registry::passes`] lists them.
pub(crate) fn register(r: &mut Registry) {
	for pass in lo_passes() {
		r.passes.0.push(Box::new(Entry {
			name: pass.name(),
			phase: Phase::Logical,
		}));
	}
	r.passes.0.push(Box::new(Entry {
		name: PHYS_CSE,
		phase: Phase::Physical,
	}));
}
