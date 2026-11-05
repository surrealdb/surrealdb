//! The effect context an analysis answers under when its caller holds a function registry and no
//! catalog.
//!
//! A datastore's function registry is the set its constructor names, so production code hands
//! every analysis the registry of the datastore it runs for. [`structural_cx`] wraps that
//! registry with no definitions and the container's own frames: every answer that would need a
//! definition is the most conservative answer. The entry points that take no context
//! (`effects::of`, `fallibility::of`, ...) answer under the full builtin registry and are for
//! tests.

use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::iface::catalog::{EffectCx, SemanticCatalog};

/// A semantic catalog that holds no definition.
pub(crate) struct NoDefinitions;

impl SemanticCatalog for NoDefinitions {}

/// The context for an analysis of `ir` under `registry`, with no definitions and the container's
/// own frames.
pub(crate) fn structural_cx<'a>(registry: &'a FunctionRegistry, ir: &'a Ir) -> EffectCx<'a> {
	static NO_DEFINITIONS: NoDefinitions = NoDefinitions;
	EffectCx {
		registry,
		catalog: &NO_DEFINITIONS,
		frames: ir.frames(),
	}
}

/// The full builtin function registry: one per process.
#[cfg(test)]
pub(crate) fn builtin_registry() -> &'static FunctionRegistry {
	static REGISTRY: std::sync::OnceLock<FunctionRegistry> = std::sync::OnceLock::new();
	REGISTRY.get_or_init(FunctionRegistry::with_builtins)
}

/// The context the entry points without a context answer under: the builtin registry, no
/// definitions and the container's own frames.
#[cfg(test)]
pub(crate) fn default_cx(ir: &Ir) -> EffectCx<'_> {
	structural_cx(builtin_registry(), ir)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::iface::region_kind::RegionKind;

	#[test]
	fn default_cx_uses_the_builtin_registry() {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		assert!(std::ptr::eq(builtin_registry(), builtin_registry()));
		assert!(std::ptr::eq(default_cx(&ir).registry, builtin_registry()));
		assert!(std::ptr::eq(default_cx(&ir).frames, ir.frames()));
	}

	#[test]
	fn a_structural_cx_answers_under_the_registry_it_is_given() {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let registry = FunctionRegistry::with_edge_builtins();
		let cx = structural_cx(&registry, &ir);
		assert!(std::ptr::eq(cx.registry, &registry));
		assert!(std::ptr::eq(cx.frames, ir.frames()));
	}
}
