//! The effect context the entry points that take no context answer under.
//!
//! Every datastore builds its function registry with `FunctionRegistry::with_builtins()`, so the
//! builtin registry held here answers every effect and fallibility question the same way a
//! datastore's own registry does. The catalog holds no definition: every answer that would need
//! one is the most conservative answer.

use std::sync::OnceLock;

use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::iface::catalog::{EffectCx, SemanticCatalog};

/// The builtin function registry every datastore builds: one per process.
pub(crate) fn builtin_registry() -> &'static FunctionRegistry {
	static REGISTRY: OnceLock<FunctionRegistry> = OnceLock::new();
	REGISTRY.get_or_init(FunctionRegistry::with_builtins)
}

/// A semantic catalog that holds no definition.
pub(crate) struct NoDefinitions;

impl SemanticCatalog for NoDefinitions {}

/// The context the entry points without a context answer under: the builtin registry, no
/// definitions and the container's own frames.
pub(crate) fn default_cx(ir: &Ir) -> EffectCx<'_> {
	static NO_DEFINITIONS: NoDefinitions = NoDefinitions;
	EffectCx {
		registry: builtin_registry(),
		catalog: &NO_DEFINITIONS,
		frames: ir.frames(),
	}
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
}
