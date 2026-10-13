//! Registration for the expression dialect: its attribute syntax and its patterns.
//!
//! The expression dialect registers no pattern. Folding, truthy and convert erasure, and every
//! other greedy rewrite over expression ops belong to the layer that owns the facts they read.

use super::print::ExprSyntax;
use crate::exec::ir::registry::Registry;

static EXPR_SYNTAX: ExprSyntax = ExprSyntax;

/// Installs the expression attribute syntax in registry slot 0 and adds no pattern.
pub(crate) fn register(r: &mut Registry) {
	r.attr_syntax[0] = Some(&EXPR_SYNTAX);
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
	use crate::exec::ir::op_kind::{OpKind, OpTag};

	#[test]
	fn registers_expr_syntax_and_no_pattern() {
		let mut registry = Registry::empty();
		register(&mut registry);
		let syntax = registry.attr_syntax[0].expect("the expression syntax is registered");
		assert!(registry.attr_syntax[1].is_none());
		assert!(registry.attr_syntax[2].is_none());
		assert!(registry.expr_patterns.is_empty());
		assert!(registry.logical_patterns.is_empty());
		assert!(registry.phys_patterns.is_empty());

		let mut text = String::new();
		syntax.print_attrs(&OpKind::Expr(ExprOp::Doc), &mut text);
		assert_eq!(text, ExprTag::Doc.mnemonic());
		assert!(syntax.parse_attrs(OpTag::Expr(ExprTag::Doc), &text).is_ok());
	}
}
