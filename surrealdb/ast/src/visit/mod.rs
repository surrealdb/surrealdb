use crate::{NodeId, Query, TopLevelExpr};

macro_rules! impl_visitor {
	($(fn $name:ident($this:ident, $ast:ident, $node:ident: $node_ty:ty){
	    $($t:tt)*
	})*) => {
		pub trait Visit: Sized {
			type Error;

			$(
				fn $name(&mut self,$ast: &$crate::Ast, $node: $node_ty) -> Result<(), Self::Error>{
					let $this = self;
					impl_visitor!(@body, $name,$this,$ast,$node,$($t)*)
				}
			)*
		}

		$(
			impl_visitor!(@visitor,$name,$this,$ast,$node,$node_ty, $($t)*);
		)*

	};

	(@body, $name:ident, $this:ident, $ast:ident, $node:ident, ) => {
		Ok(())
	};

	(@body, $name:ident, $this:ident,$ast:ident, $node:ident, $($t:tt)+) => {
		$name($this,$ast,$node)
	};


	(@visitor, $name:ident, $this:ident,$ast:ident, $node:ident,$node_ty:ty, $($t:tt)+) => {
		pub fn $name<V: Visit>($this: &mut V, $ast: &$crate::Ast, $node: $node_ty) -> Result<(), V::Error>{
			$($t)*
		}
	};

	(@visitor, $name:ident, $this:ident,$ast:ident, $node:ident,$node_ty:ty,) => {};
}

impl_visitor! {
	fn visit_query(visit, ast, m: NodeId<Query>) {
		for e in ast.iter_list(ast[m].exprs){
			visit.visit_top_level_expr(ast, e)?
		}
		Ok(())
	}

	fn visit_top_level_expr(visit, ast, m: NodeId<TopLevelExpr>){
		match ast[m]{
			TopLevelExpr::Transaction(node_id) => todo!(),
			TopLevelExpr::Use(node_id) => todo!(),
			TopLevelExpr::Option(node_id) => todo!(),
			TopLevelExpr::Expr(node_id) => todo!(),
			TopLevelExpr::Kill(node_id) => todo!(),
		}
		Ok(())
	}


}
