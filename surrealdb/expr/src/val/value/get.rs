use crate::expr::Expr;

/// Returns true if the expression references the `$parent` parameter.
/// Used to avoid allocating a child context in Part::Where when the
/// predicate does not need `$parent`.
pub fn expr_references_parent(expr: &Expr) -> bool {
	use crate::expr::visit::{Visit, Visitor};
	struct Check(bool);
	impl Visitor for Check {
		type Error = std::convert::Infallible;
		fn visit_expr(&mut self, e: &Expr) -> Result<(), Self::Error> {
			if let Expr::Param(p) = e
				&& p.as_str() == "parent"
			{
				self.0 = true;
			}
			if self.0 {
				return Ok(());
			}
			e.visit(self)
		}
		fn visit_select(&mut self, _: &crate::expr::SelectStatement) -> Result<(), Self::Error> {
			Ok(())
		}
	}
	let mut c = Check(false);
	let _ = c.visit_expr(expr);
	c.0
}
