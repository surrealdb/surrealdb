use crate::expr::visit::{MutVisitor, VisitMut};
use crate::expr::{Expr, Literal};
use crate::idx::planner::executor::KnnExpressions;

pub(super) struct KnnConditionRewriter<'a>(pub &'a KnnExpressions);

impl<'a> MutVisitor for KnnConditionRewriter<'a> {
	type Error = ();

	fn visit_mut_expr(&mut self, e: &mut Expr) -> Result<(), Self::Error> {
		if self.0.contains(e) {
			*e = Expr::Literal(Literal::Bool(true));
			return Ok(());
		}

		match e {
			// Ignore most statements.
			//
			// TODO: This is probably incorrect, most statements have parts which are evaluated
			// within the current context, like the `FROM` part of select. This is currently
			// replicating old behavior from before this pass was written with a visitor.
			Expr::Param(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_) => {}

			_ => {
				e.visit_mut(self)?;
			}
		}
		Ok(())
	}
}
