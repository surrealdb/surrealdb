use surrealdb_types::{SqlFormat, ToSql};

use crate::Expr;

/// Renders an [`Expr`] in a position where a statement-shaped expression would
/// be ambiguous, parenthesising it when required.
///
/// Value-shaped expressions render bare. Statement-shaped ones (SELECT, CREATE,
/// IF, ...) are wrapped in parentheses so the result reparses to the same tree
/// when spliced into a larger query. `RETURN` is the one conditional case: it
/// only needs covering when it carries a FETCH clause.
pub struct CoverStmts<'a>(pub &'a Expr);

impl ToSql for CoverStmts<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self.0 {
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::FunctionCall(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Throw(_) => self.0.fmt_sql(f, fmt),
			Expr::Return(x) => {
				if x.fetch.is_some() {
					f.push('(');
					self.0.fmt_sql(f, fmt);
					f.push(')')
				} else {
					self.0.fmt_sql(f, fmt);
				}
			}

			Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => {
				f.push('(');
				self.0.fmt_sql(f, fmt);
				f.push(')')
			}
		}
	}
}
