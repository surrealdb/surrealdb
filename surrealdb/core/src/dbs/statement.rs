//! The `FOR UPDATE` admission rules and their rejection messages.

/// Rejection message for a `FOR UPDATE` FROM target that is not a
/// specific record id. Shared by the plan-time guards and the runtime
/// re-validation in the scan operators so both executors report the same
/// error for the same query.
pub(crate) const FOR_UPDATE_TARGETS_ERROR: &str =
	"SELECT ... FOR UPDATE requires record ids as targets";

/// Rejection message for a `FOR UPDATE` read combined with a version,
/// whether from a statement-level VERSION clause or an enclosing version
/// scope. Shared for the same reason as [`FOR_UPDATE_TARGETS_ERROR`].
pub(crate) const FOR_UPDATE_VERSION_ERROR: &str =
	"SELECT ... FOR UPDATE cannot be combined with VERSION";

/// Rejection message for a `FOR UPDATE` read that groups or splits its rows.
/// Shared for the same reason as [`FOR_UPDATE_TARGETS_ERROR`].
pub(crate) const FOR_UPDATE_GROUP_ERROR: &str =
	"SELECT ... FOR UPDATE cannot be combined with GROUP BY or SPLIT";

/// Rejection message for a `FOR UPDATE` read with a `LIMIT` over more than
/// one target. Shared for the same reason as [`FOR_UPDATE_TARGETS_ERROR`].
pub(crate) const FOR_UPDATE_LIMIT_ERROR: &str =
	"SELECT ... FOR UPDATE cannot be combined with LIMIT over multiple targets";

/// Validate the clause combinations a `FOR UPDATE` SELECT permits.
///
/// `FOR UPDATE` locks the selected records for the duration of the
/// enclosing transaction, so it only supports plain reads of specific
/// records: VERSION, GROUP BY, and SPLIT are rejected, and every FROM
/// target must be a record id (see [`check_for_update_targets`]).
///
/// LIMIT is also rejected alongside more than one FROM target: the executor
/// stops pulling from the sources once the limit is filled, so the targets
/// past that point would never be read and never registered for conflict
/// detection. A single target is unaffected — it is read before the limit
/// discards its row.
///
/// The executor validates before lowering, so the surface statement is the
/// form the rules are checked on. A `SELECT` in `FROM` position is a
/// statement in its own right, with its own clauses to validate: lowering
/// flattens the two into one plan and distributes `FOR UPDATE` onto the
/// stored sources it finds, so a statement whose only source is a subquery
/// leaves no trace of the clause in the plan.
///
/// A no-op when the statement does not carry `FOR UPDATE`.
pub(crate) fn validate_for_update(
	stm: &crate::expr::statements::SelectStatement,
) -> std::result::Result<(), crate::exec::Error> {
	use crate::expr::{Expr, Literal};
	for expr in &stm.what {
		if let Expr::Select(inner) = expr {
			validate_for_update(inner)?;
		}
	}
	if !stm.for_update {
		return Ok(());
	}
	if !matches!(stm.version, Expr::Literal(Literal::None)) {
		return Err(crate::exec::Error::Query {
			message: FOR_UPDATE_VERSION_ERROR.to_string(),
		});
	}
	if stm.group.is_some() || stm.split.is_some() {
		return Err(crate::exec::Error::Query {
			message: FOR_UPDATE_GROUP_ERROR.to_string(),
		});
	}
	if stm.limit.is_some() && stm.what.len() > 1 {
		return Err(crate::exec::Error::Query {
			message: FOR_UPDATE_LIMIT_ERROR.to_string(),
		});
	}
	check_for_update_targets(&stm.what)
}

/// Check that every statically-classifiable FROM target of a `FOR UPDATE`
/// SELECT is a specific (non-range) record id.
///
/// Targets whose shape is only known at runtime — params, function calls, and
/// call/method postfix expressions — pass this check and must be re-validated
/// at the site where they resolve to a value.
fn check_for_update_targets<'a>(
	what: impl IntoIterator<Item = &'a crate::expr::Expr>,
) -> std::result::Result<(), crate::exec::Error> {
	use crate::expr::operator::PostfixOperator;
	use crate::expr::{Expr, Literal, RecordIdKeyLit};
	for expr in what {
		match expr {
			Expr::Literal(Literal::RecordId(rid))
				if !matches!(rid.key, RecordIdKeyLit::Range(_)) => {}
			Expr::Param(_) | Expr::FunctionCall(_) => {}
			Expr::Postfix {
				op: PostfixOperator::MethodCall(..) | PostfixOperator::Call(..),
				..
			} => {}
			Expr::Literal(_)
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
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
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
				return Err(crate::exec::Error::Query {
					message: FOR_UPDATE_TARGETS_ERROR.to_string(),
				});
			}
		}
	}
	Ok(())
}
