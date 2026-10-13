use anyhow::Result;
use uuid::Uuid;

use crate::expr::visit::{Visit, Visitor};
use crate::expr::{Cond, Expr, Fetchs, Fields, Idiom, Param};

/// The set of parameter names that carry per-event document data and are therefore
/// only meaningful at notification time, not at LIVE query registration time.
const DOC_PARAMS: &[&str] = &["value", "before", "after", "event", "this"];

/// Visitor that short-circuits with `Err(())` on the first expression node that
/// depends on the document being processed:
///
/// - Any field-access [`Idiom`] (e.g. `name`, `user.age`).
/// - Any of the live-query event params: `$value`, `$before`, `$after`, `$event`, `$this`.
///
/// Session params (`$access`, `$auth`, `$token`, `$session`) and user-defined LET
/// params are captured at registration time and treated as constants.
///
/// If the visitor completes without returning `Err`, the expression is
/// "document-independent": its result is the same for every record that triggers
/// the LIVE query, so it can be evaluated once at registration time to catch
/// errors early.
struct IsDocumentDependentChecker;

impl Visitor for IsDocumentDependentChecker {
	/// `()` is used as an early-termination signal — `Err(())` means "found a
	/// document-dependent node, stop traversal".
	type Error = ();

	fn visit_idiom(&mut self, _idiom: &Idiom) -> Result<(), ()> {
		Err(()) // any field access → document-dependent, stop
	}

	fn visit_param(&mut self, param: &Param) -> Result<(), ()> {
		if DOC_PARAMS.contains(&param.as_str()) {
			Err(()) // document-event param → document-dependent, stop
		} else {
			Ok(())
		}
	}
}

/// Returns `true` if `expr` contains any field-access idiom or document-event
/// param, meaning its result can differ per-document and it cannot be safely
/// evaluated at LIVE query registration time.
pub fn is_document_dependent(expr: &Expr) -> bool {
	expr.visit(&mut IsDocumentDependentChecker).is_err()
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum LiveFields {
	Diff,
	Select(Fields),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct LiveStatement {
	pub id: Uuid,
	pub node: Uuid,
	pub fields: LiveFields,
	pub what: Expr,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
}
