//! The access statements: `ACCESS` grant, show, revoke and purge, `KILL` and `SHOW CHANGES`.
//!
//! A grant's subject is a user or a record. Secrets an access statement carries (a password, a
//! key, a bearer token) are operands and never attributes, so they are neither printed nor
//! digested. A show or revoke holds a `Where` clause region that filters the grants.
//!
//! [`plan_access`] states the attributes, the operands and the filter of an `ACCESS` statement;
//! [`show_attrs`] the attributes of `SHOW CHANGES`. `KILL` has no attribute: its one operand is
//! the live query id.

#![deny(clippy::wildcard_enum_match_arm)]

use surrealdb_strand::Strand;

use super::ops::{AccessAttrs, ShowAttrs};
use crate::exec::ir::iface::statements::{AccessOp, AccessSubjectShape};
use crate::expr::statements::access::{AccessStatement, Subject};
use crate::expr::statements::show::ShowStatement;
use crate::expr::{Expr, Literal};

/// An `ACCESS` statement ready to bind.
pub(crate) struct AccessPlan<'e> {
	pub(crate) attrs: AccessAttrs,
	/// The operands the statement evaluates in order: the record of a record grant, else none.
	pub(crate) operands: Vec<Expr>,
	/// The predicate that filters the grants a show or a revoke names, when it has one.
	pub(crate) filter: Option<&'e Expr>,
}

/// The attributes, operands and filter of `statement`.
pub(crate) fn plan_access(statement: &AccessStatement) -> AccessPlan<'_> {
	match statement {
		AccessStatement::Grant(grant) => {
			let (subject, operands) = match &grant.subject {
				Subject::User(user) => (AccessSubjectShape::User(user.clone()), Vec::new()),
				Subject::Record(record) => (
					AccessSubjectShape::Record,
					vec![Expr::Literal(Literal::RecordId(record.clone()))],
				),
			};
			AccessPlan {
				attrs: AccessAttrs {
					ac: grant.ac.clone(),
					base: grant.base,
					op: AccessOp::Grant {
						subject,
					},
				},
				operands,
				filter: None,
			}
		}
		AccessStatement::Show(show) => AccessPlan {
			attrs: AccessAttrs {
				ac: show.ac.clone(),
				base: show.base,
				op: AccessOp::Show {
					gr: show.gr.clone(),
				},
			},
			operands: Vec::new(),
			filter: show.cond.as_ref().map(|cond| &cond.0),
		},
		AccessStatement::Revoke(revoke) => AccessPlan {
			attrs: AccessAttrs {
				ac: revoke.ac.clone(),
				base: revoke.base,
				op: AccessOp::Revoke {
					gr: revoke.gr.clone(),
				},
			},
			operands: Vec::new(),
			filter: revoke.cond.as_ref().map(|cond| &cond.0),
		},
		AccessStatement::Purge(purge) => AccessPlan {
			attrs: AccessAttrs {
				ac: purge.ac.clone(),
				base: purge.base,
				op: AccessOp::Purge {
					kind: purge.kind.clone(),
					grace: purge.grace.0,
				},
			},
			operands: Vec::new(),
			filter: None,
		},
	}
}

/// The name of the access method `statement` addresses.
pub(crate) fn method_of(statement: &AccessStatement) -> &Strand {
	match statement {
		AccessStatement::Grant(grant) => &grant.ac,
		AccessStatement::Show(show) => &show.ac,
		AccessStatement::Revoke(revoke) => &revoke.ac,
		AccessStatement::Purge(purge) => &purge.ac,
	}
}

/// The attributes of a `SHOW CHANGES` statement.
pub(crate) fn show_attrs(statement: &ShowStatement) -> ShowAttrs {
	ShowAttrs {
		table: statement.table.clone(),
		since: statement.since.clone(),
		limit: statement.limit,
	}
}
