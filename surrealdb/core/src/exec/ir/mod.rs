//! The intermediate representation a request is planned through.
//!
//! One arena-backed IR carries a request from bind to run, in three dialects that differ in what
//! they decide, not in how they are stored:
//!
//! - **expr** and **logical** say *what* is computed: meaning, legality, effects, fallibility and
//!   the regions a statement opens. Nothing in them names a storage path, a worker or an
//!   instruction.
//! - **phys** says *where and by which method*: access paths and indexes, placement, pipelines and
//!   the guarded alternatives chosen between. A placement decision is derived from the logical
//!   facts and the catalog's statistics, and is never read back into the logical dialect.
//! - **bytecode** (the VM's [`Program`](crate::exec::vm)) says *how*: it is produced from the
//!   physical dialect only, and the interpreter never compiles while it runs.
//!
//! There is one path from statements to a program: [`plan_request`] plans them into a container
//! past the physical boundary, and the VM lowers that container natively. No setting selects the
//! path and no request is planned twice.
//!
//! The frozen interface types live in [`iface`]; the `core` module holds the container and the
//! operations on it, [`registry`] collects each lane's registrations and [`error`] holds the one
//! error type every IR entry point returns. [`plan_request`] is the one entry from statements to a
//! container, and [`UnitKind`] names what a container is planned as.

/// Declares an op-tag enum, its `ALL` list and its mnemonics from one list of rows, so the three
/// cannot disagree.
///
/// The tag is the payload-free identity of an op variant: patterns index on it and the printer
/// names the op by its mnemonic. Each mnemonic is `<dialect>.<name>`, unique across the dialect.
macro_rules! define_op_tags {
	(
		$(#[$meta:meta])*
		$vis:vis enum $tag:ident {
			$($(#[$vmeta:meta])* $variant:ident => $mnemonic:literal),+ $(,)?
		}
	) => {
		$(#[$meta])*
		#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
		$vis enum $tag {
			$($(#[$vmeta])* $variant),+
		}

		impl $tag {
			/// Every tag, in declaration order.
			pub(crate) const ALL: &'static [$tag] = &[$($tag::$variant),+];

			/// The name the printer writes and the parser reads.
			pub(crate) const fn mnemonic(self) -> &'static str {
				match self {
					$($tag::$variant => $mnemonic),+
				}
			}
		}
	};
}

pub(crate) mod analysis;
pub(crate) mod build;
pub(crate) mod cache;
pub(crate) mod core;
#[cfg(feature = "egraph")]
pub(crate) mod egraph;
#[cfg(not(feature = "egraph"))]
#[path = "egraph_off.rs"]
pub(crate) mod egraph;
pub(crate) mod error;
pub(crate) mod expr;
pub(crate) mod fold;
pub(crate) mod iface;
pub(crate) mod logical;
pub(crate) mod op_kind;
pub(crate) mod passes;
pub(crate) mod phys;
mod planning;
pub(crate) mod registry;
#[cfg(all(test, feature = "kv-mem"))]
pub(crate) mod tests_plan_request;
pub(crate) mod timers;
pub(crate) mod unit;

use surrealdb_expr::parser::TopLevelExpr;

use self::core::ir::IrScope;
#[cfg(all(test, feature = "kv-mem"))]
pub(crate) use self::planning::bind_request_over;
#[allow(unused_imports)]
pub(crate) use self::planning::{
	PlanOutcome, PlannedRequest, PlanningInput, SubscriptionRequest, plan_request,
	plan_subscription, request_rejection,
};
pub(crate) use self::unit::UnitKind;
use crate::expr::logical::LogicalOp;

/// One statement of a request, in the form the caller holds it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RequestStatement<'a> {
	/// A statement as the parser wrote it. The binder lowers it to its logical form.
	Ast(&'a TopLevelExpr),
	/// A statement as the planner's binding step returned it: the logical form the binder reads
	/// for an expression statement, with no surface text.
	Logical(&'a LogicalOp),
}

impl<'a> RequestStatement<'a> {
	/// Every statement of `statements` as the parser wrote it.
	pub(crate) fn of(statements: &'a [TopLevelExpr]) -> Vec<RequestStatement<'a>> {
		statements.iter().map(RequestStatement::Ast).collect()
	}
}

/// The statements to plan, the scope the container is created for, and what it is planned as.
pub(crate) struct RequestInput<'a> {
	pub(crate) statements: &'a [RequestStatement<'a>],
	pub(crate) scope: IrScope,
	/// What the container is planned as: it decides the top region, what the body is, and
	/// whether the region-constraint check runs. A unit that is not a request holds exactly one
	/// statement, except a unit built from the catalog (`UnitForm::Catalog`), which holds none.
	pub(crate) unit: UnitKind,
	/// Whether a statement that runs after an earlier statement of its list may have changed the
	/// catalog is bound when the program reaches it, from the statement as written, and not
	/// against the catalog the request was planned with
	/// ([`LogOp::Statement`](logical::ops::LogOp::Statement)'s `served`). Only a request's
	/// program serves such statements.
	pub(crate) serves_late: bool,
	/// Whether the container runs after a change to the catalog its planning cannot know
	/// ([`Planner::late_binding`](crate::exec::planner::Planner::late_binding)): a write it holds
	/// binds no table's definitions, and takes them for the catalog version each record's write
	/// meets.
	pub(crate) late_bound: bool,
}

impl<'a> RequestInput<'a> {
	/// The statements of a request or fragment, planned as one container.
	pub(crate) fn request(statements: &'a [RequestStatement<'a>], scope: IrScope) -> Self {
		Self {
			statements,
			scope,
			unit: UnitKind::Request,
			serves_late: false,
			late_bound: false,
		}
	}

	/// The statements as the parser wrote them, or `None` when any was handed over bound.
	pub(crate) fn parsed(&self) -> Option<Vec<&'a TopLevelExpr>> {
		self.statements
			.iter()
			.map(|statement| match statement {
				RequestStatement::Ast(statement) => Some(*statement),
				RequestStatement::Logical(_) => None,
			})
			.collect()
	}
}
