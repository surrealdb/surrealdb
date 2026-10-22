//! What a container is planned as.
//!
//! A request is not the only thing the planner turns into a container. A function body, a
//! subquery in a frame, the read unit of a table, the stages a write applies to one table and the
//! body of a closure are each planned into a container of their own and lowered as a unit of the
//! program that needs them. The semantics of `RETURN`, `BREAK`, `CONTINUE` and the
//! region-constraint check depend on the region the code stands in, so a container is planned
//! *as* a [`UnitKind`]: the kind names the container's top region, what its body is, and whether
//! the constraint check runs on it.
//!
//! This is distinct from [`iface::unit::UnitKind`](super::iface::unit::UnitKind), which names
//! the kinds of unit a running program may load from the compile cache.

use surrealdb_strand::TableName;

use super::iface::region_kind::{RegionKind, StatementCommit};
use super::iface::types::TypeKnowledge;
use super::logical::verify::timestamp_arg;
use crate::doc::Action;

/// The names a subscription's notification unit reads as block arguments after the timestamp, in
/// argument order. They are the parameters of the subscriber's frame.
pub(crate) const SUBSCRIBER_NAMES: [&str; 8] =
	["before", "after", "value", "event", "auth", "session", "access", "token"];

/// How many names a subscription's notification unit reads as block arguments after the
/// timestamp.
const SUBSCRIBER_ARGS: usize = SUBSCRIBER_NAMES.len();

/// What a container is planned as.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum UnitKind {
	/// The statements of one request or fragment, each at its own position.
	Request,
	/// One statement of a request, at `position` in it.
	RequestStatement {
		position: usize,
	},
	/// A statement a program serves when it reaches it, at `position` in the request that holds
	/// the program. It is planned as a request statement is.
	ServedStatement {
		position: usize,
	},
	/// The body of a user-defined function: isolated, `RETURN` ends it, and `BREAK` and
	/// `CONTINUE` are barriers.
	FunctionBody,
	/// The read unit of a table, a statement the planner synthesises.
	TableRead,
	/// A statement written in an expression. `stored` says the code that holds the subquery is
	/// stored code (a field clause, an event, a permission), which the constraint check never
	/// judges, and `false` that it is code of the request.
	///
	/// A subquery that the compile extracts with [`Ir::copy_region`](super::core::Ir) is not
	/// bound from a statement. This kind is the container of a statement written as a subquery,
	/// which is bound into it.
	Subquery {
		stored: bool,
	},
	/// The read of one stage of a `MATCH`.
	MatchStage,
	/// A statement a deletion performs on its own behalf (a reference cascade, the edge bundle).
	Cascade,
	/// The stages a write applies to the records of `table` for `action`, with the code the
	/// table stores for each.
	StageBundle {
		table: TableName,
		action: Action,
	},
	/// The events of `table` that fire for `action`: one `logical.events` op holding each event's
	/// `WHEN` and `THEN` clauses as regions, built from the definitions the catalog holds for the
	/// table ([`UnitForm::Catalog`]). A write that fires the events asks for this unit by the table
	/// and the action, so events that write back to their own table close on the one unit.
	Events {
		table: TableName,
		action: Action,
	},
	/// The own write order of the view table `table`: the stages a row written to a view table
	/// runs, one list for each action, as the view's definition states them. The three lists
	/// are the unit's statements in the order a created, an updated and a deleted row take.
	ViewRows {
		table: TableName,
	},
	/// The body of a closure value.
	ClosureBody,
	/// The predicate a called function's `PERMISSIONS` states, judged over the row the call is
	/// handed. It is a value of one expression, `$this`, `$self` and `$value` are the row, and
	/// it is definer code, which the constraint check never judges.
	Guard,
	/// The unit that runs a live query for the records of `table`.
	Subscription {
		table: TableName,
	},
	/// The body of a loop of a planned container, run as a unit over the one value it is given.
	/// It is extracted from a planned container ([`Ir::copy_region`](super::core::Ir)), never
	/// bound from a statement.
	LoopBody,
}

/// How the body of a container planned as a unit is bound under its top region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnitForm {
	/// Statements in a request: the top region is a request and each statement has a region of
	/// its own.
	Request,
	/// One statement, which is the whole body of a statement region.
	Statement,
	/// A statement list closed by the value of its last statement.
	List,
	/// One statement standing as a value, closed by an expression yield.
	Value,
	/// Not bound from a statement.
	Extracted,
	/// Built from the definitions the catalog holds for a table, with no statement to bind.
	Catalog,
}

impl UnitKind {
	/// The kind of the top region of a container planned as this unit.
	///
	/// A request statement and a served statement stand in a statement region of a request
	/// region, as they do in a whole request, so their top region is a request.
	///
	/// Stored code that runs, the stages of a write, a view table's write order, a cascade and a
	/// subscription, stands under a running kind and never under `StoredClause`: the footprint,
	/// the effect walk and the callee walk skip a stored region, and each would lose the calls,
	/// reads and writes of the unit. `FunctionBody` is not used for them because the inliner
	/// reads a `FunctionBody` region as an inlinable function. The kind is fixed by the unit, not
	/// by its builder.
	pub(crate) fn top_region(&self) -> RegionKind {
		match self {
			UnitKind::Request
			| UnitKind::RequestStatement {
				..
			}
			| UnitKind::ServedStatement {
				..
			} => RegionKind::Request,
			UnitKind::FunctionBody => RegionKind::FunctionBody,
			UnitKind::TableRead | UnitKind::MatchStage => {
				RegionKind::Statement(StatementCommit::Auto)
			}
			UnitKind::Subquery {
				..
			} => RegionKind::Subquery,
			UnitKind::Cascade
			| UnitKind::StageBundle {
				..
			}
			| UnitKind::Events {
				..
			}
			| UnitKind::ViewRows {
				..
			} => RegionKind::WriteStage,
			UnitKind::Subscription {
				..
			} => RegionKind::SubscriberFrame,
			UnitKind::ClosureBody => RegionKind::ClosureBody,
			UnitKind::Guard => RegionKind::PermissionFrame,
			UnitKind::LoopBody => RegionKind::Loop,
		}
	}

	/// The types of the arguments of the top block of a container planned as this unit.
	///
	/// A unit has no statement around it to supply the statement timestamp, so the timestamp is
	/// the first argument. A subscription's notification unit then takes the names its region
	/// kind documents ([`RegionKind::SubscriberFrame`]), in that order: the unit reads them as
	/// block arguments, never as session names. A guard takes the row it judges after the
	/// timestamp, as a permission predicate of a statement does, and reads it as `$this`, `$self`
	/// and `$value`. A function body takes the row of its caller after the timestamp, which a
	/// field read or `@` in the body reads.
	pub(crate) fn top_args(&self) -> Vec<TypeKnowledge> {
		let mut args = vec![timestamp_arg()];
		match self {
			UnitKind::Subscription {
				..
			} => args.extend(std::iter::repeat_n(TypeKnowledge::Unknown, SUBSCRIBER_ARGS)),
			UnitKind::Guard | UnitKind::FunctionBody => args.push(TypeKnowledge::Unknown),
			UnitKind::Request
			| UnitKind::RequestStatement {
				..
			}
			| UnitKind::ServedStatement {
				..
			}
			| UnitKind::TableRead
			| UnitKind::Subquery {
				..
			}
			| UnitKind::MatchStage
			| UnitKind::Cascade
			| UnitKind::StageBundle {
				..
			}
			| UnitKind::Events {
				..
			}
			| UnitKind::ViewRows {
				..
			}
			| UnitKind::ClosureBody
			| UnitKind::LoopBody => {}
		}
		args
	}

	/// How the body of the unit is bound under the top region.
	pub(crate) fn form(&self) -> UnitForm {
		match self {
			UnitKind::Request
			| UnitKind::RequestStatement {
				..
			}
			| UnitKind::ServedStatement {
				..
			} => UnitForm::Request,
			UnitKind::TableRead | UnitKind::MatchStage => UnitForm::Statement,
			UnitKind::Events {
				..
			} => UnitForm::Catalog,
			UnitKind::FunctionBody
			| UnitKind::Cascade
			| UnitKind::StageBundle {
				..
			}
			| UnitKind::ViewRows {
				..
			}
			| UnitKind::Subscription {
				..
			} => UnitForm::List,
			UnitKind::Subquery {
				..
			}
			| UnitKind::ClosureBody
			| UnitKind::Guard => UnitForm::Value,
			UnitKind::LoopBody => UnitForm::Extracted,
		}
	}

	/// The position in its request of the statement the unit is, for the `(statement N)` text of
	/// a rejection. A unit that is no statement of the request answers `0`.
	pub(crate) fn position(&self) -> usize {
		match self {
			UnitKind::RequestStatement {
				position,
			}
			| UnitKind::ServedStatement {
				position,
			} => *position,
			UnitKind::Request
			| UnitKind::FunctionBody
			| UnitKind::TableRead
			| UnitKind::Subquery {
				..
			}
			| UnitKind::MatchStage
			| UnitKind::Cascade
			| UnitKind::StageBundle {
				..
			}
			| UnitKind::Events {
				..
			}
			| UnitKind::ViewRows {
				..
			}
			| UnitKind::ClosureBody
			| UnitKind::Guard
			| UnitKind::Subscription {
				..
			}
			| UnitKind::LoopBody => 0,
		}
	}

	/// Whether the region-constraint check judges a container planned as this unit.
	///
	/// The check judges only code written in the request: a request, a served statement, a
	/// subquery in request code and the read of a `MATCH` stage. Code the catalog stores, and
	/// code the planner synthesises, is never judged, so a definition stored before a constraint
	/// existed keeps working.
	pub(crate) fn checked(&self) -> bool {
		match self {
			UnitKind::Request
			| UnitKind::RequestStatement {
				..
			}
			| UnitKind::ServedStatement {
				..
			}
			| UnitKind::MatchStage
			| UnitKind::Subquery {
				stored: false,
			} => true,
			UnitKind::Subquery {
				stored: true,
			}
			| UnitKind::FunctionBody
			| UnitKind::TableRead
			| UnitKind::Cascade
			| UnitKind::StageBundle {
				..
			}
			| UnitKind::Events {
				..
			}
			| UnitKind::ViewRows {
				..
			}
			| UnitKind::ClosureBody
			| UnitKind::Guard
			| UnitKind::Subscription {
				..
			}
			| UnitKind::LoopBody => false,
		}
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::*;
	use crate::exec::ir::core::ir::{Ir, IrScope};

	/// The units that run stored code stand under a running kind, never under `StoredClause`,
	/// and none stands under `FunctionBody`, which the inliner reads as an inlinable function.
	#[test]
	fn the_top_of_a_stored_code_unit_is_a_running_kind() {
		let table = TableName::new("t");
		let bundle = UnitKind::StageBundle {
			table: table.clone(),
			action: Action::Update,
		};
		let view_rows = UnitKind::ViewRows {
			table: table.clone(),
		};
		let subscription = UnitKind::Subscription {
			table,
		};
		assert_eq!(UnitKind::Cascade.top_region(), RegionKind::WriteStage);
		assert_eq!(bundle.top_region(), RegionKind::WriteStage);
		assert_eq!(view_rows.top_region(), RegionKind::WriteStage);
		assert_eq!(subscription.top_region(), RegionKind::SubscriberFrame);
		for unit in [UnitKind::Cascade, bundle, view_rows, subscription] {
			assert!(!unit.checked(), "{unit:?} is never judged");
			assert_eq!(unit.form(), UnitForm::List, "{unit:?} binds a statement list");
		}
	}

	/// The top of a stored-code unit holds the unit's statements as a list, and the same kinds
	/// nested under a write or a subscription hold one clause.
	#[test]
	fn a_stored_code_unit_top_holds_a_statement_list() {
		for kind in [RegionKind::WriteStage, RegionKind::SubscriberFrame] {
			let ir = Ir::new(
				IrScope::Request {
					fragment: None,
				},
				kind,
			);
			assert!(ir.holds_statement_list(ir.top()), "{kind:?} at the top");
			assert!(!kind.holds_statement_list(false), "{kind:?} nested");
		}
	}

	/// The events of a table stand under a running kind, are built from the catalog's definitions
	/// rather than bound from a statement, and are never judged.
	#[test]
	fn an_events_unit_is_built_from_the_catalog_under_a_running_kind() {
		let events = UnitKind::Events {
			table: TableName::new("t"),
			action: Action::Create,
		};
		assert_eq!(events.top_region(), RegionKind::WriteStage);
		assert_eq!(events.form(), UnitForm::Catalog);
		assert_eq!(events.top_args(), vec![timestamp_arg()]);
		assert!(!events.checked());
		assert_eq!(events.position(), 0);
	}

	/// A guard is a value over the row it is handed, under a permission frame, and is never
	/// judged.
	#[test]
	fn a_guard_is_a_value_over_a_row_under_a_permission_frame() {
		assert_eq!(UnitKind::Guard.top_region(), RegionKind::PermissionFrame);
		assert_eq!(UnitKind::Guard.form(), UnitForm::Value);
		assert!(!UnitKind::Guard.checked());
		assert_eq!(UnitKind::Guard.top_args(), vec![timestamp_arg(), TypeKnowledge::Unknown]);
	}

	/// Every unit's top block takes the statement timestamp first, and a subscription then the
	/// eight names of its notification unit.
	#[test]
	fn a_subscription_unit_takes_the_names_of_its_frame_after_the_timestamp() {
		assert_eq!(UnitKind::Request.top_args(), vec![timestamp_arg()]);
		assert_eq!(UnitKind::ClosureBody.top_args(), vec![timestamp_arg()]);
		let subscription = UnitKind::Subscription {
			table: TableName::new("t"),
		};
		let args = subscription.top_args();
		assert_eq!(args.len(), 1 + SUBSCRIBER_ARGS);
		assert_eq!(args[0], timestamp_arg());
		assert!(args[1..].iter().all(|arg| *arg == TypeKnowledge::Unknown));
	}
}
