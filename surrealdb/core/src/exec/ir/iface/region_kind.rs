//! Region kinds and the properties that are a function of a kind alone.
//!
//! Every region of the logical dialect carries one [`RegionKind`], fixed by the op that owns the
//! region and the slot it fills. Every payload is a fieldless `Copy` enum, so each property of a
//! region is computed from its kind, and no property reads an attribute of the region.
//!
//! There is no session region (the session is host-side state) and no fragment region (a
//! fragment is a request scope whose top region is [`RegionKind::Request`]). A physical op's
//! region keeps the kind of the logical region it replaces; a new physical region is a
//! [`RegionKind::Pipeline`], a [`RegionKind::ChooseArm`] or a [`RegionKind::JoinLookupArm`].

use smallvec::SmallVec;

/// The kinds of an op's regions, in region order.
pub(crate) type RegionKinds = SmallVec<[RegionKind; 4]>;

/// The kind of a region.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RegionKind {
	/// The top-level statement list of one fragment, and the top region of a nested query.
	Request,
	/// The statements from `BEGIN`, or from the start of a continued fragment, to the end of the
	/// transaction.
	Transaction(TxnKind),
	/// One top-level statement, or one statement of a transaction.
	Statement(StatementCommit),
	/// `{ ... }` standing as a statement.
	Block,
	/// `{ ... }` standing as a value.
	ValueBlock,
	/// An arm of an `IF` standing as a statement. Every arm of a chain takes the position of the
	/// head `IF`.
	StatementIf,
	/// An arm of an `IF` standing as a value.
	ValueIf,
	/// The body of a `FOR` standing as a statement.
	StatementFor,
	/// The body of a `FOR` standing as a value.
	ValueFor,
	/// The per-element body of every loop that is not a `FOR`: select, write, closure, element
	/// filter, graph step, recursion and broadcast.
	Loop,
	/// `( ... )` wrapping a statement.
	Subquery,
	/// A function body.
	FunctionBody,
	/// A closure literal's body.
	ClosureBody,
	/// A table or field permission predicate.
	PermissionFrame,
	/// A field clause, by the role it plays.
	FieldFrame(FieldRole),
	/// An event's `WHEN` or `THEN` code.
	EventFrame,
	/// A live subscription's notification unit: its block arguments are `$before`, `$after`,
	/// `$value`, `$event`, `$auth`, `$session`, `$access` and `$token`.
	SubscriberFrame,
	/// A statement-owned expression position.
	Clause(ClauseRole),
	/// The right-hand side of a short-circuit operator, the `then` of an optional chain, a
	/// destructure alias.
	LazyOperand,
	/// A region in which an error is caught or reshaped.
	ErrorScope(ErrorScope),
	/// One arm of a `Choose`.
	ChooseArm,
	/// A pipeline of physical operators.
	Pipeline,
	/// The read of a join's right input through an index lookup: a copy of the right input's
	/// chain whose scan is an index lookup keyed by the block argument, which is the left row's
	/// key value. It runs per left row, for the first rows only and only while no build table
	/// exists, so its ops may run zero times.
	JoinLookupArm,
	/// The stored code at one stage of a write: field state, permissions, events, view
	/// maintenance, reference actions. The statement's own clauses are never inside one.
	WriteStage,
	/// A catalog unit: a stored definition compiled on its own.
	CatalogUnit,
	/// The timed body of a timeout.
	Deadline,
	/// Code a statement stores and never runs, by the role it plays.
	StoredClause(StoredRole),
}

/// Whether a transaction is written in the request or held by the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TxnKind {
	/// `BEGIN` ... `COMMIT` or `CANCEL` in the request text.
	Explicit,
	/// A transaction the client holds across requests.
	Session,
}

/// How a statement is committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum StatementCommit {
	/// Its own transaction.
	Auto,
	/// Inside an enclosing transaction.
	InTxn,
}

/// The role of a field clause, which decides what a construct in it may do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FieldRole {
	Value,
	Default,
	Assert,
	Computed,
}

/// The role of a statement-owned expression position.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ClauseRole {
	/// A read filter: `SELECT`, an element or graph filter.
	Where,
	/// The `WHERE` of `UPDATE`, `DELETE` and `UPSERT`.
	WriteCond,
	OrderKey,
	GroupKey,
	Split,
	Projection,
	SetValue,
	Content,
	InsertValues,
	/// `INSERT ... ON DUPLICATE KEY UPDATE`.
	DuplicateUpdate,
	ReturnClause,
	From,
	Limit,
	Start,
	Timeout,
	/// A column of an index, or the guard of a `COUNT` index, evaluated over an image of a
	/// written record when the write maintains the index.
	IndexColumn,
	/// A clause of a view the written table feeds, evaluated over one image in the frame of a
	/// view when the write maintains the view.
	ViewClause,
	/// The `THEN` expression of a `REFERENCE` field's `ON DELETE` clause, evaluated over the id of
	/// the referencing record when the write purges a record the field can name. The region takes
	/// that id as its row and the purged record as `$reference`.
	ReferenceRepair,
}

/// Why a region catches or reshapes an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ErrorScope {
	/// An error of the ignorable set becomes `NONE`: an aggregate argument, a `GROUP BY` key.
	ShapeToNone,
	/// A method call's body.
	MethodBody,
	/// A permission check, which turns an error into a denial.
	PermissionCheck,
}

/// The role of a stored clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum StoredRole {
	Value,
	Assert,
	Default,
	Computed,
	Permissions,
	When,
	Then,
	Body,
	View,
	IndexColumn,
	Authenticate,
	Context,
	Signup,
	Signin,
	Action,
	Fallback,
	LiveFields,
	LiveCond,
	LiveFetch,
}

impl RegionKind {
	/// Whether a region of this kind holds a statement list: the request, a transaction, a
	/// statement, a block, a statement branch, a `FOR` body, a function or closure body, or a
	/// stored function body.
	///
	/// `unit_top` says the region is the top region of its container. The top of a unit that
	/// runs stored code, a [`WriteStage`](Self::WriteStage) or a
	/// [`SubscriberFrame`](Self::SubscriberFrame), holds the unit's statements as a list; the
	/// same kinds nested in a write or a subscription hold the one clause of their stage, which
	/// is an expression and not a statement list.
	pub(crate) fn holds_statement_list(self, unit_top: bool) -> bool {
		match self {
			Self::Request
			| Self::Transaction(_)
			| Self::Statement(_)
			| Self::Block
			| Self::ValueBlock
			| Self::StatementIf
			| Self::StatementFor
			| Self::ValueFor
			| Self::FunctionBody
			| Self::ClosureBody => true,
			Self::StoredClause(role) => role == StoredRole::Body,
			Self::WriteStage | Self::SubscriberFrame => unit_top,
			Self::ValueIf
			| Self::Loop
			| Self::Subquery
			| Self::PermissionFrame
			| Self::FieldFrame(_)
			| Self::EventFrame
			| Self::Clause(_)
			| Self::LazyOperand
			| Self::ErrorScope(_)
			| Self::ChooseArm
			| Self::Pipeline
			| Self::JoinLookupArm
			| Self::CatalogUnit
			| Self::Deadline => false,
		}
	}
}

/// Whether a `Statement` region whose owner stands in a region of kind `outer` passes a `RETURN`
/// outward: it does when it is a statement of a statement list, and catches it when it is a
/// statement of the request or of a transaction. `None` is a statement that is the top region of
/// its container, which catches.
pub(crate) fn statement_passes_return(outer: Option<RegionKind>) -> bool {
	outer.is_some_and(|outer| !matches!(outer, RegionKind::Request | RegionKind::Transaction(_)))
}

/// What a `RETURN` does on reaching a region.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ReturnRule {
	/// A context: the walk of a `RETURN` stops here and its value is the region's.
	CatchesReturn,
	PassesOutward,
	/// No `RETURN` walk reaches it: the request, the transactions and the catalog unit sit above
	/// every statement or body that catches. Reaching one is a verifier failure.
	NeverReached,
}

/// What a `BREAK` or `CONTINUE` does on reaching a region.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LoopRule {
	/// The nearest region that is a target ends the walk.
	Target,
	Passes,
	/// The code is not written in the user's loop body, so the walk raises instead of leaving.
	Barrier,
}

/// Which bindings of the enclosing regions a region may read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Visibility {
	/// Every binding in scope at the region's position.
	Inherits,
	/// Its declared arguments, the statement timestamp, and nothing of its caller's bindings.
	ArgumentsOnly,
	/// Only what is passed in as an operand or a block argument.
	Isolated,
}

/// How a region's evaluation authority is set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FrameRule {
	/// The authority of the enclosing region.
	Inherits,
	/// A frame of its own: the definer's, the caller's capped by the definition's limit, or the
	/// frame a view or a purge evaluates its clause in.
	Sets,
}

/// A construct a region may refuse to hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Construct {
	Write,
	Ddl,
	Let,
	Sleep,
	ExternalCall,
	NonDeterministic,
	OtherRecordRead,
	UnenclosedBreak,
	EscapingReturn,
	TxnControl,
	Use,
}

/// Whether a region admits a construct.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Admission {
	Admit,
	Reject {
		construct: Construct,
		row: Section4bRow,
	},
}

/// The rows of the region-constraint table, each naming the regions it constrains.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Section4bRow {
	/// A filter, order key, group key or split clause: rejects a write, DDL and `SLEEP`.
	ConditionClause,
	/// A permission frame or an assertion: rejects a write and DDL.
	PermissionOrAssertFrame,
	/// A field's value, default or computed clause, and the stored clause of the same at `DEFINE`:
	/// rejects a write, including one through a function's summary, and DDL.
	FieldValueFrame,
	/// An index column: rejects a non-deterministic expression and a read of another record.
	IndexColumn,
	/// Any expression region: rejects a `LET` as an expression, transaction control and `USE`.
	ExpressionRegion,
}

/// The properties of a region, each a function of its kind.
pub(crate) trait RegionProperties: Copy {
	/// What a `RETURN` does on reaching the region.
	///
	/// A `RETURN` sets the value of the context it stands in and ends it. The contexts are a
	/// statement, a function or closure body, the stored code a write or a subscription runs, and
	/// each evaluation a statement makes over one row or one value it produces: a filter, a
	/// projection, an order, group or split key, a value a write stores, a write's `RETURN`
	/// clause, and the clauses a write maintains (an index column, a view clause, a reference
	/// repair). Every other region passes a `RETURN` outward: a block, an arm of an `IF`, the body
	/// of a `FOR`, a parenthesised statement, an operand, and the clauses a statement evaluates
	/// once rather than per row (`FROM`, `LIMIT`, `START`, `TIMEOUT`, the rows an `INSERT`
	/// writes).
	///
	/// A statement written in a statement list (a block, a body, an arm), rather than directly in
	/// a request or a transaction, is no context either. That depends on where the region stands
	/// and not on its kind, so a walk asks [`statement_passes_return`] of a `Statement` region
	/// before it reads this rule.
	fn return_rule(self) -> ReturnRule;
	/// What a `BREAK` or `CONTINUE` does on reaching the region.
	fn loop_rule(self) -> LoopRule;
	/// Which enclosing bindings the region may read.
	fn binding_visibility(self) -> Visibility;
	/// Whether the region's effects are undone when it is cancelled.
	fn undo_on_cancel(self) -> bool;
	/// How the region's evaluation authority is set.
	fn frame_rule(self) -> FrameRule;
	/// Whether the region's host-side state outlives the fragment that holds it.
	fn may_span_fragments(self) -> bool;
	/// Whether no value defined outside the region is used inside it except through an operand
	/// or a block argument.
	fn isolated_from_above(self) -> bool;
	/// Whether the region admits `construct`.
	fn admits(self, construct: Construct) -> Admission;
}

impl RegionProperties for RegionKind {
	fn return_rule(self) -> ReturnRule {
		match self {
			Self::Request | Self::Transaction(_) | Self::CatalogUnit => ReturnRule::NeverReached,
			Self::Statement(_)
			| Self::FunctionBody
			| Self::ClosureBody
			| Self::PermissionFrame
			| Self::FieldFrame(_)
			| Self::EventFrame
			| Self::SubscriberFrame
			| Self::WriteStage
			| Self::StoredClause(_) => ReturnRule::CatchesReturn,
			Self::Clause(role) => match role {
				ClauseRole::Where
				| ClauseRole::WriteCond
				| ClauseRole::OrderKey
				| ClauseRole::GroupKey
				| ClauseRole::Split
				| ClauseRole::Projection
				| ClauseRole::SetValue
				| ClauseRole::Content
				| ClauseRole::DuplicateUpdate
				| ClauseRole::ReturnClause
				| ClauseRole::IndexColumn
				| ClauseRole::ViewClause
				| ClauseRole::ReferenceRepair => ReturnRule::CatchesReturn,
				ClauseRole::InsertValues
				| ClauseRole::From
				| ClauseRole::Limit
				| ClauseRole::Start
				| ClauseRole::Timeout => ReturnRule::PassesOutward,
			},
			Self::Block
			| Self::ValueBlock
			| Self::StatementIf
			| Self::ValueIf
			| Self::StatementFor
			| Self::ValueFor
			| Self::Subquery
			| Self::Loop
			| Self::LazyOperand
			| Self::ErrorScope(_)
			| Self::ChooseArm
			| Self::Pipeline
			| Self::JoinLookupArm
			| Self::Deadline => ReturnRule::PassesOutward,
		}
	}

	fn loop_rule(self) -> LoopRule {
		match self {
			Self::StatementFor | Self::ValueFor => LoopRule::Target,
			Self::Request
			| Self::Transaction(_)
			| Self::FunctionBody
			| Self::ClosureBody
			| Self::PermissionFrame
			| Self::FieldFrame(_)
			| Self::EventFrame
			| Self::SubscriberFrame
			| Self::WriteStage
			| Self::CatalogUnit
			| Self::StoredClause(_) => LoopRule::Barrier,
			Self::Statement(_)
			| Self::Block
			| Self::ValueBlock
			| Self::StatementIf
			| Self::ValueIf
			| Self::Loop
			| Self::Subquery
			| Self::Clause(_)
			| Self::LazyOperand
			| Self::ErrorScope(_)
			| Self::ChooseArm
			| Self::Pipeline
			| Self::JoinLookupArm
			| Self::Deadline => LoopRule::Passes,
		}
	}

	fn binding_visibility(self) -> Visibility {
		match self {
			Self::FunctionBody => Visibility::ArgumentsOnly,
			Self::ClosureBody
			| Self::PermissionFrame
			| Self::FieldFrame(_)
			| Self::EventFrame
			| Self::SubscriberFrame
			| Self::WriteStage
			| Self::CatalogUnit
			| Self::StoredClause(_) => Visibility::Isolated,
			Self::Request
			| Self::Transaction(_)
			| Self::Statement(_)
			| Self::Block
			| Self::ValueBlock
			| Self::StatementIf
			| Self::ValueIf
			| Self::StatementFor
			| Self::ValueFor
			| Self::Loop
			| Self::Subquery
			| Self::Clause(_)
			| Self::LazyOperand
			| Self::ErrorScope(_)
			| Self::ChooseArm
			| Self::Pipeline
			| Self::JoinLookupArm
			| Self::Deadline => Visibility::Inherits,
		}
	}

	fn undo_on_cancel(self) -> bool {
		match self {
			Self::Transaction(TxnKind::Explicit) => true,
			Self::Transaction(TxnKind::Session)
			| Self::Request
			| Self::Statement(_)
			| Self::Block
			| Self::ValueBlock
			| Self::StatementIf
			| Self::ValueIf
			| Self::StatementFor
			| Self::ValueFor
			| Self::Loop
			| Self::Subquery
			| Self::FunctionBody
			| Self::ClosureBody
			| Self::PermissionFrame
			| Self::FieldFrame(_)
			| Self::EventFrame
			| Self::SubscriberFrame
			| Self::Clause(_)
			| Self::LazyOperand
			| Self::ErrorScope(_)
			| Self::ChooseArm
			| Self::Pipeline
			| Self::JoinLookupArm
			| Self::WriteStage
			| Self::CatalogUnit
			| Self::Deadline
			| Self::StoredClause(_) => false,
		}
	}

	fn frame_rule(self) -> FrameRule {
		match self {
			Self::FunctionBody
			| Self::PermissionFrame
			| Self::FieldFrame(_)
			| Self::EventFrame
			| Self::SubscriberFrame
			| Self::WriteStage
			| Self::StoredClause(_)
			| Self::Clause(ClauseRole::ViewClause | ClauseRole::ReferenceRepair) => FrameRule::Sets,
			Self::Request
			| Self::Transaction(_)
			| Self::Statement(_)
			| Self::Block
			| Self::ValueBlock
			| Self::StatementIf
			| Self::ValueIf
			| Self::StatementFor
			| Self::ValueFor
			| Self::Loop
			| Self::Subquery
			| Self::ClosureBody
			| Self::Clause(_)
			| Self::LazyOperand
			| Self::ErrorScope(_)
			| Self::ChooseArm
			| Self::Pipeline
			| Self::JoinLookupArm
			| Self::CatalogUnit
			| Self::Deadline => FrameRule::Inherits,
		}
	}

	fn may_span_fragments(self) -> bool {
		match self {
			Self::Request | Self::Transaction(_) => true,
			Self::Statement(_)
			| Self::Block
			| Self::ValueBlock
			| Self::StatementIf
			| Self::ValueIf
			| Self::StatementFor
			| Self::ValueFor
			| Self::Loop
			| Self::Subquery
			| Self::FunctionBody
			| Self::ClosureBody
			| Self::PermissionFrame
			| Self::FieldFrame(_)
			| Self::EventFrame
			| Self::SubscriberFrame
			| Self::Clause(_)
			| Self::LazyOperand
			| Self::ErrorScope(_)
			| Self::ChooseArm
			| Self::Pipeline
			| Self::JoinLookupArm
			| Self::WriteStage
			| Self::CatalogUnit
			| Self::Deadline
			| Self::StoredClause(_) => false,
		}
	}

	fn isolated_from_above(self) -> bool {
		match self {
			Self::FunctionBody
			| Self::ClosureBody
			| Self::PermissionFrame
			| Self::FieldFrame(_)
			| Self::EventFrame
			| Self::SubscriberFrame
			| Self::WriteStage
			| Self::CatalogUnit
			| Self::StoredClause(_) => true,
			Self::Request
			| Self::Transaction(_)
			| Self::Statement(_)
			| Self::Block
			| Self::ValueBlock
			| Self::StatementIf
			| Self::ValueIf
			| Self::StatementFor
			| Self::ValueFor
			| Self::Loop
			| Self::Subquery
			| Self::Clause(_)
			| Self::LazyOperand
			| Self::ErrorScope(_)
			| Self::ChooseArm
			| Self::Pipeline
			| Self::JoinLookupArm
			| Self::Deadline => false,
		}
	}

	/// The answer of the constraint table: a region admits a construct unless an enabled row
	/// names the region's kind and rejects the construct.
	fn admits(self, construct: Construct) -> Admission {
		crate::exec::ir::logical::constraints::admits(self, construct)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_statement_catches_return_and_the_request_never_sees_one() {
		assert_eq!(
			RegionKind::Statement(StatementCommit::Auto).return_rule(),
			ReturnRule::CatchesReturn
		);
		assert_eq!(RegionKind::Request.return_rule(), ReturnRule::NeverReached);
		assert_eq!(
			RegionKind::Transaction(TxnKind::Explicit).return_rule(),
			ReturnRule::NeverReached
		);
	}

	#[test]
	fn a_return_ends_a_context_and_passes_every_other_region() {
		for context in [
			RegionKind::FunctionBody,
			RegionKind::ClosureBody,
			RegionKind::Clause(ClauseRole::Where),
			RegionKind::Clause(ClauseRole::WriteCond),
			RegionKind::Clause(ClauseRole::Projection),
			RegionKind::Clause(ClauseRole::SetValue),
			RegionKind::Clause(ClauseRole::Content),
			RegionKind::Clause(ClauseRole::ReturnClause),
		] {
			assert_eq!(context.return_rule(), ReturnRule::CatchesReturn, "{context:?}");
		}
		for passing in [
			RegionKind::Block,
			RegionKind::ValueBlock,
			RegionKind::StatementIf,
			RegionKind::ValueIf,
			RegionKind::StatementFor,
			RegionKind::ValueFor,
			RegionKind::Subquery,
			RegionKind::LazyOperand,
			RegionKind::Clause(ClauseRole::From),
			RegionKind::Clause(ClauseRole::Limit),
			RegionKind::Clause(ClauseRole::Start),
			RegionKind::Clause(ClauseRole::Timeout),
			RegionKind::Clause(ClauseRole::InsertValues),
		] {
			assert_eq!(passing.return_rule(), ReturnRule::PassesOutward, "{passing:?}");
		}
	}

	#[test]
	fn only_a_statement_of_the_request_or_a_transaction_is_a_context() {
		assert!(!statement_passes_return(None));
		assert!(!statement_passes_return(Some(RegionKind::Request)));
		assert!(!statement_passes_return(Some(RegionKind::Transaction(TxnKind::Explicit))));
		assert!(statement_passes_return(Some(RegionKind::FunctionBody)));
		assert!(statement_passes_return(Some(RegionKind::Block)));
	}

	#[test]
	fn only_a_for_body_is_a_loop_target_and_stored_code_is_a_barrier() {
		assert_eq!(RegionKind::StatementFor.loop_rule(), LoopRule::Target);
		assert_eq!(RegionKind::Loop.loop_rule(), LoopRule::Passes);
		assert_eq!(RegionKind::Clause(ClauseRole::SetValue).loop_rule(), LoopRule::Passes);
		assert_eq!(
			RegionKind::StoredClause(StoredRole::IndexColumn).loop_rule(),
			LoopRule::Barrier
		);
		assert_eq!(RegionKind::FunctionBody.loop_rule(), LoopRule::Barrier);
	}

	#[test]
	fn a_function_body_sees_its_arguments_only() {
		assert_eq!(RegionKind::FunctionBody.binding_visibility(), Visibility::ArgumentsOnly);
		assert_eq!(RegionKind::ClosureBody.binding_visibility(), Visibility::Isolated);
		assert_eq!(RegionKind::Block.binding_visibility(), Visibility::Inherits);
		assert!(RegionKind::FunctionBody.isolated_from_above());
		assert!(!RegionKind::Block.isolated_from_above());
	}

	#[test]
	fn only_an_explicit_transaction_undoes_on_cancel() {
		assert!(RegionKind::Transaction(TxnKind::Explicit).undo_on_cancel());
		assert!(!RegionKind::Transaction(TxnKind::Session).undo_on_cancel());
		assert!(!RegionKind::Statement(StatementCommit::InTxn).undo_on_cancel());
	}

	#[test]
	fn admission_is_the_answer_of_the_constraint_table() {
		assert_eq!(
			RegionKind::Clause(ClauseRole::Where).admits(Construct::Write),
			Admission::Reject {
				construct: Construct::Write,
				row: Section4bRow::ConditionClause,
			}
		);
		// A region no enabled row names admits every construct.
		assert_eq!(RegionKind::Block.admits(Construct::Write), Admission::Admit);
		assert_eq!(
			RegionKind::Clause(ClauseRole::Projection).admits(Construct::Let),
			Admission::Admit
		);
	}
}
