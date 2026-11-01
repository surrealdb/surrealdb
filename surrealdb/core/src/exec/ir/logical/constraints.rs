//! The region constraints: which constructs a region of a given kind admits.
//!
//! One rule per row of the constraint table. A condition, order, group or split clause admits no
//! write, definition or `SLEEP`, and a block in it is an ordinary block that binds a `LET`; a
//! permission or assertion frame admits no write or definition; a computed, value or default field
//! clause admits no write (through a function's summary as well as directly) and no definition; a
//! stored index column admits no non-deterministic construct and no read of another record. The
//! expression row would reject a `LET` used as an expression, a transaction-control statement and
//! `USE`. The parser and the binder never build those shapes, so that row is disabled and rejects
//! nothing. A row or a construct that is not enabled admits.
//!
//! [`admits`] is the table and nothing else. [`check`] and [`rejection_of`] apply it to a
//! container: they walk the ops of a region and of every region nested in it, and judge each op
//! against the constrained regions that enclose it. Three rules decide which regions count:
//!
//! - Only code the request writes is judged. A frame that holds persisted catalog code (a
//!   permission or field frame, an event or subscriber frame, a write stage, a function body, a
//!   catalog unit) is not entered, so a definition stored before a row existed keeps working. The
//!   exception is a region the walk is started on.
//! - A stored clause is constrained only under the definition that stores it: a table's and a
//!   field's permissions, a field's assertion, value, default and computed clauses, an index's
//!   columns.
//! - An effect answer rejects only when it is known: the lattice top, and an answer that holds an
//!   opaque body, admit.
//!
//! The walk enters every nested region of code the request writes, closures and subqueries
//! included. A `LET` written in a closure body binds a name local to the closure, so it is not a
//! `LET` of the clause that holds the closure.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;

use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::build::membership::is_membership_key;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope};
use crate::exec::ir::core::location::Location;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::OpEffects;
use crate::exec::ir::iface::ids::{OpId, RegionId};
use crate::exec::ir::iface::region_kind::{
	Admission, ClauseRole, Construct, ErrorScope, FieldRole, RegionKind, Section4bRow, StoredRole,
};
use crate::exec::ir::iface::statements::{DefineObject, WriteForm};
use crate::exec::ir::logical::ops::LogOp;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::vm::payload::Refusal;
use crate::expr::logical::EffectSet;

/// One row of the constraint table.
struct Row {
	row: Section4bRow,
	/// The constructs the row rejects in the regions it names.
	///
	/// Not listed, because no row enables them: `UnenclosedBreak` (a `BREAK` with no loop around
	/// it raises when it runs), `ExternalCall`, and `EscapingReturn`, which is a verifier
	/// invariant and never an admission answer.
	rejects: &'static [Construct],
	/// Whether the row rejects. A row that is not enabled admits every construct.
	enabled: bool,
}

/// A read filter, an order key, a group key and a split clause. The condition of a write
/// (`Clause(WriteCond)`) is not covered.
const CONDITION_CLAUSE: Row = Row {
	row: Section4bRow::ConditionClause,
	rejects: &[Construct::Write, Construct::Ddl, Construct::Sleep],
	enabled: true,
};

/// A permission and an assertion. A permission or assertion clause of a definition is judged where
/// the definition is written, and only a table's and a field's permissions are covered.
const PERMISSION_OR_ASSERT_FRAME: Row = Row {
	row: Section4bRow::PermissionOrAssertFrame,
	rejects: &[Construct::Write, Construct::Ddl],
	enabled: true,
};

/// A field's value, default and computed clauses. The summary of a called function comes from the
/// effect analysis, and only a known write rejects.
const FIELD_VALUE_FRAME: Row = Row {
	row: Section4bRow::FieldValueFrame,
	rejects: &[Construct::Write, Construct::Ddl],
	enabled: true,
};

/// A stored index column.
const INDEX_COLUMN: Row = Row {
	row: Section4bRow::IndexColumn,
	rejects: &[Construct::NonDeterministic, Construct::OtherRecordRead],
	enabled: true,
};

/// Every expression region. The row names a `LET` used as an expression, transaction control and
/// `USE`. Each of these is already an error of its own statement, so the row is disabled.
const EXPRESSION_REGION: Row = Row {
	row: Section4bRow::ExpressionRegion,
	rejects: &[Construct::Let, Construct::TxnControl, Construct::Use],
	enabled: false,
};

/// The row that names a region of `kind`, if any.
///
/// A function of the kind alone: whether a stored clause is under the definition that makes it
/// constrained is the walk's, which knows the owner.
fn row_of(kind: RegionKind) -> Option<&'static Row> {
	match kind {
		RegionKind::Clause(role) => Some(match role {
			ClauseRole::Where | ClauseRole::OrderKey | ClauseRole::GroupKey | ClauseRole::Split => {
				&CONDITION_CLAUSE
			}
			ClauseRole::WriteCond
			| ClauseRole::Projection
			| ClauseRole::SetValue
			| ClauseRole::Content
			| ClauseRole::InsertValues
			| ClauseRole::DuplicateUpdate
			| ClauseRole::ReturnClause
			| ClauseRole::From
			| ClauseRole::Limit
			| ClauseRole::Start
			| ClauseRole::Timeout
			| ClauseRole::IndexColumn
			| ClauseRole::ViewClause
			| ClauseRole::ReferenceRepair => &EXPRESSION_REGION,
		}),
		RegionKind::PermissionFrame => Some(&PERMISSION_OR_ASSERT_FRAME),
		RegionKind::FieldFrame(role) => Some(match role {
			FieldRole::Assert => &PERMISSION_OR_ASSERT_FRAME,
			FieldRole::Value | FieldRole::Default | FieldRole::Computed => &FIELD_VALUE_FRAME,
		}),
		RegionKind::StoredClause(role) => match role {
			StoredRole::Permissions | StoredRole::Assert => Some(&PERMISSION_OR_ASSERT_FRAME),
			StoredRole::Value | StoredRole::Default | StoredRole::Computed => {
				Some(&FIELD_VALUE_FRAME)
			}
			StoredRole::IndexColumn => Some(&INDEX_COLUMN),
			StoredRole::When
			| StoredRole::Then
			| StoredRole::Body
			| StoredRole::View
			| StoredRole::Authenticate
			| StoredRole::Context
			| StoredRole::Signup
			| StoredRole::Signin
			| StoredRole::Action
			| StoredRole::Fallback
			| StoredRole::LiveFields
			| StoredRole::LiveCond
			| StoredRole::LiveFetch => None,
		},
		RegionKind::ValueBlock
		| RegionKind::ValueIf
		| RegionKind::ValueFor
		| RegionKind::Subquery
		| RegionKind::FunctionBody
		| RegionKind::ClosureBody
		| RegionKind::LazyOperand
		| RegionKind::ErrorScope(_) => Some(&EXPRESSION_REGION),
		RegionKind::Request
		| RegionKind::Transaction(_)
		| RegionKind::Statement(_)
		| RegionKind::Block
		| RegionKind::StatementIf
		| RegionKind::StatementFor
		| RegionKind::Loop
		| RegionKind::EventFrame
		| RegionKind::SubscriberFrame
		| RegionKind::ChooseArm
		| RegionKind::Pipeline
		| RegionKind::JoinLookupArm
		| RegionKind::WriteStage
		| RegionKind::CatalogUnit
		| RegionKind::Deadline => None,
	}
}

/// Whether a region of `kind` admits `construct`, by the enabled rows; a disabled row admits. The
/// one source of the table.
pub(crate) fn admits(kind: RegionKind, construct: Construct) -> Admission {
	match row_of(kind) {
		Some(row) if row.enabled && row.rejects.contains(&construct) => Admission::Reject {
			construct,
			row: row.row,
		},
		Some(_) | None => Admission::Admit,
	}
}

/// Whether a region of `kind` holds code stored in the catalog rather than code the request
/// writes.
fn holds_persisted_code(kind: RegionKind) -> bool {
	match kind {
		RegionKind::PermissionFrame
		| RegionKind::FieldFrame(_)
		| RegionKind::EventFrame
		| RegionKind::SubscriberFrame
		| RegionKind::WriteStage
		| RegionKind::FunctionBody
		| RegionKind::CatalogUnit => true,
		RegionKind::Request
		| RegionKind::Transaction(_)
		| RegionKind::Statement(_)
		| RegionKind::Block
		| RegionKind::ValueBlock
		| RegionKind::StatementIf
		| RegionKind::ValueIf
		| RegionKind::StatementFor
		| RegionKind::ValueFor
		| RegionKind::Loop
		| RegionKind::Subquery
		| RegionKind::ClosureBody
		| RegionKind::Clause(_)
		| RegionKind::LazyOperand
		| RegionKind::ErrorScope(_)
		| RegionKind::ChooseArm
		| RegionKind::Pipeline
		| RegionKind::JoinLookupArm
		| RegionKind::Deadline
		| RegionKind::StoredClause(_) => false,
	}
}

/// The kind a region of `kind` owned by `owner` is judged as, when it is a region some enabled
/// row constrains.
///
/// The key of a `GROUP BY` is bound as an error scope owned by the aggregate, so it is judged as
/// a group key. A stored clause is judged only under the definition that stores it.
fn constrained_as(kind: RegionKind, owner: Option<&OpKind>) -> Option<RegionKind> {
	let judged = match (kind, owner) {
		(
			RegionKind::ErrorScope(ErrorScope::ShapeToNone),
			Some(OpKind::Logical(LogOp::Aggregate(_))),
		) => RegionKind::Clause(ClauseRole::GroupKey),
		(kind, _) => kind,
	};
	if let RegionKind::StoredClause(role) = judged {
		let Some(OpKind::Logical(LogOp::Define(define))) = owner else {
			return None;
		};
		let stored_here = match role {
			StoredRole::Permissions => {
				matches!(define.object, DefineObject::Table(_) | DefineObject::Field(_))
			}
			StoredRole::Assert | StoredRole::Value | StoredRole::Default | StoredRole::Computed => {
				matches!(define.object, DefineObject::Field(_))
			}
			StoredRole::IndexColumn => {
				matches!(define.object, DefineObject::Index { .. })
			}
			StoredRole::When
			| StoredRole::Then
			| StoredRole::Body
			| StoredRole::View
			| StoredRole::Authenticate
			| StoredRole::Context
			| StoredRole::Signup
			| StoredRole::Signin
			| StoredRole::Action
			| StoredRole::Fallback
			| StoredRole::LiveFields
			| StoredRole::LiveCond
			| StoredRole::LiveFetch => false,
		};
		if !stored_here {
			return None;
		}
	}
	row_of(judged).filter(|row| row.enabled).map(|_| judged)
}

/// The construct a logical op is by its tag, if it is one a row names.
fn logical_construct(op: &LogOp) -> Option<Construct> {
	match op {
		LogOp::Write(_)
		| LogOp::MatchUpdate(_)
		| LogOp::MatchDelete {
			..
		}
		| LogOp::MatchInsert(_) => Some(Construct::Write),
		LogOp::BindStep(shape) if shape.writes() => Some(Construct::Write),
		LogOp::Define(_) | LogOp::Remove(_) | LogOp::Alter(_) | LogOp::Rebuild(_) => {
			Some(Construct::Ddl)
		}
		// The key a membership binds once per probe row is a `LET` the binder makes, not one the
		// request wrote.
		LogOp::Let(attrs) => (!is_membership_key(&attrs.name)).then_some(Construct::Let),
		LogOp::Sleep => Some(Construct::Sleep),
		LogOp::Source(_) => Some(Construct::OtherRecordRead),
		LogOp::Request {
			..
		}
		| LogOp::Txn {
			..
		}
		| LogOp::Stmt {
			..
		}
		| LogOp::Statement {
			..
		}
		| LogOp::Values {
			..
		}
		| LogOp::Match {
			..
		}
		| LogOp::MatchRead {
			..
		}
		| LogOp::MatchOutput(_)
		| LogOp::BindStep(_)
		| LogOp::Subquery
		| LogOp::Assign(_)
		| LogOp::Unset {
			..
		}
		| LogOp::Content {
			..
		}
		| LogOp::DdlGuard {
			..
		}
		| LogOp::UnionAll
		| LogOp::Join {
			..
		}
		| LogOp::Filter
		| LogOp::PermissionFilter {
			..
		}
		| LogOp::Split {
			..
		}
		| LogOp::Aggregate(_)
		| LogOp::Distinct
		| LogOp::Sort(_)
		| LogOp::Window(_)
		| LogOp::Limit {
			..
		}
		| LogOp::Project(_)
		| LogOp::Fetch
		| LogOp::Timeout
		| LogOp::VersionScope
		| LogOp::Only
		| LogOp::Explain {
			..
		}
		| LogOp::Eval
		| LogOp::Block
		| LogOp::Return
		| LogOp::Loop {
			..
		}
		| LogOp::If
		| LogOp::Info {
			..
		}
		| LogOp::Use {
			..
		}
		| LogOp::Option {
			..
		}
		| LogOp::Access(_)
		| LogOp::Kill
		| LogOp::Live(_)
		| LogOp::Subscriber(_)
		| LogOp::Show(_)
		| LogOp::Choose(_)
		| LogOp::Events(_)
		| LogOp::WriteStage {
			..
		}
		| LogOp::Yield
		| LogOp::Next
		| LogOp::Break => None,
	}
}

/// Whether `op` calls something: a function, a method, a dispatched method or a closure value.
fn is_call(op: &ExprOp) -> bool {
	matches!(op, ExprOp::Call(_) | ExprOp::Method { .. } | ExprOp::Dispatch(_) | ExprOp::Invoke)
}

/// Whether `effects` is an answer about the op rather than the absence of one: neither the top of
/// the lattice nor the effects of a body the call site cannot see.
fn is_known(effects: &OpEffects) -> bool {
	effects.set != EffectSet::TOP && !effects.set.contains(EffectSet::OPAQUE_BODY)
}

/// Whether `effects` is known to stage a write.
pub(crate) fn known_write(effects: &OpEffects) -> bool {
	is_known(effects) && effects.set.writes_data()
}

/// The effects that make a second evaluation answer differently.
const NON_DETERMINISTIC_EFFECTS: EffectSet =
	EffectSet::ENTROPY.union(EffectSet::ADVANCES_STATE).union(EffectSet::CLOCK);

/// Whether `effects` is known to be non-deterministic.
pub(crate) fn known_non_deterministic(effects: &OpEffects) -> bool {
	is_known(effects) && effects.set.intersects(NON_DETERMINISTIC_EFFECTS)
}

/// The first op of a region the table rejects, with where it stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegionRejection {
	/// What the op is.
	pub(crate) construct: Construct,
	/// The row that rejects it.
	pub(crate) row: Section4bRow,
	/// The kind of the constrained region that holds it.
	pub(crate) region: RegionKind,
	/// The offending op.
	pub(crate) op: OpId,
	/// Where the op was written.
	pub(crate) loc: Location,
	/// The op as a schema-clause refusal names it ([`construct_named`]), for a rejection in a
	/// schema clause that must not have effects.
	pub(crate) named: Option<String>,
}

impl RegionRejection {
	/// The refusal the request is answered with: for a schema clause that must not have effects
	/// ([`schema_clause`]), [`Refusal::SchemaClauseEffect`] naming the clause and the construct;
	/// for any other region, an invalid statement with [`Self::message`].
	pub(crate) fn refusal(&self) -> Refusal {
		match (schema_clause(self.row, self.region), &self.named) {
			(Some(clause), Some(construct)) => Refusal::SchemaClauseEffect {
				clause: clause.to_owned(),
				construct: construct.clone(),
			},
			_ => Refusal::InvalidStatement {
				message: self.message(),
			},
		}
	}

	/// The message of an invalid-statement rejection: the construct, the region and the
	/// statement's 1-based position in its request or fragment.
	pub(crate) fn message(&self) -> String {
		format!(
			"{} {REJECTION_INFIX} {} (statement {})",
			construct_text(self.construct),
			region_text(self.region),
			self.loc.stmt.saturating_add(1),
		)
	}

	/// The error the request is answered with ([`Self::refusal`]).
	pub(crate) fn error(&self) -> crate::err::Error {
		self.refusal().error()
	}
}

/// The schema clause a rejection by `row` in a region of `kind` is in, as
/// [`crate::exec::Error::SchemaClauseEffect`] names it, when the row is one of the rows of the
/// clauses that must not have effects ([`crate::exec::clause_effects`]).
fn schema_clause(row: Section4bRow, kind: RegionKind) -> Option<&'static str> {
	match row {
		Section4bRow::PermissionOrAssertFrame | Section4bRow::FieldValueFrame => {}
		Section4bRow::ConditionClause
		| Section4bRow::IndexColumn
		| Section4bRow::ExpressionRegion => {
			return None;
		}
	}
	match kind {
		RegionKind::PermissionFrame | RegionKind::StoredClause(StoredRole::Permissions) => {
			Some("PERMISSIONS")
		}
		RegionKind::FieldFrame(FieldRole::Assert)
		| RegionKind::StoredClause(StoredRole::Assert) => Some("ASSERT"),
		RegionKind::FieldFrame(FieldRole::Value) | RegionKind::StoredClause(StoredRole::Value) => {
			Some("VALUE")
		}
		RegionKind::FieldFrame(FieldRole::Default)
		| RegionKind::StoredClause(StoredRole::Default) => Some("DEFAULT"),
		RegionKind::FieldFrame(FieldRole::Computed)
		| RegionKind::StoredClause(StoredRole::Computed) => Some("COMPUTED"),
		RegionKind::StoredClause(
			StoredRole::When
			| StoredRole::Then
			| StoredRole::Body
			| StoredRole::View
			| StoredRole::IndexColumn
			| StoredRole::Authenticate
			| StoredRole::Context
			| StoredRole::Signup
			| StoredRole::Signin
			| StoredRole::Action
			| StoredRole::Fallback
			| StoredRole::LiveFields
			| StoredRole::LiveCond
			| StoredRole::LiveFetch,
		)
		| RegionKind::Request
		| RegionKind::Transaction(_)
		| RegionKind::Statement(_)
		| RegionKind::Block
		| RegionKind::ValueBlock
		| RegionKind::StatementIf
		| RegionKind::ValueIf
		| RegionKind::StatementFor
		| RegionKind::ValueFor
		| RegionKind::Loop
		| RegionKind::Subquery
		| RegionKind::FunctionBody
		| RegionKind::ClosureBody
		| RegionKind::EventFrame
		| RegionKind::SubscriberFrame
		| RegionKind::Clause(_)
		| RegionKind::LazyOperand
		| RegionKind::ErrorScope(_)
		| RegionKind::ChooseArm
		| RegionKind::Pipeline
		| RegionKind::JoinLookupArm
		| RegionKind::WriteStage
		| RegionKind::CatalogUnit
		| RegionKind::Deadline => None,
	}
}

/// The construct `kind`, rejected as `construct`, in the words a schema-clause refusal names it
/// with ([`crate::exec::clause_effects`]).
fn construct_named(kind: &OpKind, construct: Construct) -> String {
	use crate::exec::clause_effects::{SLEEP_TEXT, write_text};
	match kind {
		OpKind::Logical(LogOp::Write(attrs)) => write_text(match attrs.form {
			WriteForm::Create => "CREATE",
			WriteForm::Update => "UPDATE",
			WriteForm::Upsert => "UPSERT",
			WriteForm::Delete => "DELETE",
			WriteForm::Insert => "INSERT",
			WriteForm::Relate => "RELATE",
		}),
		OpKind::Logical(LogOp::Define(_)) => "`DEFINE` changes the schema".to_owned(),
		OpKind::Logical(LogOp::Remove(_)) => "`REMOVE` changes the schema".to_owned(),
		OpKind::Logical(LogOp::Alter(_)) => "`ALTER` changes the schema".to_owned(),
		OpKind::Logical(LogOp::Rebuild(_)) => "`REBUILD` changes the schema".to_owned(),
		OpKind::Logical(LogOp::Sleep) => SLEEP_TEXT.to_owned(),
		OpKind::Expr(ExprOp::Call(attrs)) => match &attrs.function {
			crate::expr::Function::Normal(name) => format!("a call to `{name}` writes data"),
			crate::expr::Function::Custom(name) => format!("a call to `fn::{name}` writes data"),
			crate::expr::Function::Script(_)
			| crate::expr::Function::Model(_)
			| crate::expr::Function::Module(..)
			| crate::expr::Function::Silo {
				..
			} => "a call writes data".to_owned(),
		},
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => match construct {
			Construct::Write => "a write writes data".to_owned(),
			Construct::Ddl => crate::exec::clause_effects::SCHEMA_STATEMENT_TEXT.to_owned(),
			Construct::Sleep => SLEEP_TEXT.to_owned(),
			other @ (Construct::Let
			| Construct::ExternalCall
			| Construct::NonDeterministic
			| Construct::OtherRecordRead
			| Construct::UnenclosedBreak
			| Construct::EscapingReturn
			| Construct::TxnControl
			| Construct::Use) => construct_text(other).to_owned(),
		},
	}
}

/// The words between the construct and the region in a rejection's message.
const REJECTION_INFIX: &str = "is not allowed in";

/// The refusal `error` raises when it is the error of a [`RegionRejection`], which refuses the
/// request as a whole and not the statement that holds the construct.
///
/// A rejection is a schema-clause error ([`crate::exec::Error::SchemaClauseEffect`]), which the
/// planner raises for no other reason, or an invalid-statement error whose message has the shape
/// [`RegionRejection::message`] writes: the construct, [`REJECTION_INFIX`], the region and the
/// statement's position as `(statement N)`. No other error of the planner has that shape.
pub(crate) fn request_rejection(error: &crate::err::Error) -> Option<Refusal> {
	match error {
		crate::err::Error::Exec(crate::exec::Error::SchemaClauseEffect {
			..
		}) => Refusal::of(error),
		crate::err::Error::Exec(crate::exec::Error::InvalidStatement(message)) => {
			let (head, position) = message.rsplit_once(" (statement ")?;
			position.strip_suffix(')')?.parse::<u32>().ok()?;
			head.contains(REJECTION_INFIX).then(|| Refusal::InvalidStatement {
				message: message.clone(),
			})
		}
		crate::err::Error::Engine(_)
		| crate::err::Error::Exec(_)
		| crate::err::Error::Catalog(_)
		| crate::err::Error::Idx(_)
		| crate::err::Error::Kvs(_)
		| crate::err::Error::IamError(_)
		| crate::err::Error::ApiError(_)
		| crate::err::Error::InvalidPath(_)
		| crate::err::Error::InvalidUrl(_)
		| crate::err::Error::Http(_) => None,
	}
}

/// How the message names a construct.
fn construct_text(construct: Construct) -> &'static str {
	match construct {
		Construct::Write => "a write",
		Construct::Ddl => "a schema statement",
		Construct::Let => "LET",
		Construct::Sleep => "SLEEP",
		Construct::NonDeterministic => "a non-deterministic expression",
		Construct::OtherRecordRead => "a read of another record",
		Construct::TxnControl => "a transaction statement",
		Construct::Use => "USE",
		Construct::ExternalCall => "an external call",
		Construct::UnenclosedBreak => "a BREAK or CONTINUE outside a loop",
		Construct::EscapingReturn => "a RETURN that leaves its region",
	}
}

/// How the message names a region.
fn region_text(kind: RegionKind) -> &'static str {
	match kind {
		RegionKind::Clause(role) => match role {
			ClauseRole::Where => "a WHERE clause",
			ClauseRole::OrderKey => "an ORDER BY key",
			ClauseRole::GroupKey => "a GROUP BY key",
			ClauseRole::Split => "a SPLIT clause",
			ClauseRole::WriteCond
			| ClauseRole::Projection
			| ClauseRole::SetValue
			| ClauseRole::Content
			| ClauseRole::InsertValues
			| ClauseRole::DuplicateUpdate
			| ClauseRole::ReturnClause
			| ClauseRole::From
			| ClauseRole::Limit
			| ClauseRole::Start
			| ClauseRole::Timeout
			| ClauseRole::IndexColumn
			| ClauseRole::ViewClause
			| ClauseRole::ReferenceRepair => "an expression",
		},
		RegionKind::PermissionFrame => "a permission",
		RegionKind::FieldFrame(role) => match role {
			FieldRole::Assert => "an ASSERT clause",
			FieldRole::Value => "a VALUE clause",
			FieldRole::Default => "a DEFAULT clause",
			FieldRole::Computed => "a COMPUTED clause",
		},
		RegionKind::StoredClause(role) => match role {
			StoredRole::Permissions => "a permission",
			StoredRole::Assert => "an ASSERT clause",
			StoredRole::Value => "a VALUE clause",
			StoredRole::Default => "a DEFAULT clause",
			StoredRole::Computed => "a COMPUTED clause",
			StoredRole::IndexColumn => "an index definition",
			StoredRole::When
			| StoredRole::Then
			| StoredRole::Body
			| StoredRole::View
			| StoredRole::Authenticate
			| StoredRole::Context
			| StoredRole::Signup
			| StoredRole::Signin
			| StoredRole::Action
			| StoredRole::Fallback
			| StoredRole::LiveFields
			| StoredRole::LiveCond
			| StoredRole::LiveFetch => "an expression",
		},
		RegionKind::Request
		| RegionKind::Transaction(_)
		| RegionKind::Statement(_)
		| RegionKind::Block
		| RegionKind::ValueBlock
		| RegionKind::StatementIf
		| RegionKind::ValueIf
		| RegionKind::StatementFor
		| RegionKind::ValueFor
		| RegionKind::Loop
		| RegionKind::Subquery
		| RegionKind::FunctionBody
		| RegionKind::ClosureBody
		| RegionKind::EventFrame
		| RegionKind::SubscriberFrame
		| RegionKind::LazyOperand
		| RegionKind::ErrorScope(_)
		| RegionKind::ChooseArm
		| RegionKind::Pipeline
		| RegionKind::JoinLookupArm
		| RegionKind::WriteStage
		| RegionKind::CatalogUnit
		| RegionKind::Deadline => "an expression",
	}
}

/// A constrained region on the path from the start of the walk to the op being judged.
struct Active {
	/// The kind the region is judged as.
	kind: RegionKind,
	/// How many closure bodies enclosed the region when it was entered. A `LET` is the clause's
	/// own only when no closure body has been entered since.
	closures: u32,
}

/// The walk of one container.
struct Walk<'a, 'cx> {
	ir: &'a Ir,
	/// Answers the effects of a call; without it every call is unknown and admits.
	cx: Option<&'a EffectCx<'cx>>,
	facts: FactCache,
	/// The constrained regions that enclose the op being judged, outermost first.
	active: Vec<Active>,
	/// The closure bodies that enclose the op being judged.
	closures: u32,
}

impl Walk<'_, '_> {
	/// The first rejection among the ops of `region` and of the regions nested in it.
	///
	/// `start` is true for the region the walk began on, which is entered even when it holds
	/// persisted code.
	fn region(
		&mut self,
		region: RegionId,
		start: bool,
	) -> Result<Option<RegionRejection>, IrError> {
		let ir = self.ir;
		let data = ir.region(region).ok_or(IrError::Stale("region"))?;
		if !start && holds_persisted_code(data.kind) {
			return Ok(None);
		}
		let owner = ir.parent_op(region).and_then(|op| ir.op(op)).map(|op| &op.kind);
		let judged = constrained_as(data.kind, owner);
		let is_closure = matches!(data.kind, RegionKind::ClosureBody);
		if let Some(kind) = judged {
			self.active.push(Active {
				kind,
				closures: self.closures,
			});
		}
		if is_closure {
			self.closures += 1;
		}
		let found = self.ops(region);
		if is_closure {
			self.closures -= 1;
		}
		if judged.is_some() {
			self.active.pop();
		}
		found
	}

	/// The first rejection among the ops of `region`, each op before the regions it owns.
	fn ops(&mut self, region: RegionId) -> Result<Option<RegionRejection>, IrError> {
		let ir = self.ir;
		for op in ops_in(ir, region) {
			if let Some(rejection) = self.op(op)? {
				return Ok(Some(rejection));
			}
			let data = ir.op(op).ok_or(IrError::Stale("op"))?;
			for nested in &data.regions {
				if let Some(rejection) = self.region(*nested, false)? {
					return Ok(Some(rejection));
				}
			}
		}
		Ok(None)
	}

	/// The rejection of `op` by the innermost enclosing region whose row rejects what it is.
	fn op(&mut self, op: OpId) -> Result<Option<RegionRejection>, IrError> {
		if self.active.is_empty() {
			return Ok(None);
		}
		let ir = self.ir;
		let data = ir.op(op).ok_or(IrError::Stale("op"))?;
		for construct in self.constructs_of(op, &data.kind) {
			for active in self.active.iter().rev() {
				if construct == Construct::Let && active.closures != self.closures {
					continue;
				}
				if let Admission::Reject {
					construct,
					row,
				} = admits(active.kind, construct)
				{
					let named = schema_clause(row, active.kind)
						.map(|_| construct_named(&data.kind, construct));
					return Ok(Some(RegionRejection {
						construct,
						row,
						region: active.kind,
						op,
						loc: data.loc.clone(),
						named,
					}));
				}
			}
		}
		Ok(None)
	}

	/// What `op` is, as far as a row names it: by its tag, and for a call by what its effects are
	/// known to be.
	fn constructs_of(&mut self, op: OpId, kind: &OpKind) -> SmallVec<[Construct; 2]> {
		let mut out = SmallVec::new();
		match kind {
			OpKind::Logical(log) => out.extend(logical_construct(log)),
			OpKind::Expr(ExprOp::Now {
				..
			}) => out.push(Construct::NonDeterministic),
			OpKind::Expr(ExprOp::Deref {
				..
			}) => out.push(Construct::OtherRecordRead),
			OpKind::Expr(call) if is_call(call) => {
				if let Some(cx) = self.cx {
					if self.judges_call_writes()
						&& known_write(&effects::of_with(
							self.ir,
							&mut self.facts,
							&structural_cx(cx.registry, self.ir),
							op,
						)) {
						out.push(Construct::Write);
					}
					if self.is_active(Section4bRow::IndexColumn)
						&& known_non_deterministic(&kind.local_effects(cx))
					{
						out.push(Construct::NonDeterministic);
					}
				}
			}
			OpKind::Expr(_) | OpKind::Phys(_) => {}
		}
		out
	}

	/// Whether a call's effect summary is judged for a write: a field-value region encloses the
	/// op and the innermost one is not a `COMPUTED` clause. A `COMPUTED` clause that calls a
	/// writing function is refused by the field-definition check, so this check leaves that case
	/// to it. A write written in the clause itself is rejected by its tag.
	fn judges_call_writes(&self) -> bool {
		let innermost = self.active.iter().rev().find(|active| {
			row_of(active.kind).is_some_and(|held| held.row == Section4bRow::FieldValueFrame)
		});
		innermost.is_some_and(|active| {
			!matches!(
				active.kind,
				RegionKind::FieldFrame(FieldRole::Computed)
					| RegionKind::StoredClause(StoredRole::Computed)
			)
		})
	}

	/// Whether a region of the row's kind encloses the op being judged.
	fn is_active(&self, row: Section4bRow) -> bool {
		self.active.iter().any(|active| row_of(active.kind).is_some_and(|held| held.row == row))
	}
}

/// The first rejection among the ops of `region` and of the regions nested in it, in pre-order;
/// `None` when every region admits what it holds.
///
/// `region` is judged by its own kind and by the constrained regions nested in it, even when it
/// holds persisted code. A region that is not constrained, nested in one that is, is judged by the
/// outer region.
fn first_rejection(
	ir: &Ir,
	cx: Option<&EffectCx<'_>>,
	region: RegionId,
) -> Result<Option<RegionRejection>, IrError> {
	if matches!(ir.scope(), IrScope::CatalogVersion { .. }) {
		// A catalog version holds definitions already stored; none is judged again.
		return Ok(None);
	}
	let mut walk = Walk {
		ir,
		cx,
		facts: FactCache::new(),
		active: Vec::new(),
		closures: 0,
	};
	walk.region(region, true)
}

/// The answer of `rejection` as an admission.
fn admission_of(rejection: Option<RegionRejection>) -> Admission {
	match rejection {
		Some(rejection) => Admission::Reject {
			construct: rejection.construct,
			row: rejection.row,
		},
		None => Admission::Admit,
	}
}

/// Checks every construct of `region` and of the regions nested in it against the rows its kind
/// and every enclosing constrained kind name, answering the first rejection in pre-order, or
/// [`Admission::Admit`] when every construct is admitted.
///
/// A call is judged by its effects, which this entry cannot ask, so every call admits;
/// [`check_with`] judges them.
pub(crate) fn check(ir: &Ir, region: RegionId) -> Result<Admission, IrError> {
	first_rejection(ir, None, region).map(admission_of)
}

/// [`check`] with the effect context that answers a call's effects. An answer that is not known
/// admits.
pub(crate) fn check_with(
	ir: &Ir,
	cx: &EffectCx<'_>,
	region: RegionId,
) -> Result<Admission, IrError> {
	first_rejection(ir, Some(cx), region).map(admission_of)
}

/// The first rejection among the ops of `region` and of the regions nested in it, judged by their
/// tags alone, as [`check`] judges them: a call admits.
pub(crate) fn rejection_in(ir: &Ir, region: RegionId) -> Result<Option<RegionRejection>, IrError> {
	first_rejection(ir, None, region)
}

/// The first rejection of the request, with the construct, the row, the kind of the region that
/// holds it and the offending op's location; `None` when every region admits what it holds.
///
/// A rejection is an answer and never an error: the container is a valid program, and the caller
/// that answers the request with the rejection takes [`RegionRejection::error`]. `Err` is for a
/// container that names an entity that does not exist.
pub(crate) fn rejection_of(ir: &Ir, cx: &EffectCx<'_>) -> Result<Option<RegionRejection>, IrError> {
	first_rejection(ir, Some(cx), ir.top())
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The walk of an empty container with `kinds` as the constrained regions around the op.
	fn judged_under(kinds: &[RegionKind]) -> bool {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let walk = Walk {
			ir: &ir,
			cx: None,
			facts: FactCache::new(),
			active: kinds
				.iter()
				.map(|kind| Active {
					kind: *kind,
					closures: 0,
				})
				.collect(),
			closures: 0,
		};
		walk.judges_call_writes()
	}

	/// The summary of a called function is judged for a write in a `VALUE` or `DEFAULT` clause
	/// and not in a `COMPUTED` one, whose writing call the field-definition check refuses.
	#[test]
	fn a_computed_clause_leaves_a_writing_call_to_the_field_check() {
		let value = RegionKind::StoredClause(StoredRole::Value);
		let default = RegionKind::FieldFrame(FieldRole::Default);
		let stored_computed = RegionKind::StoredClause(StoredRole::Computed);
		let layer_computed = RegionKind::FieldFrame(FieldRole::Computed);
		assert!(judged_under(&[value]));
		assert!(judged_under(&[default]));
		assert!(!judged_under(&[stored_computed]));
		assert!(!judged_under(&[layer_computed]));
		// The innermost field-value region decides.
		assert!(!judged_under(&[value, stored_computed]));
		assert!(judged_under(&[stored_computed, value]));
		// With no field-value region around, nothing is judged.
		assert!(!judged_under(&[]));
		assert!(!judged_under(&[RegionKind::Clause(ClauseRole::Where)]));
	}

	/// A rejection's error is recognised by its shape, and no other invalid-statement error is.
	#[test]
	fn a_rejection_message_is_recognised_by_its_shape() {
		use crate::exec::ir::core::location::LocOrigin;
		let rejection = RegionRejection {
			construct: Construct::Write,
			row: Section4bRow::ConditionClause,
			region: RegionKind::Clause(ClauseRole::Where),
			op: OpId::default(),
			loc: Location {
				stmt: 1,
				span: None,
				origin: LocOrigin::Query,
			},
			named: None,
		};
		assert_eq!(
			request_rejection(&rejection.error()),
			Some(Refusal::InvalidStatement {
				message: "a write is not allowed in a WHERE clause (statement 2)".to_owned()
			})
		);
		let clause = RegionRejection {
			row: Section4bRow::FieldValueFrame,
			region: RegionKind::StoredClause(StoredRole::Value),
			named: Some("`CREATE` writes data".to_owned()),
			..rejection
		};
		assert_eq!(
			request_rejection(&clause.error()),
			Some(Refusal::SchemaClauseEffect {
				clause: "VALUE".to_owned(),
				construct: "`CREATE` writes data".to_owned(),
			})
		);
		let other = crate::err::Error::Exec(crate::exec::Error::InvalidStatement(
			"Import requires `OPTION IMPORT;` as the first statement".to_owned(),
		));
		assert_eq!(request_rejection(&other), None);
		let positioned = crate::err::Error::Exec(crate::exec::Error::InvalidStatement(
			"a statement is not allowed here (statement x)".to_owned(),
		));
		assert_eq!(request_rejection(&positioned), None);
	}
}
