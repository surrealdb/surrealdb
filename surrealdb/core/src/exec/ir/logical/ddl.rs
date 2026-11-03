//! Schema statements: the order their guards and operands bind in, the definitions they store and
//! the plan a statement binds from.
//!
//! A schema statement binds to its authority checks, then the operands it evaluates, then its
//! `define`, `remove`, `alter` or `rebuild` op. [`guard_order`] states, per form, the sequence of
//! those steps in the order the statement's body performs them, so the first error is the one the
//! body raises. An operand the statement writes literally (a bare identifier, a string, a
//! duration) evaluates nothing and has no position of its own: where its step precedes the
//! statement's first check it binds immediately after the last check, so that every op of a
//! refused statement stands behind the check that refuses it.
//!
//! A `DEFINE` carries the definition it stores as a [`DefinitionTemplate`], built from the
//! statement alone as the stored type encodes it, with the fields filled when the statement runs
//! left as holes ([`TemplateHole`]); the template never reads the catalog. The clauses a
//! definition stores and never runs are `StoredClause` regions, bound under the frame of the
//! statement that defines them from the clause the template holds.
//!
//! [`plan`] derives, from one statement, everything its binding needs: the form, the check
//! `Allow` states, the operands in slot order, the regions in slot order and the op itself. A slot
//! is the position in the op's operand list or region list; planning allocates both in
//! evaluation order, and the binder checks that order against [`guard_order`].

use std::borrow::Cow;
use std::time::Duration;

use surrealdb_strand::Strand;

use super::ddl_alter::plan_alter;
use super::ddl_define::plan_define;
use super::ddl_remove::{plan_rebuild, plan_remove};
use super::ops::LogOp;
use super::seam::invariant;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::SemanticCatalog;
use crate::exec::ir::iface::ids::ValueId;
use crate::exec::ir::iface::region_kind::{RegionKind, StoredRole};
use crate::exec::ir::iface::statements::{
	AlterTemplate, ApiConfigShape, DdlChecks, DefinitionTemplate, MiddlewareShape, OperandSlot,
	PermissionShape, PermissionsShape, RegionSlot, TemplateHole,
};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::vm::codec::Codec;
use crate::exec::vm::payload::Refusal;
use crate::expr::logical::LogicalDdlStatement;
use crate::expr::permission::{Permission, Permissions};
use crate::expr::statements::define::config::api::ApiConfig;
use crate::expr::statements::{AlterStatement, DefineStatement, RemoveStatement};
use crate::expr::{Base, Expr, Literal, SelectStatement};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// One schema statement form: a variant of `DEFINE`, `REMOVE` or `ALTER`, or `REBUILD INDEX`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DdlForm {
	DefineNamespace,
	DefineDatabase,
	DefineFunction,
	DefineAnalyzer,
	DefineParam,
	DefineTable,
	DefineEvent,
	DefineField,
	DefineIndex,
	DefineUser,
	DefineModel,
	DefineModule,
	DefineAccess,
	DefineConfig,
	DefineApi,
	DefineBucket,
	DefineSequence,
	DefineType,
	RemoveNamespace,
	RemoveDatabase,
	RemoveFunction,
	RemoveAnalyzer,
	RemoveAccess,
	RemoveParam,
	RemoveTable,
	RemoveEvent,
	RemoveField,
	RemoveIndex,
	RemoveUser,
	RemoveModel,
	RemoveApi,
	RemoveBucket,
	RemoveSequence,
	RemoveModule,
	RemoveConfig,
	RemoveType,
	AlterSystem,
	AlterNamespace,
	AlterDatabase,
	AlterTable,
	AlterApi,
	AlterEvent,
	AlterIndex,
	AlterSequence,
	AlterField,
	AlterParam,
	AlterBucket,
	AlterConfig,
	AlterAnalyzer,
	AlterFunction,
	AlterUser,
	AlterAccess,
	AlterModule,
	AlterType,
	RebuildIndex,
}

impl DdlForm {
	/// Every form, in declaration order.
	pub(crate) const ALL: [DdlForm; 55] = [
		DdlForm::DefineNamespace,
		DdlForm::DefineDatabase,
		DdlForm::DefineFunction,
		DdlForm::DefineAnalyzer,
		DdlForm::DefineParam,
		DdlForm::DefineTable,
		DdlForm::DefineEvent,
		DdlForm::DefineField,
		DdlForm::DefineIndex,
		DdlForm::DefineUser,
		DdlForm::DefineModel,
		DdlForm::DefineModule,
		DdlForm::DefineAccess,
		DdlForm::DefineConfig,
		DdlForm::DefineApi,
		DdlForm::DefineBucket,
		DdlForm::DefineSequence,
		DdlForm::DefineType,
		DdlForm::RemoveNamespace,
		DdlForm::RemoveDatabase,
		DdlForm::RemoveFunction,
		DdlForm::RemoveAnalyzer,
		DdlForm::RemoveAccess,
		DdlForm::RemoveParam,
		DdlForm::RemoveTable,
		DdlForm::RemoveEvent,
		DdlForm::RemoveField,
		DdlForm::RemoveIndex,
		DdlForm::RemoveUser,
		DdlForm::RemoveModel,
		DdlForm::RemoveApi,
		DdlForm::RemoveBucket,
		DdlForm::RemoveSequence,
		DdlForm::RemoveModule,
		DdlForm::RemoveConfig,
		DdlForm::RemoveType,
		DdlForm::AlterSystem,
		DdlForm::AlterNamespace,
		DdlForm::AlterDatabase,
		DdlForm::AlterTable,
		DdlForm::AlterApi,
		DdlForm::AlterEvent,
		DdlForm::AlterIndex,
		DdlForm::AlterSequence,
		DdlForm::AlterField,
		DdlForm::AlterParam,
		DdlForm::AlterBucket,
		DdlForm::AlterConfig,
		DdlForm::AlterAnalyzer,
		DdlForm::AlterFunction,
		DdlForm::AlterUser,
		DdlForm::AlterAccess,
		DdlForm::AlterModule,
		DdlForm::AlterType,
		DdlForm::RebuildIndex,
	];
}

/// The form of `statement`.
pub(crate) fn form_of(statement: &LogicalDdlStatement) -> DdlForm {
	match statement {
		LogicalDdlStatement::Define(define) => match define.as_ref() {
			DefineStatement::Namespace(_) => DdlForm::DefineNamespace,
			DefineStatement::Database(_) => DdlForm::DefineDatabase,
			DefineStatement::Function(_) => DdlForm::DefineFunction,
			DefineStatement::Analyzer(_) => DdlForm::DefineAnalyzer,
			DefineStatement::Param(_) => DdlForm::DefineParam,
			DefineStatement::Table(_) => DdlForm::DefineTable,
			DefineStatement::Event(_) => DdlForm::DefineEvent,
			DefineStatement::Field(_) => DdlForm::DefineField,
			DefineStatement::Index(_) => DdlForm::DefineIndex,
			DefineStatement::User(_) => DdlForm::DefineUser,
			DefineStatement::Model(_) => DdlForm::DefineModel,
			DefineStatement::Module(_) => DdlForm::DefineModule,
			DefineStatement::Access(_) => DdlForm::DefineAccess,
			DefineStatement::Config(_) => DdlForm::DefineConfig,
			DefineStatement::Api(_) => DdlForm::DefineApi,
			DefineStatement::Bucket(_) => DdlForm::DefineBucket,
			DefineStatement::Sequence(_) => DdlForm::DefineSequence,
			DefineStatement::Type(_) => DdlForm::DefineType,
		},
		LogicalDdlStatement::Remove(remove) => match remove.as_ref() {
			RemoveStatement::Namespace(_) => DdlForm::RemoveNamespace,
			RemoveStatement::Database(_) => DdlForm::RemoveDatabase,
			RemoveStatement::Function(_) => DdlForm::RemoveFunction,
			RemoveStatement::Analyzer(_) => DdlForm::RemoveAnalyzer,
			RemoveStatement::Access(_) => DdlForm::RemoveAccess,
			RemoveStatement::Param(_) => DdlForm::RemoveParam,
			RemoveStatement::Table(_) => DdlForm::RemoveTable,
			RemoveStatement::Event(_) => DdlForm::RemoveEvent,
			RemoveStatement::Field(_) => DdlForm::RemoveField,
			RemoveStatement::Index(_) => DdlForm::RemoveIndex,
			RemoveStatement::User(_) => DdlForm::RemoveUser,
			RemoveStatement::Model(_) => DdlForm::RemoveModel,
			RemoveStatement::Api(_) => DdlForm::RemoveApi,
			RemoveStatement::Bucket(_) => DdlForm::RemoveBucket,
			RemoveStatement::Sequence(_) => DdlForm::RemoveSequence,
			RemoveStatement::Module(_) => DdlForm::RemoveModule,
			RemoveStatement::Config(_) => DdlForm::RemoveConfig,
			RemoveStatement::Type(_) => DdlForm::RemoveType,
		},
		LogicalDdlStatement::Alter(alter) => match alter.as_ref() {
			AlterStatement::System(_) => DdlForm::AlterSystem,
			AlterStatement::Namespace(_) => DdlForm::AlterNamespace,
			AlterStatement::Database(_) => DdlForm::AlterDatabase,
			AlterStatement::Table(_) => DdlForm::AlterTable,
			AlterStatement::Api(_) => DdlForm::AlterApi,
			AlterStatement::Event(_) => DdlForm::AlterEvent,
			AlterStatement::Index(_) => DdlForm::AlterIndex,
			AlterStatement::Sequence(_) => DdlForm::AlterSequence,
			AlterStatement::Field(_) => DdlForm::AlterField,
			AlterStatement::Param(_) => DdlForm::AlterParam,
			AlterStatement::Bucket(_) => DdlForm::AlterBucket,
			AlterStatement::Config(_) => DdlForm::AlterConfig,
			AlterStatement::Analyzer(_) => DdlForm::AlterAnalyzer,
			AlterStatement::Function(_) => DdlForm::AlterFunction,
			AlterStatement::User(_) => DdlForm::AlterUser,
			AlterStatement::Access(_) => DdlForm::AlterAccess,
			AlterStatement::Module(_) => DdlForm::AlterModule,
			AlterStatement::Type(_) => DdlForm::AlterType,
		},
		LogicalDdlStatement::Rebuild(_) => DdlForm::RebuildIndex,
	}
}

/// What an operand of a schema statement is, as [`guard_order`] names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DdlOperand {
	/// The name of the object the statement defines, removes or alters.
	Name,
	/// The table, or the table an event or index belongs to.
	Table,
	/// The path of a field, resolved as a field path.
	FieldName,
	/// The path of an API.
	Path,
	/// The `COMMENT` clause.
	Comment,
	/// The value of a parameter.
	Value,
	/// A timeout duration.
	Timeout,
	/// The `BATCH` of a sequence.
	Batch,
	/// The `START` of a sequence.
	Start,
	/// The `DURATION FOR GRANT` of an access method.
	GrantDuration,
	/// The `DURATION FOR TOKEN` of a user or an access method.
	TokenDuration,
	/// The `DURATION FOR SESSION` of a user or an access method.
	SessionDuration,
	/// The subscripts of an index column that are not literals.
	Columns,
	/// The keys, URLs and audiences of an access method's JWT configuration, in the order the
	/// body reads them.
	Credentials,
	/// The arguments of an API's middleware: the API's, then each action's.
	Middleware,
	/// The backend of a bucket.
	Backend,
	/// The namespace of a `DEFAULT` config.
	ConfigNamespace,
	/// The database of a `DEFAULT` config.
	ConfigDatabase,
	/// The pending password of a user. The body reads none, so the operand has no position of
	/// its own and binds last. It binds as `NONE` and marks that the statement still holds a
	/// password; the plaintext never enters the IR.
	Password,
}

/// One step of a schema statement's binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DdlStep {
	/// `logical.ddl_guard { RefuseNoWrite }`.
	RefuseNoWrite,
	/// `logical.ddl_guard { Allow { .. } }`, whose action, resource and base [`plan`] states.
	Allow,
	/// The operands of the role, in slot order.
	Operand(DdlOperand),
	/// The statement's own op.
	Statement,
}

const W: DdlStep = DdlStep::RefuseNoWrite;
const A: DdlStep = DdlStep::Allow;
const S: DdlStep = DdlStep::Statement;
const NAME: DdlStep = DdlStep::Operand(DdlOperand::Name);
const TABLE: DdlStep = DdlStep::Operand(DdlOperand::Table);
const FIELD: DdlStep = DdlStep::Operand(DdlOperand::FieldName);
const PATH: DdlStep = DdlStep::Operand(DdlOperand::Path);
const COMMENT: DdlStep = DdlStep::Operand(DdlOperand::Comment);
const VALUE: DdlStep = DdlStep::Operand(DdlOperand::Value);
const TIMEOUT: DdlStep = DdlStep::Operand(DdlOperand::Timeout);
const BATCH: DdlStep = DdlStep::Operand(DdlOperand::Batch);
const START: DdlStep = DdlStep::Operand(DdlOperand::Start);
const GRANT: DdlStep = DdlStep::Operand(DdlOperand::GrantDuration);
const TOKEN: DdlStep = DdlStep::Operand(DdlOperand::TokenDuration);
const SESSION: DdlStep = DdlStep::Operand(DdlOperand::SessionDuration);
const COLUMNS: DdlStep = DdlStep::Operand(DdlOperand::Columns);
const CREDENTIALS: DdlStep = DdlStep::Operand(DdlOperand::Credentials);
const MIDDLEWARE: DdlStep = DdlStep::Operand(DdlOperand::Middleware);
const BACKEND: DdlStep = DdlStep::Operand(DdlOperand::Backend);
const CONFIG_NS: DdlStep = DdlStep::Operand(DdlOperand::ConfigNamespace);
const CONFIG_DB: DdlStep = DdlStep::Operand(DdlOperand::ConfigDatabase);
const PASSWORD: DdlStep = DdlStep::Operand(DdlOperand::Password);

/// The steps a statement of `form` binds, in the order its body performs them.
///
/// The order follows the lowering of each statement in `vm/lower_ddl.rs`, which is the order the
/// body takes. Three forms resolve their table name before the permission check
/// (`DEFINE TABLE`, `REMOVE TABLE`, `ALTER TABLE`); `DEFINE EVENT` resolves its names between the
/// no-write refusal and `Allow`; `DEFINE FIELD` resolves its comment, path and table before
/// `Allow`; `DEFINE USER` and `DEFINE ANALYZER` read their comment before their name; `REMOVE
/// EVENT` resolves its table before its name and `ALTER FIELD` its path before its table.
pub(crate) fn guard_order(form: DdlForm) -> &'static [DdlStep] {
	match form {
		DdlForm::DefineNamespace | DdlForm::DefineDatabase => &[W, A, NAME, COMMENT, S],
		DdlForm::DefineFunction
		| DdlForm::DefineModel
		| DdlForm::DefineModule
		| DdlForm::DefineType => &[W, A, COMMENT, S],
		DdlForm::DefineAnalyzer => &[W, A, COMMENT, NAME, S],
		DdlForm::DefineParam => &[W, A, VALUE, COMMENT, S],
		DdlForm::DefineTable => &[NAME, W, A, COMMENT, S],
		DdlForm::DefineEvent => &[W, NAME, TABLE, A, COMMENT, S],
		DdlForm::DefineField => &[W, COMMENT, FIELD, TABLE, A, S],
		DdlForm::DefineIndex => &[W, A, NAME, TABLE, COLUMNS, COMMENT, S],
		DdlForm::DefineUser => &[W, A, TOKEN, SESSION, COMMENT, NAME, PASSWORD, S],
		DdlForm::DefineAccess => &[W, A, GRANT, TOKEN, SESSION, COMMENT, CREDENTIALS, NAME, S],
		DdlForm::DefineConfig => &[W, A, CONFIG_NS, CONFIG_DB, MIDDLEWARE, S],
		DdlForm::DefineApi => &[W, A, PATH, MIDDLEWARE, COMMENT, S],
		DdlForm::DefineBucket => &[W, A, NAME, BACKEND, COMMENT, S],
		DdlForm::DefineSequence => &[W, A, NAME, TIMEOUT, BATCH, START, S],
		DdlForm::RemoveNamespace
		| DdlForm::RemoveDatabase
		| DdlForm::RemoveAnalyzer
		| DdlForm::RemoveAccess
		| DdlForm::RemoveUser
		| DdlForm::RemoveApi
		| DdlForm::RemoveBucket
		| DdlForm::RemoveSequence => &[W, A, NAME, S],
		DdlForm::RemoveFunction
		| DdlForm::RemoveParam
		| DdlForm::RemoveModel
		| DdlForm::RemoveModule
		| DdlForm::RemoveConfig
		| DdlForm::AlterNamespace
		| DdlForm::AlterFunction
		| DdlForm::AlterModule
		| DdlForm::RemoveType
		| DdlForm::AlterType
		| DdlForm::RebuildIndex => &[W, A, S],
		DdlForm::RemoveTable => &[NAME, W, A, S],
		DdlForm::RemoveEvent => &[W, A, TABLE, NAME, S],
		DdlForm::RemoveField => &[W, A, TABLE, FIELD, S],
		DdlForm::RemoveIndex | DdlForm::AlterEvent | DdlForm::AlterIndex => &[W, A, NAME, TABLE, S],
		DdlForm::AlterSystem => &[W, A, TIMEOUT, S],
		DdlForm::AlterDatabase => &[W, A, NAME, S],
		DdlForm::AlterTable => &[NAME, W, A, S],
		DdlForm::AlterApi => &[W, A, PATH, MIDDLEWARE, S],
		DdlForm::AlterSequence => &[W, A, NAME, TIMEOUT, S],
		DdlForm::AlterField => &[W, A, FIELD, TABLE, S],
		DdlForm::AlterParam => &[W, A, VALUE, S],
		DdlForm::AlterBucket | DdlForm::AlterAnalyzer | DdlForm::AlterAccess => &[W, A, NAME, S],
		DdlForm::AlterConfig => &[W, A, CONFIG_NS, CONFIG_DB, MIDDLEWARE, S],
		DdlForm::AlterUser => &[W, A, NAME, PASSWORD, S],
	}
}

/// What `statement` checks when it is planned.
///
/// A `DEFINE TABLE` forms its checks from its own clauses: the refusal of a lightweight relation's
/// clauses, the type the table takes, and the definition the statement judges against the table it
/// meets when it runs. The checks of a field statement read the catalog, which is not answered
/// while the container is bound, so a statement of another form has none and is checked by its
/// program against the catalog it meets when it runs.
pub(crate) fn checks_of(
	catalog: &dyn SemanticCatalog,
	statement: &LogicalDdlStatement,
) -> DdlChecks {
	let _ = catalog;
	match statement {
		LogicalDdlStatement::Define(define) => {
			if let DefineStatement::Table(stmt) = define.as_ref() {
				define_table_checks(stmt)
			} else {
				DdlChecks::None
			}
		}
		LogicalDdlStatement::Remove(_)
		| LogicalDdlStatement::Alter(_)
		| LogicalDdlStatement::Rebuild(_) => DdlChecks::None,
	}
}

/// The checks of a `DEFINE TABLE`, from its clauses alone. A refusal of the clauses that is no
/// refusal of a definition leaves the statement with no checks.
fn define_table_checks(stmt: &crate::expr::statements::define::DefineTableStatement) -> DdlChecks {
	use crate::catalog::TableType;
	// A view the catalog refuses is refused when the statement runs, after the authority check;
	// the rest of the definition is judged without it.
	let (view, view_refusal) =
		match stmt.view.as_ref().map(crate::catalog::view::view_to_definition).transpose() {
			Ok(view) => (view, None),
			Err(error) => match crate::exec::vm::program::FieldRefusal::of(&error) {
				Some(refusal) => (None, Some(refusal)),
				None => return DdlChecks::None,
			},
		};
	let written = written_text(&stmt.name);
	let tb = surrealdb_strand::TableName::new(written.as_deref().unwrap_or(""));
	let mut template = super::ddl_define::table_template(stmt, tb, view);
	// The definition's own clauses are judged here; what the catalog and the table's contents hold
	// of it is judged when the statement runs, against the table it meets.
	let mut definition_refusal = None;
	if matches!(&stmt.table_type, TableType::Relation(rel) if rel.lightweight)
		&& let Err(error) = crate::ddl::define::table::validate_lightweight_clauses(&mut template)
	{
		match crate::exec::vm::program::FieldRefusal::of(&error) {
			Some(refusal) => definition_refusal = Some(refusal),
			None => return DdlChecks::None,
		}
	}
	DdlChecks::DefineTable {
		view_refusal,
		definition_refusal,
		table_type: template.table_type.clone(),
		redefined: written.is_some().then(|| std::sync::Arc::new(template)),
	}
}

/// Why a statement could not be planned.
#[derive(Debug)]
pub(crate) enum PlanError {
	/// A broken invariant: the statement has a shape the planner relies on never occurring.
	Invariant(String),
	/// A statement the planner declines to cover, with the reason it states.
	Unsupported(String),
}

impl From<PlanError> for IrError {
	fn from(error: PlanError) -> Self {
		match error {
			PlanError::Invariant(detail) => invariant(&detail),
			PlanError::Unsupported(reason) => IrError::Unsupported {
				reason,
			},
		}
	}
}

/// The type every planning function answers.
pub(crate) type Planned<T> = Result<T, PlanError>;

/// Where an operand's value comes from.
#[derive(Clone, Debug)]
pub(crate) enum Src<'e> {
	/// The statement writes an expression, which the binder evaluates.
	Expr(&'e Expr),
	/// The statement writes the value itself, which binds as a literal.
	Literal(Value),
}

impl Src<'_> {
	/// Whether the operand evaluates nothing: a value the statement writes literally.
	pub(crate) fn is_written(&self) -> bool {
		match self {
			Src::Literal(_) => true,
			Src::Expr(expr) => matches!(expr, Expr::Literal(_)),
		}
	}
}

/// One operand of a planned statement.
#[derive(Clone, Debug)]
pub(crate) struct PlannedOperand<'e> {
	pub(crate) role: DdlOperand,
	pub(crate) src: Src<'e>,
}

/// What a region of a planned statement holds.
#[derive(Clone, Debug)]
pub(crate) enum Body<'e> {
	/// One expression, bound against the row the region receives.
	Clause(Cow<'e, Expr>),
	/// A stored function body: the statement list of `block`, entered with `params` as the
	/// arguments after the statement timestamp.
	Function {
		block: Box<Expr>,
		params: Vec<Strand>,
	},
	/// A statement bound in value position: the query of a view and its backfill.
	Select(Box<SelectStatement>),
}

/// One region of a planned statement.
#[derive(Clone, Debug)]
pub(crate) struct PlannedRegion<'e> {
	pub(crate) kind: RegionKind,
	pub(crate) body: Body<'e>,
}

/// A schema statement ready to bind.
#[derive(Debug)]
pub(crate) struct DdlPlan<'e> {
	pub(crate) form: DdlForm,
	/// The action, resource and base the `Allow` check states.
	pub(crate) allow: (Action, ResourceKind, Base),
	/// The refusal the planner raises for the statement before any of it runs: the statement
	/// binds as the raise alone, with no check before it.
	pub(crate) planning_refusal: Option<Refusal>,
	/// The refusal the body raises while forming the definition, which the binder raises after
	/// the checks.
	pub(crate) refusal: Option<Refusal>,
	/// The operands, indexed by slot.
	pub(crate) operands: Vec<PlannedOperand<'e>>,
	/// The regions, indexed by slot.
	pub(crate) regions: Vec<PlannedRegion<'e>>,
	pub(crate) op: LogOp,
}

/// Plans `statement`. `checks` is what the statement checks against the catalog (see
/// [`checks_of`]), and `codec` reads the definitions the statement templates.
pub(crate) fn plan<'e>(
	statement: &'e LogicalDdlStatement,
	checks: DdlChecks,
	codec: &'static Codec,
) -> Planned<DdlPlan<'e>> {
	match statement {
		LogicalDdlStatement::Define(define) => plan_define(define, checks, codec),
		LogicalDdlStatement::Remove(remove) => plan_remove(remove, checks),
		LogicalDdlStatement::Alter(alter) => plan_alter(alter, checks, codec),
		LogicalDdlStatement::Rebuild(rebuild) => plan_rebuild(rebuild),
	}
}

/// Checks that the operands of `plan` stand in the order [`guard_order`] gives its form: the
/// roles of the operands, runs of one role collapsed, are a subsequence of the operand steps.
pub(crate) fn operands_follow_table(plan: &DdlPlan<'_>) -> bool {
	let mut steps = guard_order(plan.form).iter().filter_map(|step| match step {
		DdlStep::Operand(role) => Some(*role),
		DdlStep::RefuseNoWrite | DdlStep::Allow | DdlStep::Statement => None,
	});
	let mut last = None;
	for operand in &plan.operands {
		if last == Some(operand.role) {
			continue;
		}
		last = Some(operand.role);
		if !steps.any(|role| role == operand.role) {
			return false;
		}
	}
	true
}

/// Accumulates the operands and regions of a statement in evaluation order, handing out the
/// slot of each.
#[derive(Default)]
pub(super) struct Slots<'e> {
	operands: Vec<PlannedOperand<'e>>,
	regions: Vec<PlannedRegion<'e>>,
	credentials: Vec<(OperandSlot, String)>,
}

impl<'e> Slots<'e> {
	pub(super) fn new() -> Self {
		Self::default()
	}

	fn operand(&mut self, role: DdlOperand, src: Src<'e>) -> Planned<OperandSlot> {
		let slot = OperandSlot::try_from(self.operands.len()).map_err(|_| {
			PlanError::Invariant("a statement with more than 65535 operands".into())
		})?;
		self.operands.push(PlannedOperand {
			role,
			src,
		});
		Ok(slot)
	}

	/// An operand the statement evaluates.
	pub(super) fn evaluated(&mut self, role: DdlOperand, expr: &'e Expr) -> Planned<OperandSlot> {
		self.operand(role, Src::Expr(expr))
	}

	/// An operand holding the value itself.
	pub(super) fn literal(&mut self, role: DdlOperand, value: Value) -> Planned<OperandSlot> {
		self.operand(role, Src::Literal(value))
	}

	/// A key or URL of an access method. One the statement writes as a literal that converts to
	/// text (coerced when `coerce`, else cast) binds as a `NONE` marker, and its text is held in
	/// [`AccessSecrets`], so no bound program, text form or `Debug` output prints it; any other
	/// expression is evaluated.
	pub(super) fn credential(&mut self, expr: &'e Expr, coerce: bool) -> Planned<OperandSlot> {
		let text = literal_value(expr).and_then(|value| {
			if coerce {
				value.coerce_to::<String>().ok()
			} else {
				value.cast_to::<String>().ok()
			}
		});
		match text {
			Some(text) => {
				let slot = self.literal(DdlOperand::Credentials, Value::None)?;
				self.credentials.push((slot, text));
				Ok(slot)
			}
			None => self.evaluated(DdlOperand::Credentials, expr),
		}
	}

	/// The keys and URLs [`Self::credential`] has taken as written.
	pub(super) fn take_credentials(&mut self) -> Vec<(OperandSlot, String)> {
		std::mem::take(&mut self.credentials)
	}

	/// The operand that marks a pending password: `NONE` when the statement holds one, absent
	/// otherwise. The plaintext stays in the parsed statement, where the executor derives the hash
	/// and the code from it before the statement runs, so no bound program, text form or `Debug`
	/// output carries it.
	pub(super) fn password(
		&mut self,
		pending: &Option<crate::expr::user::PendingPassword>,
	) -> Planned<Option<OperandSlot>> {
		pending.as_ref().map(|_| self.literal(DdlOperand::Password, Value::None)).transpose()
	}

	/// A name: the text of a bare identifier or a string literal, else the expression.
	pub(super) fn name(&mut self, role: DdlOperand, expr: &'e Expr) -> Planned<OperandSlot> {
		match written_text(expr) {
			Some(text) => self.literal(role, Value::String(text.into())),
			None => self.evaluated(role, expr),
		}
	}

	/// The `COMMENT` clause: `NONE` or a string literal as the value itself, else the expression.
	pub(super) fn comment(&mut self, expr: &'e Expr) -> Planned<OperandSlot> {
		match written_comment(expr) {
			Some(value) => self.literal(DdlOperand::Comment, value),
			None => self.evaluated(DdlOperand::Comment, expr),
		}
	}

	/// A field path: its canonical text when the statement writes it. A path headed by a field
	/// that has a subscript which is no literal is the text of the path with a placeholder
	/// parameter at each such subscript, followed by one operand per subscript, in the order the
	/// path holds them. Any other expression is evaluated.
	pub(super) fn field_name(&mut self, expr: &'e Expr) -> Planned<OperandSlot> {
		use crate::expr::Part;
		if let Expr::Idiom(idiom) = expr {
			if let Some(path) = canonical_field_name(idiom) {
				return self
					.literal(DdlOperand::FieldName, Value::String(path.to_raw_string().into()));
			}
			if matches!(idiom.0.first(), Some(Part::Field(_))) {
				let mut parts = Vec::with_capacity(idiom.0.len());
				let mut subscripts = Vec::new();
				for part in &idiom.0 {
					if let Part::Value(subscript) = part
						&& !matches!(subscript, Expr::Literal(_))
					{
						parts.push(Part::Value(field_hole()));
						subscripts.push(subscript);
					} else {
						parts.push(part.clone());
					}
				}
				let text = crate::expr::Idiom(parts).to_raw_string();
				let slot = self.literal(DdlOperand::FieldName, Value::String(text.into()))?;
				for subscript in subscripts {
					self.evaluated(DdlOperand::FieldName, subscript)?;
				}
				return Ok(slot);
			}
		}
		self.evaluated(DdlOperand::FieldName, expr)
	}

	/// A duration clause: a written duration or `NONE` as the value itself, else the expression.
	pub(super) fn duration(&mut self, role: DdlOperand, expr: &'e Expr) -> Planned<OperandSlot> {
		match literal_duration(expr) {
			Some(Some(duration)) => {
				self.literal(role, Value::Duration(crate::val::Duration(duration)))
			}
			Some(None) => self.literal(role, Value::None),
			None => self.evaluated(role, expr),
		}
	}

	/// A region of `kind`, bound from `body`.
	pub(super) fn region(&mut self, kind: RegionKind, body: Body<'e>) -> Planned<RegionSlot> {
		let slot = RegionSlot::try_from(self.regions.len())
			.map_err(|_| PlanError::Invariant("a statement with more than 65535 regions".into()))?;
		self.regions.push(PlannedRegion {
			kind,
			body,
		});
		Ok(slot)
	}

	/// A stored clause of `role`, held as the expression written.
	pub(super) fn stored(&mut self, role: StoredRole, expr: &'e Expr) -> Planned<RegionSlot> {
		self.region(RegionKind::StoredClause(role), Body::Clause(Cow::Borrowed(expr)))
	}

	/// A permission clause: a stored region when it holds a predicate.
	pub(super) fn permission(&mut self, permission: &'e Permission) -> Planned<PermissionShape> {
		Ok(match permission {
			Permission::None => PermissionShape::None,
			Permission::Full => PermissionShape::Full,
			Permission::Specific(expr) => {
				PermissionShape::Specific(self.stored(StoredRole::Permissions, expr)?)
			}
		})
	}

	/// The four permission clauses of a table or field, `SELECT` first.
	pub(super) fn permissions(
		&mut self,
		permissions: &'e Permissions,
	) -> Planned<PermissionsShape> {
		Ok(PermissionsShape {
			select: self.permission(&permissions.select)?,
			create: self.permission(&permissions.create)?,
			update: self.permission(&permissions.update)?,
			delete: self.permission(&permissions.delete)?,
		})
	}

	/// An API configuration: the arguments of its middleware as operands of the `Middleware`
	/// role, then its permission.
	pub(super) fn api_config(&mut self, config: &'e ApiConfig) -> Planned<ApiConfigShape> {
		let mut middleware = Vec::with_capacity(config.middleware.len());
		for entry in &config.middleware {
			let mut args = Vec::with_capacity(entry.args.len());
			for arg in &entry.args {
				args.push(self.evaluated(DdlOperand::Middleware, arg)?);
			}
			middleware.push(MiddlewareShape {
				name: entry.name.clone(),
				args: args.into_boxed_slice(),
			});
		}
		Ok(ApiConfigShape {
			middleware: middleware.into_boxed_slice(),
			permissions: self.permission(&config.permissions)?,
		})
	}

	/// Finishes the plan of a statement of `form` whose op is `op`.
	pub(super) fn finish(
		self,
		form: DdlForm,
		allow: (Action, ResourceKind, Base),
		op: LogOp,
	) -> DdlPlan<'e> {
		DdlPlan {
			form,
			allow,
			planning_refusal: None,
			refusal: None,
			operands: self.operands,
			regions: self.regions,
			op,
		}
	}
}

// ---------------------------------------------------------------------------------------------
// Written clauses and stored encodings
// ---------------------------------------------------------------------------------------------

/// The name `expr` gives by being written, an identifier or a string literal, or `None` when it
/// gives one only by being evaluated.
pub(super) fn written_text(expr: &Expr) -> Option<String> {
	if let Expr::Literal(Literal::String(text)) = expr {
		Some(text.as_str().to_owned())
	} else {
		crate::ddl::scalar::written_name(expr)
	}
}

/// The comment `expr` gives by being written, a string or `NONE`, or `None` when it gives one
/// only by being evaluated.
pub(super) fn written_comment(expr: &Expr) -> Option<Value> {
	match expr {
		Expr::Literal(Literal::None) => Some(Value::None),
		Expr::Literal(Literal::String(text)) => Some(Value::String(text.clone())),
		Expr::Literal(
			Literal::Null
			| Literal::UnboundedRange
			| Literal::Bool(_)
			| Literal::Float(_)
			| Literal::Integer(_)
			| Literal::Decimal(_)
			| Literal::BigDecimal(_)
			| Literal::I8(_)
			| Literal::U8(_)
			| Literal::I16(_)
			| Literal::U16(_)
			| Literal::I32(_)
			| Literal::U32(_)
			| Literal::U64(_)
			| Literal::F16(_)
			| Literal::F32(_)
			| Literal::Bytes(_)
			| Literal::Regex(_)
			| Literal::RecordId(_)
			| Literal::Array(_)
			| Literal::Set(_)
			| Literal::Object(_)
			| Literal::Duration(_)
			| Literal::Datetime(_)
			| Literal::Date(_)
			| Literal::Time(_)
			| Literal::Timestamp(_)
			| Literal::Interval(_)
			| Literal::Uuid(_)
			| Literal::Geometry(_)
			| Literal::File(_),
		)
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
		} => None,
	}
}

/// The comment a definition is formed with: the written string, or none where the comment is
/// evaluated or `NONE`.
pub(super) fn comment_text(expr: &Expr) -> Option<String> {
	if let Some(Value::String(text)) = written_comment(expr) {
		Some(text.as_str().to_owned())
	} else {
		None
	}
}

/// A duration clause written as a duration (`Some(Some(_))`) or as `NONE` (`Some(None)`); `None`
/// for one that is evaluated.
pub(super) fn literal_duration(expr: &Expr) -> Option<Option<Duration>> {
	if let Expr::Literal(Literal::None) = expr {
		Some(None)
	} else if let Expr::Literal(Literal::Duration(duration)) = expr {
		Some(Some(duration.0))
	} else {
		None
	}
}

/// A clause's value when it is written as a literal; `None` when it is evaluated.
pub(super) fn literal_value(expr: &Expr) -> Option<Value> {
	if let Expr::Literal(literal) = expr {
		literal.as_static_value()
	} else {
		None
	}
}

/// A stored definition as the VM's codec reads it.
///
/// `codec` is the one of the entry the statement is bound for: a kind it leaves out is not read.
pub(super) fn stored_value<T: crate::key::KVValue>(
	codec: &'static Codec,
	value: &T,
	ty: &'static str,
) -> Planned<Value> {
	decode_stored(codec, value.kv_encode_value(), ty)
}

/// [`stored_value`] over the bytes the definition encoded to.
fn decode_stored(
	codec: &'static Codec,
	bytes: anyhow::Result<Vec<u8>>,
	ty: &'static str,
) -> Planned<Value> {
	let bytes = bytes.map_err(|_| PlanError::Invariant(format!("a {ty} did not encode")))?;
	crate::exec::vm::codec::decode(codec, ty, &bytes)
		.map_err(|_| PlanError::Invariant(format!("a {ty} the codec does not read")))
}

/// A template of the stored type `ty` whose fields are final except `holes`.
pub(super) fn template(
	ty: &'static str,
	value: Value,
	holes: &[TemplateHole],
) -> DefinitionTemplate {
	DefinitionTemplate {
		ty,
		value,
		holes: holes.iter().copied().collect(),
	}
}

/// A template of an alteration of the stored type `ty`: `value` encodes the definition the
/// statement forms, and `fields` are the fields of it that replace the altered definition's.
pub(super) fn alter_template(ty: &'static str, value: Value, fields: &[&str]) -> AlterTemplate {
	AlterTemplate {
		ty,
		value,
		fields: fields.iter().map(|field| Strand::new(*field)).collect(),
	}
}

/// The refusal the body raises for a user-defined function argument that takes a
/// protected name, if one does.
///
/// SECURITY: a function body reads its argument of a protected name
/// (`$auth`, `$access`, `$token`, `$session`) in place of the session's value,
/// so a permission predicate judged beneath such a body would be handed a
/// caller-chosen identity. `DEFINE FUNCTION` refuses the argument, the way a
/// `FOR` variable and a closure parameter are refused (GHSA-ffvm-mqh3-7986).
pub(super) fn protected_arg_refusal<K>(args: &[(String, K)]) -> Option<Refusal> {
	args.iter().find(|(name, _)| surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name.as_str())).map(
		|(name, _)| Refusal::ProtectedParam {
			name: name.clone(),
		},
	)
}

/// The refusal the body raises for an alias that is no GraphQL name, if it raises one.
pub(super) fn alias_refusal(alias: &Option<String>, kind: &str) -> Option<Refusal> {
	let error = crate::ddl::define::validate_graphql_alias(alias, kind).err()?;
	match error.downcast_ref::<crate::exec::Error>() {
		Some(crate::exec::Error::InvalidStatement(message)) => Some(Refusal::InvalidStatement {
			message: message.clone(),
		}),
		_ => None,
	}
}

/// The placeholder [`Slots::field_name`] writes in the text of a path at each subscript whose
/// value an operand holds. The text of a path holds no other parameter as a subscript, so a
/// reader counts the placeholders to find how many operands follow the path's.
fn field_hole() -> Expr {
	Expr::Param(crate::expr::Param::from("hole".to_owned()))
}

/// An idiom's parts as a stored field path: a string literal subscript names a field and an
/// integer literal subscript stays. `None` for a path that needs evaluating.
pub(super) fn canonical_field_name(idiom: &crate::expr::Idiom) -> Option<crate::expr::Idiom> {
	use crate::expr::Part;
	let mut out = Vec::with_capacity(idiom.0.len());
	for part in &idiom.0 {
		match part {
			Part::Field(_) | Part::All | Part::First | Part::Last | Part::Flatten => {
				out.push(part.clone())
			}
			Part::Value(Expr::Literal(Literal::String(text))) => {
				out.push(Part::Field(text.clone()))
			}
			Part::Value(Expr::Literal(Literal::Integer(i))) => {
				out.push(Part::Value(Expr::Literal(Literal::Integer(*i))))
			}
			Part::Where(_)
			| Part::Lookup(_)
			| Part::Value(_)
			| Part::Start(_)
			| Part::Method(..)
			| Part::Destructure(_)
			| Part::Optional
			| Part::Recurse(..)
			| Part::Doc
			| Part::RepeatRecurse => return None,
		}
	}
	Some(crate::expr::Idiom(out))
}

/// The constant the operand `slot` of a statement's op holds, when a literal defines it.
pub(crate) fn literal_operand<'a>(
	ir: &'a Ir,
	operands: &[ValueId],
	slot: OperandSlot,
) -> Result<Option<&'a Value>, IrError> {
	let value = operands
		.get(usize::from(slot))
		.copied()
		.ok_or(IrError::Stale("ir::logical::ddl: operand slot"))?;
	let data = ir.value(value).ok_or(IrError::Stale("ir::logical::ddl: value"))?;
	let ValueDef::OpResult {
		op,
		index: 0,
	} = data.def
	else {
		return Ok(None);
	};
	match &ir.op(op).ok_or(IrError::Stale("ir::logical::ddl: op"))?.kind {
		OpKind::Expr(ExprOp::Literal(value)) => Ok(Some(value)),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => Ok(None),
	}
}

/// The path the field name operand `slot` writes, and the slots of the operands that follow it
/// for the subscripts it leaves to be evaluated, in the order the path holds them. `None` for an
/// operand that is no written text.
pub(crate) fn written_path(
	ir: &Ir,
	operands: &[ValueId],
	slot: OperandSlot,
) -> Result<Option<(crate::expr::Idiom, Vec<OperandSlot>)>, IrError> {
	use std::str::FromStr as _;
	let Some(Value::String(text)) = literal_operand(ir, operands, slot)? else {
		return Ok(None);
	};
	let idiom = crate::expr::Idiom::from_str(text.as_str()).map_err(|_| {
		IrError::Stale("ir::logical::ddl: a written field path that does not parse")
	})?;
	let evaluated = idiom
		.0
		.iter()
		.filter(|part| matches!(part, crate::expr::Part::Value(Expr::Param(_))))
		.count();
	let mut holes = Vec::with_capacity(evaluated);
	for offset in 1..=evaluated {
		let hole = usize::from(slot) + offset;
		if hole >= operands.len() {
			return Err(IrError::Stale(
				"ir::logical::ddl: a field path with fewer subscript operands than it holds",
			));
		}
		holes.push(OperandSlot::try_from(hole).map_err(|_| {
			IrError::Stale("ir::logical::ddl: a subscript operand out of the slot range")
		})?);
	}
	Ok(Some((idiom, holes)))
}

/// The field path and the table a statement writes, when both are written and the path holds
/// nothing the statement evaluates and no step beyond a field, `*`, `.first()`, `.last()`, `...`
/// or an integer subscript; the host resolves any other path.
pub(crate) fn written_field(
	ir: &Ir,
	operands: &[ValueId],
	name: OperandSlot,
	table: OperandSlot,
) -> Result<Option<(crate::expr::Idiom, surrealdb_strand::TableName)>, IrError> {
	use crate::expr::Part;
	let Some((path, holes)) = written_path(ir, operands, name)? else {
		return Ok(None);
	};
	let Some(Value::String(table)) = literal_operand(ir, operands, table)? else {
		return Ok(None);
	};
	let stored = path.0.iter().all(|part| {
		matches!(
			part,
			Part::Field(_)
				| Part::All | Part::First
				| Part::Last | Part::Flatten
				| Part::Value(Expr::Literal(Literal::Integer(_)))
		)
	});
	if !holes.is_empty() || !stored {
		return Ok(None);
	}
	Ok(Some((path, surrealdb_strand::TableName::new(table.as_str()))))
}

/// The field definition a stored definition `value` holds.
pub(crate) fn field_definition_of(
	value: &Value,
) -> Result<crate::catalog::FieldDefinition, IrError> {
	use crate::catalog::FromStored as _;
	use crate::key::KVValue as _;
	// A field definition is a kind every entry serves, so it is written through its own row's
	// function and names no catalog table.
	let bytes = crate::catalog::stored::vtable::encode_at::<crate::catalog::StoredFieldDefinition>(
		value,
		&surrealdb_kvs::EncodeContext::LEGACY,
	)
	.map(|encoded| encoded.bytes)
	.map_err(|_| IrError::Stale("ir::logical::ddl: a field template that does not encode"))?;
	let stored = crate::catalog::StoredFieldDefinition::kv_decode_value(&bytes, ())
		.map_err(|_| IrError::Stale("ir::logical::ddl: a field template that does not decode"))?;
	crate::catalog::FieldDefinition::from_stored(&stored)
		.map_err(|_| IrError::Stale("ir::logical::ddl: a field template that does not compile"))
}

/// `template` with its `name` and `table` set to the ones the statement writes.
pub(crate) fn written_field_template(template: &Value, name: &str, table: &str) -> Value {
	let mut template = template.clone();
	if let Value::Object(object) = &mut template {
		object.insert("name".to_owned(), Value::String(name.into()));
		object.insert("table".to_owned(), Value::String(table.into()));
	}
	template
}
