//! Statement AST representations for PostgreSQL.

use crate::ast::expr::{Expr, QualifiedName, TypeName};
pub use crate::ast::expr::{NullsOrder, OrderByItem, OrderDirection, WindowSpec};
use crate::span::Span;

/// A target item in a SELECT projection list (e.g. `col AS alias`, `*`, `tbl.*`).
#[derive(Debug, Clone, PartialEq)]
pub struct ResTarget {
	/// Expression being selected.
	pub expr: Expr,
	/// Optional column alias (`AS name`).
	pub alias: Option<String>,
	/// Span of the target expression and alias.
	pub span: Span,
}

/// Table alias with optional column aliases (e.g. `AS u(id, name)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAlias {
	/// Table alias name.
	pub name: String,
	/// Optional explicit column alias list.
	pub columns: Vec<String>,
	/// Span of the alias.
	pub span: Span,
}

/// A `WITH` clause (Common Table Expression list).
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
	/// True if `WITH RECURSIVE`.
	pub recursive: bool,
	/// CTE definitions.
	pub ctes: Vec<CommonTableExpr>,
	/// Source span.
	pub span: Span,
}

/// A single Common Table Expression (CTE).
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
	/// CTE name.
	pub name: String,
	/// Optional column alias names: `name(c1, c2, ...)`.
	pub columns: Vec<String>,
	/// Optional `MATERIALIZED` (Some(true)) or `NOT MATERIALIZED` (Some(false)).
	pub materialized: Option<bool>,
	/// The query definition.
	pub query: Box<Statement>,
	/// Source span.
	pub span: Span,
}

/// Named window definition in a `SELECT` statement (`WINDOW name AS (spec)`).
#[derive(Debug, Clone, PartialEq)]
pub struct WindowDef {
	/// Window name.
	pub name: String,
	/// Window specification.
	pub spec: WindowSpec,
	/// Source span.
	pub span: Span,
}

/// Locking strength in `FOR UPDATE`, `FOR SHARE`, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockingStrength {
	/// `FOR UPDATE`.
	Update,
	/// `FOR NO KEY UPDATE`.
	NoKeyUpdate,
	/// `FOR SHARE`.
	Share,
	/// `FOR KEY SHARE`.
	KeyShare,
}

/// Locking wait policy (`NOWAIT`, `SKIP LOCKED`, or default blocking).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockingWaitPolicy {
	/// Wait until lock is available.
	Block,
	/// Fail immediately if locked (`NOWAIT`).
	NoWait,
	/// Skip locked rows (`SKIP LOCKED`).
	SkipLocked,
}

/// A `FOR UPDATE / SHARE` locking clause.
#[derive(Debug, Clone, PartialEq)]
pub struct LockingClause {
	/// Locking strength (`UPDATE`, `SHARE`, etc.).
	pub strength: LockingStrength,
	/// Optional table names: `OF tbl1, tbl2`.
	pub tables: Vec<QualifiedName>,
	/// Wait policy (`NOWAIT`, `SKIP LOCKED`, or default).
	pub wait_policy: LockingWaitPolicy,
	/// Source span.
	pub span: Span,
}

/// Join type for table references.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
	/// Inner join (`[INNER] JOIN`).
	Inner,
	/// Left outer join (`LEFT [OUTER] JOIN`).
	Left,
	/// Right outer join (`RIGHT [OUTER] JOIN`).
	Right,
	/// Full outer join (`FULL [OUTER] JOIN`).
	Full,
	/// Cross join (`CROSS JOIN`).
	Cross,
}

/// Join condition in a table join.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
	/// `ON <expr>` condition.
	On(Expr),
	/// `USING (col1, col2)` list.
	Using(Vec<String>),
}

/// A reference to a table, subquery, or join in a `FROM` clause.
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
	/// Simple table reference (e.g. `users AS u`).
	Table {
		/// Name of table.
		name: QualifiedName,
		/// Optional alias.
		alias: Option<TableAlias>,
		/// Source span.
		span: Span,
	},
	/// A single function expression in FROM (`func_table`), optionally with ordinality.
	Function {
		/// Function expression; window calls are excluded from a table factor.
		function: Box<Expr>,
		/// Optional table and column aliases.
		alias: Option<TableAlias>,
		/// Append a one-based ordinality column.
		with_ordinality: bool,
		/// Source span.
		span: Span,
	},
	/// Subquery in `FROM` clause (e.g. `(SELECT * FROM t) AS sub`).
	Subquery {
		/// Whether the subquery may read the relations to its left (`LATERAL`).
		lateral: bool,
		/// Subquery statement.
		query: Box<SelectStmt>,
		/// Table alias (required in PostgreSQL).
		alias: TableAlias,
		/// Source span.
		span: Span,
	},
	/// Joined relation (e.g. `t1 JOIN t2 ON t1.id = t2.id`).
	Join {
		/// Left table reference.
		left: Box<TableRef>,
		/// Join type.
		join_type: JoinType,
		/// Right table reference.
		right: Box<TableRef>,
		/// Join condition (`ON` or `USING`).
		condition: Option<JoinCondition>,
		/// Source span.
		span: Span,
	},
	/// SQL:2023 Part 16 GRAPH_TABLE reference.
	GraphTable {
		/// Graph name.
		graph_name: QualifiedName,
		/// Pattern to match.
		pattern: crate::ast::graph::GraphPattern,
		/// Columns to project.
		columns: Vec<crate::ast::graph::GraphColumnDef>,
		/// Optional table alias.
		alias: Option<TableAlias>,
		/// Source span.
		span: Span,
	},
	/// SQL/JSON JSON_TABLE reference.
	JsonTable(crate::ast::json::JsonTable),
}

/// Set operation kind (e.g. `UNION`, `INTERSECT`, `EXCEPT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpKind {
	/// Union of sets.
	Union,
	/// Intersection of sets.
	Intersect,
	/// Difference of sets.
	Except,
}

/// A set operation chaining two SELECT statements.
#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
	/// The operation kind.
	pub op: SetOpKind,
	/// True if `ALL` was specified (preserving duplicates).
	pub all: bool,
	/// Right-hand query.
	pub right: Box<SelectStmt>,
}

/// Distinct specification in a SELECT query.
#[derive(Debug, Clone, PartialEq)]
pub enum DistinctClause {
	/// Standard `SELECT DISTINCT`.
	All,
	/// `SELECT DISTINCT ON (expr, ...)`.
	On(Vec<Expr>),
}

/// A PostgreSQL `SELECT` statement AST node.
///
/// Implements `gram.y: SelectStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
	/// Optional `WITH` clause.
	pub with: Option<WithClause>,
	/// Optional `DISTINCT` or `DISTINCT ON` clause.
	pub distinct: Option<DistinctClause>,
	/// Target projection list (`SELECT target1, target2, ...`).
	pub targets: Vec<ResTarget>,
	/// `FROM` clause references.
	pub from: Vec<TableRef>,
	/// `WHERE` clause expression.
	pub where_clause: Option<Expr>,
	/// `GROUP BY` expressions.
	pub group_by: Vec<Expr>,
	/// `HAVING` condition.
	pub having: Option<Expr>,
	/// `WINDOW` definitions (`WINDOW name AS (...)`).
	pub window: Vec<WindowDef>,
	/// `ORDER BY` specifications.
	pub order_by: Vec<OrderByItem>,
	/// `LIMIT` expression.
	pub limit: Option<Expr>,
	/// `OFFSET` expression.
	pub offset: Option<Expr>,
	/// `FOR UPDATE / SHARE` locking clauses.
	pub locking: Vec<LockingClause>,
	/// Optional chained set operation (`UNION`, etc.).
	pub set_op: Option<SetOperation>,
	/// Statement span.
	pub span: Span,
}

/// A column assignment in an `UPDATE` statement (`col = expr`).
#[derive(Debug, Clone, PartialEq)]
pub struct SetClause {
	/// Column name being updated.
	pub column: String,
	/// New value expression.
	pub value: Expr,
	/// Source span.
	pub span: Span,
}

/// Conflict action in `ON CONFLICT`.
#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictAction {
	/// `DO NOTHING`.
	DoNothing,
	/// `DO UPDATE SET col = expr, ...`.
	DoUpdate(Vec<SetClause>),
}

/// `ON CONFLICT` clause in an `INSERT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflictClause {
	/// Target columns for conflict inference (e.g. `(id)`).
	pub target: Option<Vec<String>>,
	/// Predicate of a partial unique index that the conflict target must infer
	/// (`ON CONFLICT (col) WHERE pred`).
	pub target_where: Option<Expr>,
	/// Action to take on conflict.
	pub action: OnConflictAction,
	/// Condition of `DO UPDATE SET ... WHERE cond`: the existing row is updated only when it is
	/// true. Always `None` for `DO NOTHING`.
	pub where_clause: Option<Expr>,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `INSERT` statement AST node.
///
/// Implements `gram.y: InsertStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
	/// Optional `WITH` clause.
	pub with: Option<WithClause>,
	/// Target table name.
	pub relation: QualifiedName,
	/// Alias of the target table (`INSERT INTO t AS a`).
	pub alias: Option<String>,
	/// Explicit column list (e.g. `INSERT INTO t (a, b)`).
	pub columns: Vec<String>,
	/// Values rows (`VALUES (...), (...)`).
	pub values: Option<Vec<Vec<Expr>>>,
	/// Subquery providing values (`INSERT INTO t SELECT ...`).
	pub query: Option<Box<SelectStmt>>,
	/// Optional `ON CONFLICT` clause.
	pub on_conflict: Option<OnConflictClause>,
	/// Optional `RETURNING` projection list.
	pub returning: Option<Vec<ResTarget>>,
	/// Statement span.
	pub span: Span,
}

/// A PostgreSQL `UPDATE` statement AST node.
///
/// Implements `gram.y: UpdateStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
	/// Optional `WITH` clause.
	pub with: Option<WithClause>,
	/// Target table name.
	pub relation: QualifiedName,
	/// Assignments (`SET col1 = val1, col2 = val2`).
	pub assignments: Vec<SetClause>,
	/// Optional `FROM` clause for table joins.
	pub from: Option<Vec<TableRef>>,
	/// Optional `WHERE` condition.
	pub where_clause: Option<Expr>,
	/// Optional `RETURNING` projection list.
	pub returning: Option<Vec<ResTarget>>,
	/// Statement span.
	pub span: Span,
}

/// A PostgreSQL `DELETE` statement AST node.
///
/// Implements `gram.y: DeleteStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
	/// Optional `WITH` clause.
	pub with: Option<WithClause>,
	/// Target table name.
	pub relation: QualifiedName,
	/// Optional `USING` clause.
	pub using: Option<Vec<TableRef>>,
	/// Optional `WHERE` condition.
	pub where_clause: Option<Expr>,
	/// Optional `RETURNING` projection list.
	pub returning: Option<Vec<ResTarget>>,
	/// Statement span.
	pub span: Span,
}

/// What a foreign key does to a referencing row when the row it references is deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReferentialAction {
	/// `NO ACTION`, the default.
	#[default]
	NoAction,
	/// `RESTRICT`.
	Restrict,
	/// `CASCADE`.
	Cascade,
	/// `SET NULL`.
	SetNull,
	/// `SET DEFAULT`.
	SetDefault,
}

impl ReferentialAction {
	/// The action as the SQL text writes it.
	pub const fn sql(self) -> &'static str {
		match self {
			Self::NoAction => "NO ACTION",
			Self::Restrict => "RESTRICT",
			Self::Cascade => "CASCADE",
			Self::SetNull => "SET NULL",
			Self::SetDefault => "SET DEFAULT",
		}
	}
}

/// Column constraint in `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
	/// `PRIMARY KEY`.
	PrimaryKey,
	/// `NOT NULL`.
	NotNull,
	/// `NULL`.
	Null,
	/// `DEFAULT <expr>`.
	Default(Expr),
	/// `UNIQUE`.
	Unique,
	/// `REFERENCES table(col) [ON DELETE action]`.
	References {
		/// Referenced table name.
		table: QualifiedName,
		/// Referenced column name.
		column: Option<String>,
		/// What deleting the referenced row does to this one.
		on_delete: ReferentialAction,
	},
}

/// Column definition in `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
	/// Column name.
	pub name: String,
	/// Column data type.
	pub data_type: TypeName,
	/// Constraints on the column.
	pub constraints: Vec<ColumnConstraint>,
	/// Source span.
	pub span: Span,
}

/// Table-level constraint in `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableConstraint {
	/// `PRIMARY KEY (col1, col2)`.
	PrimaryKey(Vec<String>),
	/// `UNIQUE (col1, col2)`.
	Unique(Vec<String>),
	/// `FOREIGN KEY (cols) REFERENCES ref_table (ref_cols) [ON DELETE action]`.
	ForeignKey {
		/// Local columns.
		columns: Vec<String>,
		/// Foreign table.
		ref_table: QualifiedName,
		/// Foreign columns.
		ref_columns: Vec<String>,
		/// What deleting the referenced row does to a referencing row.
		on_delete: ReferentialAction,
	},
}

/// A PostgreSQL `CREATE TABLE` statement AST node.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
	/// Table name.
	pub name: QualifiedName,
	/// True if `IF NOT EXISTS` was specified.
	pub if_not_exists: bool,
	/// True if `TEMPORARY` or `TEMP` table.
	pub temporary: bool,
	/// Column definitions.
	pub columns: Vec<ColumnDef>,
	/// Table-level constraints.
	pub constraints: Vec<TableConstraint>,
	/// Statement span.
	pub span: Span,
}

/// Object type for `DROP` statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropObjectType {
	/// Table object.
	Table,
	/// Index object.
	Index,
	/// View object.
	View,
	/// Schema object.
	Schema,
	/// Type object.
	Type,
	/// Domain object.
	Domain,
}

/// A PostgreSQL `DROP` statement AST node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropStmt {
	/// Type of object being dropped.
	pub object_type: DropObjectType,
	/// True if `IF EXISTS` was specified.
	pub if_exists: bool,
	/// Names of objects to drop.
	pub names: Vec<QualifiedName>,
	/// True if `CASCADE` was specified (vs `RESTRICT`).
	pub cascade: bool,
	/// Statement span.
	pub span: Span,
}

/// The isolation level of a PostgreSQL `BEGIN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
	ReadUncommitted,
	ReadCommitted,
	RepeatableRead,
	Serializable,
}

/// A PostgreSQL transaction management statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionStmt {
	/// `BEGIN [TRANSACTION] [ISOLATION LEVEL l] [READ ONLY | READ WRITE]
	/// [[NOT] DEFERRABLE]`.
	Begin {
		read_only: bool,
		isolation: Option<IsolationLevel>,
		span: Span,
	},
	/// `COMMIT [TRANSACTION]`.
	Commit {
		span: Span,
	},
	/// `ROLLBACK [TRANSACTION]`.
	Rollback {
		span: Span,
	},
	/// `SAVEPOINT <name>`.
	Savepoint {
		name: String,
		span: Span,
	},
	/// `RELEASE SAVEPOINT <name>`.
	ReleaseSavepoint {
		name: String,
		span: Span,
	},
	/// `ROLLBACK TO SAVEPOINT <name>`.
	RollbackToSavepoint {
		name: String,
		span: Span,
	},
}

/// A PostgreSQL `SET [SESSION | LOCAL] <name> = <value>` or `SET <name> TO <value>` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetStmt {
	/// Parameter name.
	pub name: String,
	/// Parameter value.
	pub value: String,
	/// Whether this is a transaction-local setting (`SET LOCAL`).
	pub is_local: bool,
	/// Statement span.
	pub span: Span,
}

/// A PostgreSQL `SHOW <name>` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowStmt {
	/// Parameter name.
	pub name: String,
	/// Statement span.
	pub span: Span,
}

/// A PostgreSQL `RESET <name>` or `RESET ALL` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetStmt {
	/// Parameter name, or `ALL`.
	pub name: String,
	/// Statement span.
	pub span: Span,
}

/// A PostgreSQL `EXPLAIN [options] <stmt>` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStmt {
	/// Options specified in parentheses (e.g. `ANALYZE`, `VERBOSE`).
	pub options: Vec<String>,
	/// Query being explained.
	pub statement: Box<Statement>,
	/// Statement span.
	pub span: Span,
}

/// A PostgreSQL `CREATE INDEX` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
	/// `UNIQUE` index.
	pub unique: bool,
	/// `CONCURRENTLY` index creation.
	pub concurrently: bool,
	/// `IF NOT EXISTS` clause.
	pub if_not_exists: bool,
	/// Index name (optional in PG).
	pub name: Option<String>,
	/// Target table name.
	pub table: QualifiedName,
	/// Access method (`btree`, `hash`, `gin`, `gist`, etc.).
	pub method: Option<String>,
	/// Indexed columns or expressions.
	pub columns: Vec<IndexElem>,
	/// Storage parameters of the `WITH (...)` clause, in the order written.
	pub options: Vec<RelOption>,
	/// Partial index `WHERE` predicate.
	pub predicate: Option<Expr>,
	/// Source span.
	pub span: Span,
}

/// A storage parameter of a `WITH (name = value)` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct RelOption {
	/// Parameter name, folded to lower case.
	pub name: String,
	/// Parameter value: the literal as written, or `None` for a bare name (`WITH (fillfactor)`).
	pub value: Option<Expr>,
	/// Source span.
	pub span: Span,
}

/// An element in an index definition (column name or expression with sort options).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexElem {
	/// Column or expression.
	pub expr: Expr,
	/// Operator class (`vector_l2_ops`), possibly schema-qualified.
	pub opclass: Option<QualifiedName>,
	/// Sort direction.
	pub direction: Option<OrderDirection>,
	/// Nulls ordering.
	pub nulls: Option<NullsOrder>,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `CREATE VIEW` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt {
	/// `OR REPLACE`.
	pub or_replace: bool,
	/// `TEMP` or `TEMPORARY`.
	pub temporary: bool,
	/// `RECURSIVE`.
	pub recursive: bool,
	/// View name.
	pub name: QualifiedName,
	/// Optional column aliases.
	pub columns: Vec<String>,
	/// Query defining the view.
	pub query: Box<SelectStmt>,
	/// Source span.
	pub span: Span,
}

/// Action in an `ALTER TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
	/// `ADD [COLUMN] [IF NOT EXISTS] col_def`.
	AddColumn {
		if_not_exists: bool,
		column: ColumnDef,
	},
	/// `DROP [COLUMN] [IF EXISTS] col_name [CASCADE | RESTRICT]`.
	DropColumn {
		if_exists: bool,
		name: String,
		cascade: bool,
	},
	/// `ALTER [COLUMN] col_name TYPE data_type`.
	AlterColumnType {
		name: String,
		data_type: TypeName,
	},
	/// `ALTER [COLUMN] col_name SET DEFAULT expr`.
	SetDefault {
		column: String,
		default: Expr,
	},
	/// `ALTER [COLUMN] col_name DROP DEFAULT`.
	DropDefault {
		column: String,
	},
	/// `ALTER [COLUMN] col_name SET NOT NULL`.
	SetNotNull {
		column: String,
	},
	/// `ALTER [COLUMN] col_name DROP NOT NULL`.
	DropNotNull {
		column: String,
	},
	/// `ADD table_constraint`.
	AddConstraint(TableConstraint),
	/// `DROP CONSTRAINT [IF EXISTS] name [CASCADE | RESTRICT]`.
	DropConstraint {
		if_exists: bool,
		name: String,
		cascade: bool,
	},
	/// `RENAME [COLUMN] old_name TO new_name`.
	RenameColumn {
		old_name: String,
		new_name: String,
	},
	/// `RENAME TO new_table_name`.
	RenameTable {
		new_name: String,
	},
}

/// A PostgreSQL `ALTER TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStmt {
	/// `IF EXISTS`.
	pub if_exists: bool,
	/// Target table name.
	pub name: QualifiedName,
	/// Alter actions.
	pub actions: Vec<AlterTableAction>,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `CREATE TYPE name AS ENUM ('a', 'b', ...)` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTypeStmt {
	/// Type name.
	pub name: QualifiedName,
	/// The labels in declaration order.
	pub labels: Vec<String>,
	/// Source span.
	pub span: Span,
}

/// Where `ALTER TYPE ... ADD VALUE` places a new label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnumValuePlacement {
	/// `BEFORE 'label'`.
	Before(String),
	/// `AFTER 'label'`.
	After(String),
}

/// The change an `ALTER TYPE` makes to an enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTypeAction {
	/// `ADD VALUE [IF NOT EXISTS] 'label' [BEFORE 'x' | AFTER 'x']`.
	AddValue {
		/// `IF NOT EXISTS`.
		if_not_exists: bool,
		/// The new label.
		value: String,
		/// Where the label goes; last when absent.
		placement: Option<EnumValuePlacement>,
	},
	/// `RENAME VALUE 'a' TO 'b'`.
	RenameValue {
		/// The label renamed.
		from: String,
		/// Its new name.
		to: String,
	},
}

/// A PostgreSQL `ALTER TYPE` statement over an enum.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTypeStmt {
	/// Type name.
	pub name: QualifiedName,
	/// The change.
	pub action: AlterTypeAction,
	/// Source span.
	pub span: Span,
}

/// A constraint of a `CREATE DOMAIN` statement or an `ALTER DOMAIN ... ADD` action.
#[derive(Debug, Clone, PartialEq)]
pub struct DomainConstraint {
	/// The name given by `CONSTRAINT name`.
	pub name: Option<String>,
	/// The constraint itself.
	pub kind: DomainConstraintKind,
	/// Source span.
	pub span: Span,
}

/// The kind of a domain constraint.
#[derive(Debug, Clone, PartialEq)]
pub enum DomainConstraintKind {
	/// `NOT NULL`.
	NotNull,
	/// `NULL`, which allows the null value and is the default.
	Null,
	/// `CHECK (expr)`, where `VALUE` names the value being checked.
	Check(Expr),
	/// `DEFAULT expr`, the value a column of the domain takes when none is given.
	Default(Expr),
}

/// A PostgreSQL `CREATE DOMAIN name [AS] type [constraint ...]` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateDomainStmt {
	/// Domain name.
	pub name: QualifiedName,
	/// The base type the domain restricts.
	pub data_type: TypeName,
	/// The `DEFAULT`, `NOT NULL`, `NULL` and `CHECK` elements in declaration order.
	pub constraints: Vec<DomainConstraint>,
	/// Source span.
	pub span: Span,
}

/// An attribute of a composite type: `name type`.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositeAttribute {
	/// Attribute name.
	pub name: String,
	/// Attribute type.
	pub data_type: TypeName,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `CREATE TYPE name AS (attribute type, ...)` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateCompositeTypeStmt {
	/// Type name.
	pub name: QualifiedName,
	/// The attributes in declaration order; empty for `AS ()`.
	pub attributes: Vec<CompositeAttribute>,
	/// Source span.
	pub span: Span,
}

/// The change an `ALTER DOMAIN` statement makes to a domain.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterDomainAction {
	/// `SET DEFAULT expr`.
	SetDefault(Expr),
	/// `DROP DEFAULT`.
	DropDefault,
	/// `SET NOT NULL`.
	SetNotNull,
	/// `DROP NOT NULL`.
	DropNotNull,
	/// `ADD [CONSTRAINT name] CHECK (expr)`.
	AddConstraint {
		/// The constraint name, when given.
		name: Option<String>,
		/// The checked expression.
		check: Expr,
	},
	/// `DROP CONSTRAINT [IF EXISTS] name`. `CASCADE` and `RESTRICT` are accepted and not kept,
	/// because a domain constraint has no dependent objects.
	DropConstraint {
		/// The constraint dropped.
		name: String,
		/// `IF EXISTS`.
		if_exists: bool,
	},
	/// `VALIDATE CONSTRAINT name`.
	ValidateConstraint {
		/// The constraint validated.
		name: String,
	},
}

/// A PostgreSQL `ALTER DOMAIN name <action>` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterDomainStmt {
	/// Domain name.
	pub name: QualifiedName,
	/// The change.
	pub action: AlterDomainAction,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `CREATE SCHEMA` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchemaStmt {
	/// `IF NOT EXISTS`.
	pub if_not_exists: bool,
	/// Schema name.
	pub name: String,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `CREATE EXTENSION` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateExtensionStmt {
	/// `IF NOT EXISTS`.
	pub if_not_exists: bool,
	/// Extension name, folded to lower case.
	pub name: String,
	/// `SCHEMA` option.
	pub schema: Option<String>,
	/// `VERSION` option.
	pub version: Option<String>,
	/// `CASCADE` option.
	pub cascade: bool,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `PREPARE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct PrepareStmt {
	/// Prepared statement name.
	pub name: String,
	/// Argument type names.
	pub param_types: Vec<TypeName>,
	/// Prepared query statement.
	pub statement: Box<Statement>,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `EXECUTE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteStmt {
	/// Prepared statement name to execute.
	pub name: String,
	/// Arguments passed to the statement.
	pub params: Vec<Expr>,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `DEALLOCATE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeallocateStmt {
	/// Statement name, or `None` if `DEALLOCATE ALL`.
	pub name: Option<String>,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `TRUNCATE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateStmt {
	/// Target table names, in the order written; at least one.
	pub tables: Vec<QualifiedName>,
	/// Whether the statement says `RESTART IDENTITY`: the sequences owned by the tables' columns
	/// start over. `CONTINUE IDENTITY`, the default, leaves them running.
	pub restart_identity: bool,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `COPY` statement (`COPY table [(cols)] FROM STDIN ...` or `COPY { table | (query) }
/// TO STDOUT ...`).
#[derive(Debug, Clone, PartialEq)]
pub struct CopyStmt {
	/// Target table or query.
	pub target: CopyTarget,
	/// Direction (FROM STDIN or TO STDOUT).
	pub direction: CopyDirection,
	/// COPY options (FORMAT, DELIMITER, NULL, HEADER, QUOTE, ESCAPE).
	pub options: CopyOptions,
	/// Source span.
	pub span: Span,
}

/// Target of a COPY operation: either a table with optional column list, or a subquery.
#[derive(Debug, Clone, PartialEq)]
pub enum CopyTarget {
	Table {
		table: QualifiedName,
		columns: Vec<String>,
	},
	Query(Box<SelectStmt>),
}

/// Direction of the COPY stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyDirection {
	FromStdin,
	ToStdout,
}

/// Format of the COPY data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyFormat {
	#[default]
	Text,
	Csv,
	Binary,
}

/// Options configuring the COPY operation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CopyOptions {
	pub format: CopyFormat,
	pub delimiter: Option<u8>,
	pub null: Option<String>,
	pub header: Option<bool>,
	pub quote: Option<u8>,
	pub escape: Option<u8>,
}

/// Top-level PostgreSQL statement AST node.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
	/// `COPY` data streaming statement.
	Copy(Box<CopyStmt>),
	/// `SELECT` query.
	Select(Box<SelectStmt>),
	/// `INSERT` query.
	Insert(Box<InsertStmt>),
	/// `UPDATE` query.
	Update(Box<UpdateStmt>),
	/// `DELETE` query.
	Delete(Box<DeleteStmt>),
	/// `TRUNCATE` query.
	Truncate(Box<TruncateStmt>),
	/// `CREATE TABLE` DDL.
	CreateTable(Box<CreateTableStmt>),
	/// `CREATE INDEX` DDL.
	CreateIndex(Box<CreateIndexStmt>),
	/// `CREATE VIEW` DDL.
	CreateView(Box<CreateViewStmt>),
	/// `ALTER TABLE` DDL.
	AlterTable(Box<AlterTableStmt>),
	/// `CREATE SCHEMA` DDL.
	CreateSchema(CreateSchemaStmt),
	/// `CREATE TYPE ... AS ENUM` DDL.
	CreateType(Box<CreateTypeStmt>),
	/// `ALTER TYPE` DDL over an enum.
	AlterType(Box<AlterTypeStmt>),
	/// `CREATE DOMAIN` DDL.
	CreateDomain(Box<CreateDomainStmt>),
	/// `ALTER DOMAIN` DDL.
	AlterDomain(Box<AlterDomainStmt>),
	/// `CREATE TYPE ... AS (attributes)` DDL over a composite type.
	CreateCompositeType(Box<CreateCompositeTypeStmt>),
	/// `CREATE EXTENSION` DDL.
	CreateExtension(CreateExtensionStmt),
	/// `DROP` DDL.
	Drop(Box<DropStmt>),
	/// Transaction control statement.
	Transaction(TransactionStmt),
	/// `SET` configuration parameter.
	Set(SetStmt),
	/// `SHOW` configuration parameter.
	Show(ShowStmt),
	/// `RESET` configuration parameter.
	Reset(ResetStmt),
	/// `EXPLAIN` query plan analysis.
	Explain(Box<ExplainStmt>),
	/// `PREPARE` statement.
	Prepare(Box<PrepareStmt>),
	/// `EXECUTE` statement.
	Execute(ExecuteStmt),
	/// `DEALLOCATE` statement.
	Deallocate(DeallocateStmt),
	/// `CREATE PROPERTY GRAPH` DDL (SQL:2023 Part 16).
	CreatePropertyGraph(Box<crate::ast::graph::CreatePropertyGraphStmt>),
	/// `DROP PROPERTY GRAPH` DDL (SQL:2023 Part 16).
	DropPropertyGraph(Box<crate::ast::graph::DropPropertyGraphStmt>),
	/// Empty statement (e.g. standalone semicolon `;`).
	Empty,
}

impl SelectStmt {
	/// Recursively set all spans in this SELECT statement to zero for semantic equality
	/// comparisons.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		if let Some(w) = &mut self.with {
			w.span = zero;
			for cte in &mut w.ctes {
				cte.span = zero;
				cte.query.normalize_spans();
			}
		}
		if let Some(DistinctClause::On(exprs)) = &mut self.distinct {
			for e in exprs {
				e.normalize_spans();
			}
		}
		for t in &mut self.targets {
			t.span = zero;
			t.expr.normalize_spans();
		}
		for r in &mut self.from {
			normalize_table_ref(r);
		}
		if let Some(w) = &mut self.where_clause {
			w.normalize_spans();
		}
		for g in &mut self.group_by {
			g.normalize_spans();
		}
		if let Some(h) = &mut self.having {
			h.normalize_spans();
		}
		for win in &mut self.window {
			win.span = zero;
			win.spec.normalize_spans();
		}
		for o in &mut self.order_by {
			o.span = zero;
			o.expr.normalize_spans();
		}
		if let Some(l) = &mut self.limit {
			l.normalize_spans();
		}
		if let Some(o) = &mut self.offset {
			o.normalize_spans();
		}
		for lock in &mut self.locking {
			lock.span = zero;
			for t in &mut lock.tables {
				t.span = zero;
			}
		}
		if let Some(op) = &mut self.set_op {
			op.right.normalize_spans();
		}
	}
}

impl Statement {
	/// Return the span of the statement.
	pub const fn span(&self) -> Span {
		match self {
			Self::Select(s) => s.span,
			Self::Insert(s) => s.span,
			Self::Update(s) => s.span,
			Self::Delete(s) => s.span,
			Self::Truncate(s) => s.span,
			Self::CreateTable(s) => s.span,
			Self::CreateIndex(s) => s.span,
			Self::CreateView(s) => s.span,
			Self::AlterTable(s) => s.span,
			Self::CreateSchema(s) => s.span,
			Self::CreateExtension(s) => s.span,
			Self::CreateType(s) => s.span,
			Self::AlterType(s) => s.span,
			Self::CreateDomain(s) => s.span,
			Self::AlterDomain(s) => s.span,
			Self::CreateCompositeType(s) => s.span,
			Self::Drop(s) => s.span,
			Self::Transaction(tx) => match tx {
				TransactionStmt::Begin {
					span,
					..
				}
				| TransactionStmt::Commit {
					span,
				}
				| TransactionStmt::Rollback {
					span,
				}
				| TransactionStmt::Savepoint {
					span,
					..
				}
				| TransactionStmt::ReleaseSavepoint {
					span,
					..
				}
				| TransactionStmt::RollbackToSavepoint {
					span,
					..
				} => *span,
			},
			Self::Set(s) => s.span,
			Self::Show(s) => s.span,
			Self::Reset(s) => s.span,
			Self::Explain(s) => s.span,
			Self::Prepare(s) => s.span,
			Self::Execute(s) => s.span,
			Self::Deallocate(s) => s.span,
			Self::CreatePropertyGraph(s) => s.span,
			Self::DropPropertyGraph(s) => s.span,
			Self::Copy(s) => s.span,
			Self::Empty => Span::empty(0),
		}
	}

	/// Recursively set all spans in this statement to zero for semantic equality comparisons.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		match self {
			Self::Select(s) => s.normalize_spans(),
			Self::Insert(s) => {
				s.span = zero;
				if let Some(w) = &mut s.with {
					w.span = zero;
					for cte in &mut w.ctes {
						cte.span = zero;
						cte.query.normalize_spans();
					}
				}
				s.relation.span = zero;
				if let Some(rows) = &mut s.values {
					for row in rows {
						for val in row {
							val.normalize_spans();
						}
					}
				}
				if let Some(q) = &mut s.query {
					q.normalize_spans();
				}
				if let Some(c) = &mut s.on_conflict {
					c.span = zero;
					if let Some(w) = &mut c.target_where {
						w.normalize_spans();
					}
					if let Some(w) = &mut c.where_clause {
						w.normalize_spans();
					}
					if let OnConflictAction::DoUpdate(updates) = &mut c.action {
						for u in updates {
							u.span = zero;
							u.value.normalize_spans();
						}
					}
				}
				if let Some(ret) = &mut s.returning {
					for t in ret {
						t.span = zero;
						t.expr.normalize_spans();
					}
				}
			}
			Self::Update(s) => {
				s.span = zero;
				if let Some(w) = &mut s.with {
					w.span = zero;
					for cte in &mut w.ctes {
						cte.span = zero;
						cte.query.normalize_spans();
					}
				}
				s.relation.span = zero;
				for a in &mut s.assignments {
					a.span = zero;
					a.value.normalize_spans();
				}
				if let Some(from) = &mut s.from {
					for r in from {
						normalize_table_ref(r);
					}
				}
				if let Some(w) = &mut s.where_clause {
					w.normalize_spans();
				}
				if let Some(ret) = &mut s.returning {
					for t in ret {
						t.span = zero;
						t.expr.normalize_spans();
					}
				}
			}
			Self::Delete(s) => {
				s.span = zero;
				if let Some(w) = &mut s.with {
					w.span = zero;
					for cte in &mut w.ctes {
						cte.span = zero;
						cte.query.normalize_spans();
					}
				}
				s.relation.span = zero;
				if let Some(using) = &mut s.using {
					for r in using {
						normalize_table_ref(r);
					}
				}
				if let Some(w) = &mut s.where_clause {
					w.normalize_spans();
				}
				if let Some(ret) = &mut s.returning {
					for t in ret {
						t.span = zero;
						t.expr.normalize_spans();
					}
				}
			}
			Self::Truncate(s) => {
				s.span = zero;
				for table in &mut s.tables {
					table.span = zero;
				}
			}
			Self::CreateTable(s) => {
				s.span = zero;
				s.name.span = zero;
				for col in &mut s.columns {
					col.span = zero;
					col.data_type.span = zero;
					for c in &mut col.constraints {
						if let ColumnConstraint::Default(e) = c {
							e.normalize_spans();
						} else if let ColumnConstraint::References {
							table,
							..
						} = c
						{
							table.span = zero;
						}
					}
				}
				for c in &mut s.constraints {
					if let TableConstraint::ForeignKey {
						ref_table,
						..
					} = c
					{
						ref_table.span = zero;
					}
				}
			}
			Self::CreateIndex(s) => {
				s.span = zero;
				s.table.span = zero;
				for col in &mut s.columns {
					col.span = zero;
					col.expr.normalize_spans();
					if let Some(opclass) = &mut col.opclass {
						opclass.span = zero;
					}
				}
				for option in &mut s.options {
					option.span = zero;
					if let Some(value) = &mut option.value {
						value.normalize_spans();
					}
				}
				if let Some(pred) = &mut s.predicate {
					pred.normalize_spans();
				}
			}
			Self::CreateView(s) => {
				s.span = zero;
				s.name.span = zero;
				s.query.normalize_spans();
			}
			Self::AlterTable(s) => {
				s.span = zero;
				s.name.span = zero;
				for a in &mut s.actions {
					if let AlterTableAction::AddColumn {
						column,
						..
					} = a
					{
						column.span = zero;
						column.data_type.span = zero;
						for c in &mut column.constraints {
							if let ColumnConstraint::Default(e) = c {
								e.normalize_spans();
							}
						}
					} else if let AlterTableAction::AlterColumnType {
						data_type,
						..
					} = a
					{
						data_type.span = zero;
					} else if let AlterTableAction::SetDefault {
						default,
						..
					} = a
					{
						default.normalize_spans();
					}
				}
			}
			Self::CreateSchema(s) => s.span = zero,
			Self::CreateExtension(s) => s.span = zero,
			Self::CreateType(s) => s.span = zero,
			Self::AlterType(s) => s.span = zero,
			Self::CreateDomain(s) => {
				s.span = zero;
				s.name.span = zero;
				s.data_type.span = zero;
				for constraint in &mut s.constraints {
					constraint.span = zero;
					match &mut constraint.kind {
						DomainConstraintKind::Check(expr) | DomainConstraintKind::Default(expr) => {
							expr.normalize_spans()
						}
						DomainConstraintKind::NotNull | DomainConstraintKind::Null => {}
					}
				}
			}
			Self::AlterDomain(s) => {
				s.span = zero;
				s.name.span = zero;
				match &mut s.action {
					AlterDomainAction::SetDefault(expr)
					| AlterDomainAction::AddConstraint {
						check: expr,
						..
					} => expr.normalize_spans(),
					AlterDomainAction::DropDefault
					| AlterDomainAction::SetNotNull
					| AlterDomainAction::DropNotNull
					| AlterDomainAction::DropConstraint {
						..
					}
					| AlterDomainAction::ValidateConstraint {
						..
					} => {}
				}
			}
			Self::CreateCompositeType(s) => {
				s.span = zero;
				s.name.span = zero;
				for attr in &mut s.attributes {
					attr.span = zero;
					attr.data_type.span = zero;
				}
			}
			Self::Drop(s) => {
				s.span = zero;
				for n in &mut s.names {
					n.span = zero;
				}
			}
			Self::Transaction(tx) => match tx {
				TransactionStmt::Begin {
					span,
					..
				}
				| TransactionStmt::Commit {
					span,
				}
				| TransactionStmt::Rollback {
					span,
				}
				| TransactionStmt::Savepoint {
					span,
					..
				}
				| TransactionStmt::ReleaseSavepoint {
					span,
					..
				}
				| TransactionStmt::RollbackToSavepoint {
					span,
					..
				} => *span = zero,
			},
			Self::Set(s) => s.span = zero,
			Self::Show(s) => s.span = zero,
			Self::Reset(s) => s.span = zero,
			Self::Explain(s) => {
				s.span = zero;
				s.statement.normalize_spans();
			}
			Self::Prepare(s) => {
				s.span = zero;
				for t in &mut s.param_types {
					t.span = zero;
				}
				s.statement.normalize_spans();
			}
			Self::Execute(s) => {
				s.span = zero;
				for p in &mut s.params {
					p.normalize_spans();
				}
			}
			Self::Deallocate(s) => s.span = zero,
			Self::CreatePropertyGraph(s) => s.normalize_spans(),
			Self::DropPropertyGraph(s) => s.normalize_spans(),
			Self::Copy(s) => {
				s.span = zero;
				match &mut s.target {
					CopyTarget::Table {
						table,
						..
					} => {
						table.span = zero;
					}
					CopyTarget::Query(q) => {
						q.normalize_spans();
					}
				}
			}
			Self::Empty => {}
		}
	}
}

fn normalize_table_ref(r: &mut TableRef) {
	let zero = Span::empty(0);
	match r {
		TableRef::Table {
			name,
			alias,
			span,
		} => {
			*span = zero;
			name.span = zero;
			if let Some(a) = alias {
				a.span = zero;
			}
		}
		TableRef::Function {
			function,
			alias,
			span,
			..
		} => {
			*span = zero;
			function.normalize_spans();
			if let Some(alias) = alias {
				alias.span = zero;
			}
		}
		TableRef::Subquery {
			query,
			alias,
			span,
			..
		} => {
			*span = zero;
			alias.span = zero;
			query.normalize_spans();
		}
		TableRef::Join {
			left,
			right,
			condition,
			span,
			..
		} => {
			*span = zero;
			normalize_table_ref(left);
			normalize_table_ref(right);
			if let Some(JoinCondition::On(e)) = condition {
				e.normalize_spans();
			}
		}
		TableRef::GraphTable {
			graph_name,
			pattern,
			columns,
			alias,
			span,
		} => {
			*span = zero;
			graph_name.span = zero;
			pattern.normalize_spans();
			for col in columns {
				col.normalize_spans();
			}
			if let Some(a) = alias {
				a.span = zero;
			}
		}
		TableRef::JsonTable(t) => {
			t.normalize_spans();
		}
	}
}
