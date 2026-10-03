//! Statement AST representations for PostgreSQL.

use crate::ast::expr::{Expr, QualifiedName, TypeName};
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

/// Sort direction for `ORDER BY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
	/// Ascending sort (`ASC`).
	Asc,
	/// Descending sort (`DESC`).
	Desc,
}

/// Ordering of NULL values in `ORDER BY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
	/// NULL values sort first (`NULLS FIRST`).
	First,
	/// NULL values sort last (`NULLS LAST`).
	Last,
}

/// An item in an `ORDER BY` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
	/// Expression to order by.
	pub expr: Expr,
	/// Sort direction (`ASC` or `DESC`).
	pub direction: Option<OrderDirection>,
	/// Null ordering (`NULLS FIRST` or `NULLS LAST`).
	pub nulls: Option<NullsOrder>,
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
	/// Subquery in `FROM` clause (e.g. `(SELECT * FROM t) AS sub`).
	Subquery {
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
	/// `ORDER BY` specifications.
	pub order_by: Vec<OrderByItem>,
	/// `LIMIT` expression.
	pub limit: Option<Expr>,
	/// `OFFSET` expression.
	pub offset: Option<Expr>,
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
	/// Action to take on conflict.
	pub action: OnConflictAction,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL `INSERT` statement AST node.
///
/// Implements `gram.y: InsertStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
	/// Target table name.
	pub relation: QualifiedName,
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
	/// `REFERENCES table(col)`.
	References {
		/// Referenced table name.
		table: QualifiedName,
		/// Referenced column name.
		column: Option<String>,
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
	/// `FOREIGN KEY (cols) REFERENCES ref_table (ref_cols)`.
	ForeignKey {
		/// Local columns.
		columns: Vec<String>,
		/// Foreign table.
		ref_table: QualifiedName,
		/// Foreign columns.
		ref_columns: Vec<String>,
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

/// A PostgreSQL transaction management statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionStmt {
	/// `BEGIN [TRANSACTION]`.
	Begin {
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

/// A PostgreSQL `SET <name> = <value>` or `SET <name> TO <value>` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetStmt {
	/// Parameter name.
	pub name: String,
	/// Parameter value.
	pub value: String,
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

/// Top-level PostgreSQL statement AST node.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
	/// `SELECT` query.
	Select(Box<SelectStmt>),
	/// `INSERT` query.
	Insert(Box<InsertStmt>),
	/// `UPDATE` query.
	Update(Box<UpdateStmt>),
	/// `DELETE` query.
	Delete(Box<DeleteStmt>),
	/// `CREATE TABLE` DDL.
	CreateTable(Box<CreateTableStmt>),
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
	/// Empty statement (e.g. standalone semicolon `;`).
	Empty,
}

impl Statement {
	/// Return the span of the statement.
	pub const fn span(&self) -> Span {
		match self {
			Self::Select(s) => s.span,
			Self::Insert(s) => s.span,
			Self::Update(s) => s.span,
			Self::Delete(s) => s.span,
			Self::CreateTable(s) => s.span,
			Self::Drop(s) => s.span,
			Self::Transaction(tx) => match tx {
				TransactionStmt::Begin {
					span,
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
			Self::Empty => Span::empty(0),
		}
	}
}
