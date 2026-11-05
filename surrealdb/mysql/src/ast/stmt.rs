use crate::ast::expr::Expr;
pub use crate::ast::expr::{OrderByExpr, WindowSpec};
use crate::lexer::span::Span;

/// A qualified identifier name (e.g. `col`, `tbl.col`, `db.tbl`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedName {
	pub parts: Vec<String>,
	pub span: Span,
}

impl QualifiedName {
	pub fn new(parts: Vec<String>, span: Span) -> Self {
		Self {
			parts,
			span,
		}
	}

	pub fn simple(name: String, span: Span) -> Self {
		Self {
			parts: vec![name],
			span,
		}
	}

	pub fn to_mysql_string(&self) -> String {
		self.parts
			.iter()
			.map(|p| {
				if p.contains('`') || p.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_') {
					format!("`{}`", p.replace('`', "``"))
				} else {
					p.clone()
				}
			})
			.collect::<Vec<_>>()
			.join(".")
	}
}

/// Selected item in a SELECT projection list.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
	Wildcard(Span),
	/// `t.*` or `db.t.*`: the parts of the qualified name of the relation.
	QualifiedWildcard(Vec<String>, Span),
	Expr {
		expr: Expr,
		alias: Option<String>,
		/// The source text of the expression as written, parentheses included and the alias
		/// left out.
		span: Span,
	},
}

impl SelectItem {
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		match self {
			Self::Wildcard(s) => *s = zero,
			Self::QualifiedWildcard(_, s) => *s = zero,
			Self::Expr {
				expr,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
			}
		}
	}
}

/// Named window definition in a `SELECT` statement (`WINDOW name AS (spec)`).
#[derive(Debug, Clone, PartialEq)]
pub struct WindowDef {
	pub name: String,
	pub spec: WindowSpec,
	pub span: Span,
}

impl WindowDef {
	pub fn normalize_spans(&mut self) {
		self.span = Span::empty(0);
		self.spec.normalize_spans();
	}
}

/// A LIMIT clause in MySQL (`LIMIT count`, `LIMIT offset, count`, or `LIMIT count OFFSET offset`).
#[derive(Debug, Clone, PartialEq)]
pub struct LimitClause {
	pub offset: Option<Expr>,
	pub count: Expr,
	pub span: Span,
}

impl LimitClause {
	pub fn normalize_spans(&mut self) {
		self.span = Span::empty(0);
		if let Some(off) = &mut self.offset {
			off.normalize_spans();
		}
		self.count.normalize_spans();
	}
}

/// Row locking clause in MySQL SELECT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockType {
	ForUpdate,
	ForShare,
	LockInShareMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockClause {
	pub lock_type: LockType,
	pub nowait: bool,
	pub skip_locked: bool,
	pub span: Span,
}

/// Join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
	Inner,
	Left,
	Right,
	Cross,
	Straight,
}

/// Join condition.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
	On(Expr),
	Using(Vec<String>),
}

/// An index hint type in MySQL (`USE`, `FORCE`, `IGNORE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexHintType {
	Use,
	Force,
	Ignore,
}

/// An index hint on a table factor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHint {
	pub hint_type: IndexHintType,
	pub indexes: Vec<String>,
	pub span: Span,
}

/// Table reference in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
	Table {
		name: QualifiedName,
		alias: Option<String>,
		index_hints: Vec<IndexHint>,
		span: Span,
	},
	Subquery {
		subquery: Box<SelectStmt>,
		alias: String,
		span: Span,
	},
	Join {
		left: Box<TableRef>,
		join_type: JoinType,
		right: Box<TableRef>,
		condition: Option<JoinCondition>,
		span: Span,
	},
}

impl TableRef {
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		match self {
			Self::Table {
				name,
				index_hints,
				span,
				..
			} => {
				*span = zero;
				name.span = zero;
				for h in index_hints {
					h.span = zero;
				}
			}
			Self::Subquery {
				subquery,
				span,
				..
			} => {
				*span = zero;
				subquery.normalize_spans();
			}
			Self::Join {
				left,
				right,
				condition,
				span,
				..
			} => {
				*span = zero;
				left.normalize_spans();
				right.normalize_spans();
				if let Some(JoinCondition::On(expr)) = condition {
					expr.normalize_spans();
				}
			}
		}
	}
}

/// A set operation kind in MySQL (e.g. `UNION`, `INTERSECT`, `EXCEPT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpKind {
	Union,
	Intersect,
	Except,
}

/// A set operation chaining two queries in MySQL.
#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
	pub op: SetOpKind,
	pub all: bool,
	pub right: Box<SelectStmt>,
}

impl SetOperation {
	pub fn normalize_spans(&mut self) {
		self.right.normalize_spans();
	}
}

/// A `WITH` clause (Common Table Expression list) in MySQL.
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
	pub recursive: bool,
	pub ctes: Vec<CommonTableExpr>,
	pub span: Span,
}

impl WithClause {
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		for cte in &mut self.ctes {
			cte.normalize_spans();
		}
	}
}

/// A single Common Table Expression (CTE) in MySQL.
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
	pub name: String,
	pub columns: Vec<String>,
	pub query: Box<SelectStmt>,
	pub span: Span,
}

impl CommonTableExpr {
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		self.query.normalize_spans();
	}
}

/// A MySQL `SELECT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
	pub with: Option<WithClause>,
	pub distinct: bool,
	pub projection: Vec<SelectItem>,
	pub from: Option<TableRef>,
	pub where_clause: Option<Expr>,
	pub group_by: Vec<Expr>,
	pub with_rollup: bool,
	pub having: Option<Expr>,
	pub window: Vec<WindowDef>,
	pub order_by: Vec<OrderByExpr>,
	pub limit: Option<LimitClause>,
	pub lock: Option<LockClause>,
	pub set_op: Option<SetOperation>,
	pub span: Span,
}

impl SelectStmt {
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		if let Some(w) = &mut self.with {
			w.normalize_spans();
		}
		for p in &mut self.projection {
			p.normalize_spans();
		}
		if let Some(f) = &mut self.from {
			f.normalize_spans();
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
		for w in &mut self.window {
			w.normalize_spans();
		}
		for o in &mut self.order_by {
			o.normalize_spans();
		}
		if let Some(l) = &mut self.limit {
			l.normalize_spans();
		}
		if let Some(lk) = &mut self.lock {
			lk.span = zero;
		}
		if let Some(s) = &mut self.set_op {
			s.normalize_spans();
		}
	}
}

/// A MySQL `INSERT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
	pub ignore: bool,
	pub table: QualifiedName,
	pub columns: Vec<String>,
	pub values: Vec<Vec<Expr>>,
	pub query: Option<Box<SelectStmt>>,
	pub on_duplicate_key_update: Vec<(String, Expr)>,
	pub span: Span,
}

/// A MySQL `REPLACE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplaceStmt {
	pub table: QualifiedName,
	pub columns: Vec<String>,
	pub values: Vec<Vec<Expr>>,
	pub query: Option<Box<SelectStmt>>,
	pub span: Span,
}

/// A MySQL `TRUNCATE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateStmt {
	pub table: QualifiedName,
	pub span: Span,
}

/// A MySQL `UPDATE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
	pub table: TableRef,
	pub assignments: Vec<(String, Expr)>,
	pub where_clause: Option<Expr>,
	pub order_by: Vec<OrderByExpr>,
	pub limit: Option<Expr>,
	pub span: Span,
}

/// A MySQL `DELETE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
	pub table: QualifiedName,
	pub where_clause: Option<Expr>,
	pub order_by: Vec<OrderByExpr>,
	pub limit: Option<Expr>,
	pub span: Span,
}

/// Column definition in `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
	pub name: String,
	pub data_type: String,
	pub not_null: bool,
	pub default: Option<Expr>,
	pub auto_increment: bool,
	pub primary_key: bool,
	pub unique: bool,
	pub span: Span,
}

/// Table-level constraint in `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
	PrimaryKey {
		name: Option<String>,
		columns: Vec<String>,
		span: Span,
	},
	Unique {
		name: Option<String>,
		columns: Vec<String>,
		span: Span,
	},
	ForeignKey {
		name: Option<String>,
		columns: Vec<String>,
		ref_table: QualifiedName,
		ref_columns: Vec<String>,
		on_delete: Option<String>,
		on_update: Option<String>,
		span: Span,
	},
}

/// A MySQL `CREATE TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
	pub if_not_exists: bool,
	pub temporary: bool,
	pub name: QualifiedName,
	pub columns: Vec<ColumnDef>,
	pub constraints: Vec<TableConstraint>,
	pub span: Span,
}

/// A MySQL `DROP TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
	pub if_exists: bool,
	pub temporary: bool,
	pub names: Vec<QualifiedName>,
	pub span: Span,
}

/// A MySQL `USE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseStmt {
	pub database: String,
	pub span: Span,
}

/// Target variable in a `SET` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariableTarget {
	User(String),
	System {
		name: String,
		is_global: bool,
	},
}

/// A MySQL `SET` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SetStmt {
	pub assignments: Vec<(VariableTarget, Expr)>,
	pub span: Span,
}

/// A MySQL `SHOW` statement variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShowStmt {
	Tables {
		full: bool,
		database: Option<String>,
		like: Option<String>,
		span: Span,
	},
	TableStatus {
		database: Option<String>,
		like: Option<String>,
		span: Span,
	},
	Triggers {
		database: Option<String>,
		like: Option<String>,
		span: Span,
	},
	Databases {
		like: Option<String>,
		span: Span,
	},
	Columns {
		full: bool,
		table: QualifiedName,
		like: Option<String>,
		span: Span,
	},
	CreateTable {
		table: QualifiedName,
		span: Span,
	},
	Variables {
		is_global: bool,
		like: Option<String>,
		span: Span,
	},
	Status {
		is_global: bool,
		like: Option<String>,
		span: Span,
	},
	Processlist {
		full: bool,
		span: Span,
	},
	Warnings {
		span: Span,
	},
	Errors {
		span: Span,
	},
	Index {
		table: QualifiedName,
		span: Span,
	},
	Engines {
		span: Span,
	},
	Collation {
		like: Option<String>,
		span: Span,
	},
	Charset {
		like: Option<String>,
		span: Span,
	},
}

/// A MySQL `DESCRIBE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeStmt {
	pub table: QualifiedName,
	pub column: Option<String>,
	pub span: Span,
}

/// A MySQL `EXPLAIN` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStmt {
	pub statement: Box<Statement>,
	pub span: Span,
}

/// Transaction control statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionStmt {
	Begin(Span),
	StartTransaction {
		read_only: bool,
		span: Span,
	},
	Commit(Span),
	Rollback(Span),
	Savepoint {
		name: String,
		span: Span,
	},
	ReleaseSavepoint {
		name: String,
		span: Span,
	},
	RollbackToSavepoint {
		name: String,
		span: Span,
	},
}

/// A MySQL `CREATE DATABASE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabaseStmt {
	pub if_not_exists: bool,
	pub name: String,
	pub span: Span,
}

/// A MySQL `DROP DATABASE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDatabaseStmt {
	pub if_exists: bool,
	pub name: String,
	pub span: Span,
}

/// A MySQL `CREATE INDEX` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
	pub unique: bool,
	pub name: String,
	pub table: QualifiedName,
	pub columns: Vec<String>,
	pub span: Span,
}

/// A MySQL `DROP INDEX` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropIndexStmt {
	pub name: String,
	pub table: QualifiedName,
	pub span: Span,
}

/// A MySQL `CREATE VIEW` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt {
	pub or_replace: bool,
	pub name: QualifiedName,
	pub columns: Vec<String>,
	pub query: Box<SelectStmt>,
	pub span: Span,
}

/// A MySQL `DROP VIEW` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropViewStmt {
	pub if_exists: bool,
	pub names: Vec<QualifiedName>,
	pub span: Span,
}

/// Action in an `ALTER TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
	AddColumn(ColumnDef),
	DropColumn {
		name: String,
	},
	ModifyColumn(ColumnDef),
	ChangeColumn {
		old_name: String,
		new_def: ColumnDef,
	},
	AddConstraint(TableConstraint),
	DropPrimaryKey,
	DropIndex {
		name: String,
	},
	RenameTable {
		new_name: QualifiedName,
	},
}

/// A MySQL `ALTER TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStmt {
	pub table: QualifiedName,
	pub actions: Vec<AlterTableAction>,
	pub span: Span,
}

/// A MySQL `CREATE USER` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUserStmt {
	pub if_not_exists: bool,
	pub user: String,
	pub host: Option<String>,
	pub identified_by: Option<String>,
	pub span: Span,
}

/// A MySQL `DROP USER` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropUserStmt {
	pub if_exists: bool,
	pub users: Vec<(String, Option<String>)>,
	pub span: Span,
}

/// A MySQL `GRANT` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantStmt {
	pub privileges: Vec<String>,
	pub object: Option<String>,
	pub user: String,
	pub host: Option<String>,
	pub span: Span,
}

/// A MySQL `REVOKE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeStmt {
	pub privileges: Vec<String>,
	pub object: Option<String>,
	pub user: String,
	pub host: Option<String>,
	pub span: Span,
}

/// Lock type for `LOCK TABLES`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLockType {
	Read,
	Write,
}

/// Table lock entry in `LOCK TABLES`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableLock {
	pub table: QualifiedName,
	pub lock_type: TableLockType,
}

/// A MySQL `LOCK TABLES` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockTablesStmt {
	pub locks: Vec<TableLock>,
	pub span: Span,
}

/// A MySQL `UNLOCK TABLES` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlockTablesStmt {
	pub span: Span,
}

/// A MySQL `PREPARE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareStmt {
	pub name: String,
	pub query: String,
	pub span: Span,
}

/// A MySQL `EXECUTE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteStmt {
	pub name: String,
	pub using: Vec<Expr>,
	pub span: Span,
}

/// A MySQL `DEALLOCATE PREPARE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeallocateStmt {
	pub name: String,
	pub span: Span,
}

/// Top-level MySQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
	Select(Box<SelectStmt>),
	Insert(Box<InsertStmt>),
	Update(Box<UpdateStmt>),
	Delete(Box<DeleteStmt>),
	Replace(Box<ReplaceStmt>),
	Truncate(Box<TruncateStmt>),
	CreateTable(Box<CreateTableStmt>),
	DropTable(Box<DropTableStmt>),
	CreateDatabase(CreateDatabaseStmt),
	DropDatabase(DropDatabaseStmt),
	CreateIndex(Box<CreateIndexStmt>),
	DropIndex(DropIndexStmt),
	CreateView(Box<CreateViewStmt>),
	DropView(DropViewStmt),
	AlterTable(Box<AlterTableStmt>),
	CreateUser(CreateUserStmt),
	DropUser(DropUserStmt),
	Grant(GrantStmt),
	Revoke(RevokeStmt),
	LockTables(LockTablesStmt),
	UnlockTables(UnlockTablesStmt),
	Prepare(PrepareStmt),
	Execute(ExecuteStmt),
	Deallocate(DeallocateStmt),
	Use(UseStmt),
	Set(SetStmt),
	Show(ShowStmt),
	Describe(DescribeStmt),
	Explain(Box<ExplainStmt>),
	Transaction(TransactionStmt),
}

impl Statement {
	/// Return source span covering this statement.
	pub fn span(&self) -> Span {
		match self {
			Self::Select(s) => s.span,
			Self::Insert(s) => s.span,
			Self::Update(s) => s.span,
			Self::Delete(s) => s.span,
			Self::Replace(s) => s.span,
			Self::Truncate(s) => s.span,
			Self::CreateTable(s) => s.span,
			Self::DropTable(s) => s.span,
			Self::CreateDatabase(s) => s.span,
			Self::DropDatabase(s) => s.span,
			Self::CreateIndex(s) => s.span,
			Self::DropIndex(s) => s.span,
			Self::CreateView(s) => s.span,
			Self::DropView(s) => s.span,
			Self::AlterTable(s) => s.span,
			Self::CreateUser(s) => s.span,
			Self::DropUser(s) => s.span,
			Self::Grant(s) => s.span,
			Self::Revoke(s) => s.span,
			Self::LockTables(s) => s.span,
			Self::UnlockTables(s) => s.span,
			Self::Prepare(s) => s.span,
			Self::Execute(s) => s.span,
			Self::Deallocate(s) => s.span,
			Self::Use(s) => s.span,
			Self::Set(s) => s.span,
			Self::Show(s) => match s {
				ShowStmt::Tables {
					span,
					..
				}
				| ShowStmt::TableStatus {
					span,
					..
				}
				| ShowStmt::Triggers {
					span,
					..
				}
				| ShowStmt::Databases {
					span,
					..
				}
				| ShowStmt::Columns {
					span,
					..
				}
				| ShowStmt::CreateTable {
					span,
					..
				}
				| ShowStmt::Variables {
					span,
					..
				}
				| ShowStmt::Status {
					span,
					..
				}
				| ShowStmt::Processlist {
					span,
					..
				}
				| ShowStmt::Warnings {
					span,
				}
				| ShowStmt::Errors {
					span,
				}
				| ShowStmt::Index {
					span,
					..
				}
				| ShowStmt::Engines {
					span,
				}
				| ShowStmt::Collation {
					span,
					..
				}
				| ShowStmt::Charset {
					span,
					..
				} => *span,
			},
			Self::Describe(s) => s.span,
			Self::Explain(s) => s.span,
			Self::Transaction(s) => match s {
				TransactionStmt::Begin(span)
				| TransactionStmt::StartTransaction {
					span,
					..
				}
				| TransactionStmt::Commit(span)
				| TransactionStmt::Rollback(span)
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
		}
	}

	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		match self {
			Self::Select(s) => s.normalize_spans(),
			Self::Insert(s) => {
				s.span = zero;
				s.table.span = zero;
				for row in &mut s.values {
					for v in row {
						v.normalize_spans();
					}
				}
				if let Some(q) = &mut s.query {
					q.normalize_spans();
				}
				for (_, v) in &mut s.on_duplicate_key_update {
					v.normalize_spans();
				}
			}
			Self::Update(s) => {
				s.span = zero;
				s.table.normalize_spans();
				for (_, v) in &mut s.assignments {
					v.normalize_spans();
				}
				if let Some(w) = &mut s.where_clause {
					w.normalize_spans();
				}
				for o in &mut s.order_by {
					o.normalize_spans();
				}
				if let Some(l) = &mut s.limit {
					l.normalize_spans();
				}
			}
			Self::Delete(s) => {
				s.span = zero;
				s.table.span = zero;
				if let Some(w) = &mut s.where_clause {
					w.normalize_spans();
				}
				for o in &mut s.order_by {
					o.normalize_spans();
				}
				if let Some(l) = &mut s.limit {
					l.normalize_spans();
				}
			}
			Self::Replace(s) => {
				s.span = zero;
				s.table.span = zero;
				for row in &mut s.values {
					for v in row {
						v.normalize_spans();
					}
				}
				if let Some(q) = &mut s.query {
					q.normalize_spans();
				}
			}
			Self::Truncate(s) => {
				s.span = zero;
				s.table.span = zero;
			}
			Self::CreateTable(s) => {
				s.span = zero;
				s.name.span = zero;
				for c in &mut s.columns {
					c.span = zero;
					if let Some(d) = &mut c.default {
						d.normalize_spans();
					}
				}
				for t in &mut s.constraints {
					match t {
						TableConstraint::PrimaryKey {
							span,
							..
						}
						| TableConstraint::Unique {
							span,
							..
						} => *span = zero,
						TableConstraint::ForeignKey {
							ref_table,
							span,
							..
						} => {
							*span = zero;
							ref_table.span = zero;
						}
					}
				}
			}
			Self::DropTable(s) => {
				s.span = zero;
				for n in &mut s.names {
					n.span = zero;
				}
			}
			Self::CreateDatabase(s) => s.span = zero,
			Self::DropDatabase(s) => s.span = zero,
			Self::CreateIndex(s) => {
				s.span = zero;
				s.table.span = zero;
			}
			Self::DropIndex(s) => {
				s.span = zero;
				s.table.span = zero;
			}
			Self::CreateView(s) => {
				s.span = zero;
				s.name.span = zero;
				s.query.normalize_spans();
			}
			Self::DropView(s) => {
				s.span = zero;
				for n in &mut s.names {
					n.span = zero;
				}
			}
			Self::AlterTable(s) => {
				s.span = zero;
				s.table.span = zero;
				for action in &mut s.actions {
					match action {
						AlterTableAction::AddColumn(c) | AlterTableAction::ModifyColumn(c) => {
							c.span = zero;
							if let Some(d) = &mut c.default {
								d.normalize_spans();
							}
						}
						AlterTableAction::ChangeColumn {
							new_def,
							..
						} => {
							new_def.span = zero;
							if let Some(d) = &mut new_def.default {
								d.normalize_spans();
							}
						}
						AlterTableAction::AddConstraint(c) => match c {
							TableConstraint::PrimaryKey {
								span,
								..
							}
							| TableConstraint::Unique {
								span,
								..
							} => *span = zero,
							TableConstraint::ForeignKey {
								ref_table,
								span,
								..
							} => {
								*span = zero;
								ref_table.span = zero;
							}
						},
						AlterTableAction::RenameTable {
							new_name,
						} => {
							new_name.span = zero;
						}
						AlterTableAction::DropColumn {
							..
						}
						| AlterTableAction::DropPrimaryKey
						| AlterTableAction::DropIndex {
							..
						} => {}
					}
				}
			}
			Self::CreateUser(s) => s.span = zero,
			Self::DropUser(s) => s.span = zero,
			Self::Grant(s) => s.span = zero,
			Self::Revoke(s) => s.span = zero,
			Self::LockTables(s) => {
				s.span = zero;
				for lock in &mut s.locks {
					lock.table.span = zero;
				}
			}
			Self::UnlockTables(s) => s.span = zero,
			Self::Prepare(s) => s.span = zero,
			Self::Execute(s) => {
				s.span = zero;
				for e in &mut s.using {
					e.normalize_spans();
				}
			}
			Self::Deallocate(s) => s.span = zero,
			Self::Use(s) => s.span = zero,
			Self::Set(s) => {
				s.span = zero;
				for (_, e) in &mut s.assignments {
					e.normalize_spans();
				}
			}
			Self::Show(s) => match s {
				ShowStmt::Columns {
					table,
					span,
					..
				} => {
					*span = zero;
					table.span = zero;
				}
				ShowStmt::CreateTable {
					table,
					span,
					..
				} => {
					*span = zero;
					table.span = zero;
				}
				ShowStmt::Index {
					table,
					span,
					..
				} => {
					*span = zero;
					table.span = zero;
				}
				ShowStmt::Tables {
					span,
					..
				}
				| ShowStmt::TableStatus {
					span,
					..
				}
				| ShowStmt::Triggers {
					span,
					..
				}
				| ShowStmt::Databases {
					span,
					..
				}
				| ShowStmt::Variables {
					span,
					..
				}
				| ShowStmt::Status {
					span,
					..
				}
				| ShowStmt::Processlist {
					span,
					..
				}
				| ShowStmt::Warnings {
					span,
				}
				| ShowStmt::Errors {
					span,
				}
				| ShowStmt::Engines {
					span,
				}
				| ShowStmt::Collation {
					span,
					..
				}
				| ShowStmt::Charset {
					span,
					..
				} => *span = zero,
			},
			Self::Describe(s) => {
				s.span = zero;
				s.table.span = zero;
			}
			Self::Explain(s) => {
				s.span = zero;
				s.statement.normalize_spans();
			}
			Self::Transaction(s) => match s {
				TransactionStmt::Begin(span)
				| TransactionStmt::StartTransaction {
					span,
					..
				}
				| TransactionStmt::Commit(span)
				| TransactionStmt::Rollback(span)
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
		}
	}
}
