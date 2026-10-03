//! Statement AST nodes for MySQL.

use crate::ast::expr::Expr;
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
	QualifiedWildcard(String, Span),
	Expr {
		expr: Expr,
		alias: Option<String>,
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

/// An ORDER BY item.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
	pub expr: Expr,
	pub asc: bool,
	pub span: Span,
}

impl OrderByExpr {
	pub fn normalize_spans(&mut self) {
		self.span = Span::empty(0);
		self.expr.normalize_spans();
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

/// Table reference in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
	Table {
		name: QualifiedName,
		alias: Option<String>,
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
				span,
				..
			} => {
				*span = zero;
				name.span = zero;
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

/// A MySQL `SELECT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
	pub distinct: bool,
	pub projection: Vec<SelectItem>,
	pub from: Option<TableRef>,
	pub where_clause: Option<Expr>,
	pub group_by: Vec<Expr>,
	pub having: Option<Expr>,
	pub order_by: Vec<OrderByExpr>,
	pub limit: Option<LimitClause>,
	pub lock: Option<LockClause>,
	pub span: Span,
}

impl SelectStmt {
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
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
		for o in &mut self.order_by {
			o.normalize_spans();
		}
		if let Some(l) = &mut self.limit {
			l.normalize_spans();
		}
		if let Some(lk) = &mut self.lock {
			lk.span = zero;
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
	pub on_duplicate_key_update: Vec<(String, Expr)>,
	pub span: Span,
}

/// A MySQL `REPLACE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplaceStmt {
	pub table: QualifiedName,
	pub columns: Vec<String>,
	pub values: Vec<Vec<Expr>>,
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
		database: Option<String>,
		like: Option<String>,
		span: Span,
	},
	Databases {
		like: Option<String>,
		span: Span,
	},
	Columns {
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
}

/// Top-level MySQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
	Select(Box<SelectStmt>),
	Insert(Box<InsertStmt>),
	Update(Box<UpdateStmt>),
	Delete(Box<DeleteStmt>),
	Replace(Box<ReplaceStmt>),
	CreateTable(Box<CreateTableStmt>),
	DropTable(Box<DropTableStmt>),
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
			Self::CreateTable(s) => s.span,
			Self::DropTable(s) => s.span,
			Self::Use(s) => s.span,
			Self::Set(s) => s.span,
			Self::Show(s) => match s {
				ShowStmt::Tables {
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
				| TransactionStmt::Rollback(span) => *span,
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
			Self::Use(s) => s.span = zero,
			Self::Set(s) => {
				s.span = zero;
				for (_, e) in &mut s.assignments {
					e.normalize_spans();
				}
			}
			Self::Show(s) => match s {
				ShowStmt::Tables {
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
				| TransactionStmt::Rollback(span) => *span = zero,
			},
		}
	}
}
