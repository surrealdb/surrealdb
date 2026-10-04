//! Pretty-printer formatting MySQL AST nodes back into standard MySQL SQL.

use crate::ast::expr::{ColumnRef, Expr, TruthValue};
use crate::ast::stmt::{
	ColumnDef, CreateTableStmt, DeleteStmt, DescribeStmt, DropTableStmt, ExplainStmt, InsertStmt,
	JoinCondition, JoinType, LockType, QualifiedName, ReplaceStmt, SelectItem, SelectStmt, SetStmt,
	ShowStmt, Statement, TableConstraint, TableRef, TransactionStmt, TruncateStmt, UpdateStmt,
	UseStmt, VariableTarget,
};

/// Trait for AST nodes that can be formatted as MySQL SQL syntax.
pub trait ToMySql {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;

	fn to_mysql_string(&self) -> String {
		struct Adapter<'a, T: ?Sized>(&'a T);
		impl<'a, T: ToMySql + ?Sized> std::fmt::Display for Adapter<'a, T> {
			fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
				self.0.fmt_mysql(f)
			}
		}
		Adapter(self).to_string()
	}
}

pub fn escape_mysql_ident(s: &str) -> String {
	if s == "*" {
		return "*".to_string();
	}
	let needs_quote = s.is_empty()
		|| s.contains('`')
		|| s.chars().next().is_none_or(|c| c.is_ascii_digit())
		|| s.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_');

	if needs_quote {
		format!("`{}`", s.replace('`', "``"))
	} else {
		s.to_string()
	}
}

impl ToMySql for QualifiedName {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (i, part) in self.parts.iter().enumerate() {
			if i > 0 {
				write!(f, ".")?;
			}
			write!(f, "{}", escape_mysql_ident(part))?;
		}
		Ok(())
	}
}

impl ToMySql for ColumnRef {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (i, field) in self.fields.iter().enumerate() {
			if i > 0 {
				write!(f, ".")?;
			}
			write!(f, "{}", escape_mysql_ident(field))?;
		}
		Ok(())
	}
}

impl ToMySql for Expr {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Literal(lit, _) => lit.to_mysql(f),
			Self::ColumnRef(col) => col.fmt_mysql(f),
			Self::UserVar(v, _) => write!(f, "@{v}"),
			Self::SystemVar {
				name,
				is_global,
				..
			} => {
				if *is_global {
					write!(f, "@@global.{name}")
				} else {
					write!(f, "@@{name}")
				}
			}
			Self::Param(_, _) => write!(f, "?"),
			Self::UnaryOp {
				op,
				expr,
				..
			} => {
				write!(f, "{} ", op.as_str())?;
				expr.fmt_mysql(f)
			}
			Self::BinaryOp {
				left,
				op,
				right,
				..
			} => {
				left.fmt_mysql(f)?;
				write!(f, " {} ", op.as_str())?;
				right.fmt_mysql(f)
			}
			Self::FunctionCall {
				name,
				distinct,
				args,
				..
			} => {
				write!(f, "{name}(")?;
				if *distinct {
					write!(f, "DISTINCT ")?;
				}
				for (i, arg) in args.iter().enumerate() {
					if i > 0 {
						write!(f, ", ")?;
					}
					arg.fmt_mysql(f)?;
				}
				write!(f, ")")
			}
			Self::Cast {
				expr,
				target_type,
				..
			} => {
				write!(f, "CAST(")?;
				expr.fmt_mysql(f)?;
				write!(f, " AS {target_type})")
			}
			Self::Case {
				operand,
				branches,
				else_expr,
				..
			} => {
				write!(f, "CASE")?;
				if let Some(op) = operand {
					write!(f, " ")?;
					op.fmt_mysql(f)?;
				}
				for b in branches {
					write!(f, " WHEN ")?;
					b.when.fmt_mysql(f)?;
					write!(f, " THEN ")?;
					b.then.fmt_mysql(f)?;
				}
				if let Some(el) = else_expr {
					write!(f, " ELSE ")?;
					el.fmt_mysql(f)?;
				}
				write!(f, " END")
			}
			Self::InList {
				expr,
				negated,
				list,
				..
			} => {
				expr.fmt_mysql(f)?;
				if *negated {
					write!(f, " NOT IN (")?;
				} else {
					write!(f, " IN (")?;
				}
				for (i, item) in list.iter().enumerate() {
					if i > 0 {
						write!(f, ", ")?;
					}
					item.fmt_mysql(f)?;
				}
				write!(f, ")")
			}
			Self::InSubquery {
				expr,
				negated,
				subquery,
				..
			} => {
				expr.fmt_mysql(f)?;
				if *negated {
					write!(f, " NOT IN (")?;
				} else {
					write!(f, " IN (")?;
				}
				subquery.fmt_mysql(f)?;
				write!(f, ")")
			}
			Self::Between {
				expr,
				negated,
				low,
				high,
				..
			} => {
				expr.fmt_mysql(f)?;
				if *negated {
					write!(f, " NOT BETWEEN ")?;
				} else {
					write!(f, " BETWEEN ")?;
				}
				low.fmt_mysql(f)?;
				write!(f, " AND ")?;
				high.fmt_mysql(f)
			}
			Self::IsNull {
				expr,
				negated,
				..
			} => {
				expr.fmt_mysql(f)?;
				if *negated {
					write!(f, " IS NOT NULL")
				} else {
					write!(f, " IS NULL")
				}
			}
			Self::IsTruthValue {
				expr,
				negated,
				value,
				..
			} => {
				expr.fmt_mysql(f)?;
				if *negated {
					write!(f, " IS NOT ")?;
				} else {
					write!(f, " IS ")?;
				}
				match value {
					TruthValue::True => write!(f, "TRUE"),
					TruthValue::False => write!(f, "FALSE"),
					TruthValue::Unknown => write!(f, "UNKNOWN"),
				}
			}
			Self::Exists {
				subquery,
				..
			} => {
				write!(f, "EXISTS (")?;
				subquery.fmt_mysql(f)?;
				write!(f, ")")
			}
			Self::Subquery {
				subquery,
				..
			} => {
				write!(f, "(")?;
				subquery.fmt_mysql(f)?;
				write!(f, ")")
			}
			Self::Collate {
				expr,
				collation,
				..
			} => {
				expr.fmt_mysql(f)?;
				write!(f, " COLLATE {collation}")
			}
			Self::Interval {
				expr,
				unit,
				..
			} => {
				write!(f, "INTERVAL ")?;
				expr.fmt_mysql(f)?;
				write!(f, " {unit}")
			}
		}
	}
}

impl ToMySql for SelectItem {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Wildcard(_) => write!(f, "*"),
			Self::QualifiedWildcard(tbl, _) => write!(f, "{}.*", escape_mysql_ident(tbl)),
			Self::Expr {
				expr,
				alias,
				..
			} => {
				expr.fmt_mysql(f)?;
				if let Some(a) = alias {
					write!(f, " AS {}", escape_mysql_ident(a))?;
				}
				Ok(())
			}
		}
	}
}

impl ToMySql for TableRef {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Table {
				name,
				alias,
				..
			} => {
				name.fmt_mysql(f)?;
				if let Some(a) = alias {
					write!(f, " AS {}", escape_mysql_ident(a))?;
				}
				Ok(())
			}
			Self::Subquery {
				subquery,
				alias,
				..
			} => {
				write!(f, "(")?;
				subquery.fmt_mysql(f)?;
				write!(f, ") AS {}", escape_mysql_ident(alias))
			}
			Self::Join {
				left,
				join_type,
				right,
				condition,
				..
			} => {
				left.fmt_mysql(f)?;
				let jt = match join_type {
					JoinType::Inner => " JOIN ",
					JoinType::Left => " LEFT JOIN ",
					JoinType::Right => " RIGHT JOIN ",
					JoinType::Cross => " CROSS JOIN ",
					JoinType::Straight => " STRAIGHT_JOIN ",
				};
				write!(f, "{jt}")?;
				right.fmt_mysql(f)?;
				if let Some(cond) = condition {
					match cond {
						JoinCondition::On(expr) => {
							write!(f, " ON ")?;
							expr.fmt_mysql(f)?;
						}
						JoinCondition::Using(cols) => {
							write!(f, " USING (")?;
							for (i, col) in cols.iter().enumerate() {
								if i > 0 {
									write!(f, ", ")?;
								}
								write!(f, "{}", escape_mysql_ident(col))?;
							}
							write!(f, ")")?;
						}
					}
				}
				Ok(())
			}
		}
	}
}

impl ToMySql for SelectStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "SELECT ")?;
		if self.distinct {
			write!(f, "DISTINCT ")?;
		}
		for (i, p) in self.projection.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			p.fmt_mysql(f)?;
		}
		if let Some(from) = &self.from {
			write!(f, " FROM ")?;
			from.fmt_mysql(f)?;
		}
		if let Some(w) = &self.where_clause {
			write!(f, " WHERE ")?;
			w.fmt_mysql(f)?;
		}
		if !self.group_by.is_empty() {
			write!(f, " GROUP BY ")?;
			for (i, g) in self.group_by.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				g.fmt_mysql(f)?;
			}
		}
		if let Some(h) = &self.having {
			write!(f, " HAVING ")?;
			h.fmt_mysql(f)?;
		}
		if !self.order_by.is_empty() {
			write!(f, " ORDER BY ")?;
			for (i, o) in self.order_by.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				o.expr.fmt_mysql(f)?;
				if o.asc {
					write!(f, " ASC")?;
				} else {
					write!(f, " DESC")?;
				}
			}
		}
		if let Some(lim) = &self.limit {
			write!(f, " LIMIT ")?;
			if let Some(off) = &lim.offset {
				off.fmt_mysql(f)?;
				write!(f, ", ")?;
			}
			lim.count.fmt_mysql(f)?;
		}
		if let Some(lock) = &self.lock {
			match lock.lock_type {
				LockType::ForUpdate => write!(f, " FOR UPDATE")?,
				LockType::ForShare => write!(f, " FOR SHARE")?,
				LockType::LockInShareMode => write!(f, " LOCK IN SHARE MODE")?,
			}
			if lock.nowait {
				write!(f, " NOWAIT")?;
			} else if lock.skip_locked {
				write!(f, " SKIP LOCKED")?;
			}
		}
		Ok(())
	}
}

impl ToMySql for InsertStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "INSERT ")?;
		if self.ignore {
			write!(f, "IGNORE ")?;
		}
		write!(f, "INTO ")?;
		self.table.fmt_mysql(f)?;
		if !self.columns.is_empty() {
			write!(f, " (")?;
			for (i, c) in self.columns.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				write!(f, "{}", escape_mysql_ident(c))?;
			}
			write!(f, ")")?;
		}
		if let Some(q) = &self.query {
			write!(f, " ")?;
			q.fmt_mysql(f)?;
		} else if self.values.is_empty() {
			write!(f, " DEFAULT VALUES")?;
		} else {
			write!(f, " VALUES ")?;
			for (i, row) in self.values.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				write!(f, "(")?;
				for (j, val) in row.iter().enumerate() {
					if j > 0 {
						write!(f, ", ")?;
					}
					val.fmt_mysql(f)?;
				}
				write!(f, ")")?;
			}
		}
		if !self.on_duplicate_key_update.is_empty() {
			write!(f, " ON DUPLICATE KEY UPDATE ")?;
			for (i, (col, val)) in self.on_duplicate_key_update.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				write!(f, "{} = ", escape_mysql_ident(col))?;
				val.fmt_mysql(f)?;
			}
		}
		Ok(())
	}
}

impl ToMySql for ReplaceStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "REPLACE INTO ")?;
		self.table.fmt_mysql(f)?;
		if !self.columns.is_empty() {
			write!(f, " (")?;
			for (i, c) in self.columns.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				write!(f, "{}", escape_mysql_ident(c))?;
			}
			write!(f, ")")?;
		}
		if let Some(q) = &self.query {
			write!(f, " ")?;
			q.fmt_mysql(f)?;
		} else if self.values.is_empty() {
			write!(f, " DEFAULT VALUES")?;
		} else {
			write!(f, " VALUES ")?;
			for (i, row) in self.values.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				write!(f, "(")?;
				for (j, val) in row.iter().enumerate() {
					if j > 0 {
						write!(f, ", ")?;
					}
					val.fmt_mysql(f)?;
				}
				write!(f, ")")?;
			}
		}
		Ok(())
	}
}

impl ToMySql for TruncateStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "TRUNCATE TABLE ")?;
		self.table.fmt_mysql(f)
	}
}

impl ToMySql for UpdateStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "UPDATE ")?;
		self.table.fmt_mysql(f)?;
		write!(f, " SET ")?;
		for (i, (col, val)) in self.assignments.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			write!(f, "{} = ", escape_mysql_ident(col))?;
			val.fmt_mysql(f)?;
		}
		if let Some(w) = &self.where_clause {
			write!(f, " WHERE ")?;
			w.fmt_mysql(f)?;
		}
		if !self.order_by.is_empty() {
			write!(f, " ORDER BY ")?;
			for (i, o) in self.order_by.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				o.expr.fmt_mysql(f)?;
				if o.asc {
					write!(f, " ASC")?;
				} else {
					write!(f, " DESC")?;
				}
			}
		}
		if let Some(lim) = &self.limit {
			write!(f, " LIMIT ")?;
			lim.fmt_mysql(f)?;
		}
		Ok(())
	}
}

impl ToMySql for DeleteStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "DELETE FROM ")?;
		self.table.fmt_mysql(f)?;
		if let Some(w) = &self.where_clause {
			write!(f, " WHERE ")?;
			w.fmt_mysql(f)?;
		}
		if !self.order_by.is_empty() {
			write!(f, " ORDER BY ")?;
			for (i, o) in self.order_by.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				o.expr.fmt_mysql(f)?;
				if o.asc {
					write!(f, " ASC")?;
				} else {
					write!(f, " DESC")?;
				}
			}
		}
		if let Some(lim) = &self.limit {
			write!(f, " LIMIT ")?;
			lim.fmt_mysql(f)?;
		}
		Ok(())
	}
}

impl ToMySql for ColumnDef {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{} {}", escape_mysql_ident(&self.name), self.data_type)?;
		if self.not_null {
			write!(f, " NOT NULL")?;
		}
		if let Some(def) = &self.default {
			write!(f, " DEFAULT ")?;
			def.fmt_mysql(f)?;
		}
		if self.auto_increment {
			write!(f, " AUTO_INCREMENT")?;
		}
		if self.primary_key {
			write!(f, " PRIMARY KEY")?;
		}
		if self.unique {
			write!(f, " UNIQUE")?;
		}
		Ok(())
	}
}

impl ToMySql for CreateTableStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "CREATE ")?;
		if self.temporary {
			write!(f, "TEMPORARY ")?;
		}
		write!(f, "TABLE ")?;
		if self.if_not_exists {
			write!(f, "IF NOT EXISTS ")?;
		}
		self.name.fmt_mysql(f)?;
		write!(f, " (")?;
		let mut first = true;
		for col in &self.columns {
			if !first {
				write!(f, ", ")?;
			}
			first = false;
			col.fmt_mysql(f)?;
		}
		for con in &self.constraints {
			if !first {
				write!(f, ", ")?;
			}
			first = false;
			match con {
				TableConstraint::PrimaryKey {
					columns,
					..
				} => {
					write!(f, "PRIMARY KEY (")?;
					for (i, c) in columns.iter().enumerate() {
						if i > 0 {
							write!(f, ", ")?;
						}
						write!(f, "{}", escape_mysql_ident(c))?;
					}
					write!(f, ")")?;
				}
				TableConstraint::Unique {
					columns,
					..
				} => {
					write!(f, "UNIQUE (")?;
					for (i, c) in columns.iter().enumerate() {
						if i > 0 {
							write!(f, ", ")?;
						}
						write!(f, "{}", escape_mysql_ident(c))?;
					}
					write!(f, ")")?;
				}
				TableConstraint::ForeignKey {
					columns,
					ref_table,
					ref_columns,
					..
				} => {
					write!(f, "FOREIGN KEY (")?;
					for (i, c) in columns.iter().enumerate() {
						if i > 0 {
							write!(f, ", ")?;
						}
						write!(f, "{}", escape_mysql_ident(c))?;
					}
					write!(f, ") REFERENCES ")?;
					ref_table.fmt_mysql(f)?;
					write!(f, " (")?;
					for (i, c) in ref_columns.iter().enumerate() {
						if i > 0 {
							write!(f, ", ")?;
						}
						write!(f, "{}", escape_mysql_ident(c))?;
					}
					write!(f, ")")?;
				}
			}
		}
		write!(f, ")")
	}
}

impl ToMySql for DropTableStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "DROP ")?;
		if self.temporary {
			write!(f, "TEMPORARY ")?;
		}
		write!(f, "TABLE ")?;
		if self.if_exists {
			write!(f, "IF EXISTS ")?;
		}
		for (i, name) in self.names.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			name.fmt_mysql(f)?;
		}
		Ok(())
	}
}

impl ToMySql for UseStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "USE {}", escape_mysql_ident(&self.database))
	}
}

impl ToMySql for SetStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "SET ")?;
		for (i, (target, val)) in self.assignments.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			match target {
				VariableTarget::User(v) => write!(f, "@{v} = ")?,
				VariableTarget::System {
					name,
					is_global,
				} => {
					if *is_global {
						write!(f, "@@global.{name} = ")?;
					} else {
						write!(f, "@@{name} = ")?;
					}
				}
			}
			val.fmt_mysql(f)?;
		}
		Ok(())
	}
}

impl ToMySql for ShowStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Tables {
				database,
				like,
				..
			} => {
				write!(f, "SHOW TABLES")?;
				if let Some(db) = database {
					write!(f, " FROM {}", escape_mysql_ident(db))?;
				}
				if let Some(l) = like {
					write!(f, " LIKE '{l}'")?;
				}
				Ok(())
			}
			Self::Databases {
				like,
				..
			} => {
				write!(f, "SHOW DATABASES")?;
				if let Some(l) = like {
					write!(f, " LIKE '{l}'")?;
				}
				Ok(())
			}
			Self::Columns {
				table,
				like,
				..
			} => {
				write!(f, "SHOW COLUMNS FROM ")?;
				table.fmt_mysql(f)?;
				if let Some(l) = like {
					write!(f, " LIKE '{l}'")?;
				}
				Ok(())
			}
			Self::CreateTable {
				table,
				..
			} => {
				write!(f, "SHOW CREATE TABLE ")?;
				table.fmt_mysql(f)
			}
			Self::Variables {
				is_global,
				like,
				..
			} => {
				if *is_global {
					write!(f, "SHOW GLOBAL VARIABLES")?;
				} else {
					write!(f, "SHOW SESSION VARIABLES")?;
				}
				if let Some(l) = like {
					write!(f, " LIKE '{l}'")?;
				}
				Ok(())
			}
			Self::Status {
				is_global,
				like,
				..
			} => {
				if *is_global {
					write!(f, "SHOW GLOBAL STATUS")?;
				} else {
					write!(f, "SHOW SESSION STATUS")?;
				}
				if let Some(l) = like {
					write!(f, " LIKE '{l}'")?;
				}
				Ok(())
			}
			Self::Processlist {
				full,
				..
			} => {
				if *full {
					write!(f, "SHOW FULL PROCESSLIST")
				} else {
					write!(f, "SHOW PROCESSLIST")
				}
			}
			Self::Warnings {
				..
			} => write!(f, "SHOW WARNINGS"),
			Self::Errors {
				..
			} => write!(f, "SHOW ERRORS"),
		}
	}
}

impl ToMySql for DescribeStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "DESCRIBE ")?;
		self.table.fmt_mysql(f)?;
		if let Some(c) = &self.column {
			write!(f, " {}", escape_mysql_ident(c))?;
		}
		Ok(())
	}
}

impl ToMySql for ExplainStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "EXPLAIN ")?;
		self.statement.fmt_mysql(f)
	}
}

impl ToMySql for TransactionStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Begin(_) => write!(f, "BEGIN"),
			Self::StartTransaction {
				read_only,
				..
			} => {
				if *read_only {
					write!(f, "START TRANSACTION READ ONLY")
				} else {
					write!(f, "START TRANSACTION")
				}
			}
			Self::Commit(_) => write!(f, "COMMIT"),
			Self::Rollback(_) => write!(f, "ROLLBACK"),
		}
	}
}

impl ToMySql for Statement {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Select(s) => s.fmt_mysql(f),
			Self::Insert(s) => s.fmt_mysql(f),
			Self::Update(s) => s.fmt_mysql(f),
			Self::Delete(s) => s.fmt_mysql(f),
			Self::Replace(s) => s.fmt_mysql(f),
			Self::Truncate(s) => s.fmt_mysql(f),
			Self::CreateTable(s) => s.fmt_mysql(f),
			Self::DropTable(s) => s.fmt_mysql(f),
			Self::Use(s) => s.fmt_mysql(f),
			Self::Set(s) => s.fmt_mysql(f),
			Self::Show(s) => s.fmt_mysql(f),
			Self::Describe(s) => s.fmt_mysql(f),
			Self::Explain(s) => s.fmt_mysql(f),
			Self::Transaction(s) => s.fmt_mysql(f),
		}
	}
}
