//! Pretty-printer formatting MySQL AST nodes back into standard MySQL SQL.

use crate::ast::expr::{
	ColumnRef, Expr, TruthValue, WindowFrame, WindowFrameBound, WindowFrameExclusion,
	WindowFrameUnits, WindowSpec,
};
use crate::ast::stmt::{
	AlterTableAction, AlterTableStmt, ColumnDef, CommonTableExpr, CreateDatabaseStmt,
	CreateIndexStmt, CreateTableStmt, CreateUserStmt, CreateViewStmt, DeallocateStmt, DeleteStmt,
	DescribeStmt, DropDatabaseStmt, DropIndexStmt, DropTableStmt, DropUserStmt, DropViewStmt,
	ExecuteStmt, ExplainStmt, GrantStmt, IndexHint, IndexHintType, InsertStmt, JoinCondition,
	JoinType, LockTablesStmt, LockType, PrepareStmt, QualifiedName, ReplaceStmt, RevokeStmt,
	SelectItem, SelectStmt, SetOpKind, SetOperation, SetStmt, ShowStmt, Statement, TableConstraint,
	TableLockType, TableRef, TransactionStmt, TruncateStmt, UnlockTablesStmt, UpdateStmt, UseStmt,
	VariableTarget, WindowDef, WithClause,
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
				over,
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
				write!(f, ")")?;
				if let Some(spec) = over {
					write!(f, " OVER ")?;
					spec.fmt_mysql(f)?;
				}
				Ok(())
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
			Self::Quantified {
				expr,
				op,
				quantifier,
				subquery,
				..
			} => {
				expr.fmt_mysql(f)?;
				write!(f, " {} {} (", op.as_str(), quantifier.as_str())?;
				subquery.fmt_mysql(f)?;
				write!(f, ")")
			}
		}
	}
}

impl ToMySql for SelectItem {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Wildcard(_) => write!(f, "*"),
			Self::QualifiedWildcard(parts, _) => {
				for part in parts {
					write!(f, "{}.", escape_mysql_ident(part))?;
				}
				write!(f, "*")
			}
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

impl ToMySql for IndexHint {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let ht = match self.hint_type {
			IndexHintType::Use => "USE INDEX",
			IndexHintType::Force => "FORCE INDEX",
			IndexHintType::Ignore => "IGNORE INDEX",
		};
		write!(f, "{ht} (")?;
		for (i, idx) in self.indexes.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			write!(f, "{}", escape_mysql_ident(idx))?;
		}
		write!(f, ")")
	}
}

impl ToMySql for TableRef {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Table {
				name,
				alias,
				index_hints,
				..
			} => {
				name.fmt_mysql(f)?;
				if let Some(a) = alias {
					write!(f, " AS {}", escape_mysql_ident(a))?;
				}
				for hint in index_hints {
					write!(f, " ")?;
					hint.fmt_mysql(f)?;
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

impl ToMySql for SetOperation {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self.op {
			SetOpKind::Union => write!(f, " UNION ")?,
			SetOpKind::Intersect => write!(f, " INTERSECT ")?,
			SetOpKind::Except => write!(f, " EXCEPT ")?,
		}
		if self.all {
			write!(f, "ALL ")?;
		}
		self.right.fmt_mysql(f)
	}
}

impl ToMySql for WithClause {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if self.recursive {
			write!(f, "WITH RECURSIVE ")?;
		} else {
			write!(f, "WITH ")?;
		}
		for (i, cte) in self.ctes.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			cte.fmt_mysql(f)?;
		}
		Ok(())
	}
}

impl ToMySql for CommonTableExpr {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.name)?;
		if !self.columns.is_empty() {
			write!(f, " (")?;
			for (i, c) in self.columns.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				write!(f, "{c}")?;
			}
			write!(f, ")")?;
		}
		write!(f, " AS (")?;
		self.query.fmt_mysql(f)?;
		write!(f, ")")
	}
}

impl ToMySql for SelectStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(with) = &self.with {
			with.fmt_mysql(f)?;
			write!(f, " ")?;
		}
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
			if self.with_rollup {
				write!(f, " WITH ROLLUP")?;
			}
		}
		if let Some(h) = &self.having {
			write!(f, " HAVING ")?;
			h.fmt_mysql(f)?;
		}
		if !self.window.is_empty() {
			write!(f, " WINDOW ")?;
			for (i, w) in self.window.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				w.fmt_mysql(f)?;
			}
		}
		if let Some(set_op) = &self.set_op {
			set_op.fmt_mysql(f)?;
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
				full,
				database,
				like,
				..
			} => {
				if *full {
					write!(f, "SHOW FULL TABLES")?;
				} else {
					write!(f, "SHOW TABLES")?;
				}
				if let Some(db) = database {
					write!(f, " FROM {}", escape_mysql_ident(db))?;
				}
				if let Some(l) = like {
					write!(f, " LIKE '{l}'")?;
				}
				Ok(())
			}
			Self::Triggers {
				database,
				like,
				..
			} => {
				write!(f, "SHOW TRIGGERS")?;
				if let Some(db) = database {
					write!(f, " FROM {}", escape_mysql_ident(db))?;
				}
				if let Some(l) = like {
					write!(f, " LIKE '{l}'")?;
				}
				Ok(())
			}
			Self::TableStatus {
				database,
				like,
				..
			} => {
				write!(f, "SHOW TABLE STATUS")?;
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
				full,
				table,
				like,
				..
			} => {
				if *full {
					write!(f, "SHOW FULL COLUMNS FROM ")?;
				} else {
					write!(f, "SHOW COLUMNS FROM ")?;
				}
				table.fmt_mysql(f)?;
				if let Some(l) = like {
					write!(f, " LIKE '{l}'")?;
				}
				Ok(())
			}
			Self::Index {
				table,
				..
			} => {
				write!(f, "SHOW INDEX FROM ")?;
				table.fmt_mysql(f)
			}
			Self::Engines {
				..
			} => write!(f, "SHOW ENGINES"),
			Self::Collation {
				like,
				..
			} => {
				write!(f, "SHOW COLLATION")?;
				if let Some(l) = like {
					write!(f, " LIKE '{l}'")?;
				}
				Ok(())
			}
			Self::Charset {
				like,
				..
			} => {
				write!(f, "SHOW CHARACTER SET")?;
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
			Self::Savepoint {
				name,
				..
			} => write!(f, "SAVEPOINT `{name}`"),
			Self::ReleaseSavepoint {
				name,
				..
			} => write!(f, "RELEASE SAVEPOINT `{name}`"),
			Self::RollbackToSavepoint {
				name,
				..
			} => write!(f, "ROLLBACK TO SAVEPOINT `{name}`"),
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
			Self::CreateDatabase(s) => s.fmt_mysql(f),
			Self::DropDatabase(s) => s.fmt_mysql(f),
			Self::CreateIndex(s) => s.fmt_mysql(f),
			Self::DropIndex(s) => s.fmt_mysql(f),
			Self::CreateView(s) => s.fmt_mysql(f),
			Self::DropView(s) => s.fmt_mysql(f),
			Self::AlterTable(s) => s.fmt_mysql(f),
			Self::CreateUser(s) => s.fmt_mysql(f),
			Self::DropUser(s) => s.fmt_mysql(f),
			Self::Grant(s) => s.fmt_mysql(f),
			Self::Revoke(s) => s.fmt_mysql(f),
			Self::LockTables(s) => s.fmt_mysql(f),
			Self::UnlockTables(s) => s.fmt_mysql(f),
			Self::Prepare(s) => s.fmt_mysql(f),
			Self::Execute(s) => s.fmt_mysql(f),
			Self::Deallocate(s) => s.fmt_mysql(f),
			Self::Use(s) => s.fmt_mysql(f),
			Self::Set(s) => s.fmt_mysql(f),
			Self::Show(s) => s.fmt_mysql(f),
			Self::Describe(s) => s.fmt_mysql(f),
			Self::Explain(s) => s.fmt_mysql(f),
			Self::Transaction(s) => s.fmt_mysql(f),
		}
	}
}

impl ToMySql for CreateDatabaseStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "CREATE DATABASE ")?;
		if self.if_not_exists {
			write!(f, "IF NOT EXISTS ")?;
		}
		write!(f, "{}", escape_mysql_ident(&self.name))
	}
}

impl ToMySql for DropDatabaseStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "DROP DATABASE ")?;
		if self.if_exists {
			write!(f, "IF EXISTS ")?;
		}
		write!(f, "{}", escape_mysql_ident(&self.name))
	}
}

impl ToMySql for CreateIndexStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "CREATE ")?;
		if self.unique {
			write!(f, "UNIQUE ")?;
		}
		write!(f, "INDEX {} ON ", escape_mysql_ident(&self.name))?;
		self.table.fmt_mysql(f)?;
		write!(f, " (")?;
		for (i, c) in self.columns.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			write!(f, "{}", escape_mysql_ident(c))?;
		}
		write!(f, ")")
	}
}

impl ToMySql for DropIndexStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "DROP INDEX {} ON ", escape_mysql_ident(&self.name))?;
		self.table.fmt_mysql(f)
	}
}

impl ToMySql for CreateViewStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if self.or_replace {
			write!(f, "CREATE OR REPLACE VIEW ")?;
		} else {
			write!(f, "CREATE VIEW ")?;
		}
		self.name.fmt_mysql(f)?;
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
		write!(f, " AS ")?;
		self.query.fmt_mysql(f)
	}
}

impl ToMySql for DropViewStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "DROP VIEW ")?;
		if self.if_exists {
			write!(f, "IF EXISTS ")?;
		}
		for (i, n) in self.names.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			n.fmt_mysql(f)?;
		}
		Ok(())
	}
}

impl ToMySql for AlterTableStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "ALTER TABLE ")?;
		self.table.fmt_mysql(f)?;
		for (i, action) in self.actions.iter().enumerate() {
			if i > 0 {
				write!(f, ",")?;
			}
			write!(f, " ")?;
			match action {
				AlterTableAction::AddColumn(col) => {
					write!(f, "ADD COLUMN ")?;
					col.fmt_mysql(f)?;
				}
				AlterTableAction::DropColumn {
					name,
				} => {
					write!(f, "DROP COLUMN {}", escape_mysql_ident(name))?;
				}
				AlterTableAction::ModifyColumn(col) => {
					write!(f, "MODIFY COLUMN ")?;
					col.fmt_mysql(f)?;
				}
				AlterTableAction::ChangeColumn {
					old_name,
					new_def,
				} => {
					write!(f, "CHANGE COLUMN {} ", escape_mysql_ident(old_name))?;
					new_def.fmt_mysql(f)?;
				}
				AlterTableAction::AddConstraint(c) => {
					write!(f, "ADD ")?;
					match c {
						TableConstraint::PrimaryKey {
							columns,
							..
						} => {
							write!(f, "PRIMARY KEY (")?;
							for (j, col) in columns.iter().enumerate() {
								if j > 0 {
									write!(f, ", ")?;
								}
								write!(f, "{}", escape_mysql_ident(col))?;
							}
							write!(f, ")")?;
						}
						TableConstraint::Unique {
							name,
							columns,
							..
						} => {
							write!(f, "UNIQUE ")?;
							if let Some(n) = name {
								write!(f, "{} ", escape_mysql_ident(n))?;
							}
							write!(f, "(")?;
							for (j, col) in columns.iter().enumerate() {
								if j > 0 {
									write!(f, ", ")?;
								}
								write!(f, "{}", escape_mysql_ident(col))?;
							}
							write!(f, ")")?;
						}
						TableConstraint::ForeignKey {
							name,
							columns,
							ref_table,
							ref_columns,
							..
						} => {
							if let Some(n) = name {
								write!(f, "CONSTRAINT {} ", escape_mysql_ident(n))?;
							}
							write!(f, "FOREIGN KEY (")?;
							for (j, col) in columns.iter().enumerate() {
								if j > 0 {
									write!(f, ", ")?;
								}
								write!(f, "{}", escape_mysql_ident(col))?;
							}
							write!(f, ") REFERENCES ")?;
							ref_table.fmt_mysql(f)?;
							write!(f, " (")?;
							for (j, col) in ref_columns.iter().enumerate() {
								if j > 0 {
									write!(f, ", ")?;
								}
								write!(f, "{}", escape_mysql_ident(col))?;
							}
							write!(f, ")")?;
						}
					}
				}
				AlterTableAction::DropPrimaryKey => {
					write!(f, "DROP PRIMARY KEY")?;
				}
				AlterTableAction::DropIndex {
					name,
				} => {
					write!(f, "DROP INDEX {}", escape_mysql_ident(name))?;
				}
				AlterTableAction::RenameTable {
					new_name,
				} => {
					write!(f, "RENAME TO ")?;
					new_name.fmt_mysql(f)?;
				}
			}
		}
		Ok(())
	}
}

impl ToMySql for CreateUserStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "CREATE USER ")?;
		if self.if_not_exists {
			write!(f, "IF NOT EXISTS ")?;
		}
		write!(f, "'{}'", self.user)?;
		if let Some(ref h) = self.host {
			write!(f, "@'{h}'")?;
		}
		if let Some(ref p) = self.identified_by {
			write!(f, " IDENTIFIED BY '{p}'")?;
		}
		Ok(())
	}
}

impl ToMySql for DropUserStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "DROP USER ")?;
		if self.if_exists {
			write!(f, "IF EXISTS ")?;
		}
		for (i, (user, host)) in self.users.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			write!(f, "'{user}'")?;
			if let Some(h) = host {
				write!(f, "@'{h}'")?;
			}
		}
		Ok(())
	}
}

impl ToMySql for GrantStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "GRANT ")?;
		for (i, p) in self.privileges.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			write!(f, "{p}")?;
		}
		if let Some(ref obj) = self.object {
			write!(f, " ON {obj}")?;
		}
		write!(f, " TO '{}'", self.user)?;
		if let Some(ref h) = self.host {
			write!(f, "@'{h}'")?;
		}
		Ok(())
	}
}

impl ToMySql for RevokeStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "REVOKE ")?;
		for (i, p) in self.privileges.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			write!(f, "{p}")?;
		}
		if let Some(ref obj) = self.object {
			write!(f, " ON {obj}")?;
		}
		write!(f, " FROM '{}'", self.user)?;
		if let Some(ref h) = self.host {
			write!(f, "@'{h}'")?;
		}
		Ok(())
	}
}

impl ToMySql for LockTablesStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "LOCK TABLES ")?;
		for (i, lock) in self.locks.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			lock.table.fmt_mysql(f)?;
			match lock.lock_type {
				TableLockType::Read => write!(f, " READ")?,
				TableLockType::Write => write!(f, " WRITE")?,
			}
		}
		Ok(())
	}
}

impl ToMySql for UnlockTablesStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "UNLOCK TABLES")
	}
}

impl ToMySql for PrepareStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "PREPARE {} FROM '{}'", escape_mysql_ident(&self.name), self.query)
	}
}

impl ToMySql for ExecuteStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "EXECUTE {}", escape_mysql_ident(&self.name))?;
		if !self.using.is_empty() {
			write!(f, " USING ")?;
			for (i, u) in self.using.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				u.fmt_mysql(f)?;
			}
		}
		Ok(())
	}
}

impl ToMySql for DeallocateStmt {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "DEALLOCATE PREPARE {}", escape_mysql_ident(&self.name))
	}
}

impl ToMySql for WindowDef {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{} AS ", self.name)?;
		self.spec.fmt_mysql(f)
	}
}

impl ToMySql for WindowSpec {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "(")?;
		let mut has_prev = false;

		if let Some(ref name) = self.window_name {
			write!(f, "{name}")?;
			has_prev = true;
		}

		if !self.partition_by.is_empty() {
			if has_prev {
				write!(f, " ")?;
			}
			write!(f, "PARTITION BY ")?;
			for (i, expr) in self.partition_by.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				expr.fmt_mysql(f)?;
			}
			has_prev = true;
		}

		if !self.order_by.is_empty() {
			if has_prev {
				write!(f, " ")?;
			}
			write!(f, "ORDER BY ")?;
			for (i, item) in self.order_by.iter().enumerate() {
				if i > 0 {
					write!(f, ", ")?;
				}
				item.expr.fmt_mysql(f)?;
				if item.asc {
					write!(f, " ASC")?;
				} else {
					write!(f, " DESC")?;
				}
			}
			has_prev = true;
		}

		if let Some(ref frame) = self.frame {
			if has_prev {
				write!(f, " ")?;
			}
			frame.fmt_mysql(f)?;
		}

		write!(f, ")")
	}
}

impl ToMySql for WindowFrame {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self.units {
			WindowFrameUnits::Rows => write!(f, "ROWS ")?,
			WindowFrameUnits::Range => write!(f, "RANGE ")?,
			WindowFrameUnits::Groups => write!(f, "GROUPS ")?,
		}

		if let Some(ref end) = self.end {
			write!(f, "BETWEEN ")?;
			self.start.fmt_mysql(f)?;
			write!(f, " AND ")?;
			end.fmt_mysql(f)?;
		} else {
			self.start.fmt_mysql(f)?;
		}

		if let Some(ref excl) = self.exclusion {
			write!(f, " ")?;
			match excl {
				WindowFrameExclusion::CurrentRow => write!(f, "EXCLUDE CURRENT ROW")?,
				WindowFrameExclusion::Group => write!(f, "EXCLUDE GROUP")?,
				WindowFrameExclusion::Ties => write!(f, "EXCLUDE TIES")?,
				WindowFrameExclusion::NoOthers => write!(f, "EXCLUDE NO OTHERS")?,
			}
		}

		Ok(())
	}
}

impl ToMySql for WindowFrameBound {
	fn fmt_mysql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::UnboundedPreceding => write!(f, "UNBOUNDED PRECEDING"),
			Self::UnboundedFollowing => write!(f, "UNBOUNDED FOLLOWING"),
			Self::CurrentRow => write!(f, "CURRENT ROW"),
			Self::Preceding(expr) => {
				expr.fmt_mysql(f)?;
				write!(f, " PRECEDING")
			}
			Self::Following(expr) => {
				expr.fmt_mysql(f)?;
				write!(f, " FOLLOWING")
			}
		}
	}
}
