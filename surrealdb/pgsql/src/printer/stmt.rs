//! PostgreSQL Statement printing and formatting.

use core::fmt;

use crate::ast::stmt::{
	ColumnConstraint, ColumnDef, CreateTableStmt, DeleteStmt, DistinctClause, DropObjectType,
	DropStmt, ExplainStmt, InsertStmt, JoinCondition, JoinType, NullsOrder, OnConflictAction,
	OnConflictClause, OrderByItem, OrderDirection, ResTarget, ResetStmt, SelectStmt, SetClause,
	SetOpKind, SetOperation, SetStmt, ShowStmt, Statement, TableAlias, TableConstraint, TableRef,
	TransactionStmt, UpdateStmt,
};
use crate::printer::ToPgSql;
use crate::printer::expr::print_ident;

impl ToPgSql for ResTarget {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.expr.to_pgsql(f)?;
		if let Some(ref alias) = self.alias {
			f.write_str(" AS ")?;
			print_ident(f, alias)?;
		}
		Ok(())
	}
}

impl ToPgSql for TableAlias {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(" AS ")?;
		print_ident(f, &self.name)?;
		if !self.columns.is_empty() {
			f.write_str("(")?;
			for (i, col) in self.columns.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				print_ident(f, col)?;
			}
			f.write_str(")")?;
		}
		Ok(())
	}
}

impl ToPgSql for OrderByItem {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.expr.to_pgsql(f)?;
		if let Some(dir) = self.direction {
			match dir {
				OrderDirection::Asc => f.write_str(" ASC")?,
				OrderDirection::Desc => f.write_str(" DESC")?,
			}
		}
		if let Some(nulls) = self.nulls {
			match nulls {
				NullsOrder::First => f.write_str(" NULLS FIRST")?,
				NullsOrder::Last => f.write_str(" NULLS LAST")?,
			}
		}
		Ok(())
	}
}

impl ToPgSql for JoinType {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Inner => f.write_str("INNER JOIN"),
			Self::Left => f.write_str("LEFT JOIN"),
			Self::Right => f.write_str("RIGHT JOIN"),
			Self::Full => f.write_str("FULL JOIN"),
			Self::Cross => f.write_str("CROSS JOIN"),
		}
	}
}

impl ToPgSql for JoinCondition {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::On(expr) => {
				f.write_str(" ON ")?;
				expr.to_pgsql(f)
			}
			Self::Using(cols) => {
				f.write_str(" USING (")?;
				for (i, col) in cols.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					print_ident(f, col)?;
				}
				f.write_str(")")
			}
		}
	}
}

impl ToPgSql for TableRef {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Table {
				name,
				alias,
				..
			} => {
				name.to_pgsql(f)?;
				if let Some(a) = alias {
					a.to_pgsql(f)?;
				}
				Ok(())
			}
			Self::Subquery {
				query,
				alias,
				..
			} => {
				f.write_str("(")?;
				query.to_pgsql(f)?;
				f.write_str(")")?;
				alias.to_pgsql(f)
			}
			Self::Join {
				left,
				join_type,
				right,
				condition,
				..
			} => {
				left.to_pgsql(f)?;
				f.write_str(" ")?;
				join_type.to_pgsql(f)?;
				f.write_str(" ")?;
				right.to_pgsql(f)?;
				if let Some(cond) = condition {
					cond.to_pgsql(f)?;
				}
				Ok(())
			}
		}
	}
}

impl ToPgSql for SetOpKind {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Union => f.write_str("UNION"),
			Self::Intersect => f.write_str("INTERSECT"),
			Self::Except => f.write_str("EXCEPT"),
		}
	}
}

impl ToPgSql for SetOperation {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(" ")?;
		self.op.to_pgsql(f)?;
		if self.all {
			f.write_str(" ALL")?;
		}
		f.write_str(" ")?;
		self.right.to_pgsql(f)
	}
}

impl ToPgSql for SelectStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("SELECT")?;

		if let Some(ref distinct) = self.distinct {
			match distinct {
				DistinctClause::All => f.write_str(" DISTINCT")?,
				DistinctClause::On(exprs) => {
					f.write_str(" DISTINCT ON (")?;
					for (i, expr) in exprs.iter().enumerate() {
						if i > 0 {
							f.write_str(", ")?;
						}
						expr.to_pgsql(f)?;
					}
					f.write_str(")")?;
				}
			}
		}

		f.write_str(" ")?;
		for (i, target) in self.targets.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			target.to_pgsql(f)?;
		}

		if !self.from.is_empty() {
			f.write_str(" FROM ")?;
			for (i, r) in self.from.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				r.to_pgsql(f)?;
			}
		}

		if let Some(ref w) = self.where_clause {
			f.write_str(" WHERE ")?;
			w.to_pgsql(f)?;
		}

		if !self.group_by.is_empty() {
			f.write_str(" GROUP BY ")?;
			for (i, g) in self.group_by.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				g.to_pgsql(f)?;
			}
		}

		if let Some(ref h) = self.having {
			f.write_str(" HAVING ")?;
			h.to_pgsql(f)?;
		}

		if !self.order_by.is_empty() {
			f.write_str(" ORDER BY ")?;
			for (i, o) in self.order_by.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				o.to_pgsql(f)?;
			}
		}

		if let Some(ref l) = self.limit {
			f.write_str(" LIMIT ")?;
			l.to_pgsql(f)?;
		}

		if let Some(ref o) = self.offset {
			f.write_str(" OFFSET ")?;
			o.to_pgsql(f)?;
		}

		if let Some(ref s) = self.set_op {
			s.to_pgsql(f)?;
		}

		Ok(())
	}
}

impl ToPgSql for SetClause {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		print_ident(f, &self.column)?;
		f.write_str(" = ")?;
		self.value.to_pgsql(f)
	}
}

impl ToPgSql for OnConflictAction {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::DoNothing => f.write_str("DO NOTHING"),
			Self::DoUpdate(assignments) => {
				f.write_str("DO UPDATE SET ")?;
				for (i, a) in assignments.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					a.to_pgsql(f)?;
				}
				Ok(())
			}
		}
	}
}

impl ToPgSql for OnConflictClause {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(" ON CONFLICT")?;
		if let Some(ref target) = self.target {
			f.write_str(" (")?;
			for (i, col) in target.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				print_ident(f, col)?;
			}
			f.write_str(")")?;
		}
		f.write_str(" ")?;
		self.action.to_pgsql(f)
	}
}

impl ToPgSql for InsertStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("INSERT INTO ")?;
		self.relation.to_pgsql(f)?;

		if !self.columns.is_empty() {
			f.write_str(" (")?;
			for (i, col) in self.columns.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				print_ident(f, col)?;
			}
			f.write_str(")")?;
		}

		if let Some(ref rows) = self.values {
			f.write_str(" VALUES ")?;
			for (r_idx, row) in rows.iter().enumerate() {
				if r_idx > 0 {
					f.write_str(", ")?;
				}
				f.write_str("(")?;
				for (c_idx, val) in row.iter().enumerate() {
					if c_idx > 0 {
						f.write_str(", ")?;
					}
					val.to_pgsql(f)?;
				}
				f.write_str(")")?;
			}
		} else if let Some(ref query) = self.query {
			f.write_str(" ")?;
			query.to_pgsql(f)?;
		}

		if let Some(ref conflict) = self.on_conflict {
			conflict.to_pgsql(f)?;
		}

		if let Some(ref ret) = self.returning {
			f.write_str(" RETURNING ")?;
			for (i, t) in ret.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				t.to_pgsql(f)?;
			}
		}

		Ok(())
	}
}

impl ToPgSql for UpdateStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("UPDATE ")?;
		self.relation.to_pgsql(f)?;
		f.write_str(" SET ")?;

		for (i, a) in self.assignments.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			a.to_pgsql(f)?;
		}

		if let Some(ref from) = self.from {
			f.write_str(" FROM ")?;
			for (i, r) in from.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				r.to_pgsql(f)?;
			}
		}

		if let Some(ref w) = self.where_clause {
			f.write_str(" WHERE ")?;
			w.to_pgsql(f)?;
		}

		if let Some(ref ret) = self.returning {
			f.write_str(" RETURNING ")?;
			for (i, t) in ret.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				t.to_pgsql(f)?;
			}
		}

		Ok(())
	}
}

impl ToPgSql for DeleteStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("DELETE FROM ")?;
		self.relation.to_pgsql(f)?;

		if let Some(ref using) = self.using {
			f.write_str(" USING ")?;
			for (i, r) in using.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				r.to_pgsql(f)?;
			}
		}

		if let Some(ref w) = self.where_clause {
			f.write_str(" WHERE ")?;
			w.to_pgsql(f)?;
		}

		if let Some(ref ret) = self.returning {
			f.write_str(" RETURNING ")?;
			for (i, t) in ret.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				t.to_pgsql(f)?;
			}
		}

		Ok(())
	}
}

impl ToPgSql for ColumnConstraint {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::PrimaryKey => f.write_str("PRIMARY KEY"),
			Self::NotNull => f.write_str("NOT NULL"),
			Self::Null => f.write_str("NULL"),
			Self::Unique => f.write_str("UNIQUE"),
			Self::Default(expr) => {
				f.write_str("DEFAULT ")?;
				expr.to_pgsql(f)
			}
			Self::References {
				table,
				column,
			} => {
				f.write_str("REFERENCES ")?;
				table.to_pgsql(f)?;
				if let Some(col) = column {
					f.write_str("(")?;
					print_ident(f, col)?;
					f.write_str(")")?;
				}
				Ok(())
			}
		}
	}
}

impl ToPgSql for ColumnDef {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		print_ident(f, &self.name)?;
		f.write_str(" ")?;
		self.data_type.to_pgsql(f)?;
		for c in &self.constraints {
			f.write_str(" ")?;
			c.to_pgsql(f)?;
		}
		Ok(())
	}
}

impl ToPgSql for TableConstraint {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::PrimaryKey(cols) => {
				f.write_str("PRIMARY KEY (")?;
				for (i, col) in cols.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					print_ident(f, col)?;
				}
				f.write_str(")")
			}
			Self::Unique(cols) => {
				f.write_str("UNIQUE (")?;
				for (i, col) in cols.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					print_ident(f, col)?;
				}
				f.write_str(")")
			}
			Self::ForeignKey {
				columns,
				ref_table,
				ref_columns,
			} => {
				f.write_str("FOREIGN KEY (")?;
				for (i, col) in columns.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					print_ident(f, col)?;
				}
				f.write_str(") REFERENCES ")?;
				ref_table.to_pgsql(f)?;
				if !ref_columns.is_empty() {
					f.write_str(" (")?;
					for (i, col) in ref_columns.iter().enumerate() {
						if i > 0 {
							f.write_str(", ")?;
						}
						print_ident(f, col)?;
					}
					f.write_str(")")?;
				}
				Ok(())
			}
		}
	}
}

impl ToPgSql for CreateTableStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("CREATE ")?;
		if self.temporary {
			f.write_str("TEMPORARY ")?;
		}
		f.write_str("TABLE ")?;
		if self.if_not_exists {
			f.write_str("IF NOT EXISTS ")?;
		}
		self.name.to_pgsql(f)?;
		f.write_str(" (")?;

		let mut first = true;
		for col in &self.columns {
			if !first {
				f.write_str(", ")?;
			}
			first = false;
			col.to_pgsql(f)?;
		}

		for c in &self.constraints {
			if !first {
				f.write_str(", ")?;
			}
			first = false;
			c.to_pgsql(f)?;
		}

		f.write_str(")")
	}
}

impl ToPgSql for DropObjectType {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Table => f.write_str("TABLE"),
			Self::Index => f.write_str("INDEX"),
			Self::View => f.write_str("VIEW"),
			Self::Schema => f.write_str("SCHEMA"),
			Self::Type => f.write_str("TYPE"),
		}
	}
}

impl ToPgSql for DropStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("DROP ")?;
		self.object_type.to_pgsql(f)?;
		if self.if_exists {
			f.write_str(" IF EXISTS")?;
		}
		f.write_str(" ")?;
		for (i, name) in self.names.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			name.to_pgsql(f)?;
		}
		if self.cascade {
			f.write_str(" CASCADE")?;
		}
		Ok(())
	}
}

impl ToPgSql for TransactionStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Begin {
				..
			} => f.write_str("BEGIN"),
			Self::Commit {
				..
			} => f.write_str("COMMIT"),
			Self::Rollback {
				..
			} => f.write_str("ROLLBACK"),
			Self::Savepoint {
				name,
				..
			} => {
				f.write_str("SAVEPOINT ")?;
				print_ident(f, name)
			}
			Self::ReleaseSavepoint {
				name,
				..
			} => {
				f.write_str("RELEASE SAVEPOINT ")?;
				print_ident(f, name)
			}
			Self::RollbackToSavepoint {
				name,
				..
			} => {
				f.write_str("ROLLBACK TO SAVEPOINT ")?;
				print_ident(f, name)
			}
		}
	}
}

impl ToPgSql for SetStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("SET ")?;
		print_ident(f, &self.name)?;
		f.write_str(" TO ")?;
		f.write_str(&self.value)
	}
}

impl ToPgSql for ShowStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("SHOW ")?;
		print_ident(f, &self.name)
	}
}

impl ToPgSql for ResetStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("RESET ")?;
		if self.name.eq_ignore_ascii_case("ALL") {
			f.write_str("ALL")
		} else {
			print_ident(f, &self.name)
		}
	}
}

impl ToPgSql for ExplainStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("EXPLAIN")?;
		if !self.options.is_empty() {
			f.write_str(" (")?;
			for (i, opt) in self.options.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				f.write_str(opt)?;
			}
			f.write_str(")")?;
		}
		f.write_str(" ")?;
		self.statement.to_pgsql(f)
	}
}

impl ToPgSql for Statement {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Select(s) => s.to_pgsql(f),
			Self::Insert(s) => s.to_pgsql(f),
			Self::Update(s) => s.to_pgsql(f),
			Self::Delete(s) => s.to_pgsql(f),
			Self::CreateTable(s) => s.to_pgsql(f),
			Self::Drop(s) => s.to_pgsql(f),
			Self::Transaction(tx) => tx.to_pgsql(f),
			Self::Set(s) => s.to_pgsql(f),
			Self::Show(s) => s.to_pgsql(f),
			Self::Reset(s) => s.to_pgsql(f),
			Self::Explain(s) => s.to_pgsql(f),
			Self::Empty => Ok(()),
		}
	}
}
