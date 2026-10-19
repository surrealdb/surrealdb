//! PostgreSQL Statement printing and formatting.

use core::fmt;

use crate::ast::expr::{NullsOrder, OrderByItem, OrderDirection};
use crate::ast::graph::{
	CreatePropertyGraphStmt, DropPropertyGraphStmt, EdgeTableDef, GraphColumnDef,
	GraphEdgeDirection, GraphEdgePattern, GraphElementPattern, GraphPathPattern, GraphPattern,
	GraphQuantifier, GraphVertexPattern, VertexTableDef,
};
use crate::ast::json::{JsonTable, JsonTableColumn};
use crate::ast::stmt::{
	AlterTableAction, AlterTableStmt, ColumnConstraint, ColumnDef, CommonTableExpr, CopyDirection,
	CopyFormat, CopyStmt, CopyTarget, CreateIndexStmt, CreateSchemaStmt, CreateTableStmt,
	CreateViewStmt, DeallocateStmt, DeleteStmt, DistinctClause, DropObjectType, DropStmt,
	ExecuteStmt, ExplainStmt, IndexElem, InsertStmt, IsolationLevel, JoinCondition, JoinType,
	LockingClause, LockingStrength, LockingWaitPolicy, OnConflictAction, OnConflictClause,
	PrepareStmt, ResTarget, ResetStmt, SelectStmt, SetClause, SetOpKind, SetOperation, SetStmt,
	ShowStmt, Statement, TableAlias, TableConstraint, TableRef, TransactionStmt, TruncateStmt,
	UpdateStmt, WindowDef, WithClause,
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
			Self::GraphTable {
				graph_name,
				pattern,
				columns,
				alias,
				..
			} => {
				f.write_str("GRAPH_TABLE(")?;
				graph_name.to_pgsql(f)?;
				f.write_str(", MATCH ")?;
				pattern.to_pgsql(f)?;
				if !columns.is_empty() {
					f.write_str(" COLUMNS (")?;
					for (i, col) in columns.iter().enumerate() {
						if i > 0 {
							f.write_str(", ")?;
						}
						col.to_pgsql(f)?;
					}
					f.write_str(")")?;
				}
				f.write_str(")")?;
				if let Some(a) = alias {
					a.to_pgsql(f)?;
				}
				Ok(())
			}
			Self::JsonTable(jt) => jt.to_pgsql(f),
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

impl ToPgSql for WithClause {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("WITH ")?;
		if self.recursive {
			f.write_str("RECURSIVE ")?;
		}
		for (i, cte) in self.ctes.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			cte.to_pgsql(f)?;
		}
		Ok(())
	}
}

impl ToPgSql for CommonTableExpr {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		print_ident(f, &self.name)?;
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
		f.write_str(" AS ")?;
		if let Some(mat) = self.materialized {
			if mat {
				f.write_str("MATERIALIZED ")?;
			} else {
				f.write_str("NOT MATERIALIZED ")?;
			}
		}
		f.write_str("(")?;
		self.query.to_pgsql(f)?;
		f.write_str(")")
	}
}

impl ToPgSql for WindowDef {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		print_ident(f, &self.name)?;
		f.write_str(" AS ")?;
		self.spec.to_pgsql(f)
	}
}

impl ToPgSql for LockingClause {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self.strength {
			LockingStrength::Update => f.write_str("FOR UPDATE")?,
			LockingStrength::NoKeyUpdate => f.write_str("FOR NO KEY UPDATE")?,
			LockingStrength::Share => f.write_str("FOR SHARE")?,
			LockingStrength::KeyShare => f.write_str("FOR KEY SHARE")?,
		}
		if !self.tables.is_empty() {
			f.write_str(" OF ")?;
			for (i, table) in self.tables.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				table.to_pgsql(f)?;
			}
		}
		match self.wait_policy {
			LockingWaitPolicy::Block => {}
			LockingWaitPolicy::NoWait => f.write_str(" NOWAIT")?,
			LockingWaitPolicy::SkipLocked => f.write_str(" SKIP LOCKED")?,
		}
		Ok(())
	}
}

impl ToPgSql for SelectStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if let Some(ref w) = self.with {
			w.to_pgsql(f)?;
			f.write_str(" ")?;
		}

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

		if !self.window.is_empty() {
			f.write_str(" WINDOW ")?;
			for (i, w) in self.window.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				w.to_pgsql(f)?;
			}
		}

		if let Some(ref s) = self.set_op {
			s.to_pgsql(f)?;
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

		for lock in &self.locking {
			f.write_str(" ")?;
			lock.to_pgsql(f)?;
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
		if let Some(ref w) = self.with {
			w.to_pgsql(f)?;
			f.write_str(" ")?;
		}

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
		if let Some(ref w) = self.with {
			w.to_pgsql(f)?;
			f.write_str(" ")?;
		}

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
		if let Some(ref w) = self.with {
			w.to_pgsql(f)?;
			f.write_str(" ")?;
		}

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

impl ToPgSql for TruncateStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("TRUNCATE TABLE ")?;
		self.table.to_pgsql(f)
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
				read_only,
				isolation,
				..
			} => {
				f.write_str("BEGIN")?;
				if let Some(isolation) = isolation {
					f.write_str(" ISOLATION LEVEL ")?;
					f.write_str(match isolation {
						IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
						IsolationLevel::ReadCommitted => "READ COMMITTED",
						IsolationLevel::RepeatableRead => "REPEATABLE READ",
						IsolationLevel::Serializable => "SERIALIZABLE",
					})?;
				}
				if *read_only {
					f.write_str(" READ ONLY")?;
				}
				Ok(())
			}
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
		if self.is_local {
			f.write_str("SET LOCAL ")?;
		} else {
			f.write_str("SET ")?;
		}
		print_ident(f, &self.name)?;
		f.write_str(" TO ")?;
		f.write_str(&self.value)
	}
}

impl ToPgSql for ShowStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("SHOW ")?;
		if self.name.eq_ignore_ascii_case("ALL") {
			f.write_str("ALL")
		} else {
			print_ident(f, &self.name)
		}
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

impl ToPgSql for IndexElem {
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

impl ToPgSql for CreateIndexStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("CREATE ")?;
		if self.unique {
			f.write_str("UNIQUE ")?;
		}
		f.write_str("INDEX ")?;
		if self.concurrently {
			f.write_str("CONCURRENTLY ")?;
		}
		if self.if_not_exists {
			f.write_str("IF NOT EXISTS ")?;
		}
		if let Some(ref name) = self.name {
			print_ident(f, name)?;
			f.write_str(" ")?;
		}
		f.write_str("ON ")?;
		self.table.to_pgsql(f)?;
		if let Some(ref method) = self.method {
			f.write_str(" USING ")?;
			print_ident(f, method)?;
		}
		f.write_str(" (")?;
		for (i, elem) in self.columns.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			elem.to_pgsql(f)?;
		}
		f.write_str(")")?;
		if let Some(ref pred) = self.predicate {
			f.write_str(" WHERE ")?;
			pred.to_pgsql(f)?;
		}
		Ok(())
	}
}

impl ToPgSql for CreateViewStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("CREATE ")?;
		if self.or_replace {
			f.write_str("OR REPLACE ")?;
		}
		if self.temporary {
			f.write_str("TEMPORARY ")?;
		}
		if self.recursive {
			f.write_str("RECURSIVE ")?;
		}
		f.write_str("VIEW ")?;
		self.name.to_pgsql(f)?;
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
		f.write_str(" AS ")?;
		self.query.to_pgsql(f)
	}
}

impl ToPgSql for CreateSchemaStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("CREATE SCHEMA ")?;
		if self.if_not_exists {
			f.write_str("IF NOT EXISTS ")?;
		}
		print_ident(f, &self.name)
	}
}

impl ToPgSql for AlterTableAction {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::AddColumn {
				if_not_exists,
				column,
			} => {
				f.write_str("ADD COLUMN ")?;
				if *if_not_exists {
					f.write_str("IF NOT EXISTS ")?;
				}
				column.to_pgsql(f)
			}
			Self::DropColumn {
				if_exists,
				name,
				cascade,
			} => {
				f.write_str("DROP COLUMN ")?;
				if *if_exists {
					f.write_str("IF EXISTS ")?;
				}
				print_ident(f, name)?;
				if *cascade {
					f.write_str(" CASCADE")?;
				}
				Ok(())
			}
			Self::AlterColumnType {
				name,
				data_type,
			} => {
				f.write_str("ALTER COLUMN ")?;
				print_ident(f, name)?;
				f.write_str(" TYPE ")?;
				data_type.to_pgsql(f)
			}
			Self::SetDefault {
				column,
				default,
			} => {
				f.write_str("ALTER COLUMN ")?;
				print_ident(f, column)?;
				f.write_str(" SET DEFAULT ")?;
				default.to_pgsql(f)
			}
			Self::DropDefault {
				column,
			} => {
				f.write_str("ALTER COLUMN ")?;
				print_ident(f, column)?;
				f.write_str(" DROP DEFAULT")
			}
			Self::SetNotNull {
				column,
			} => {
				f.write_str("ALTER COLUMN ")?;
				print_ident(f, column)?;
				f.write_str(" SET NOT NULL")
			}
			Self::DropNotNull {
				column,
			} => {
				f.write_str("ALTER COLUMN ")?;
				print_ident(f, column)?;
				f.write_str(" DROP NOT NULL")
			}
			Self::AddConstraint(tc) => {
				f.write_str("ADD ")?;
				tc.to_pgsql(f)
			}
			Self::DropConstraint {
				if_exists,
				name,
				cascade,
			} => {
				f.write_str("DROP CONSTRAINT ")?;
				if *if_exists {
					f.write_str("IF EXISTS ")?;
				}
				print_ident(f, name)?;
				if *cascade {
					f.write_str(" CASCADE")?;
				}
				Ok(())
			}
			Self::RenameColumn {
				old_name,
				new_name,
			} => {
				f.write_str("RENAME COLUMN ")?;
				print_ident(f, old_name)?;
				f.write_str(" TO ")?;
				print_ident(f, new_name)
			}
			Self::RenameTable {
				new_name,
			} => {
				f.write_str("RENAME TO ")?;
				print_ident(f, new_name)
			}
		}
	}
}

impl ToPgSql for AlterTableStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("ALTER TABLE ")?;
		if self.if_exists {
			f.write_str("IF EXISTS ")?;
		}
		self.name.to_pgsql(f)?;
		f.write_str(" ")?;
		for (i, action) in self.actions.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			action.to_pgsql(f)?;
		}
		Ok(())
	}
}

impl ToPgSql for PrepareStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("PREPARE ")?;
		print_ident(f, &self.name)?;
		if !self.param_types.is_empty() {
			f.write_str(" (")?;
			for (i, t) in self.param_types.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				t.to_pgsql(f)?;
			}
			f.write_str(")")?;
		}
		f.write_str(" AS ")?;
		self.statement.to_pgsql(f)
	}
}

impl ToPgSql for ExecuteStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("EXECUTE ")?;
		print_ident(f, &self.name)?;
		if !self.params.is_empty() {
			f.write_str(" (")?;
			for (i, p) in self.params.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				p.to_pgsql(f)?;
			}
			f.write_str(")")?;
		}
		Ok(())
	}
}

impl ToPgSql for DeallocateStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("DEALLOCATE ")?;
		if let Some(ref name) = self.name {
			print_ident(f, name)
		} else {
			f.write_str("ALL")
		}
	}
}

impl ToPgSql for Statement {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Select(s) => s.to_pgsql(f),
			Self::Insert(s) => s.to_pgsql(f),
			Self::Update(s) => s.to_pgsql(f),
			Self::Delete(s) => s.to_pgsql(f),
			Self::Truncate(s) => s.to_pgsql(f),
			Self::CreateTable(s) => s.to_pgsql(f),
			Self::CreateIndex(s) => s.to_pgsql(f),
			Self::CreateView(s) => s.to_pgsql(f),
			Self::AlterTable(s) => s.to_pgsql(f),
			Self::CreateSchema(s) => s.to_pgsql(f),
			Self::Drop(s) => s.to_pgsql(f),
			Self::Transaction(tx) => tx.to_pgsql(f),
			Self::Set(s) => s.to_pgsql(f),
			Self::Show(s) => s.to_pgsql(f),
			Self::Reset(s) => s.to_pgsql(f),
			Self::Explain(s) => s.to_pgsql(f),
			Self::Prepare(s) => s.to_pgsql(f),
			Self::Execute(s) => s.to_pgsql(f),
			Self::Deallocate(s) => s.to_pgsql(f),
			Self::CreatePropertyGraph(s) => s.to_pgsql(f),
			Self::DropPropertyGraph(s) => s.to_pgsql(f),
			Self::Copy(s) => s.to_pgsql(f),
			Self::Empty => Ok(()),
		}
	}
}

impl ToPgSql for CopyStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("COPY ")?;
		match &self.target {
			CopyTarget::Table {
				table,
				columns,
			} => {
				table.to_pgsql(f)?;
				if !columns.is_empty() {
					f.write_str(" (")?;
					for (i, col) in columns.iter().enumerate() {
						if i > 0 {
							f.write_str(", ")?;
						}
						print_ident(f, col)?;
					}
					f.write_str(")")?;
				}
			}
			CopyTarget::Query(q) => {
				f.write_str("(")?;
				q.to_pgsql(f)?;
				f.write_str(")")?;
			}
		}
		match self.direction {
			CopyDirection::FromStdin => f.write_str(" FROM STDIN")?,
			CopyDirection::ToStdout => f.write_str(" TO STDOUT")?,
		}
		let mut opts = Vec::new();
		match self.options.format {
			CopyFormat::Text => {}
			CopyFormat::Csv => opts.push("FORMAT CSV".to_string()),
			CopyFormat::Binary => opts.push("FORMAT BINARY".to_string()),
		}
		if let Some(h) = self.options.header {
			opts.push(format!("HEADER {h}"));
		}
		if let Some(d) = self.options.delimiter {
			opts.push(format!("DELIMITER '{}'", d as char));
		}
		if let Some(ref n) = self.options.null {
			opts.push(format!("NULL '{n}'"));
		}
		if !opts.is_empty() {
			f.write_str(" WITH (")?;
			for (i, opt) in opts.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				f.write_str(opt)?;
			}
			f.write_str(")")?;
		}
		Ok(())
	}
}

impl ToPgSql for CreatePropertyGraphStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("CREATE PROPERTY GRAPH ")?;
		if self.if_not_exists {
			f.write_str("IF NOT EXISTS ")?;
		}
		self.name.to_pgsql(f)?;
		f.write_str(" VERTEX TABLES (")?;
		for (i, vt) in self.vertex_tables.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			vt.to_pgsql(f)?;
		}
		f.write_str(")")?;
		if !self.edge_tables.is_empty() {
			f.write_str(" EDGE TABLES (")?;
			for (i, et) in self.edge_tables.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				et.to_pgsql(f)?;
			}
			f.write_str(")")?;
		}
		Ok(())
	}
}

impl ToPgSql for VertexTableDef {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.table_name.to_pgsql(f)?;
		if !self.key_columns.is_empty() {
			f.write_str(" KEY (")?;
			for (i, col) in self.key_columns.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				print_ident(f, col)?;
			}
			f.write_str(")")?;
		}
		if let Some(ref l) = self.label {
			f.write_str(" LABEL ")?;
			print_ident(f, l)?;
		}
		if !self.properties.is_empty() {
			f.write_str(" PROPERTIES (")?;
			for (i, prop) in self.properties.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				print_ident(f, prop)?;
			}
			f.write_str(")")?;
		}
		Ok(())
	}
}

impl ToPgSql for EdgeTableDef {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.table_name.to_pgsql(f)?;
		if !self.key_columns.is_empty() {
			f.write_str(" KEY (")?;
			for (i, col) in self.key_columns.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				print_ident(f, col)?;
			}
			f.write_str(")")?;
		}
		f.write_str(" SOURCE KEY (")?;
		for (i, col) in self.source_key.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			print_ident(f, col)?;
		}
		f.write_str(") REFERENCES ")?;
		self.source_table.to_pgsql(f)?;
		if !self.source_references.is_empty() {
			f.write_str(" (")?;
			for (i, col) in self.source_references.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				print_ident(f, col)?;
			}
			f.write_str(")")?;
		}

		f.write_str(" TARGET KEY (")?;
		for (i, col) in self.target_key.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			print_ident(f, col)?;
		}
		f.write_str(") REFERENCES ")?;
		self.target_table.to_pgsql(f)?;
		if !self.target_references.is_empty() {
			f.write_str(" (")?;
			for (i, col) in self.target_references.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				print_ident(f, col)?;
			}
			f.write_str(")")?;
		}

		if let Some(ref l) = self.label {
			f.write_str(" LABEL ")?;
			print_ident(f, l)?;
		}
		if !self.properties.is_empty() {
			f.write_str(" PROPERTIES (")?;
			for (i, prop) in self.properties.iter().enumerate() {
				if i > 0 {
					f.write_str(", ")?;
				}
				print_ident(f, prop)?;
			}
			f.write_str(")")?;
		}
		Ok(())
	}
}

impl ToPgSql for DropPropertyGraphStmt {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("DROP PROPERTY GRAPH ")?;
		if self.if_exists {
			f.write_str("IF EXISTS ")?;
		}
		self.name.to_pgsql(f)?;
		if self.cascade {
			f.write_str(" CASCADE")
		} else {
			f.write_str(" RESTRICT")
		}
	}
}

impl ToPgSql for GraphPattern {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for (i, path) in self.paths.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			path.to_pgsql(f)?;
		}
		if let Some(ref w) = self.where_clause {
			f.write_str(" WHERE ")?;
			w.to_pgsql(f)?;
		}
		Ok(())
	}
}

impl ToPgSql for GraphPathPattern {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if let Some(ref var) = self.path_variable {
			print_ident(f, var)?;
			f.write_str(" = ")?;
		}
		for elem in &self.elements {
			elem.to_pgsql(f)?;
		}
		Ok(())
	}
}

impl ToPgSql for GraphElementPattern {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Vertex(v) => v.to_pgsql(f),
			Self::Edge(e) => e.to_pgsql(f),
		}
	}
}

impl ToPgSql for GraphVertexPattern {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("(")?;
		if let Some(ref var) = self.variable {
			print_ident(f, var)?;
		}
		for l in &self.labels {
			f.write_str(":")?;
			print_ident(f, l)?;
		}
		if let Some(ref w) = self.where_clause {
			f.write_str(" WHERE ")?;
			w.to_pgsql(f)?;
		}
		f.write_str(")")
	}
}

impl ToPgSql for GraphEdgePattern {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self.direction {
			GraphEdgeDirection::Right => f.write_str("-[")?,
			GraphEdgeDirection::Left => f.write_str("<-[")?,
			GraphEdgeDirection::Any => f.write_str("-[")?,
			GraphEdgeDirection::Bidirectional => f.write_str("<-[")?,
		}
		if let Some(ref var) = self.variable {
			print_ident(f, var)?;
		}
		for l in &self.labels {
			f.write_str(":")?;
			print_ident(f, l)?;
		}
		if let Some(q) = self.quantifier {
			match q {
				GraphQuantifier::ZeroOrMore => f.write_str("*")?,
				GraphQuantifier::OneOrMore => f.write_str("+")?,
				GraphQuantifier::ZeroOrOne => f.write_str("?")?,
				GraphQuantifier::Exact(n) => write!(f, "{{{n}}}")?,
				GraphQuantifier::Range(min, Some(max)) => write!(f, "{{{min}, {max}}}")?,
				GraphQuantifier::Range(min, None) => write!(f, "{{{min},}}")?,
			}
		}
		if let Some(ref w) = self.where_clause {
			f.write_str(" WHERE ")?;
			w.to_pgsql(f)?;
		}
		match self.direction {
			GraphEdgeDirection::Right => f.write_str("]->"),
			GraphEdgeDirection::Left => f.write_str("]-"),
			GraphEdgeDirection::Any => f.write_str("]-"),
			GraphEdgeDirection::Bidirectional => f.write_str("]->"),
		}
	}
}

impl ToPgSql for GraphColumnDef {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.expr.to_pgsql(f)?;
		if let Some(ref a) = self.alias {
			f.write_str(" AS ")?;
			print_ident(f, a)?;
		}
		Ok(())
	}
}

impl ToPgSql for JsonTable {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("JSON_TABLE(")?;
		self.context_item.to_pgsql(f)?;
		f.write_str(", '")?;
		f.write_str(&self.path)?;
		f.write_str("' COLUMNS (")?;
		for (i, col) in self.columns.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			col.to_pgsql(f)?;
		}
		f.write_str("))")?;
		if let Some(ref a) = self.alias {
			a.to_pgsql(f)?;
		}
		Ok(())
	}
}

impl ToPgSql for JsonTableColumn {
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		print_ident(f, &self.name)?;
		f.write_str(" ")?;
		self.data_type.to_pgsql(f)?;
		if let Some(ref p) = self.path {
			f.write_str(" PATH '")?;
			f.write_str(p)?;
			f.write_str("'")?;
		}
		Ok(())
	}
}
