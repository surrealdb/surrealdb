use std::{
	fmt::{self, Write},
	ops::Bound,
};

use crate::{
	sql::{
		access_type::{BearerAccessSubject, JwtAccessVerify},
		escape::{EscapeIdent, EscapeKey, EscapeKwFreeIdent, EscapePath, QuoteStr},
		graph::GraphSubject,
		order::Ordering,
		part::{DestructurePart, RecurseInstruction},
		reference::{self, Reference, Refs},
		statements::{
			access::{
				AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke,
				AccessStatementShow, Subject,
			},
			define::{
				config::{api::ApiConfig, graphql::GraphQLConfig},
				ApiAction, DefineConfigStatement,
			},
			rebuild::RebuildStatement,
			AlterTableStatement, AnalyzeStatement, CreateStatement, DefineAccessStatement,
			DefineAnalyzerStatement, DefineApiStatement, DefineDatabaseStatement,
			DefineEventStatement, DefineFieldStatement, DefineFunctionStatement,
			DefineIndexStatement, DefineModelStatement, DefineNamespaceStatement,
			DefineParamStatement, DefineTableStatement, DefineUserStatement, DeleteStatement,
			ForeachStatement, IfelseStatement, InfoStatement, InsertStatement, KillStatement,
			LiveStatement, OptionStatement, OutputStatement, RelateStatement, RemoveStatement,
			SelectStatement, SetStatement, ShowStatement, SleepStatement, ThrowStatement,
			UpdateStatement, UpsertStatement, UseStatement,
		},
		visit::{Visit, Visitor},
		AccessType, Array, Block, Cast, Closure, Data, Edges, Entry, Expression, Field, Fields,
		Function, Future, Geometry, Graph, Id, IdRange, Idiom, Index, Kind, Literal, Mock, Model,
		Number, Object, Operator, Output, Param, Part, Permission, Permissions, Range, Statements,
		Subquery, Thing, Value, View, With,
	},
	syn::error::{Location, MessageKind, Snippet},
};

mod util;
use util::v3_function_name;

use super::{IssueKind, MigrationIssue, Severity};

pub struct PassWriter<'a> {
	indent: usize,
	buffer: String,
	target: &'a mut String,
	line: usize,
}

impl<'a> PassWriter<'a> {
	pub fn new(target: &'a mut String) -> Self {
		PassWriter {
			target,
			indent: 0,
			buffer: String::new(),
			line: 0,
		}
	}

	fn location(&self) -> Location {
		Location {
			line: self.line + 1,
			column: self.target.chars().count() + self.buffer.chars().count() + 1,
		}
	}

	fn flush_source(&mut self) -> &str {
		self.target.push_str(&self.buffer);
		self.buffer.clear();
		self.target.as_str()
	}
}

impl Drop for PassWriter<'_> {
	fn drop(&mut self) {
		self.target.push_str(&self.buffer)
	}
}

impl fmt::Write for PassWriter<'_> {
	fn write_str(&mut self, mut s: &str) -> fmt::Result {
		while let Some(l) = s.find('\n') {
			self.target.push_str(&self.buffer);
			self.buffer.clear();
			for _ in 0..self.indent {
				self.buffer.push(' ');
			}
			self.target.push_str(&s[..l]);
			self.target.push('\n');
			self.line += 1;
			s = &s[(l + 1)..]
		}

		self.buffer.push_str(s);
		Ok(())
	}
}

#[derive(Clone, Copy, Default)]
pub struct PassState {
	pub breaking_futures: bool,
	pub breaking_closures: bool,
	pub non_expression_idiom: bool,
	pub mock_allowed: bool,
	pub skip_relation_field: bool,
	pub _tmp: (),
}

impl PassState {
	pub fn with_breaking_futures(mut self) -> Self {
		self.breaking_futures = true;
		self
	}
	pub fn with_breaking_closures(mut self) -> Self {
		self.breaking_closures = true;
		self
	}

	pub fn with_breaking_storage(self) -> Self {
		self.with_breaking_futures().with_breaking_closures()
	}
}

pub struct MigratorPass<'a> {
	issues: &'a mut Vec<MigrationIssue>,
	pub path: &'a mut Vec<Value>,
	w: PassWriter<'a>,
	state: PassState,
}

impl<'a> MigratorPass<'a> {
	pub fn new(
		issues: &'a mut Vec<MigrationIssue>,
		export: &'a mut String,
		path: &'a mut Vec<Value>,
		state: PassState,
	) -> Self {
		export.clear();
		MigratorPass {
			issues,
			path,
			w: PassWriter::new(export),
			state,
		}
	}

	fn with_state<Fs, Fc, R>(&mut self, fs: Fs, fc: Fc) -> Result<R, fmt::Error>
	where
		Fs: FnOnce(PassState) -> PassState,
		Fc: FnOnce(&mut Self) -> Result<R, fmt::Error>,
	{
		let old_state = self.state;
		self.state = fs(old_state);
		let r = fc(self);
		self.state = old_state;
		r
	}

	fn check_function(&mut self, range: std::ops::Range<Location>, name: &str, _args: &[Value]) {
		match name {
			"array::logical_and" => {
				self.issues.push(MigrationIssue {
					severity: Severity::UnlikelyBreak,
					error: "Found use of function array::logical_and, this function now has different behavior".to_string(),
					details: String::new(),
					kind: IssueKind::FunctionLogicalAnd,
					origin: self.path.clone(),
					error_location: Some(Snippet::from_source_location_range(
						self.w.flush_source(),
						range,
						None,
						MessageKind::Error,
					)),
					resolution: None,
				})
			}
			"array::logical_or" => {
				self.issues.push(MigrationIssue {
					severity: Severity::UnlikelyBreak,
					error: "Found use of function array::logical_or, this function now has different behavior".to_string(),
					details: String::new(),
					kind: IssueKind::FunctionLogicalOr,
					origin: self.path.clone(),
					error_location: Some(Snippet::from_source_location_range(
						self.w.flush_source(),
						range,
						None,
						MessageKind::Error,
					)),
					resolution: None,
				})
			}
			"array::range" => {
				self.issues.push(MigrationIssue {
					severity: Severity::CanBreak,
					error: "Found use of function array::range, this function now has different behavior".to_string(),
					details: String::new(),
					kind: IssueKind::FunctionLogicalOr,
					origin: self.path.clone(),
					error_location: Some(Snippet::from_source_location_range(
						self.w.flush_source(),
						range,
						None,
						MessageKind::Error,
					)),
					resolution: None,
				})
			}
			"math::sqrt" => {
				self.issues.push(MigrationIssue {
					severity: Severity::UnlikelyBreak,
					error: "Found use of function math::sqrt, this function now returns `NaN` when called with a negative number".to_string(),
					details: String::new(),
					kind: IssueKind::FunctionLogicalOr,
					origin: self.path.clone(),
					error_location: Some(Snippet::from_source_location_range(
						self.w.flush_source(),
						range,
						None,
						MessageKind::Error,
					)),
					resolution: None,
				})
			}
			"math::min" => {
				self.issues.push(MigrationIssue {
					severity: Severity::UnlikelyBreak,
					error: "Found use of function math::min, this function now return `Infinity` when called with an empty array".to_string(),
					details: String::new(),
					kind: IssueKind::FunctionLogicalOr,
					origin: self.path.clone(),
					error_location: Some(Snippet::from_source_location_range(
						self.w.flush_source(),
						range,
						None,
						MessageKind::Error,
					)),
					resolution: None,
				})
			}
			"math::max" => {
				self.issues.push(MigrationIssue {
					severity: Severity::UnlikelyBreak,
					error: "Found use of function math::max, this function now returns `-Infinity` when called with an empty array".to_string(),
					details: String::new(),
					kind: IssueKind::FunctionLogicalOr,
					origin: self.path.clone(),
					error_location: Some(Snippet::from_source_location_range(
						self.w.flush_source(),
						range,
						None,
						MessageKind::Error,
					)),
					resolution: None,
				})
			}
			_ => {}
		}
	}
}

impl Visitor for MigratorPass<'_> {
	type Error = fmt::Error;

	fn visit_statements(&mut self, s: &Statements) -> Result<(), Self::Error> {
		for s in s.0.iter() {
			s.visit(self)?;
			writeln!(self.w, ";")?;
		}

		Ok(())
	}

	fn visit_access_grant(&mut self, a: &AccessStatementGrant) -> Result<(), Self::Error> {
		write!(self.w, "ACCESS {}", a.ac)?;
		if let Some(ref v) = a.base {
			write!(self.w, " ON {v}")?;
		}
		write!(self.w, " GRANT")?;
		match &a.subject {
			Subject::User(x) => write!(self.w, " FOR USER {}", EscapeKwFreeIdent(&x.0))?,
			Subject::Record(x) => {
				write!(self.w, " FOR RECORD ")?;
				self.visit_thing(x)?
			}
		}
		Ok(())
	}

	fn visit_access_purge(&mut self, a: &AccessStatementPurge) -> Result<(), Self::Error> {
		write!(self.w, "ACCESS {}", a.ac)?;
		if let Some(ref v) = a.base {
			write!(self.w, " ON {v}")?;
		}
		write!(self.w, " PURGE")?;
		match (a.expired, a.revoked) {
			(true, false) => write!(self.w, " EXPIRED")?,
			(false, true) => write!(self.w, " REVOKED")?,
			(true, true) => write!(self.w, " EXPIRED, REVOKED")?,
			// This case should not parse.
			(false, false) => write!(self.w, " NONE")?,
		};
		if !a.grace.is_zero() {
			write!(self.w, " FOR {}", a.grace)?;
		}
		Ok(())
	}

	fn visit_access_show(&mut self, a: &AccessStatementShow) -> Result<(), Self::Error> {
		write!(self.w, "ACCESS {}", a.ac)?;
		if let Some(ref v) = a.base {
			write!(self.w, " ON {v}")?;
		}
		write!(self.w, " SHOW")?;
		match &a.gr {
			Some(v) => write!(self.w, " GRANT {v}")?,
			None => match &a.cond {
				Some(v) => {
					write!(self.w, " WHERE ")?;
					self.visit_value(&v.0)?;
				}
				None => write!(self.w, " ALL")?,
			},
		};
		Ok(())
	}

	fn visit_access_revoke(&mut self, a: &AccessStatementRevoke) -> Result<(), Self::Error> {
		write!(self.w, "ACCESS {}", a.ac)?;
		if let Some(ref v) = a.base {
			write!(self.w, " ON {v}")?;
		}
		write!(self.w, " REVOKE")?;
		match &a.gr {
			Some(v) => write!(self.w, " GRANT {v}")?,
			None => match &a.cond {
				Some(v) => {
					write!(self.w, " WHERE ")?;
					self.visit_value(&v.0)?;
				}
				None => write!(self.w, " ALL")?,
			},
		};
		Ok(())
	}

	fn visit_alter_table(&mut self, a: &AlterTableStatement) -> Result<(), Self::Error> {
		write!(self.w, "ALTER TABLE ")?;
		if a.if_exists {
			write!(self.w, "IF EXISTS ")?;
		}

		write!(self.w, " {}", a.name)?;
		if let Some(kind) = &a.kind {
			write!(self.w, " TYPE {kind}")?;
		}
		if let Some(full) = a.full {
			let s = if full {
				" SCHEMAFULL"
			} else {
				" SCHEMALESS"
			};
			self.w.write_str(s)?;
		}
		if let Some(comment) = &a.comment {
			write!(self.w, " COMMENT {}", comment.clone().unwrap_or("NONE".into()))?
		}
		if let Some(changefeed) = &a.changefeed {
			write!(self.w, " {}", changefeed.map_or("DROP CHANGEFEED".into(), |v| v.to_string()))?
		}

		if let Some(permissions) = &a.permissions {
			self.w.write_char(' ')?;
			self.visit_permissions(permissions)?;
		}
		Ok(())
	}

	fn visit_upsert(&mut self, u: &UpsertStatement) -> Result<(), Self::Error> {
		self.with_state(
			|s| PassState {
				breaking_futures: true,
				breaking_closures: true,
				..s
			},
			|this| {
				this.w.write_str("UPSERT ")?;
				if u.only {
					this.w.write_str("ONLY ")?;
				}
				for (idx, v) in u.what.0.iter().enumerate() {
					if idx != 0 {
						this.w.write_str(",")?;
					}
					this.visit_value(v)?;
				}
				match &u.with {
					Some(With::NoIndex) => {
						this.w.write_str(" WITH NOINDEX")?;
					}
					Some(With::Index(x)) => {
						this.w.write_str(" WITH INDEX ")?;
						for (idx, x) in x.iter().enumerate() {
							if idx != 0 {
								this.w.write_str(",")?;
							}
							write!(this.w, "{}", EscapeKwFreeIdent(x))?;
						}
					}
					None => {}
				}

				if let Some(d) = u.data.as_ref() {
					this.w.write_str(" ")?;
					this.visit_data(d)?;
				}

				if let Some(c) = u.cond.as_ref() {
					this.w.write_str(" WHERE ")?;
					this.with_state(
						|s| PassState {
							breaking_futures: false,
							breaking_closures: false,
							..s
						},
						|this| this.visit_value(&c.0),
					)?;
				}

				if let Some(o) = u.output.as_ref() {
					this.w.write_str(" ")?;
					this.visit_output(o)?;
				}
				Ok(())
			},
		)
	}

	fn visit_rebuild(&mut self, r: &RebuildStatement) -> Result<(), Self::Error> {
		// TODO: Check if syntax hasn't changed.
		write!(self.w, "{r}")
	}

	fn visit_use(&mut self, t: &UseStatement) -> Result<(), Self::Error> {
		write!(self.w, "{t}")
	}

	fn visit_throw(&mut self, t: &ThrowStatement) -> Result<(), Self::Error> {
		self.w.write_str("THROW ")?;
		t.visit(self)
	}

	fn visit_update(&mut self, u: &UpdateStatement) -> Result<(), Self::Error> {
		self.with_state(
			|s| PassState {
				breaking_futures: true,
				..s
			},
			|this| {
				this.w.write_str("UPDATE ")?;
				if u.only {
					this.w.write_str("ONLY ")?;
				}
				for (idx, v) in u.what.0.iter().enumerate() {
					if idx != 0 {
						this.w.write_str(",")?;
					}
					this.visit_value(v)?;
				}
				match &u.with {
					Some(With::NoIndex) => {
						this.w.write_str(" WITH NOINDEX")?;
					}
					Some(With::Index(x)) => {
						this.w.write_str(" WITH INDEX ")?;
						for (idx, x) in x.iter().enumerate() {
							if idx != 0 {
								this.w.write_str(",")?;
							}
							write!(this.w, "{}", EscapeKwFreeIdent(x))?;
						}
					}
					None => {}
				}

				if let Some(d) = u.data.as_ref() {
					this.w.write_str(" ")?;
					this.visit_data(d)?;
				}

				if let Some(c) = u.cond.as_ref() {
					this.w.write_str(" WHERE ")?;
					this.with_state(
						|s| PassState {
							breaking_futures: false,
							breaking_closures: false,
							..s
						},
						|this| this.visit_value(&c.0),
					)?;
				}

				if let Some(o) = u.output.as_ref() {
					this.w.write_str(" ")?;
					this.visit_output(o)?;
				}

				Ok(())
			},
		)
	}

	fn visit_sleep(&mut self, s: &SleepStatement) -> Result<(), Self::Error> {
		write!(self.w, "{s}")
	}

	fn visit_show(&mut self, s: &ShowStatement) -> Result<(), Self::Error> {
		write!(self.w, "{s}")
	}

	fn visit_set(&mut self, s: &SetStatement) -> Result<(), Self::Error> {
		write!(self.w, "LET ${}", EscapeIdent(s.name.as_str()))?;
		if let Some(k) = &s.kind {
			write!(self.w, ":{k}")?;
		};
		self.w.write_str(" = ")?;
		self.visit_value(&s.what)
	}

	fn visit_select(&mut self, s: &SelectStatement) -> Result<(), Self::Error> {
		write!(self.w, "SELECT ")?;
		self.visit_fields(&s.expr)?;
		if let Some(ref v) = s.omit {
			write!(self.w, " OMIT ")?;
			for (idx, i) in v.0.iter().enumerate() {
				if idx != 0 {
					write!(self.w, ",")?;
				}
				self.visit_idiom(i)?;
			}
		}
		write!(self.w, " FROM ")?;
		if s.only {
			self.w.write_str("ONLY ")?
		}
		for (idx, v) in s.what.0.iter().enumerate() {
			if idx != 0 {
				write!(self.w, ", ")?;
			}
			self.visit_value(v)?;
		}

		match &s.with {
			Some(With::NoIndex) => {
				self.w.write_str(" WITH NOINDEX")?;
			}
			Some(With::Index(x)) => {
				self.w.write_str(" WITH INDEX ")?;
				for (idx, x) in x.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(",")?;
					}
					write!(self.w, "{}", EscapeKwFreeIdent(x))?;
				}
			}
			None => {}
		}

		if let Some(ref v) = s.cond {
			self.w.write_str(" WHERE ")?;
			self.visit_value(&v.0)?;
		}

		// Just don't generate the split on when a group by is present
		// Since it doesn't do anything and having both breaks in 3.0
		if s.group.is_none() {
			if let Some(ref v) = s.split {
				self.w.write_str(" SPLIT ON ")?;
				for (idx, i) in v.0.iter().enumerate() {
					if idx != 0 {
						write!(self.w, ",")?;
					}
					self.visit_idiom(i)?;
				}
			}
		}

		if let Some(ref v) = s.group {
			if v.0.is_empty() {
				self.w.write_str(" GROUP ALL")?;
			} else {
				self.w.write_str(" GROUP BY ")?;
				for (idx, i) in v.0.iter().enumerate() {
					if idx != 0 {
						write!(self.w, ",")?;
					}
					self.visit_idiom(i)?;
				}
			}
		}

		if let Some(ref v) = s.order {
			match v {
				Ordering::Random => {
					self.w.write_str(" ORDER BY RAND()")?;
				}
				Ordering::Order(order_list) => {
					self.w.write_str(" ORDER BY ")?;
					for (idx, o) in order_list.iter().enumerate() {
						if idx != 0 {
							self.w.write_str(", ")?;
						}

						self.visit_idiom(&o.value)?;
						if o.collate {
							self.w.write_str(" COLLATE")?;
						}
						if o.numeric {
							self.w.write_str(" NUMERIC")?;
						}
						if !o.direction {
							self.w.write_str(" DESC")?;
						}
					}
				}
			}
		}

		if let Some(ref v) = s.limit {
			self.w.write_str(" LIMIT ")?;
			self.visit_value(&v.0)?;
		}

		if let Some(ref v) = s.start {
			self.w.write_str(" START ")?;
			self.visit_value(&v.0)?;
		}
		if let Some(ref v) = s.fetch {
			self.w.write_str(" FETCH ")?;
			for (idx, v) in v.0.iter().enumerate() {
				if idx != 0 {
					write!(self.w, ", ")?;
				}
				self.visit_value(&v.0)?;
			}
		}
		if let Some(ref v) = s.version {
			self.w.write_str(" VERSION ")?;
			self.visit_value(&v.0)?;
		}
		if let Some(ref v) = s.timeout {
			write!(self.w, " {v}")?
		}

		if let Some(ref v) = s.explain {
			write!(self.w, " {v}")?
		}
		Ok(())
	}

	fn visit_remove(&mut self, r: &RemoveStatement) -> Result<(), Self::Error> {
		write!(self.w, "{r}")
	}

	fn visit_relate(&mut self, o: &RelateStatement) -> Result<(), Self::Error> {
		self.with_state(
			|s| PassState {
				breaking_futures: true,
				breaking_closures: true,
				..s
			},
			|this| {
				this.w.write_str("RELATE ")?;
				if o.only {
					this.w.write_str(" ONLY ")?;
				}
				this.w.write_char('(')?;
				this.visit_value(&o.from)?;
				this.w.write_char(')')?;

				this.w.write_str(" -> ")?;

				let escape = !matches!(o.kind, Value::Table(_) | Value::Thing(_));
				if escape {
					this.w.write_char('(')?;
				}
				this.visit_value(&o.kind)?;
				if escape {
					this.w.write_char(')')?;
				}

				this.w.write_str(" -> ")?;

				this.w.write_char('(')?;
				this.visit_value(&o.with)?;
				this.w.write_char(')')?;

				if o.uniq {
					this.w.write_str(" UNIQUE")?;
				}
				if let Some(ref v) = o.data {
					this.w.write_str(" ")?;
					this.visit_data(v)?;
				}
				if let Some(ref v) = o.output {
					this.w.write_str(" ")?;
					this.visit_output(v)?;
				}
				if let Some(ref v) = o.timeout {
					write!(this.w, " {v}")?
				}

				Ok(())
			},
		)
	}

	fn visit_output_stmt(&mut self, o: &OutputStatement) -> Result<(), Self::Error> {
		self.with_state(
			|s| PassState {
				breaking_futures: false,
				..s
			},
			|this| {
				this.w.write_str("RETURN ")?;
				this.visit_value(&o.what)?;
				if let Some(ref v) = o.fetch {
					this.w.write_str(" FETCH ")?;
					for (idx, v) in v.0.iter().enumerate() {
						if idx != 0 {
							write!(this.w, ", ")?;
						}
						this.visit_value(&v.0)?;
					}
				}
				Ok(())
			},
		)
	}

	fn visit_option(&mut self, o: &OptionStatement) -> Result<(), Self::Error> {
		write!(self.w, " {o}")
	}

	fn visit_live(&mut self, l: &LiveStatement) -> Result<(), Self::Error> {
		self.w.write_str("LIVE SELECT ")?;
		self.visit_fields(&l.expr)?;
		self.w.write_str(" FROM ")?;
		self.visit_value(&l.what)?;

		if let Some(ref v) = l.cond {
			self.w.write_str(" WHERE ")?;
			self.visit_value(&v.0)?;
		}
		if let Some(ref v) = l.fetch {
			self.w.write_str(" FETCH ")?;
			for (idx, v) in v.0.iter().enumerate() {
				if idx != 0 {
					write!(self.w, ", ")?;
				}
				self.visit_value(&v.0)?;
			}
		}
		Ok(())
	}

	fn visit_kill(&mut self, i: &KillStatement) -> Result<(), Self::Error> {
		self.w.write_str("KILL ")?;
		self.visit_value(&i.id)?;
		Ok(())
	}

	fn visit_insert(&mut self, i: &InsertStatement) -> Result<(), Self::Error> {
		self.with_state(
			|s| PassState {
				breaking_futures: true,
				..s
			},
			|this| {
				this.w.write_str("INSERT")?;
				if i.relation {
					this.w.write_str(" RELATION")?;
				}
				if i.ignore {
					this.w.write_str(" IGNORE")?;
				}
				if let Some(ref into) = i.into {
					this.w.write_str(" INTO ")?;
					this.visit_value(into)?;
				}
				this.w.write_str(" ")?;
				this.visit_data(&i.data)?;

				if let Some(ref v) = i.update {
					this.w.write_str(" ")?;
					this.visit_data(v)?;
				}

				if let Some(ref v) = i.output {
					this.w.write_str(" ")?;
					this.visit_output(v)?;
				}

				if let Some(ref v) = i.version {
					this.w.write_str(" VERSION ")?;
					this.visit_value(&v.0)?;
				}

				if let Some(ref v) = i.timeout {
					write!(this.w, " {v}")?
				}
				Ok(())
			},
		)
	}

	fn visit_info(&mut self, i: &InfoStatement) -> Result<(), Self::Error> {
		match i {
			InfoStatement::Root(_)
			| InfoStatement::Ns(_)
			| InfoStatement::User(_, _, _)
			| InfoStatement::Index(_, _, _) => {
				write!(self.w, "{i}")?;
			}
			InfoStatement::Db(structure, version) => {
				self.w.write_str("INFO FOR DATABASE")?;
				if let Some(v) = version.as_ref() {
					self.w.write_str(" VERSION ")?;
					self.visit_value(&v.0)?;
				}
				if *structure {
					self.w.write_str(" STRUCTURE")?;
				}
			}
			InfoStatement::Tb(ident, structure, version) => {
				write!(self.w, "INFO FOR TABLE {ident}")?;
				if let Some(v) = version.as_ref() {
					self.w.write_str(" VERSION ")?;
					self.visit_value(&v.0)?;
				}
				if *structure {
					self.w.write_str(" STRUCTURE")?;
				}
			}
		}
		Ok(())
	}

	fn visit_if_else(&mut self, i: &IfelseStatement) -> Result<(), Self::Error> {
		if i.bracketed() {
			for (idx, (cond, then)) in i.exprs.iter().enumerate() {
				if idx != 0 {
					self.w.write_str(" ELSE ")?;
				}
				self.w.write_str("IF ")?;
				self.visit_value(cond)?;
				self.w.write_str(" ")?;
				self.visit_value(then)?;
			}
			if let Some(x) = i.close.as_ref() {
				self.w.write_str(" ELSE ")?;
				self.visit_value(x)?;
			}
		} else {
			for (idx, (cond, then)) in i.exprs.iter().enumerate() {
				if idx != 0 {
					self.w.write_str(" ELSE ")?;
				}
				self.w.write_str("IF ")?;
				self.visit_value(cond)?;
				self.w.write_str(" THEN ")?;
				self.visit_value(then)?;
			}
			if let Some(x) = i.close.as_ref() {
				self.w.write_str(" ELSE ")?;
				self.visit_value(x)?;
			}
			self.w.write_str(" END ")?;
		}
		Ok(())
	}

	fn visit_foreach(&mut self, f: &ForeachStatement) -> Result<(), Self::Error> {
		write!(self.w, "FOR {} IN ", f.param)?;
		self.visit_value(&f.range)?;
		self.w.write_char(' ')?;
		self.visit_block(&f.block)?;
		Ok(())
	}

	fn visit_delete(&mut self, d: &DeleteStatement) -> Result<(), Self::Error> {
		self.with_state(
			|s| PassState {
				breaking_futures: true,
				..s
			},
			|this| {
				this.w.write_str("DELETE ")?;
				if d.only {
					this.w.write_str("ONLY ")?;
				}
				for (idx, v) in d.what.0.iter().enumerate() {
					if idx != 0 {
						this.w.write_str(",")?;
					}
					this.visit_value(v)?;
				}
				match &d.with {
					Some(With::NoIndex) => {
						this.w.write_str(" WITH NOINDEX")?;
					}
					Some(With::Index(x)) => {
						this.w.write_str(" WITH INDEX ")?;
						for (idx, x) in x.iter().enumerate() {
							if idx != 0 {
								this.w.write_str(",")?;
							}
							write!(this.w, "{}", EscapeKwFreeIdent(x))?;
						}
					}
					None => {}
				}

				if let Some(ref v) = d.cond {
					this.w.write_str(" WHERE ")?;
					this.visit_value(&v.0)?;
				}

				if let Some(ref v) = d.output {
					this.w.write_str(" ")?;
					this.visit_output(v)?;
				}

				if let Some(ref v) = d.timeout {
					write!(this.w, " {v}")?
				}

				if let Some(ref v) = d.explain {
					write!(this.w, " {v}")?
				}
				Ok(())
			},
		)
	}

	fn visit_define_api(&mut self, d: &DefineApiStatement) -> Result<(), Self::Error> {
		self.w.write_str("DEFINE API ")?;
		if d.if_not_exists {
			self.w.write_str(" IF NOT EXISTS ")?;
		}
		if d.overwrite {
			self.w.write_str(" OVERWRITE ")?;
		}
		self.visit_value(&d.path)?;

		if d.config.is_some() || d.fallback.is_some() {
			self.w.write_str(" FOR any")?;
		}
		if let Some(config) = d.config.as_ref() {
			self.w.write_str(" ")?;
			self.visit_api_config(config)?;
		}
		if let Some(fallback) = d.fallback.as_ref() {
			self.w.write_str(" THEN ")?;
			self.visit_value(fallback)?;
		}
		for a in d.actions.iter() {
			self.w.write_str(" ")?;
			self.visit_api_action(a)?;
		}
		Ok(())
	}

	fn visit_api_action(&mut self, a: &ApiAction) -> Result<(), Self::Error> {
		self.w.write_str("FOR ")?;
		for (idx, m) in a.methods.iter().enumerate() {
			if idx == 0 {
				self.w.write_str(",")?;
			}
			write!(self.w, "{}", m)?;
		}
		if let Some(config) = &a.config {
			self.w.write_str(" ")?;
			self.visit_api_config(config)?;
		}
		self.w.write_str(" THEN ")?;
		self.visit_value(&a.action)?;
		Ok(())
	}

	fn visit_define_config(&mut self, d: &DefineConfigStatement) -> Result<(), Self::Error> {
		self.w.write_str("DEFINE CONFIG ")?;
		if d.if_not_exists {
			self.w.write_str(" IF NOT EXISTS ")?;
		}
		if d.overwrite {
			self.w.write_str(" OVERWRITE ")?;
		}
		self.visit_config_inner(&d.inner)
	}

	fn visit_api_config(&mut self, d: &ApiConfig) -> Result<(), Self::Error> {
		self.w.write_str("API")?;
		if let Some(mw) = &d.middleware {
			self.w.write_str(" MIDDLEWARE ")?;
			for (idx, (k, v)) in mw.iter().enumerate() {
				if idx == 0 {
					self.w.write_str(", ")?;
				}
				self.w.write_str(k.as_str())?;
				self.w.write_str("(")?;
				for (idx, v) in v.iter().enumerate() {
					if idx == 0 {
						self.w.write_str(", ")?;
					}
					self.visit_value(v)?;
				}
				self.w.write_str(")")?;
			}
		}

		if let Some(p) = d.permissions.as_ref() {
			// TODO: Check if this is correct, it is a single permission clause so maybe drop the
			// 's'
			self.w.write_str(" PERMISSIONS ")?;
			self.visit_permission(p)?;
		}

		Ok(())
	}

	fn visit_graphql_config(&mut self, d: &GraphQLConfig) -> Result<(), Self::Error> {
		write!(self.w, "{d}")
	}

	fn visit_define_access(&mut self, d: &DefineAccessStatement) -> Result<(), Self::Error> {
		self.w.write_str("DEFINE ACCESS ")?;
		if d.if_not_exists {
			self.w.write_str(" IF NOT EXISTS ")?;
		}
		if d.overwrite {
			self.w.write_str(" OVERWRITE ")?;
		}
		write!(self.w, " {} ON {} TYPE ", d.name, d.base)?;
		self.visit_access_type(&d.kind)?;
		if let Some(ref v) = d.authenticate {
			self.w.write_str(" AUTHENTICATE ")?;
			self.visit_value(v)?;
		}
		self.w.write_str(" DURATION")?;
		if d.kind.can_issue_grants() {
			write!(
				self.w,
				" FOR GRANT {},",
				match d.duration.grant {
					Some(dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
		if d.kind.can_issue_tokens() {
			write!(
				self.w,
				" FOR TOKEN {},",
				match d.duration.token {
					Some(dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
		write!(
			self.w,
			" FOR SESSION {}",
			match d.duration.session {
				Some(dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
		if let Some(ref v) = d.comment {
			write!(self.w, " COMMENT {v}")?
		}
		Ok(())
	}

	fn visit_access_type(&mut self, a: &AccessType) -> Result<(), Self::Error> {
		match a {
			AccessType::Jwt(ac) => {
				self.w.write_str("JWT ")?;

				match &ac.verify {
					JwtAccessVerify::Key(ref v) => {
						write!(self.w, "ALGORITHM {} KEY {}", v.alg, QuoteStr(&v.key))?;
					}
					JwtAccessVerify::Jwks(ref v) => {
						write!(self.w, "URL {}", QuoteStr(&v.url),)?;
					}
				}
				if let Some(iss) = &ac.issue {
					write!(self.w, " WITH ISSUER KEY {}", QuoteStr(&iss.key))?;
				}
			}
			AccessType::Record(ac) => {
				self.w.write_str("RECORD")?;
				if let Some(ref v) = ac.signup {
					write!(self.w, " SIGNUP ")?;
					self.visit_value(v)?;
				}
				if let Some(ref v) = ac.signin {
					write!(self.w, " SIGNIN ")?;
					self.visit_value(v)?;
				}
				if ac.bearer.is_some() {
					write!(self.w, " WITH REFRESH")?
				}
				write!(self.w, " WITH JWT {}", ac.jwt)?;
			}
			AccessType::Bearer(ac) => {
				write!(self.w, "BEARER")?;
				match ac.subject {
					BearerAccessSubject::User => write!(self.w, " FOR USER")?,
					BearerAccessSubject::Record => write!(self.w, " FOR RECORD")?,
				}
			}
		}
		Ok(())
	}

	fn visit_define_model(&mut self, d: &DefineModelStatement) -> Result<(), Self::Error> {
		write!(self.w, "{d}")
	}

	fn visit_define_user(&mut self, d: &DefineUserStatement) -> Result<(), Self::Error> {
		write!(self.w, "{d}")
	}

	fn visit_define_index(&mut self, d: &DefineIndexStatement) -> Result<(), Self::Error> {
		self.w.write_str("DEFINE INDEX")?;
		if d.if_not_exists {
			self.w.write_str(" IF NOT EXISTS")?;
		}
		if d.overwrite {
			self.w.write_str(" OVERWRITE")?;
		}
		write!(self.w, " {} ON {}", d.name, d.what)?;
		if !d.cols.0.is_empty() {
			self.w.write_str(" FIELDS ")?;
			for (idx, i) in d.cols.0.iter().enumerate() {
				if idx != 0 {
					self.w.write_str(", ")?;
				}
				self.with_state(
					|p| PassState {
						non_expression_idiom: true,
						..p
					},
					|this| this.visit_idiom(i),
				)?;
			}
		}
		if Index::Idx != d.index {
			match &d.index {
				Index::Search(x) => {
					let before = self.w.location();

					write!(
						self.w,
						" FULLTEXT ANALYZER {} {}",
						EscapeKwFreeIdent(x.az.0.as_str()),
						x.sc
					)?;
					if x.hl {
						write!(self.w, " HIGHLIGHTS")?;
					}

					if x.doc_ids_order != 100
						|| x.doc_lengths_order != 100
						|| x.postings_order != 100
						|| x.terms_order != 100
						|| x.doc_ids_cache != 100
						|| x.doc_lengths_cache != 100
						|| x.terms_cache != 100
					{
						let range = before..(self.w.location());
						self.issues.push(MigrationIssue {
					    severity: Severity::UnlikelyBreak,
					    error: "The `SEARCH` index has been renamed to `FULLTEXT` and some of it's parameters have been removed".to_string(),
					    details: "".to_string(),
					    kind: IssueKind::SearchIndex,
					    origin: self.path.clone(),
					    error_location: Some(Snippet::from_source_location_range(
						    self.w.flush_source(),
						    range,
						    None,
						    MessageKind::Error,
					    )),
					    resolution: None,
				    });
					}
				}
				Index::MTree(_) => {
					let before = self.w.location();
					write!(self.w, " {}", d.index)?;
					let after = self.w.location();
					self.issues.push(MigrationIssue {
						severity: Severity::WillBreak,
						error: "Found use of deprecated `MTREE` index".to_string(),
						details: "".to_string(),
						kind: IssueKind::MTreeIndex,
						origin: self.path.clone(),
						error_location: Some(Snippet::from_source_location_range(
							self.w.flush_source(),
							before..after,
							None,
							MessageKind::Error,
						)),
						resolution: None,
					});
				}
				_ => {
					write!(self.w, " {}", d.index)?;
				}
			}
		}
		if let Some(ref v) = d.comment {
			write!(self.w, " COMMENT {v}")?
		}
		if d.concurrently {
			self.w.write_str(" CONCURRENTLY")?
		}
		Ok(())
	}

	fn visit_define_field(&mut self, d: &DefineFieldStatement) -> Result<(), Self::Error> {
		fn kind_contains_object(kind: &Kind) -> bool {
			match kind {
				Kind::Object => true,
				Kind::Either(kinds) => kinds.iter().any(kind_contains_object),
				Kind::Array(inner, _) | Kind::Set(inner, _) => kind_contains_object(inner),
				Kind::Literal(Literal::Object(_)) => true,
				Kind::Literal(Literal::Array(x)) => x.iter().any(kind_contains_object),
				_ => false,
			}
		}

		self.w.write_str("DEFINE FIELD")?;

		if d.if_not_exists {
			self.w.write_str(" IF NOT EXISTS")?;
		}

		if d.overwrite {
			self.w.write_str(" OVERWRITE")?;
		}

		self.w.write_str(" ")?;
		self.with_state(
			|p| PassState {
				non_expression_idiom: true,
				..p
			},
			|this| this.visit_idiom(&d.name),
		)?;

		write!(self.w, " ON {}", d.what)?;

		if let Some(ref v) = d.kind {
			write!(self.w, " TYPE {v}")?
		}

		if d.flex {
			if let Some(k) = &d.kind {
				if kind_contains_object(k) {
					self.w.write_str(" FLEXIBLE")?
				}
			}
		}

		if let Some(ref v) = d.default {
			self.w.write_str(" DEFAULT ")?;
			if d.default_always {
				self.w.write_str(" ALWAYS ")?
			}

			self.with_state(|p| p.with_breaking_storage(), |this| this.visit_value(v))?;
		}

		if d.readonly {
			self.w.write_str(" READONLY")?
		}

		if let Some(v) = &d.value {
			// handle future -> computed conversion
			let mut cur = v;
			loop {
				match cur {
					// (<future> { .. }) is just the same as future
					Value::Subquery(s) => match **s {
						Subquery::Value(ref x) => cur = x,
						_ => {
							self.w.write_str(" VALUE ")?;
							self.with_state(
								|old| PassState {
									breaking_futures: true,
									..old
								},
								|this| this.visit_value(cur),
							)?;
							break;
						}
					},
					Value::Future(x) => {
						self.w.write_str(" COMPUTED ")?;
						self.with_state(
							|old| PassState {
								breaking_futures: false,
								..old
							},
							|this| this.visit_block(&x.0),
						)?;
						break;
					}
					x => {
						self.w.write_str(" VALUE ")?;
						self.with_state(
							|old| PassState {
								breaking_futures: true,
								..old
							},
							|this| this.visit_value(x),
						)?;
						break;
					}
				}
			}
		}

		if let Some(ref v) = d.assert {
			self.w.write_str(" ASSERT ")?;
			self.visit_value(v)?;
		}
		if let Some(ref v) = d.reference {
			self.w.write_str(" REFERENCE ")?;
			self.visit_reference(v)?;
		}
		if let Some(ref v) = d.comment {
			write!(self.w, " COMMENT {v}")?
		}
		self.w.write_char(' ')?;
		self.w.write_str("PERMISSIONS")?;

		if d.permissions.is_none() {
			self.w.write_str(" NONE")?;
		} else if d.permissions.is_full() {
			self.w.write_str(" FULL")?;
		} else {
			self.w.write_str(" FOR create ")?;
			self.visit_permission(&d.permissions.update)?;
			self.w.write_str(" FOR select ")?;
			self.visit_permission(&d.permissions.update)?;
			self.w.write_str(" FOR update ")?;
			self.visit_permission(&d.permissions.update)?;
		}
		Ok(())
	}

	fn visit_reference(&mut self, d: &Reference) -> Result<(), Self::Error> {
		self.w.write_str("ON DELETE ")?;
		match d.on_delete {
			reference::ReferenceDeleteStrategy::Reject => {
				self.w.write_str("REJECT")?;
			}
			reference::ReferenceDeleteStrategy::Ignore => self.w.write_str("IGNORE")?,
			reference::ReferenceDeleteStrategy::Cascade => {
				self.w.write_str("CASCADE")?;
			}
			reference::ReferenceDeleteStrategy::Unset => {
				self.w.write_str("UNSET")?;
			}
			reference::ReferenceDeleteStrategy::Custom(ref value) => {
				self.w.write_str("THEN ")?;
				self.visit_value(value)?;
			}
		}
		Ok(())
	}

	fn visit_define_event(&mut self, d: &DefineEventStatement) -> Result<(), Self::Error> {
		self.w.write_str("DEFINE EVENT")?;
		if d.if_not_exists {
			self.w.write_str(" IF NOT EXISTS")?;
		}
		if d.overwrite {
			self.w.write_str(" OVERWRITE")?;
		}
		write!(self.w, " {} ON {} WHEN ", d.name, d.what)?;
		self.visit_value(&d.when)?;
		self.w.write_str(" THEN ")?;
		for (idx, v) in d.then.0.iter().enumerate() {
			if idx != 0 {
				self.w.write_str(", ")?;
			}
			self.visit_value(v)?;
		}
		if let Some(ref v) = d.comment {
			write!(self.w, " COMMENT {v}")?
		}
		Ok(())
	}

	fn visit_define_table(&mut self, d: &DefineTableStatement) -> Result<(), Self::Error> {
		self.w.write_str("DEFINE TABLE")?;
		if d.if_not_exists {
			self.w.write_str(" IF NOT EXISTS")?;
		}
		if d.overwrite {
			self.w.write_str(" OVERWRITE")?;
		}
		write!(self.w, " {}", d.name)?;
		write!(self.w, " TYPE {}", d.kind)?;

		let s = if d.full {
			" SCHEMAFULL"
		} else {
			" SCHEMALESS"
		};
		self.w.write_str(s)?;

		if let Some(ref v) = d.comment {
			write!(self.w, " COMMENT {v}")?
		}
		if let Some(ref v) = d.view {
			self.w.write_char(' ')?;
			self.visit_view(v)?
		}
		if let Some(ref v) = d.changefeed {
			write!(self.w, " {v}")?;
		}
		Ok(())
	}

	fn visit_permissions(&mut self, d: &Permissions) -> Result<(), Self::Error> {
		self.w.write_str("PERMISSIONS")?;

		if d.is_none() {
			self.w.write_str(" NONE")?;
		} else if d.is_full() {
			self.w.write_str(" FULL")?;
		} else {
			if !matches!(d.create, Permission::Full) {
				self.w.write_str(" FOR create ")?;
				self.visit_permission(&d.update)?;
			}
			if !matches!(d.select, Permission::Full) {
				self.w.write_str(" FOR select ")?;
				self.visit_permission(&d.update)?;
			}
			if !matches!(d.update, Permission::Full) {
				self.w.write_str(" FOR update ")?;
				self.visit_permission(&d.update)?;
			}
			if !matches!(d.delete, Permission::Full) {
				self.w.write_str(" FOR delete ")?;
				self.visit_permission(&d.delete)?;
			}
		}

		Ok(())
	}

	fn visit_view(&mut self, v: &View) -> Result<(), Self::Error> {
		self.w.write_str("AS SELECT ")?;
		self.visit_fields(&v.expr)?;
		self.w.write_str(" FROM ")?;
		for (idx, t) in v.what.iter().enumerate() {
			if idx != 0 {
				self.w.write_str(",")?;
			}
			write!(self.w, "{t}")?;
		}
		if let Some(ref v) = v.cond {
			self.w.write_str(" WHERE ")?;
			self.visit_value(v)?;
		}
		if let Some(ref v) = v.group {
			if v.0.is_empty() {
				self.w.write_str(" GROUP ALL")?;
			} else {
				self.w.write_str(" GROUP BY ")?;
				for (idx, i) in v.0.iter().enumerate() {
					if idx != 0 {
						write!(self.w, ",")?;
					}
					self.visit_idiom(i)?;
				}
			}
		}
		Ok(())
	}

	fn visit_define_param(&mut self, d: &DefineParamStatement) -> Result<(), Self::Error> {
		self.w.write_str("DEFINE PARAM")?;
		if d.if_not_exists {
			self.w.write_str(" IF NOT EXISTS")?;
		}
		if d.overwrite {
			self.w.write_str(" OVERWRITE")?;
		}
		write!(self.w, " ${} VALUE ", d.name)?;

		self.with_state(
			|s| PassState {
				breaking_closures: false,
				breaking_futures: false,
				..s
			},
			|this| this.visit_value(&d.value),
		)?;

		if let Some(ref v) = d.comment {
			write!(self.w, " COMMENT {v}")?
		}
		self.w.write_str(" PERMISSIONS ")?;
		self.visit_permission(&d.permissions)
	}

	fn visit_define_analyzer(&mut self, d: &DefineAnalyzerStatement) -> Result<(), Self::Error> {
		write!(self.w, "{d}")
	}

	fn visit_define_function(&mut self, d: &DefineFunctionStatement) -> Result<(), Self::Error> {
		self.w.write_str("DEFINE FUNCTION")?;
		if d.if_not_exists {
			self.w.write_str(" IF NOT EXISTS")?;
		}
		if d.overwrite {
			self.w.write_str(" OVERWRITE")?;
		}
		write!(self.w, " fn::{}(", EscapePath(&d.name.0))?;
		for (i, (name, kind)) in d.args.iter().enumerate() {
			if i != 0 {
				self.w.write_str(",")?;
			}
			write!(self.w, "${name}:{kind}")?;
		}
		self.w.write_str(")")?;
		if let Some(ref v) = d.returns {
			write!(self.w, "-> {v}")?;
		}
		self.visit_block(&d.block)?;
		if let Some(ref v) = d.comment {
			write!(self.w, " COMMENT {v}")?
		}
		self.w.write_str(" PERMISSIONS ")?;
		self.visit_permission(&d.permissions)
	}

	fn visit_permission(&mut self, p: &Permission) -> Result<(), Self::Error> {
		match p {
			Permission::None => self.w.write_str("NONE"),
			Permission::Full => self.w.write_str("FULL"),
			Permission::Specific(value) => {
				self.w.write_str("WHERE ")?;
				self.visit_value(value)
			}
		}
	}

	fn visit_define_database(&mut self, d: &DefineDatabaseStatement) -> Result<(), Self::Error> {
		write!(self.w, "{d}")
	}

	fn visit_define_namespace(&mut self, d: &DefineNamespaceStatement) -> Result<(), Self::Error> {
		write!(self.w, "{d}")
	}

	fn visit_create(&mut self, c: &CreateStatement) -> Result<(), Self::Error> {
		self.with_state(
			|s| PassState {
				breaking_futures: true,
				..s
			},
			|this| {
				this.w.write_str("CREATE ")?;
				if c.only {
					this.w.write_str(" ONLY ")?;
				}
				for (idx, v) in c.what.0.iter().enumerate() {
					if idx != 0 {
						this.w.write_str(",")?;
					}
					this.with_state(
						|s| PassState {
							mock_allowed: true,
							..s
						},
						|this| this.visit_value(v),
					)?;
				}

				if let Some(d) = c.data.as_ref() {
					this.w.write_str(" ")?;
					this.with_state(
						|s| PassState {
							breaking_closures: true,
							..s
						},
						|this| this.visit_data(d),
					)?;
				}
				if let Some(o) = c.output.as_ref() {
					this.w.write_str(" ")?;
					this.visit_output(o)?;
				}
				if let Some(ref v) = c.version {
					this.w.write_str(" VERSION ")?;
					this.visit_value(&v.0)?;
				}
				if let Some(ref v) = c.timeout {
					write!(this.w, " {v}")?
				}
				Ok(())
			},
		)
	}

	fn visit_output(&mut self, o: &Output) -> Result<(), Self::Error> {
		self.w.write_str("RETURN ")?;
		match o {
			Output::None => self.w.write_str("NONE"),
			Output::Null => self.w.write_str("NULL"),
			Output::Diff => self.w.write_str("DIFF"),
			Output::After => self.w.write_str("AFTER"),
			Output::Before => self.w.write_str("BEFORE"),
			Output::Fields(v) => match v.single() {
				Some(v) => write!(self.w, "VALUE {}", &v),
				None => {
					if let Some(Field::Single {
						expr,
						alias,
					}) = v.0.first()
					{
						// Avoid conflict between the value NONE with the `Output::None`.
						if expr.has_left_none_or_null() {
							self.w.write_char('(')?;
							self.visit_value(expr)?;
							self.w.write_char(')')?;
							if let Some(a) = alias {
								self.w.write_str(" AS ")?;
								self.visit_idiom(a)?;
							}

							for i in &v.0[1..] {
								self.w.write_str(", ")?;
								self.visit_field(i)?
							}
							return Ok(());
						}
					}
					self.visit_fields(v)
				}
			},
		}
	}

	fn visit_data(&mut self, d: &Data) -> Result<(), Self::Error> {
		match d {
			Data::EmptyExpression => {}
			Data::SetExpression(items) => {
				self.w.write_str("SET ")?;
				for (idx, v) in items.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(", ")?;
					}
					self.visit_idiom(&v.0)?;
					write!(self.w, " {} ", v.1)?;
					self.with_state(|s| s.with_breaking_storage(), |this| this.visit_value(&v.2))?;
				}
			}
			Data::UnsetExpression(idioms) => {
				self.w.write_str("UNSET ")?;
				for (idx, v) in idioms.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(", ")?;
					}
					self.visit_idiom(v)?;
				}
			}
			Data::PatchExpression(value) => {
				self.w.write_str("PATCH ")?;
				self.with_state(|s| s.with_breaking_storage(), |this| this.visit_value(value))?;
			}
			Data::MergeExpression(value) => {
				self.w.write_str("MERGE ")?;
				self.with_state(|s| s.with_breaking_storage(), |this| this.visit_value(value))?;
			}
			Data::ReplaceExpression(value) => {
				self.w.write_str("REPLACE ")?;
				self.with_state(|s| s.with_breaking_storage(), |this| this.visit_value(value))?;
			}
			Data::ContentExpression(value) => {
				self.w.write_str("CONTENT ")?;
				self.with_state(|s| s.with_breaking_storage(), |this| this.visit_value(value))?;
			}
			Data::SingleExpression(value) => {
				self.visit_value(value)?;
			}
			Data::ValuesExpression(items) => {
				let Some(first) = items.first() else {
					return Ok(());
				};
				self.w.write_str("(")?;
				for (idx, i) in first.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(",")?;
					}
					self.visit_idiom(&i.0)?;
				}
				self.w.write_str(") VALUES ")?;
				for (idx, i) in items.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(",")?;
					}
					self.w.write_str("(")?;
					for (idx, i) in i.iter().enumerate() {
						if idx != 0 {
							self.w.write_str(",")?;
						}
						self.with_state(
							|s| s.with_breaking_storage(),
							|this| this.visit_value(&i.1),
						)?;
					}
					self.w.write_str(")")?;
				}
			}
			Data::UpdateExpression(items) => {
				self.w.write_str("ON DUPLICATE KEY UPDATE ")?;
				for (idx, v) in items.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(",")?;
					}
					self.visit_idiom(&v.0)?;
					self.w.write_char(' ')?;
					self.visit_operator(&v.1)?;
					self.w.write_char(' ')?;
					self.with_state(|s| s.with_breaking_storage(), |this| this.visit_value(&v.2))?;
				}
			}
		}
		Ok(())
	}

	fn visit_analyze_stmt(&mut self, v: &AnalyzeStatement) -> Result<(), Self::Error> {
		let before = self.w.location();
		write!(self.w, "{v}")?;
		let after = self.w.location();
		self.issues.push(MigrationIssue {
			severity: Severity::WillBreak,
			error: "Found use of `ANALYZE` statement which has been removed".to_string(),
			details: String::new(),
			kind: IssueKind::AnalyzeStatement,
			origin: self.path.clone(),
			error_location: Some(Snippet::from_source_location_range(
				self.w.flush_source(),
				before..after,
				None,
				MessageKind::Error,
			)),
			resolution: None,
		});
		Ok(())
	}

	fn visit_value(&mut self, value: &Value) -> Result<(), Self::Error> {
		static HEX_CHARS: &[u8; 16] = b"0123456789ABCDEF";

		match value {
			Value::None => self.w.write_str("NONE"),
			Value::Null => self.w.write_str("NULL"),
			Value::Bool(x) => {
				if *x {
					self.w.write_str("true")
				} else {
					self.w.write_str("false")
				}
			}
			Value::Number(_)
			| Value::Strand(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Uuid(_)
			| Value::Table(_)
			| Value::Geometry(_)
			| Value::Regex(_)
			| Value::Constant(_) => {
				write!(self.w, "{value}")
			}

			Value::Bytes(b) => {
				self.w.write_str("b\"")?;
				for b in b.0.iter() {
					self.w.write_char(HEX_CHARS[(b >> 4) as usize] as char)?;
					self.w.write_char(HEX_CHARS[(b & 0b1111) as usize] as char)?;
				}
				self.w.write_char('"')
			}

			Value::Mock(_) => value.visit(self),

			Value::Array(_)
			| Value::Param(_)
			| Value::Object(_)
			| Value::Idiom(_)
			| Value::Cast(_)
			| Value::Block(_)
			| Value::Edges(_)
			| Value::Future(_)
			| Value::Function(_)
			| Value::Subquery(_)
			| Value::Expression(_)
			| Value::Query(_)
			| Value::Model(_)
			| Value::Closure(_)
			| Value::Refs(_) => self.with_state(
				|s| PassState {
					mock_allowed: false,
					..s
				},
				|this| value.visit(this),
			),
			Value::Thing(thing) => self.with_state(
				|s| PassState {
					mock_allowed: false,
					..s
				},
				|this| {
					if let Id::Range(_) = thing.id {
						this.w.write_char('(')?;
						this.visit_thing(thing)?;
						this.w.write_char(')')
					} else {
						this.visit_thing(thing)
					}
				},
			),
			Value::Range(_) => {
				self.w.write_char('(')?;
				self.with_state(
					|s| PassState {
						mock_allowed: false,
						..s
					},
					|this| value.visit(this),
				)?;
				self.w.write_char(')')
			}
		}
	}

	fn visit_refs(&mut self, _: &Refs) -> Result<(), Self::Error> {
		let before = self.w.location();
		write!(self.w, "[]")?;
		let after = self.w.location();

		self.issues.push(MigrationIssue {
			severity: Severity::WillBreak,
			error: "Found usage of experimental record references".to_owned(),
			details: String::new(),
			kind: IssueKind::RecordReferences,
			origin: self.path.clone(),
			error_location: Some(Snippet::from_source_location_range(
				self.w.flush_source(),
				before..after,
				None,
				MessageKind::Error,
			)),
			resolution: None,
		});
		Ok(())
	}

	fn visit_closure(&mut self, c: &Closure) -> Result<(), Self::Error> {
		let before = self.w.location();
		self.w.write_str("|")?;
		for (i, (name, kind)) in c.args.iter().enumerate() {
			if i > 0 {
				self.w.write_str(",")?;
			}
			write!(self.w, "${name}: <{kind}>")?;
		}
		self.w.write_str("|")?;
		if let Some(returns) = &c.returns {
			write!(self.w, " -> {returns}")?;
		}
		self.with_state(
			|s| PassState {
				breaking_closures: false,
				breaking_futures: false,
				..s
			},
			|this| {
				if let Value::Block(_) = c.body {
					this.visit_value(&c.body)
				} else {
					this.w.write_str("( ")?;
					this.visit_value(&c.body)?;
					this.w.write_str(" )")
				}
			},
		)?;

		if self.state.breaking_closures {
			let after = self.w.location();
			self.issues.push(MigrationIssue {
				severity: Severity::WillBreak,
				error: "Found future which can be stored in the datastore".to_owned(),
				details: String::new(),
				kind: IssueKind::StoredClosure,
				origin: self.path.clone(),
				error_location: Some(Snippet::from_source_location_range(
					self.w.flush_source(),
					before..after,
					None,
					MessageKind::Error,
				)),
				resolution: None,
			});
		}

		Ok(())
	}

	fn visit_model(&mut self, m: &Model) -> Result<(), Self::Error> {
		write!(self.w, "ml::{}<{}>(", EscapePath(m.name.as_str()), m.version)?;
		for (idx, p) in m.args.iter().enumerate() {
			if idx != 0 {
				self.w.write_str(",")?;
			}
			self.visit_value(p)?;
		}
		self.w.write_str(")")
	}

	fn visit_expression(&mut self, e: &Expression) -> Result<(), Self::Error> {
		self.with_state(
			|s| PassState {
				breaking_closures: false,
				breaking_futures: false,
				..s
			},
			|this| match e {
				Expression::Unary {
					o,
					v,
				} => {
					write!(this.w, "{o}(")?;
					this.visit_value(v)?;
					this.w.write_str(")")
				}
				Expression::Binary {
					l,
					o,
					r,
				} => {
					write!(this.w, "(")?;
					this.visit_value(l)?;
					let before = this.w.location();
					write!(this.w, " {o} ")?;
					let after = this.w.location();
					this.visit_value(r)?;
					this.w.write_str(")")?;

					if let Operator::Like
					| Operator::NotLike
					| Operator::AllLike
					| Operator::AnyLike = o
					{
						this.issues.push(MigrationIssue {
							severity: Severity::WillBreak,
							error: format!("Found usage of removed `{}` operator", o),
							details: String::new(),
							kind: IssueKind::LikeOperator,
							origin: this.path.clone(),
							error_location: Some(Snippet::from_source_location_range(
								this.w.flush_source(),
								before..after,
								None,
								MessageKind::Error,
							)),
							resolution: None,
						})
					}
					Ok(())
				}
			},
		)
	}

	fn visit_operator(&mut self, o: &Operator) -> Result<(), Self::Error> {
		write!(self.w, "{o}")
	}

	fn visit_subquery(&mut self, s: &Subquery) -> Result<(), Self::Error> {
		self.w.write_char('(')?;
		s.visit(self)?;
		self.w.write_char(')')
	}

	fn visit_function(&mut self, f: &Function) -> Result<(), Self::Error> {
		match f {
			Function::Normal(s, e) => {
				let before = self.w.location();
				write!(self.w, "{}(", v3_function_name(s))?;
				for (idx, e) in e.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(", ")?;
					}
					self.with_state(
						|s| PassState {
							breaking_futures: false,
							breaking_closures: false,
							..s
						},
						|this| this.visit_value(e),
					)?;
				}
				self.w.write_char(')')?;
				let after = self.w.location();
				self.check_function(before..after, s, e);
				Ok(())
			}
			Function::Custom(s, e) => {
				write!(self.w, "fn::{}(", EscapePath(s))?;
				for (idx, e) in e.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(", ")?;
					}
					self.with_state(
						|s| PassState {
							breaking_futures: false,
							breaking_closures: false,
							..s
						},
						|this| this.visit_value(e),
					)?;
				}
				self.w.write_char(')')
			}
			Function::Script(s, e) => {
				self.w.write_str("function(")?;
				for (idx, e) in e.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(", ")?;
					}
					self.with_state(
						|s| PassState {
							breaking_futures: false,
							breaking_closures: false,
							..s
						},
						|this| this.visit_value(e),
					)?;
				}
				self.w.write_str("){")?;
				self.w.write_str(s.0.as_str())?;
				self.w.write_str("}")
			}
			Function::Anonymous(p, e, _) => {
				self.w.write_char('(')?;
				self.visit_value(p)?;
				self.w.write_char(')')?;
				self.w.write_char('(')?;
				for (idx, e) in e.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(",")?;
					}
					self.with_state(
						|s| PassState {
							breaking_futures: false,
							breaking_closures: false,
							..s
						},
						|this| this.visit_value(e),
					)?;
				}
				self.w.write_char(')')
			}
		}
	}

	fn visit_future(&mut self, f: &Future) -> Result<(), Self::Error> {
		let before = self.w.location();

		if self.state.breaking_futures {
			self.w.write_str("<future> ")?;
		}

		self.with_state(
			|s| PassState {
				breaking_futures: false,
				..s
			},
			|this| {
				if f.0 .0.len() == 1 {
					this.w.write_str("( ")?;
					this.visit_entry(&f.0 .0[0])?;
					this.w.write_str(" )")
				} else {
					this.visit_block(&f.0)
				}
			},
		)?;

		if self.state.breaking_futures {
			let after = self.w.location();

			self.issues.push(MigrationIssue {
				severity: Severity::WillBreak,
				error: "Found a future which would be stored in the database".to_string(),
				details: String::new(),
				kind: IssueKind::IncompatibleFuture,
				origin: self.path.clone(),
				error_location: Some(Snippet::from_source_location_range(
					self.w.flush_source(),
					before..after,
					Some("Incompatible future"),
					MessageKind::Error,
				)),
				resolution: None,
			})
		}
		Ok(())
	}

	fn visit_edges(&mut self, e: &Edges) -> Result<(), Self::Error> {
		match e.what.len() {
			0 => {
				self.visit_thing(&e.from)?;
				write!(self.w, "{}?", e.dir)
			}
			1 => {
				self.visit_thing(&e.from)?;
				write!(self.w, "{}", e.dir)?;
				self.visit_graph_subject(&e.what.0[0])
			}
			_ => {
				self.visit_thing(&e.from)?;
				write!(self.w, "{}(", e.dir)?;
				for (idx, s) in e.what.0.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(",")?;
					}
					self.visit_graph_subject(s)?;
				}
				self.w.write_char(')')
			}
		}
	}

	fn visit_range(&mut self, value: &Range) -> Result<(), Self::Error> {
		match value.beg {
			Bound::Included(ref x) => {
				write!(self.w, "(")?;
				self.visit_value(x)?;
				write!(self.w, ")..")?;
			}
			Bound::Excluded(ref x) => {
				write!(self.w, "(")?;
				self.visit_value(x)?;
				write!(self.w, ")>..")?
			}
			Bound::Unbounded => self.w.write_str("..")?,
		}
		match value.end {
			Bound::Included(ref x) => {
				write!(self.w, "=(")?;
				self.visit_value(x)?;
				write!(self.w, ")")?;
			}
			Bound::Excluded(ref x) => {
				write!(self.w, "(")?;
				self.visit_value(x)?;
				write!(self.w, ")")?;
			}
			Bound::Unbounded => {}
		}
		Ok(())
	}

	fn visit_block(&mut self, value: &Block) -> Result<(), Self::Error> {
		self.w.write_str("{")?;
		for (idx, e) in value.0.iter().enumerate() {
			let mut escape = false;
			if idx == 0 {
				if let Entry::Value(Value::Edges(_)) | Entry::Value(Value::Thing(_)) = e {
					escape = true;
				}
			}
			if escape {
				self.w.write_char('(')?;
			}

			if idx != value.0.len() - 1 {
				self.with_state(
					|s| PassState {
						breaking_futures: false,
						breaking_closures: false,
						..s
					},
					|this| this.visit_entry(e),
				)?;
			} else {
				self.visit_entry(e)?;
			}

			if escape {
				self.w.write_char(')')?;
			}
			if idx != value.0.len() - 1 {
				self.w.write_str("; ")?;
			}
		}
		self.w.write_str("}")
	}

	fn visit_entry(&mut self, entry: &Entry) -> Result<(), Self::Error> {
		match entry {
			Entry::Break(_) => self.w.write_str("BREAK"),
			Entry::Continue(_) => self.w.write_str("CONTINUE"),
			entry => entry.visit(self),
		}
	}

	fn visit_cast(&mut self, value: &Cast) -> Result<(), Self::Error> {
		write!(self.w, "<{}> ", value.0)?;
		self.visit_value(&value.1)
	}

	fn visit_mock(&mut self, m: &Mock) -> Result<(), Self::Error> {
		let before = self.w.location();
		match m {
			Mock::Count(t, c) => {
				write!(self.w, "|{}:{}|", EscapeIdent(t), c)?;
			}
			Mock::Range(t, from, to) => {
				write!(self.w, "|{}:{}..={}|", EscapeIdent(t), from, to)?;
			}
		}

		if !self.state.mock_allowed {
			let after = self.w.location();

			self.issues.push(MigrationIssue {
				severity: Severity::UnlikelyBreak,
				error: "Found usage of mock value outside of CREATE or SELECT statement"
					.to_string(),
				details: String::new(),
				kind: IssueKind::MockValue,
				origin: self.path.clone(),
				error_location: Some(Snippet::from_source_location_range(
					self.w.flush_source(),
					before..after,
					None,
					MessageKind::Error,
				)),
				resolution: None,
			})
		}

		Ok(())
	}

	fn visit_number(&mut self, n: &Number) -> Result<(), Self::Error> {
		write!(self.w, "{n}")
	}

	fn visit_geometry(&mut self, g: &Geometry) -> Result<(), Self::Error> {
		write!(self.w, "{g}")
	}

	fn visit_array(&mut self, array: &Array) -> Result<(), Self::Error> {
		self.w.write_str("[")?;
		for (idx, v) in array.0.iter().enumerate() {
			if idx != 0 {
				self.w.write_str(",")?;
			}
			self.visit_value(v)?;
		}
		self.w.write_str("]")?;
		Ok(())
	}

	fn visit_object(&mut self, obj: &Object) -> Result<(), Self::Error> {
		self.w.write_str("{")?;
		let skip = self.state.skip_relation_field;
		for (idx, (k, v)) in obj.0.iter().filter(|(k, _)| !skip || *k != "__").enumerate() {
			if idx != 0 {
				self.w.write_str(",")?;
			}
			write!(self.w, "{}:", EscapeKey(k.as_str()))?;
			self.with_state(
				|p| PassState {
					skip_relation_field: false,
					..p
				},
				|this| this.visit_value(v),
			)?;
		}
		self.w.write_str("}")?;
		Ok(())
	}

	fn visit_thing(&mut self, t: &Thing) -> Result<(), Self::Error> {
		if let Id::Range(_) = t.id {
			self.w.write_char('(')?;
		}
		write!(self.w, "{}:", EscapeIdent(t.tb.as_str()))?;
		t.visit(self)?;
		if let Id::Range(_) = t.id {
			self.w.write_char(')')?;
		}
		Ok(())
	}

	fn visit_id(&mut self, id: &crate::sql::Id) -> Result<(), Self::Error> {
		match id {
			Id::Array(array) => self.visit_array(array),
			Id::Object(object) => self.visit_object(object),
			Id::Range(id_range) => self.visit_id_range(id_range),
			x => write!(self.w, "{x}"),
		}
	}

	fn visit_id_range(&mut self, id_range: &IdRange) -> Result<(), Self::Error> {
		match id_range.beg {
			Bound::Included(ref x) => {
				self.visit_id(x)?;
				write!(self.w, "..")?;
			}
			Bound::Excluded(ref x) => {
				self.visit_id(x)?;
				write!(self.w, ">..")?
			}
			Bound::Unbounded => self.w.write_str("..")?,
		}
		match id_range.end {
			Bound::Included(ref x) => {
				write!(self.w, "=")?;
				self.visit_id(x)?;
			}
			Bound::Excluded(ref x) => {
				self.visit_id(x)?;
			}
			Bound::Unbounded => {}
		}
		Ok(())
	}

	fn visit_param(&mut self, p: &Param) -> Result<(), Self::Error> {
		write!(self.w, "{p}")?;
		Ok(())
	}

	fn visit_idiom(&mut self, idiom: &Idiom) -> Result<(), Self::Error> {
		let mut last_field = None;
		let parts = match idiom.0.first() {
			Some(Part::Start(value)) => {
				self.w.write_str("(")?;
				self.with_state(
					|s| PassState {
						non_expression_idiom: false,
						..s
					},
					|this| this.visit_value(value),
				)?;
				self.w.write_str(")")?;
				&idiom.0[1..]
			}
			Some(Part::Field(value)) => {
				let loc = self.w.location();
				last_field = Some(loc);
				write!(self.w, "{}", value)?;

				if value.as_str() == "id" && idiom.0.len() > 1 && !self.state.non_expression_idiom {
					let after = self.w.location();
					self.issues.push(MigrationIssue {
						severity: Severity::CanBreak,
						error:
							"Found usage of an `.id` idiom, this field had special behavior for record-ids in 2.0 which has been removed in 3.0"
							.to_string(),
							details: String::new(),
							kind: IssueKind::IdField,
							origin: self.path.clone(),
							error_location: Some(Snippet::from_source_location_range(
									self.w.flush_source(),
									loc..after,
									None,
									MessageKind::Error,
							)),
							resolution: None,
					})
				}

				&idiom.0[1..]
			}
			_ => idiom.0.as_slice(),
		};
		for (idx, p) in parts.iter().enumerate() {
			let prev_field = last_field;
			let loc = self.w.location();
			if matches!(p, Part::Field(_)) {
				last_field = Some(loc);
			} else {
				last_field = None
			}

			self.visit_part(p)?;

			if let Part::Field(x) = p {
				if x.as_str() == "id" && parts.len() > idx + 1 && !self.state.non_expression_idiom {
					let after = self.w.location();
					self.issues.push(MigrationIssue {
						severity: Severity::CanBreak,
						error:
							"Found usage of an `.id` idiom, this field had special behavior for record-ids in 2.0 which has been removed in 3.0"
							.to_string(),
							details: String::new(),
							kind: IssueKind::IdField,
							origin: self.path.clone(),
							error_location: Some(Snippet::from_source_location_range(
									self.w.flush_source(),
									loc..after,
									None,
									MessageKind::Error,
							)),
							resolution: None,
					})
				}
			}

			if let Some(field) = prev_field {
				if last_field.is_none() && !self.state.non_expression_idiom {
					let after = self.w.location();
					self.issues.push(MigrationIssue {
						severity: Severity::CanBreak,
						error:
							"Found usage of an idiom with a field followed by another idiom part"
								.to_string(),
						details: String::new(),
						kind: IssueKind::FieldIdiomFollowed,
						origin: self.path.clone(),
						error_location: Some(Snippet::from_source_location_range(
							self.w.flush_source(),
							field..after,
							None,
							MessageKind::Error,
						)),
						resolution: None,
					})
				}
			}
		}
		Ok(())
	}

	fn visit_part(&mut self, part: &Part) -> Result<(), Self::Error> {
		match part {
			Part::All => {
				let before = self.w.location();
				self.w.write_str(".*")?;
				if !self.state.non_expression_idiom {
					let after = self.w.location();
					self.issues.push(MigrationIssue {
						severity: Severity::CanBreak,
						error: "Found usage of all idiom `.*` behavior of which has changed when used on an array or object".to_string(),
						details: String::new(),
						kind: IssueKind::AllIdiom,
						origin: self.path.clone(),
						error_location: Some(Snippet::from_source_location_range(
								self.w.flush_source(),
								before..after,
								None,
								MessageKind::Error,
						)),
						resolution: None,
					})
				}
			}
			Part::Where(value) => {
				self.w.write_str("[? ")?;
				self.with_state(
					|s| PassState {
						non_expression_idiom: false,
						breaking_futures: false,
						breaking_closures: false,
						..s
					},
					|this| this.visit_value(value),
				)?;
				self.w.write_str("]")?;
			}
			Part::Graph(graph) => self.visit_graph(graph)?,
			Part::Value(value) => {
				self.w.write_str("[")?;
				self.with_state(
					|s| PassState {
						non_expression_idiom: false,
						breaking_futures: false,
						breaking_closures: false,
						..s
					},
					|this| this.visit_value(value),
				)?;
				self.w.write_str("]")?;
			}
			Part::Start(value) => {
				// Shouldn't happen.
				self.with_state(
					|s| PassState {
						non_expression_idiom: false,
						..s
					},
					|this| this.visit_value(value),
				)?;
			}
			Part::Method(name, values) => {
				write!(self.w, ".{}(", EscapeKwFreeIdent(name))?;
				for (idx, v) in values.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(",")?;
					}
					self.with_state(
						|s| PassState {
							non_expression_idiom: false,
							breaking_futures: false,
							breaking_closures: false,
							..s
						},
						|this| this.visit_value(v),
					)?;
				}
				self.w.write_char(')')?;
			}
			Part::Last
			| Part::First
			| Part::Flatten
			| Part::Field(_)
			| Part::Index(_)
			| Part::Doc
			| Part::RepeatRecurse => {
				write!(self.w, "{part}")?;
			}
			Part::Optional => {
				write!(self.w, ".?")?;
			}
			Part::Destructure(s) => {
				self.w.write_str(".{")?;
				for (idx, v) in s.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(",")?;
					}
					self.visit_destructure_part(v)?;
				}
				self.w.write_str("}")?;
			}
			Part::Recurse(recurse, idiom, recurse_instruction) => {
				write!(self.w, ".{{{recurse}")?;
				if let Some(instruction) = recurse_instruction {
					self.w.write_str("+")?;
					self.visit_recurse_instruction(instruction)?;
				}
				self.w.write_str("}")?;

				if let Some(i) = idiom {
					self.w.write_str("(")?;
					for p in i.0.iter() {
						self.visit_part(p)?;
					}
					self.w.write_str(")")?;
				}
			}
		}
		Ok(())
	}

	fn visit_recurse_instruction(&mut self, r: &RecurseInstruction) -> Result<(), Self::Error> {
		match r {
			RecurseInstruction::Path {
				..
			}
			| RecurseInstruction::Collect {
				..
			} => {
				write!(self.w, "{r}")?;
			}
			RecurseInstruction::Shortest {
				expects,
				inclusive,
			} => {
				self.w.write_str("shortest=")?;
				self.visit_value(expects)?;
				if *inclusive {
					self.w.write_str("+inclusive")?;
				}
			}
		}
		Ok(())
	}

	fn visit_destructure_part(&mut self, p: &DestructurePart) -> Result<(), Self::Error> {
		match p {
			DestructurePart::All(ident) => write!(self.w, "{ident}.*")?,
			DestructurePart::Field(ident) => write!(self.w, "{ident}")?,
			DestructurePart::Aliased(ident, idiom) => {
				write!(self.w, "{ident}:")?;
				self.visit_idiom(idiom)?;
			}
			DestructurePart::Destructure(ident, destructure_parts) => {
				write!(self.w, "{ident}.{{")?;
				for (idx, p) in destructure_parts.iter().enumerate() {
					if idx != 0 {
						self.w.write_str(",")?;
					}
					self.visit_destructure_part(p)?;
				}
				self.w.write_str("}")?;
			}
		}
		Ok(())
	}

	fn visit_graph(&mut self, graph: &Graph) -> Result<(), Self::Error> {
		if graph.what.0.is_empty() {
			write!(self.w, "{}?", graph.dir)?;
		} else if graph.what.0.len() == 1
			&& graph.cond.is_none()
			&& graph.alias.is_none()
			&& graph.split.is_none()
			&& graph.order.is_none()
			&& graph.limit.is_none()
			&& graph.start.is_none()
			&& graph.expr.is_none()
			&& !matches!(graph.what.0[0], GraphSubject::Range(..))
		{
			write!(self.w, "{}", graph.dir)?;
			self.visit_graph_subject(&graph.what.0[0])?;
		} else {
			write!(self.w, "{}(", graph.dir)?;
			if let Some(expr) = &graph.expr {
				self.w.write_str("SELECT ")?;
				self.visit_fields(expr)?;
				self.w.write_str(" FROM ")?;
			}

			for (idx, s) in graph.what.0.iter().enumerate() {
				if idx != 0 {
					self.w.write_str(",")?;
				}
				self.visit_graph_subject(s)?;
			}

			if let Some(ref v) = graph.cond {
				self.w.write_str(" WHERE ")?;
				self.visit_value(&v.0)?;
			}
			if graph.group.is_none() {
				if let Some(ref v) = graph.split {
					self.w.write_str(" SPLIT ON ")?;
					for (idx, i) in v.0.iter().enumerate() {
						if idx != 0 {
							write!(self.w, ",")?;
						}
						self.visit_idiom(i)?;
					}
				}
			}
			if let Some(ref v) = graph.group {
				if v.0.is_empty() {
					self.w.write_str(" GROUP ALL")?;
				} else {
					self.w.write_str(" GROUP BY ")?;
					for (idx, i) in v.0.iter().enumerate() {
						if idx != 0 {
							write!(self.w, ",")?;
						}
						self.visit_idiom(i)?;
					}
				}
			}
			if let Some(ref v) = graph.order {
				match v {
					Ordering::Random => {
						self.w.write_str(" ORDER BY RAND()")?;
					}
					Ordering::Order(order_list) => {
						self.w.write_str(" ORDER BY ")?;
						for (idx, o) in order_list.iter().enumerate() {
							if idx != 0 {
								self.w.write_str(", ")?;
							}
							self.visit_idiom(&o.value)?;
							if o.collate {
								self.w.write_str(" COLLATE")?;
							}
							if o.numeric {
								self.w.write_str(" NUMERIC")?;
							}
							if !o.direction {
								self.w.write_str(" DESC")?;
							}
						}
					}
				}
			}
			if let Some(ref v) = graph.limit {
				self.w.write_str(" LIMIT ")?;
				self.visit_value(&v.0)?;
			}
			if let Some(ref v) = graph.start {
				self.w.write_str(" START ")?;
				self.visit_value(&v.0)?;
			}
			if let Some(ref v) = graph.alias {
				write!(self.w, " AS ")?;
				self.visit_idiom(v)?;
			}
			self.w.write_char(')')?;
		}
		Ok(())
	}

	fn visit_fields(&mut self, fields: &Fields) -> Result<(), Self::Error> {
		self.with_state(
			|s| PassState {
				breaking_futures: false,
				breaking_closures: false,
				..s
			},
			|this| {
				if let Some(v) = fields.single() {
					write!(this.w, "VALUE ")?;
					this.visit_field(v)?;
				} else {
					for (idx, f) in fields.0.iter().enumerate() {
						if idx != 0 {
							this.w.write_str(",")?;
						}
						this.visit_field(f)?;
					}
				}

				Ok(())
			},
		)
	}

	fn visit_field(&mut self, field: &Field) -> Result<(), Self::Error> {
		match field {
			Field::All => {
				self.w.write_char('*')?;
			}
			Field::Single {
				expr,
				alias,
			} => {
				self.with_state(
					|s| PassState {
						mock_allowed: false,
						..s
					},
					|this| this.visit_value(expr),
				)?;
				if let Some(alias) = alias {
					self.w.write_str(" AS ")?;
					for (idx, p) in alias.0.iter().enumerate() {
						match p {
							Part::Field(ident) => {
								if idx != 0 {
									self.w.write_char('.')?;
								}
								write!(self.w, "{ident}")?;
							}
							Part::All | Part::Last | Part::Index(_) => self.visit_part(p)?,
							x => {
								if idx != 0 {
									self.w.write_char('.')?;
								}
								let part = x.to_string();
								write!(self.w, "{}", EscapeKwFreeIdent(&part))?;
							}
						}
					}
				}
			}
		}
		Ok(())
	}

	fn visit_graph_subject(&mut self, subject: &GraphSubject) -> Result<(), Self::Error> {
		match subject {
			GraphSubject::Table(table) => write!(self.w, "{table}")?,
			GraphSubject::Range(table, id_range) => {
				write!(self.w, "{table}:")?;
				self.visit_id_range(id_range)?;
			}
		}
		Ok(())
	}
}
