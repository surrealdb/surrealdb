//! Compiling a mutation into its write operator.
//!
//! A mutation is one operator holding an ordered list of per-record stages, so
//! there is nothing here that resembles the pipeline a read compiles to. What
//! this does is settle three things before any record exists: which orders the
//! statement runs, what its clauses compile to, and where its subjects come
//! from. See `doc/adr/0001-streaming-write-path.md`.
//!
//! The statement's clauses are compiled **once** and shared by every record and
//! every table the statement writes to. Nothing a user wrote is planned per
//! row.

use std::sync::Arc;

use surrealdb_types::ToSql;

use crate::err::{EngineError, Error};
#[cfg(test)]
use crate::exec::ExecOperator;
use crate::exec::PhysicalExpr;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::operators::FieldSelection;
use crate::exec::operators::write::clauses::WriteProjection;
#[cfg(test)]
use crate::exec::operators::write::clauses::{WriteAssignment, WriteClauses, WriteData};
#[cfg(test)]
use crate::exec::operators::write::place::WritePlace;
#[cfg(test)]
use crate::exec::operators::write::plan::WritePlan;
use crate::exec::operators::write::plan::{WriteFacts, WriteForm};
#[cfg(test)]
use crate::exec::operators::write::{
	InsertField, InsertRows, InsertSubjects, WriteOperator, WriteSource, WriteTarget,
};
use crate::exec::planner::Planner;
use crate::expr::Expr;
use crate::expr::field::{Field, Fields, Selector};
use crate::expr::logical::{
	LogicalData, LogicalExpr, LogicalOp, LogicalSource, LogicalSubquery, LogicalUnionAll,
	LogicalValues, LogicalWrite, LogicalWriteStatement, OutputPath, OutputPathPart, SourceRef,
	lower_expr,
};
#[cfg(test)]
use crate::expr::logical::{LogicalInsertRows, LogicalInserts, LogicalWriteSource};
use crate::expr::output::Output;

/// Whether every leaf of a mutation's row source is a target the statement
/// classifies itself rather than a source the pipeline reads.
///
/// A value literal and a dynamic expression the planner compiles to a bare
/// scalar are both of them: neither resolves to a table at bind time, so
/// neither carries a schema, a layer stack or a permission node, and there is
/// nothing in the pipeline's reading of them that the write needs.
///
/// A source that becomes a value only when the statement runs — a call, a
/// closure applied to its arguments — is one too. What it answers with is
/// classified by the form that wrote it rather than read as a row: an object
/// carrying an `id` is the stored record that `id` names, and a table name is
/// scanned under the statement's own clause rather than the reader's `SELECT`.
/// That is what [`crate::exec::operators::write`] does with every target, so
/// naming one through a call needs nothing further.
///
/// A subquery is one as well, and so is a parameter the binding could not
/// resolve: one a block `LET` binds, or one bound per record or per iteration,
/// as a `FOR` variable or the referencing record a delete cascades to is. What
/// they answer with are values, not records as stored: a subquery's projection
/// chose the fields of its rows, and the caller's field permissions removed the
/// ones the caller may not see. Taking such a row as the record would store it
/// in place of the record, deleting every field it lacks. So each is evaluated
/// once, before the statement writes anything, a subquery as the read it is,
/// and each value is classified like any other target: a record id, or an
/// object carrying one, names the record that the write reads from storage for
/// itself.
///
/// All or nothing, so a list mixing the two keeps the one reading it has.
pub(crate) fn classified_targets(op: &LogicalOp) -> bool {
	match op {
		LogicalOp::Values(LogicalValues {
			for_update,
			..
		}) => !for_update,
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Dynamic(_) | SourceRef::Param(_),
			for_update,
			..
		}) => !for_update,
		LogicalOp::Subquery(_) => true,
		LogicalOp::UnionAll(LogicalUnionAll {
			inputs,
		}) => !inputs.is_empty() && inputs.iter().all(classified_targets),
		_ => false,
	}
}

/// The target expressions of a source [`classified_targets`] admitted, in the
/// order the statement wrote them.
pub(crate) fn target_exprs(op: LogicalOp) -> Vec<LogicalExpr> {
	match op {
		LogicalOp::Values(LogicalValues {
			expr,
			..
		})
		| LogicalOp::Source(LogicalSource {
			source: SourceRef::Dynamic(expr),
			..
		}) => vec![expr],
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Param(param),
			..
		}) => vec![LogicalExpr::Param(param)],
		LogicalOp::Subquery(LogicalSubquery {
			input,
		}) => vec![LogicalExpr::Subquery(input)],
		LogicalOp::UnionAll(LogicalUnionAll {
			inputs,
		}) => inputs.into_iter().flat_map(target_exprs).collect(),
		other => unreachable!("a source {other} the classification did not admit"),
	}
}

/// How a `RETURN` clause names itself in a decline.
const OUTPUT_ROLE: &str = "a RETURN clause";

impl Planner<'_> {
	#[cfg(test)]
	/// Compile one target of a target-taking form.
	///
	/// A mock is carried whole rather than compiled: the write drains its ids
	/// as it writes them, where an expression would have to evaluate the whole
	/// set first. Every other target is an ordinary expression.
	pub(crate) async fn write_target(&self, target: LogicalExpr) -> Result<WriteTarget, Error> {
		match target {
			LogicalExpr::Mock(mock) => Ok(WriteTarget::Mock(mock)),
			other => Ok(WriteTarget::Expr(self.physical_expr(other).await?)),
		}
	}

	/// Compile a lowered mutation.
	///
	/// The orders are resolved first, so a form naming a stage with no body is
	/// declined before anything is compiled and before any table is touched.
	/// The decline names the stage, which is what makes the gap legible from a
	/// query rather than only from the source.
	#[cfg(test)]
	pub(crate) async fn plan_write(
		&self,
		write: LogicalWrite,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let facts = write_facts(&write);
		let plan = WritePlan::build(facts);
		if let Err(missing) = plan.resolve() {
			return Err(EngineError::unreachable(format!(
				"{} names the {} stage, which has no body",
				write.statement.kind(),
				missing.name()
			))
			.into());
		}
		let clauses = self.write_clauses(&write).await?;
		let LogicalWrite {
			statement,
			source,
			..
		} = write;
		let source = match source {
			// `RELATE`'s three expressions are one source, not three subjects:
			// the edge count is the cross product of the two endpoint lists,
			// which a per-target loop cannot express. The list is
			// `[from, through, to]`, the order
			// [`LogicalWriteSource::Targets`] documents for this form.
			LogicalWriteSource::Targets(targets)
				if matches!(statement, LogicalWriteStatement::Relate(_)) =>
			{
				let [from, through, to] =
					<[LogicalExpr; 3]>::try_from(targets).map_err(|targets: Vec<_>| {
						Error::from(EngineError::unreachable(format!(
							"RELATE names [from, through, to] and this one names {} expressions",
							targets.len()
						)))
					})?;
				WriteSource::Edges {
					from: self.physical_expr(from).await?,
					through: self.physical_expr(through).await?,
					to: self.physical_expr(to).await?,
				}
			}
			LogicalWriteSource::Targets(targets) => {
				let mut compiled = Vec::with_capacity(targets.len());
				for target in targets {
					compiled.push(self.write_target(target).await?);
				}
				WriteSource::Targets(compiled)
			}
			LogicalWriteSource::Inserts(inserts) => {
				WriteSource::Inserts(Arc::new(self.insert_subjects(&statement, inserts).await?))
			}
			// A target that is a value rather than a stored source is
			// classified by the statement, not read by the pipeline: the read
			// pipeline yields a number, a string or an object literal as a row
			// and drops `NONE` and `NULL` outright, where a mutation must
			// refuse the first three and resolve an object carrying an `id` to
			// the record it names.
			LogicalWriteSource::Rows(input) if classified_targets(&input) => {
				let mut compiled = Vec::new();
				for target in target_exprs(*input) {
					compiled.push(self.write_target(target).await?);
				}
				WriteSource::Targets(compiled)
			}
			LogicalWriteSource::Rows(input) => {
				WriteSource::Rows(Box::pin(self.fold(*input)).await?)
			}
		};
		Ok(Arc::new(WriteOperator::new(
			source,
			Arc::new(plan),
			Arc::new(clauses),
			statement.output().cloned(),
			statement.only(),
		)))
	}

	#[cfg(test)]
	/// `INSERT`'s rows are its subjects: nothing applies them to a record the
	/// statement named elsewhere, so they are compiled here rather than as a
	/// clause. Each row is compiled **once** for the statement — a `VALUES`
	/// tuple's places and right-hand sides, or the one expression the other
	/// spellings carry — and evaluated per row from there.
	pub(crate) async fn insert_subjects(
		&self,
		statement: &LogicalWriteStatement,
		inserts: LogicalInserts,
	) -> Result<InsertSubjects, Error> {
		let LogicalWriteStatement::Insert(stmt) = &statement else {
			return Err(EngineError::unreachable(format!(
				"{} does not name its rows as its subjects",
				statement.kind()
			))
			.into());
		};
		let relation = stmt.relation;
		let into = match inserts.into {
			Some(into) => Some(self.physical_expr(into).await?),
			None => None,
		};
		let rows = match inserts.rows {
			LogicalInsertRows::Tuples(tuples) => {
				let mut compiled = Vec::with_capacity(tuples.len());
				for tuple in tuples {
					let mut fields = Vec::with_capacity(tuple.len());
					for (path, value) in tuple {
						fields.push(InsertField {
							place: WritePlace::compile(self, path).await?,
							value: self.physical_expr(value).await?,
						});
					}
					compiled.push(fields);
				}
				InsertRows::Tuples(compiled)
			}
			LogicalInsertRows::Value(expr) => InsertRows::Value(self.physical_expr(expr).await?),
		};
		Ok(InsertSubjects {
			into,
			rows,
			relation,
		})
	}

	#[cfg(test)]
	/// The statement's clauses, compiled once.
	/// The clauses the engine compiles, taken from the write's
	/// logical form rather than from the statement beside it.
	///
	/// The statement carries the clause as written. This reads the lowered one,
	/// so a rule that rewrote a clause is what gets compiled — without that, a
	/// rewrite would change the plan and nothing else.
	pub(crate) async fn write_clauses(&self, write: &LogicalWrite) -> Result<WriteClauses, Error> {
		let cond = match &write.cond {
			Some(cond) => Some(self.physical_expr(cond.clone()).await?),
			None => None,
		};
		let data = match &write.data {
			Some(data) => self.write_data(data).await?,
			None => None,
		};
		let duplicate_update = match &write.duplicate_update {
			Some(data) => self.write_data(data).await?,
			None => None,
		};
		Ok(WriteClauses {
			cond,
			data,
			duplicate_update,
			output_fields: self.write_output(write.statement.output()).await?,
		})
	}

	/// The statement's `RETURN <fields>` projection, compiled once.
	///
	/// A `RETURN` clause is something the user wrote, so it is compiled here
	/// with the statement's other clauses and shared by every record and every
	/// table the statement writes to, rather than being planned per row.
	///
	/// `None` for every other `RETURN` form: those name one of the record's
	/// images and the output stage reads them off the clause itself.
	pub(crate) async fn write_output(
		&self,
		output: Option<&Output>,
	) -> Result<Option<WriteProjection>, Error> {
		let Some(Output::Fields(fields)) = output else {
			return Ok(None);
		};
		Ok(Some(match fields {
			Fields::Value(selector) => {
				WriteProjection::Value(self.write_expr(&selector.expr, OUTPUT_ROLE).await?)
			}
			Fields::Select(list) => {
				let mut compiled = Vec::with_capacity(list.len());
				for field in list {
					if let Field::Single(selector) = field {
						compiled.push(self.write_field(selector).await?);
					}
				}
				WriteProjection::Fields {
					fields: compiled,
					include_all: list.iter().any(|field| matches!(field, Field::All)),
				}
			}
		}))
	}

	/// One projected field: where its value lands, and what produces it.
	///
	/// Named by the rules a `SELECT`'s projection is named by. An alias is the
	/// output path as written, so `AS a.b` nests and `` AS `a.b` `` is one
	/// key. Without one, a path lands at the keys it spells and anything else
	/// under its derived name as a single flat key — except where that name
	/// would render a statement in scalar position, which is no name at all,
	/// and the expression's own SurrealQL text stands in for it.
	async fn write_field(&self, selector: &Selector) -> Result<FieldSelection, Error> {
		let logical = lower_write_expr(&selector.expr, OUTPUT_ROLE)?;
		if let Some(alias) = &selector.alias {
			let path = lower_write_expr(&Expr::Idiom(alias.clone()), OUTPUT_ROLE)?.output_path();
			let expr = self.physical_expr(logical).await?;
			return Ok(FieldSelection::with_alias_path(output_field_path(&path), expr));
		}
		if logical.output_name_renders_a_statement() {
			let name = FieldPath::field(selector.expr.to_sql());
			let expr = self.physical_expr(logical).await?;
			return Ok(FieldSelection::with_alias_path(name, expr));
		}
		let path = logical.output_path();
		let is_path = logical.is_path();
		let expr = self.physical_expr(logical).await?;
		Ok(if is_path {
			FieldSelection::from_field_path(output_field_path(&path), expr)
		} else {
			FieldSelection::new(&path.name(), expr)
		})
	}

	#[cfg(test)]
	/// One data clause, compiled.
	///
	/// The four clauses that carry a value compile that value; `SET` compiles
	/// each assignment's place and right-hand side; `UNSET` compiles its places
	/// alone, since its paths name what to remove and evaluate nothing.
	///
	/// `INSERT`'s `VALUES` and single-object forms are not clauses: they produce
	/// the statement's subjects, which reach the operator as
	/// [`WriteSource::Inserts`]. No form's `data()` or `duplicate_update()`
	/// answers with one, so reaching them here means a statement carried rows
	/// where a clause belongs; that is declined rather than applied to a
	/// record the rows were never about.
	async fn write_data(&self, data: &LogicalData) -> Result<Option<WriteData>, Error> {
		Ok(Some(match data {
			LogicalData::Patch(expr) => WriteData::Patch(self.physical_expr(expr.clone()).await?),
			LogicalData::Merge(expr) => WriteData::Merge(self.physical_expr(expr.clone()).await?),
			LogicalData::Replace(expr) => {
				WriteData::Replace(self.physical_expr(expr.clone()).await?)
			}
			LogicalData::Content(expr) => {
				WriteData::Content(self.physical_expr(expr.clone()).await?)
			}
			LogicalData::Unset(paths) => {
				let mut places = Vec::with_capacity(paths.len());
				for path in paths {
					places.push(WritePlace::compile(self, path.clone()).await?);
				}
				WriteData::Unset(places)
			}
			LogicalData::Set(assignments) => {
				let mut compiled = Vec::with_capacity(assignments.len());
				for assignment in assignments {
					compiled.push(WriteAssignment {
						place: WritePlace::compile(self, assignment.place.clone()).await?,
						operator: assignment.operator.clone(),
						value: self.physical_expr(assignment.value.clone()).await?,
					});
				}
				WriteData::Set(compiled)
			}
			LogicalData::Single(_) | LogicalData::Values(_) => {
				return Err(EngineError::unreachable(
					"INSERT's rows are its subjects rather than a clause, and a clause is not \
					 where a subject is written",
				)
				.into());
			}
		}))
	}

	/// One expression of a clause, lowered and compiled.
	///
	/// `role` names the clause in the decline, so a statement refused for one
	/// unlowerable expression says which of its clauses held it.
	async fn write_expr(&self, expr: &Expr, role: &str) -> Result<Arc<dyn PhysicalExpr>, Error> {
		self.physical_expr(lower_write_expr(expr, role)?).await
	}
}

/// One expression of a clause, lowered.
///
/// An expression with no logical form is a tree no parser builds, and it fails
/// the statement at plan time rather than once records are already being
/// written.
fn lower_write_expr(expr: &Expr, role: &str) -> Result<LogicalExpr, Error> {
	lower_expr(expr.clone()).map_err(|why| {
		Error::from(EngineError::unreachable(format!(
			"{role} cannot hold {}: {why}",
			expr.to_sql()
		)))
	})
}

/// The position an [`OutputPath`] names in the projected object: every key,
/// a lookup's rendering included, is a field of the value it lands in.
fn output_field_path(path: &OutputPath) -> FieldPath {
	FieldPath(
		path.parts()
			.iter()
			.map(|part| match part {
				OutputPathPart::Field(name) | OutputPathPart::Lookup(name) => {
					FieldPathPart::Field(name.clone())
				}
			})
			.collect(),
	)
}

/// What the statement contributes to the shape of its orders.
///
/// Read once, when the orders are assembled: a difference a record decides is
/// branching, and a difference the statement decides is membership in a list.
pub(crate) fn write_facts(write: &LogicalWrite) -> WriteFacts {
	let statement = &write.statement;
	let form = match statement {
		LogicalWriteStatement::Create(_) => WriteForm::Create,
		LogicalWriteStatement::Update(_) => WriteForm::Update,
		LogicalWriteStatement::Upsert(_) => WriteForm::Upsert,
		LogicalWriteStatement::Delete(_) => WriteForm::Delete,
		LogicalWriteStatement::Insert(_) => WriteForm::Insert,
		LogicalWriteStatement::Relate(_) => WriteForm::Relate,
	};
	// Read from the lowered clauses, which are what the planner compiles. The
	// statement beside them is the surface form, and a rule that rewrote a
	// clause changed only the lowered one — so asking the statement would
	// build a stage list for a clause the plan does not run.
	WriteFacts {
		form,
		has_data: write.data.is_some(),
		on_duplicate_key_update: write.duplicate_update.is_some(),
		ignore: match statement {
			LogicalWriteStatement::Insert(stmt) => stmt.ignore,
			_ => false,
		},
		repeatable: matches!(statement, LogicalWriteStatement::Upsert(_))
			&& write.cond.is_none()
			&& matches!(
				write.data,
				Some(
					LogicalData::Set(_)
						| LogicalData::Unset(_)
						| LogicalData::Merge(_)
						| LogicalData::Patch(_)
				)
			),
	}
}
