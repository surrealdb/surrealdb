//! The write operator.
//!
//! A mutation is one [`ExecOperator`] holding an ordered list of per-record
//! stages, not a chain of operators. `ValueBatch` carries `Vec<Value>`, and the
//! state a record's stages thread through each other is not a `Value`; more
//! decisively, the security ordering is not a fixed pipeline order, because on
//! the updating shapes the per-record permission gate must precede the user's
//! condition while condition pushdown into the source scan stays a per-plan
//! planner decision. See `doc/adr/0001-streaming-write-path.md`.
//!
//! The pieces:
//!
//! - [`stage`] — [`WriteStage`](stage::WriteStage), one step of an order, and the identity an order
//!   names it by.
//! - [`row`] — [`WriteRow`], the state one record threads through its stages.
//! - [`ctx`] — [`WriteCtx`], what a statement resolves once per table.
//! - [`place`] — [`WritePlace`](place::WritePlace), the address a data clause writes to, with the
//!   expressions its path embeds compiled.
//! - [`plan`] — the nine per-record orders, and the branch and retry control flow that selects
//!   between them.
//!
//! # What this operator does not own
//!
//! It never begins, commits or cancels a transaction: it obtains one from its
//! [`ExecutionContext`] and from nowhere else, and the executor owns the
//! outcome. It does not drain its input either — a mutation over a large table
//! holds one batch, not the table — which is sound because a cursor a statement
//! opened does not observe the records that statement writes.

// The surface here is the contract the per-record stages share — where a stage
// runs, what it is handed, and what the transaction guarantees around it — not
// the subset any single stage calls. The orders, which stand on their own, are
// covered by this module's own tests.
#![allow(dead_code)]

pub(crate) mod apply;
pub(crate) mod clauses;
pub(crate) mod ctx;
pub(crate) mod metadata;
pub(crate) mod place;
pub(crate) mod plan;
pub(crate) mod row;
pub(crate) mod scope;
pub(crate) mod stage;
pub(crate) mod stages;
pub(crate) mod subject;

use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;
use surrealdb_types::ToSql;

use self::clauses::WriteClauses;
use self::place::WritePlace;
use self::plan::{WritePlan, WriteProgram};
use self::row::WriteRow;
use self::scope::{WritePurpose, WriteScope};
use self::stage::StageOutcome;
use self::subject::extract_table_and_rid_key;
use crate::catalog::PermissionKind;
use crate::exec::operators::check_cancelled;
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, Error as ExecError, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, OutputShape, PhysicalExpr, ValueBatch,
	ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::mock::{IntoIter as MockIds, Mock};
use crate::expr::output::Output;
use crate::expr::paths::{IN, OUT};
use crate::expr::{ControlFlow, ControlFlowExt, Part};
use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange, TableName, Value};

/// Where a mutation's subjects come from.
///
/// The arms are not interchangeable. A creating form's subject does not
/// exist yet, so it must not be read: a reading source resolves a record id to
/// its stored document and yields nothing when there is none, which for a
/// record being created is always, dropping the write instead of performing it.
#[derive(Debug)]
pub enum WriteSource {
	/// Targets the statement named, each naming a table to derive an id in or
	/// a record id to write at. Nothing is read.
	Targets(Vec<WriteTarget>),
	/// `RELATE`'s three expressions, which name one edge each rather than one
	/// subject each. Nothing is read: `from` and `to` evaluate to the ids the
	/// edge joins, and `through` to the table the edge record is derived in or
	/// the id it is written at. One edge is written per `from` × `to` pair,
	/// which is why the three cannot be carried as three targets.
	Edges {
		from: Arc<dyn PhysicalExpr>,
		through: Arc<dyn PhysicalExpr>,
		to: Arc<dyn PhysicalExpr>,
	},
	/// `INSERT`'s rows, which are the records to write rather than a clause
	/// applied to records named elsewhere. Nothing is read.
	Inserts(Arc<InsertSubjects>),
	/// Records read by an ordinary plan subtree, for a form whose subjects
	/// already exist. This is what lowering a mutation buys: the source
	/// inherits index selection, filter pushdown and the projection-layer
	/// encoding from the read side.
	Rows(Arc<dyn ExecOperator>),
}

/// One target of a target-taking form, compiled.
///
/// Cloned per run of the operator, as the compiled targets were: the clone
/// copies a handle and a mock's bounds, never the ids a mock generates.
#[derive(Clone, Debug)]
pub(crate) enum WriteTarget {
	/// An expression evaluated once for the statement, whose value names the
	/// subjects — a table, a record id, or an array of either.
	Expr(Arc<dyn PhysicalExpr>),
	/// `|t:n|`, carried whole so its ids are generated as the write consumes
	/// them. Compiling it as an expression would build the whole set as an
	/// array before the first record is written, which for `CREATE |t:1000000|`
	/// is a million values held at once to write them one at a time.
	Mock(Mock),
}

/// `INSERT`'s rows and where they land, compiled.
///
/// Held behind an `Arc` so one run of the operator borrows it whole: a `VALUES`
/// tuple's places are compiled once for the statement and are not `Clone`,
/// which is the point — nothing a user wrote is planned per row.
#[derive(Debug)]
pub(crate) struct InsertSubjects {
	/// The `INTO` table, evaluated once per statement to a table name or to a
	/// string naming one. `None` where the statement names none, in which case
	/// every row must carry a record id, which supplies its table.
	pub(crate) into: Option<Arc<dyn PhysicalExpr>>,
	/// The rows themselves.
	pub(crate) rows: InsertRows,
	/// `INSERT RELATION`: every row is an edge, and the `in` and `out` it
	/// carries are the endpoints the edge joins rather than ordinary fields.
	pub(crate) relation: bool,
}

/// How an `INSERT` spells its rows, compiled.
#[derive(Debug)]
pub(crate) enum InsertRows {
	/// `INSERT INTO t (a, b) VALUES (1, 2), (3, 4)` — one tuple per row. Each
	/// tuple builds a fresh empty object by writing every expression through
	/// the place paired with it.
	Tuples(Vec<Vec<InsertField>>),
	/// `INSERT INTO t { … }`, `INSERT INTO t [ … ]`, `INSERT INTO t (SELECT …)`
	/// — one expression evaluated once for the statement. An object is one
	/// row, an array one row per element, and anything else is refused.
	Value(Arc<dyn PhysicalExpr>),
}

/// One `(place, expression)` pair of a `VALUES` tuple, compiled.
///
/// The left-hand side is a place rather than a field name: `INSERT INTO t
/// (a.b) VALUES (1)` writes `1` at `a.b`, building the objects the path needs,
/// exactly as the same path on the left of a `SET` does.
#[derive(Debug)]
pub(crate) struct InsertField {
	pub(crate) place: WritePlace,
	pub(crate) value: Arc<dyn PhysicalExpr>,
}

/// The owned subjects one run of the operator works from.
///
/// [`WriteSource`] is what the plan holds; this is what the stream body took
/// from it, which for a row source is the executing stream rather than the
/// operator that produces it.
enum Subjects {
	Targets(Vec<WriteTarget>),
	Edges(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
	Inserts(Arc<InsertSubjects>),
	Rows(ValueBatchStream),
}

/// Which end of a `RELATE` a value was found at.
///
/// The two ends raise different errors for the same malformed value, which is
/// the only thing in the message that says which end was wrong.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RelateSide {
	From,
	To,
}

impl RelateSide {
	/// Refuse a value that names no record at this end.
	fn refuse(self, value: &Value) -> ControlFlow {
		let value = value.to_sql();
		ControlFlow::Err(anyhow::anyhow!(match self {
			RelateSide::From => ExecError::RelateStatementIn {
				value,
			},
			RelateSide::To => ExecError::RelateStatementId {
				value,
			},
		}))
	}
}

/// The records one end of a `RELATE` names.
///
/// An end names one record, an object carrying one, or an array of either.
/// Nothing is read: an endpoint is used as the id it evaluates to, so an id
/// naming no stored record still produces an edge — whether that is admitted
/// is the edge table's own question, asked by the enforcement check in the
/// store.
pub(crate) fn relate_endpoints(
	value: Value,
	side: RelateSide,
) -> Result<Vec<RecordId>, ControlFlow> {
	fn one(value: Value, side: RelateSide) -> Result<RecordId, ControlFlow> {
		match value {
			Value::RecordId(id) => Ok(id),
			Value::Object(object) => match object.rid() {
				Some(id) => Ok(id),
				None => Err(side.refuse(&Value::Object(object))),
			},
			other => Err(side.refuse(&other)),
		}
	}
	match value {
		Value::Array(array) => array.into_iter().map(|value| one(value, side)).collect(),
		value => Ok(vec![one(value, side)?]),
	}
}

/// The row one edge is written through.
///
/// `at` is the `through` expression's value: a table the edge record's id is
/// derived in, or the id it is written at. The endpoints ride on the row as
/// [`WriteExtras::Relate`](row::WriteExtras) rather than in the record body,
/// which is where every stage keying on them reads them from — they are the
/// statement's, and a data clause naming `in` or `out` does not move them.
pub(crate) fn relate_row(at: Value, from: RecordId, to: RecordId) -> Result<WriteRow, ControlFlow> {
	let mut row = match at {
		at @ (Value::RecordId(_) | Value::Table(_)) => WriteRow::for_target(at)?,
		other => {
			return Err(ControlFlow::Err(anyhow::anyhow!(ExecError::RelateStatementOut {
				value: other.to_sql(),
			})));
		}
	};
	row.extras = row::WriteExtras::Relate(from, to, None);
	Ok(row)
}

/// One subject an evaluated target names.
///
/// A target is a value, and a value names either a record to write or a set of
/// records to find. The two are kept apart so the scan is opened only when the
/// loop reaches it, which is what keeps a whole-table target from being
/// expanded into memory before the first record is written.
pub(crate) enum Subject {
	/// One record, named outright. Boxed: a row carries both record images
	/// and every other subject is a handful of words.
	Row(Box<WriteRow>),
	/// One record, named outright, whose stored image the form writes from.
	Read(RecordId),
	/// The records stored under `table`, narrowed to `range` where the target
	/// named one.
	Scan(TableName, Option<Box<RecordIdKeyRange>>),
}

/// The subjects one evaluated target names, appended to `out` in the order the
/// statement writes them.
///
/// `deferable` says whether a target names a record to invent rather than
/// records to find — `CREATE`, and an `UPSERT` with no condition. A form that
/// is not deferable scans a table target instead and reads the record a
/// record-id target names: an updating form's before-image is the stored record, never the value
/// the target expression happened to be written as.
///
/// A record-id range is the mirror image: every updating form resolves it by
/// scanning, and `CREATE` refuses it, because a range names records to find
/// and a creating statement has none to find.
///
/// A value that names no record refuses the statement rather than being
/// skipped. The error is the generic one, which the caller re-labels with the
/// form that rejected it.
/// The subjects one target names, drained as the write consumes them.
///
/// A mock generates one id at a time, so a statement over `|t:n|` holds one
/// record at a time however large `n` is. Every other target evaluates to a
/// value whose subjects are all known once it has.
pub(crate) enum TargetSubjects {
	Named(std::vec::IntoIter<Subject>),
	Mock(MockIds),
}

impl TargetSubjects {
	/// The next subject, or `None` when the target names no more.
	///
	/// A mock's ids are classified by [`target_subjects`] like any other, so
	/// one classification governs every target: a form that refuses a record
	/// id refuses a generated one the same way.
	pub(crate) fn next(
		&mut self,
		form: plan::WriteForm,
		deferable: bool,
	) -> Result<Option<Subject>, ControlFlow> {
		match self {
			TargetSubjects::Named(subjects) => Ok(subjects.next()),
			TargetSubjects::Mock(ids) => match ids.next() {
				None => Ok(None),
				Some(id) => {
					let mut named = Vec::new();
					target_subjects(Value::RecordId(id), form, deferable, &mut named)?;
					Ok(named.into_iter().next())
				}
			},
		}
	}
}

pub(crate) fn target_subjects(
	value: Value,
	form: plan::WriteForm,
	deferable: bool,
	out: &mut Vec<Subject>,
) -> Result<(), ControlFlow> {
	let refuse = |value: &Value| {
		ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatementTarget {
			value: value.to_sql(),
		}))
	};
	match value {
		Value::Array(array) => {
			for value in array {
				target_subjects(value, form, deferable, out)?;
			}
		}
		Value::Table(table) if deferable => {
			out.push(Subject::Row(Box::new(WriteRow::for_target(Value::Table(table))?)))
		}
		Value::Table(table) => out.push(Subject::Scan(table, None)),
		Value::RecordId(id) if id.key.is_range() => {
			if matches!(form, plan::WriteForm::Create) {
				return Err(refuse(&Value::RecordId(id)));
			}
			let RecordId {
				table,
				key: RecordIdKey::Range(range),
			} = id
			else {
				unreachable!("the key was just tested for a range")
			};
			out.push(Subject::Scan(table, Some(range)));
		}
		Value::RecordId(id) if deferable => {
			out.push(Subject::Row(Box::new(WriteRow::for_target(Value::RecordId(id))?)))
		}
		Value::RecordId(id) => out.push(Subject::Read(id)),
		Value::Object(object) => match object.rid() {
			Some(id) => target_subjects(Value::RecordId(id), form, deferable, out)?,
			None => return Err(refuse(&Value::Object(object))),
		},
		other => return Err(refuse(&other)),
	}
	Ok(())
}

/// The row for a record-id subject an updating form named, with the stored
/// record as both of its images.
///
/// A record that is not there leaves both empty, which the order's existence
/// check turns into a subject the statement passes over.
pub(crate) async fn read_subject(
	exec: &ExecutionContext,
	id: RecordId,
) -> Result<WriteRow, ControlFlow> {
	let db = exec.database().context("a write requires a database context")?;
	let stored = crate::catalog::providers::TableProvider::get_record(
		&*exec.txn(),
		db.ns_ctx.ns.namespace_id,
		db.db.database_id,
		&id.table,
		&id.key,
		exec.version_stamp(),
	)
	.await
	.map_err(ControlFlow::Err)?;
	let mut row = WriteRow::for_target(Value::RecordId(id))?;
	row.adopt_stored(&stored);
	Ok(row)
}

/// The records a [`Subject::Scan`] names, as a stream of batches of stored
/// values.
///
/// Streamed rather than collected: a target naming a table names every record
/// in it, and the statement writes them one at a time.
pub(crate) fn scan_subjects(
	exec: &ExecutionContext,
	table: &TableName,
	range: Option<&RecordIdKeyRange>,
) -> Result<ValueBatchStream, ControlFlow> {
	let db = exec.database().context("a write requires a database context")?;
	let (ns_id, db_id) = (db.ns_ctx.ns.namespace_id, db.db.database_id);
	let raw = match range {
		Some(range) => crate::exec::operators::scan::record_key_range(ns_id, db_id, table, range)?,
		None => crate::key::schema::RecordPrefix {
			ns: ns_id,
			db: db_id,
			tb: std::borrow::Cow::Borrowed(table),
		}
		.range()
		.map_err(ControlFlow::Err)?,
	};
	Ok(crate::exec::operators::scan::pipeline::kv_scan_stream(
		exec.txn(),
		raw,
		exec.version_stamp(),
		None,
		crate::kvs::Direction::Forward,
		0,
		None,
		None,
		None,
	))
}

/// The rows one [`Subject`] produces, drained one at a time.
///
/// A scan is drained in batches, so a target naming a whole table never holds
/// more than one batch of records at once.
pub(crate) enum Rows {
	/// Boxed for the same reason [`Subject::Row`] is.
	One(Option<Box<WriteRow>>),
	Scan(ValueBatchStream),
	Batch(std::vec::IntoIter<Value>, ValueBatchStream),
}

impl Rows {
	pub(crate) async fn next(&mut self) -> Result<Option<WriteRow>, ControlFlow> {
		loop {
			match self {
				Self::One(row) => return Ok(row.take().map(|row| *row)),
				Self::Scan(stream) => match stream.next().await {
					Some(batch) => {
						let batch = batch?.into_values().into_iter();
						// Replaced rather than nested: the stream moves into
						// the batch state and back when the batch is spent, so
						// the two never both hold it.
						let Self::Scan(stream) = std::mem::replace(self, Self::One(None)) else {
							unreachable!("the arm was just matched")
						};
						*self = Self::Batch(batch, stream);
					}
					None => return Ok(None),
				},
				Self::Batch(batch, _) => match batch.next() {
					Some(value) => return Ok(Some(WriteRow::for_record(value)?)),
					None => {
						let Self::Batch(_, stream) = std::mem::replace(self, Self::One(None))
						else {
							unreachable!("the arm was just matched")
						};
						*self = Self::Scan(stream);
					}
				},
			}
		}
	}
}

/// The evaluation context a mutation's subject expressions read.
///
/// A mutation written where a scalar is expected — a projection, a block, a
/// function body — names its subjects against the row that holds it, exactly
/// as `$this` denotes that row: the target of `UPSERT link.target`, the
/// endpoints of a `RELATE`, and an `INSERT`'s `INTO` and its values all
/// resolve a bare path against the enclosing record. The row is bound as both
/// the current value and the document root, so a read nested inside a subject
/// expression — a method's argument, a dynamic index — is answered from the
/// row rather than from the position the path has reached; this is what
/// `SourceExpr` binds for a read's `FROM` expression.
///
/// A statement with no enclosing row evaluates its subjects against no
/// document, so a bare path there is `NONE` and names no subject.
pub(crate) fn subject_ctx(exec: &ExecutionContext) -> EvalContext<'_> {
	let ctx = EvalContext::from_exec_ctx(exec);
	match exec.value("this") {
		Some(row) => ctx.with_value_and_doc(row),
		None => ctx,
	}
}

/// Re-label a target the subject expansion rejected with the form that
/// rejected it.
pub(crate) fn refused_by(err: ControlFlow, form: plan::WriteForm) -> ControlFlow {
	let ControlFlow::Err(err) = err else {
		return err;
	};
	let Some(ExecError::InvalidStatementTarget {
		value,
	}) = crate::err::exec_error(&err)
	else {
		return ControlFlow::Err(err);
	};
	let value = value.clone();
	ControlFlow::Err(anyhow::Error::new(match form {
		plan::WriteForm::Create => ExecError::CreateStatement {
			value,
		},
		plan::WriteForm::Upsert => ExecError::UpsertStatement {
			value,
		},
		plan::WriteForm::Update => ExecError::UpdateStatement {
			value,
		},
		plan::WriteForm::Delete => ExecError::DeleteStatement {
			value,
		},
		plan::WriteForm::Insert => ExecError::InsertStatement {
			value,
		},
		plan::WriteForm::Relate | plan::WriteForm::View => ExecError::InvalidStatementTarget {
			value,
		},
	}))
}

/// The table an `INSERT`'s `INTO` clause names.
///
/// A table name or a string naming one; anything else refuses the statement,
/// naming the expression rather than what it produced, because the expression
/// is what the author can change.
pub(crate) async fn insert_into(
	expr: &Arc<dyn PhysicalExpr>,
	exec: &ExecutionContext,
) -> Result<TableName, ControlFlow> {
	match expr.evaluate(subject_ctx(exec)).await? {
		Value::Table(into) => Ok(into),
		Value::String(into) => Ok(TableName::new(into)),
		_ => Err(ControlFlow::Err(anyhow::anyhow!(ExecError::InsertStatement {
			value: expr.to_sql(),
		}))),
	}
}

/// The values one `INSERT` writes, evaluated.
///
/// Every row is produced before any of them is written, so a row expression
/// never observes a record this statement has already written.
pub(crate) async fn insert_values(
	rows: &InsertRows,
	exec: &ExecutionContext,
) -> Result<Vec<Value>, ControlFlow> {
	match rows {
		InsertRows::Tuples(tuples) => {
			let mut out = Vec::with_capacity(tuples.len());
			for tuple in tuples {
				check_cancelled(exec)?;
				// A fresh object per row, written through the tuple's places in
				// the order the statement wrote them.
				let mut row = Value::empty_object();
				for field in tuple {
					let value = field.value.evaluate(subject_ctx(exec)).await?;
					apply::set(&mut row, field.place.steps(), value, exec).await?;
				}
				out.push(row);
			}
			Ok(out)
		}
		InsertRows::Value(expr) => match expr.evaluate(subject_ctx(exec)).await? {
			Value::Array(array) => Ok(array.into_iter().collect()),
			object @ Value::Object(_) => Ok(vec![object]),
			other => Err(ControlFlow::Err(anyhow::anyhow!(ExecError::InsertStatement {
				value: other.to_sql(),
			}))),
		},
	}
}

/// The row one `INSERT` value is written through.
///
/// The table and the key are decided by the value and the statement's `INTO`
/// together: with `INTO` the row's own `id` supplies only the key — a record id
/// there has its table **discarded** in favour of `INTO`, which is what lets
/// `INSERT INTO other (SELECT * FROM test)` migrate keys across tables — and
/// without `INTO` the row's `id` must be a record id and supplies both.
///
/// The value rides on the row as [`WriteExtras`](row::WriteExtras) rather than
/// becoming the record image: it is the statement's payload, merged onto the
/// record once the id is settled, and for `INSERT RELATION` the endpoints it
/// names are the edge's rather than two of its fields.
pub(crate) fn insert_row(
	value: Value,
	into: &Option<TableName>,
	relation: bool,
) -> Result<WriteRow, ControlFlow> {
	let (table, key) = extract_table_and_rid_key(&value, into).map_err(ControlFlow::Err)?;
	let extras = if relation {
		let from = insert_endpoint(&value, &IN, |value| ExecError::InsertStatementIn {
			value,
		})?;
		let to = insert_endpoint(&value, &OUT, |value| ExecError::InsertStatementOut {
			value,
		})?;
		row::WriteExtras::Relate(from, to, Some(Arc::new(value)))
	} else {
		row::WriteExtras::Insert(Arc::new(value))
	};
	let mut row = WriteRow::for_insert(table, key);
	row.extras = extras;
	Ok(row)
}

/// One end of an `INSERT RELATION` row.
///
/// The endpoint must be a record id. Unlike `RELATE`, which accepts an object
/// carrying one or an array of either because its ends are expressions the
/// statement wrote, here the end is a field of the row being inserted, and
/// `refuse` names which end a malformed value was found at — the only thing in
/// the message that says so.
fn insert_endpoint(
	value: &Value,
	path: &[Part],
	refuse: impl FnOnce(String) -> ExecError,
) -> Result<RecordId, ControlFlow> {
	match value.pick(path) {
		Value::RecordId(id) => Ok(id),
		other => Err(ControlFlow::Err(anyhow::anyhow!(refuse(other.to_sql())))),
	}
}

/// A mutation.
#[derive(Debug)]
pub struct WriteOperator {
	/// What the statement writes to.
	source: WriteSource,
	/// The statement's clauses, compiled once and handed to every record's
	/// context.
	clauses: Arc<WriteClauses>,
	/// The per-record orders and the control flow between them.
	plan: Arc<WritePlan>,
	/// The statement's `RETURN` clause, which `output_write` dispatches on.
	output: Option<Output>,
	/// Whether the statement asked for a single record with `ONLY`. Carried on
	/// the operator because `ONLY` is a clause of the statement rather than a
	/// node of the plan, so nothing above compiles the cardinality rule for it.
	only: bool,
	metrics: Arc<OperatorMetrics>,
}

impl WriteOperator {
	pub(crate) fn new(
		source: WriteSource,
		plan: Arc<WritePlan>,
		clauses: Arc<WriteClauses>,
		output: Option<Output>,
		only: bool,
	) -> Self {
		Self {
			source,
			plan,
			clauses,
			output,
			only,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl ExecOperator for WriteOperator {
	fn name(&self) -> &'static str {
		"Write"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("form".to_string(), self.plan.form().kind().to_string())];
		if self.only {
			attrs.push(("only".to_string(), "true".to_string()));
		}
		// A mock target carries no operator, so it is named here rather than
		// among the expressions. One entry per mock however many ids it
		// generates: the write drains them, it does not hold them.
		if let WriteSource::Targets(targets) = &self.source {
			for target in targets {
				if let WriteTarget::Mock(mock) = target {
					attrs.push(("target".to_string(), mock.to_sql()));
				}
			}
		}
		// The per-record order is the operator's own, and is what a reader of
		// an `EXPLAIN` needs in order to know when a gate runs relative to a
		// user expression.
		for (which, stages) in self.plan.orders() {
			let names: Vec<&str> = stages.iter().map(|s| s.name()).collect();
			attrs.push((which.name().to_string(), names.join(" → ")));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		match &self.source {
			WriteSource::Targets(_)
			| WriteSource::Edges {
				..
			}
			| WriteSource::Inserts(_) => ContextLevel::Database,
			WriteSource::Rows(input) => ContextLevel::Database.max(input.required_context()),
		}
	}

	/// A mutation writes storage, whatever its source and whatever its stages
	/// turn out to do for a given record.
	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadWrite
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		match &self.source {
			WriteSource::Targets(_)
			| WriteSource::Edges {
				..
			}
			| WriteSource::Inserts(_) => {
				vec![]
			}
			WriteSource::Rows(input) => vec![input],
		}
	}

	/// The compiled targets, so `EXPLAIN ANALYZE` reaches the operators
	/// embedded in them. A mock target holds no operator and is named by
	/// [`Self::attributes`] instead.
	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		match &self.source {
			WriteSource::Targets(targets) => targets
				.iter()
				.filter_map(|target| match target {
					WriteTarget::Expr(expr) => Some(("target", expr)),
					WriteTarget::Mock(_) => None,
				})
				.collect(),
			WriteSource::Edges {
				from,
				through,
				to,
			} => vec![("from", from), ("through", through), ("to", to)],
			WriteSource::Inserts(inserts) => {
				let mut out: Vec<(&str, &Arc<dyn PhysicalExpr>)> = Vec::new();
				if let Some(into) = &inserts.into {
					out.push(("into", into));
				}
				match &inserts.rows {
					InsertRows::Tuples(tuples) => {
						out.extend(tuples.iter().flatten().map(|field| ("value", &field.value)))
					}
					InsertRows::Value(expr) => out.push(("rows", expr)),
				}
				out
			}
			WriteSource::Rows(_) => vec![],
		}
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		if self.only {
			CardinalityHint::AtMostOne
		} else {
			CardinalityHint::Unbounded
		}
	}

	/// `ONLY` makes the statement's value the one row it returned rather than
	/// an array of rows. The rule counts **returned** rows, so it applies after
	/// `output_write` has dropped the records the `RETURN` clause produces
	/// nothing for: `DELETE ONLY … RETURN NONE` returns no row and answers
	/// `NONE`, not a cardinality failure.
	fn output_shape(&self) -> OutputShape {
		if self.only {
			OutputShape::Scalar
		} else {
			OutputShape::Rows
		}
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// This is where a frame that refuses a write refuses a *compiled* one.
		// A definition-time check cannot stand in for it: whether a called
		// function writes may depend on which branch its arguments select, so
		// only reaching the statement settles it.
		//
		// Code that reads the frame off `Options::no_write` refuses the same
		// frames. Both raise the frame's own error rather than choosing one
		// here, so the two routes cannot name a frame differently and a frame
		// added later is refused by both.
		if let Some(frame) = ctx.root().no_write {
			return Err(ControlFlow::Err(anyhow::anyhow!(frame.side_effect_error())));
		}
		// R2: a plan whose access mode is `ReadWrite` requires the inherited
		// transaction to be writable. This is a plan-level diagnosis raised
		// before any of the statement's work begins, rather than the
		// storage-level one a stage would eventually get, and it is an error
		// rather than a panic: the statement's transaction is selected from a
		// syntactic predicate over the parsed statement, and a clause that
		// predicate under-approximates puts a mutation on a read-only
		// transaction.
		let txn = ctx.try_txn().ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
				"Cannot run {} without a transaction",
				self.plan.form().kind()
			))))
		})?;
		if !txn.writeable() {
			return Err(ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
				"Cannot run {} on a read only transaction",
				self.plan.form().kind()
			)))));
		}

		// R4: the input is consumed a batch at a time, never drained. The mode
		// passed to the buffer is this operator's own combined with its
		// input's, which keeps the read and the write on one task: a source
		// running ahead on a spawned task would order this operator's writes by
		// the scheduler, and savepoints are a stack with no handle.
		let mut subjects = match &self.source {
			WriteSource::Targets(targets) => Subjects::Targets(targets.clone()),
			WriteSource::Edges {
				from,
				through,
				to,
			} => Subjects::Edges(Arc::clone(from), Arc::clone(through), Arc::clone(to)),
			WriteSource::Inserts(inserts) => Subjects::Inserts(Arc::clone(inserts)),
			WriteSource::Rows(input) => Subjects::Rows(buffer_stream(
				input.execute(ctx)?,
				self.access_mode().combine(input.access_mode()),
				input.cardinality_hint(),
				ctx.root().ctx.config.exec.operator_buffer_size,
			)),
		};
		let plan = Arc::clone(&self.plan);
		let output = self.output.clone();
		let clauses = Arc::clone(&self.clauses);
		let has_cond = clauses.cond.is_some();
		let only = self.only;
		let exec = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let program = plan.resolve().map_err(|missing| {
				ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
					"{} cannot run on the engine: the {} stage is not implemented",
					plan.form().kind(),
					missing.name()
				))))
			})?;
			let scope =
				WriteScope::new(exec.clone(), clauses, output, plan.form().table_existence());
			let mut gate = WriteGate::resolve(plan.form(), &exec)?;
			// Rows shown to the caller, which `ONLY` is judged against.
			let mut returned = 0usize;
			// Records the statement's condition accepted, which the guaranteed
			// record is judged against. Distinct from the rows shown: a record
			// matched under `RETURN NONE`, or withheld by the select gate after
			// it was written, counts here and not there. Only a target that scans
			// a table can leave a guarantee owing, so only that arm counts.
			let mut matched = 0usize;
			let mut guaranteed: Option<TableName> = None;

			match &mut subjects {
				Subjects::Rows(input) => {
					while let Some(batch) = input.next().await {
						check_cancelled(&exec)?;
						let mut emitted = Vec::new();
						for value in batch?.into_values() {
							// A row that is a record id names the record, as
							// a target does: a subquery yielding ids writes
							// the records they name.
							let row = match value {
								Value::RecordId(id) if !id.key.is_range() => {
									read_subject(&exec, id).await?
								}
								value => WriteRow::for_record(value)
									.map_err(|err| refused_by(err, plan.form()))?,
							};
							// Taken by value: a `WriteRow` holds a `OnceCell`,
							// so a borrow of one alive across an await would
							// make the whole write stream `!Send`.
							let table = row.table().cloned();
							if !gate.admits(table.as_ref(), &exec).await? {
								continue;
							}
							if let Some(value) = write_one(&program, row, &scope).await?.output {
								emitted.push(value);
							}
						}
						returned += emitted.len();
						if only && returned > 1 {
							return Err(ControlFlow::Err(anyhow::anyhow!(
								ExecError::SingleOnlyOutput
							)));
						}
						if !emitted.is_empty() {
							yielder.emit(ValueBatch::new(emitted)).await;
						}
					}
				}
				Subjects::Targets(targets) => {
					let form = plan.form();
					// `Statement::is_deferable`: for these a target naming a
					// table names a record to invent, and for every other form
					// it names the records stored there.
					let deferable = matches!(form, plan::WriteForm::Create)
						|| (matches!(form, plan::WriteForm::Upsert) && !has_cond);
					for target in targets.iter() {
						check_cancelled(&exec)?;
						let mut subjects = match target {
							WriteTarget::Expr(expr) => {
								let value = expr.evaluate(subject_ctx(&exec)).await?;
								let mut named = Vec::new();
								target_subjects(value, form, deferable, &mut named)
									.map_err(|err| refused_by(err, form))?;
								TargetSubjects::Named(named.into_iter())
							}
							WriteTarget::Mock(mock) => {
								TargetSubjects::Mock(mock.clone().into_iter())
							}
						};
						while let Some(subject) =
							subjects.next(form, deferable).map_err(|err| refused_by(err, form))?
						{
							check_cancelled(&exec)?;
							let mut rows = match subject {
								Subject::Row(row) => Rows::One(Some(row)),
								Subject::Read(id) => {
									Rows::One(Some(Box::new(read_subject(&exec, id).await?)))
								}
								Subject::Scan(table, range) => {
									// An `UPSERT` over a table it matched
									// nothing in writes one record, once for the
									// whole statement. The slot holds the last
									// bare-table target the gate admits, assigned
									// per admitted table: a table
									// this caller may not write claims no slot,
									// and a later admitted target supersedes an
									// earlier one. The gate memoises its answer,
									// so the per-record ask that follows costs
									// nothing more.
									if matches!(form, plan::WriteForm::Upsert)
										&& range.is_none() && gate
										.admits(Some(&table), &exec)
										.await?
									{
										guaranteed = Some(table.clone());
									}
									Rows::Scan(scan_subjects(&exec, &table, range.as_deref())?)
								}
							};
							while let Some(row) = rows.next().await? {
								check_cancelled(&exec)?;
								let table = row.table().cloned();
								if !gate.admits(table.as_ref(), &exec).await? {
									continue;
								}
								let written = write_one(&program, row, &scope).await?;
								if written.matched {
									matched += 1;
								}
								if let Some(value) = written.output {
									returned += 1;
									if only && returned > 1 {
										return Err(ControlFlow::Err(anyhow::anyhow!(
											ExecError::SingleOnlyOutput
										)));
									}
									yielder.emit(ValueBatch::new(vec![value])).await;
								}
							}
						}
					}
					// A form that owes a guaranteed record writes one when the
					// statement matched nothing. What it matched is counted, not
					// what it returned: `RETURN NONE`, `OPTION IMPORT` and a select
					// gate that withholds a record all write it and show no row for
					// it, so counting rows would read a statement that updated every
					// record it scanned as one that matched none. An import is exempt
					// outright: it replays statements whose records it also
					// writes, and a guaranteed record would be a row the export
					// never held. The table is one the gate admitted when it
					// claimed the slot, so this writes without asking again.
					if matched == 0
						&& !exec.options().is_some_and(|options| options.import)
						&& let Some(table) = guaranteed.take()
					{
						let row = WriteRow::for_target(Value::Table(table))?;
						if let Some(value) = write_one(&program, row, &scope).await?.output {
							yielder.emit(ValueBatch::new(vec![value])).await;
						}
					}
				}
				Subjects::Inserts(inserts) => {
					let into = match &inserts.into {
						Some(expr) => Some(insert_into(expr, &exec).await?),
						None => None,
					};
					for value in insert_values(&inserts.rows, &exec).await? {
						check_cancelled(&exec)?;
						let row = insert_row(value, &into, inserts.relation)?;
						if let Some(value) = write_one(&program, row, &scope).await?.output {
							returned += 1;
							if only && returned > 1 {
								return Err(ControlFlow::Err(anyhow::anyhow!(
									ExecError::SingleOnlyOutput
								)));
							}
							yielder.emit(ValueBatch::new(vec![value])).await;
						}
					}
				}
				Subjects::Edges(from, through, to) => {
					// Both sides are evaluated once, and the edges are their
					// cross product: `RELATE [a, b]->knows->[c, d]` writes four
					// edges, not two subjects.
					let froms = relate_endpoints(
						from.evaluate(subject_ctx(&exec)).await?,
						RelateSide::From,
					)?;
					let tos =
						relate_endpoints(to.evaluate(subject_ctx(&exec)).await?, RelateSide::To)?;
					for f in &froms {
						for t in &tos {
							check_cancelled(&exec)?;
							// Evaluated per edge rather than once, which is
							// what a `through` naming a fresh id per edge needs.
							let at = through.evaluate(subject_ctx(&exec)).await?;
							let row = relate_row(at, f.clone(), t.clone())?;
							if let Some(value) = write_one(&program, row, &scope).await?.output {
								returned += 1;
								if only && returned > 1 {
									return Err(ControlFlow::Err(anyhow::anyhow!(
										ExecError::SingleOnlyOutput
									)));
								}
								yielder.emit(ValueBatch::new(vec![value])).await;
							}
						}
					}
				}
			}
			// A scalar-shaped operator emits exactly one value. No rows is
			// `NONE` whatever the reason: a missing record, a condition nothing
			// satisfied, and a `RETURN` clause that produced nothing all answer
			// alike.
			if only && returned == 0 {
				yielder.emit(ValueBatch::new(vec![Value::None])).await;
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "Write", &self.metrics))
	}
}

/// The statement-level table permission gate.
///
/// A caller holding no permission for the statement's clause on a table writes
/// nothing there and is told nothing: the subject is dropped, the statement
/// succeeds, and no clause of the statement is evaluated for it. A table whose
/// clause for the statement's kind answers `None` is declined before any of the
/// statement's own clauses run.
///
/// # Which forms it gates
///
/// The forms whose subjects the iterator *prepares*: `CREATE`, `UPSERT`,
/// `UPDATE` and `DELETE`. `RELATE` and `INSERT` ingest their subjects
/// directly, so neither passes a table through the classification and neither
/// is gated here — their answer comes from the creating order's own
/// `CheckPermissionsQuickCreate`, which is why a denied caller is told the
/// table's type by those two and told nothing by the others.
///
/// # Why this is not a stage
///
/// It is not per record. It is also not merely a faster way to reach the same
/// answer as the per-record gates: those open with a test that the record has
/// an id, and a target naming a *table* has none until `generate_record_id`,
/// which every creating order runs **after** the data clause. Without this,
/// `CREATE person SET n = (UPSERT ONLY counter:1 SET c += 1).c` under
/// `PERMISSIONS FOR create NONE` would perform that write and then be denied,
/// where the clause is never evaluated at all.
///
/// It also runs before the write context is resolved, which is what keeps a
/// denied creating form from defining the table it was refused on.
///
/// A table with no definition is admitted: existence is the statement's own
/// question, answered later and differently by each form.
pub(crate) struct WriteGate {
	/// The clause this statement's form is gated by.
	clause: PermissionKind,
	/// Whether this caller's writes are checked at all. A root or owner
	/// session is gated by nothing, and resolving that once per statement is
	/// what makes the common case free.
	checked: bool,
	/// Whether a subject on a table is admitted, memoised per table: an
	/// `INSERT` whose rows name their own tables asks repeatedly, and the
	/// answer is a property of the table and the caller, not of the record.
	seen: Vec<(TableName, bool)>,
}

impl WriteGate {
	/// A gate that admits every subject, for a write no caller's permissions
	/// gate.
	pub(crate) fn unchecked(form: plan::WriteForm) -> Self {
		Self {
			clause: form.permission_kind(),
			checked: false,
			seen: Vec::new(),
		}
	}

	/// Resolve the gate once for a statement.
	pub(crate) fn resolve(
		form: plan::WriteForm,
		exec: &ExecutionContext,
	) -> Result<Self, ControlFlow> {
		let db_ctx = exec.database().context("a write requires a database context")?;
		let checked = crate::exec::permission::should_check_perms(db_ctx, crate::iam::Action::Edit)
			.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?;
		Ok(Self {
			clause: form.permission_kind(),
			checked,
			seen: Vec::new(),
		})
	}

	/// Resolve the gate for another run of the statement this gate was
	/// resolved for, keeping what it has answered of each table while the
	/// catalog it read holds, as the caller guarantees.
	pub(crate) fn again(
		&mut self,
		form: plan::WriteForm,
		exec: &ExecutionContext,
	) -> Result<(), ControlFlow> {
		let seen = std::mem::take(&mut self.seen);
		*self = Self::resolve(form, exec)?;
		self.seen = seen;
		Ok(())
	}

	/// Whether a subject on `table` is written at all.
	///
	/// A subject naming no table is admitted: the row constructors refuse such
	/// a value before it reaches here, and answering for it would turn that
	/// refusal into a silent skip.
	pub(crate) async fn admits(
		&mut self,
		table: Option<&TableName>,
		exec: &ExecutionContext,
	) -> Result<bool, ControlFlow> {
		let Some(table) = table else {
			return Ok(true);
		};
		if !self.checked {
			return Ok(true);
		}
		if let Some((_, admitted)) = self.seen.iter().find(|(seen, _)| seen == table) {
			return Ok(*admitted);
		}
		let db_ctx = exec.database().context("a write requires a database context")?;
		let def =
			db_ctx.get_table_def(table, exec.version_stamp()).await.map_err(ControlFlow::Err)?;
		// Only an outright `NONE` declines here. A predicate is judged per
		// record by the order's own gate, against a record this one has not
		// seen.
		let admitted = def.is_none_or(|def| {
			let permissions = &def.permissions;
			let clause = match self.clause {
				PermissionKind::Create => &permissions.create,
				PermissionKind::Update => &permissions.update,
				PermissionKind::Delete => &permissions.delete,
				PermissionKind::Select => &permissions.select,
			};
			!clause.is_none()
		});
		self.seen.push((table.clone(), admitted));
		Ok(admitted)
	}

	/// Whether a record may be created on `table` at all: the record an
	/// `UPSERT` guarantees when it matched nothing is created, whatever
	/// clause gates the statement's other subjects. Only an outright `NONE`
	/// declines; a predicate is judged against the record once its data is
	/// applied.
	pub(crate) async fn admits_creating(
		&self,
		table: &TableName,
		exec: &ExecutionContext,
	) -> Result<bool, ControlFlow> {
		if !self.checked {
			return Ok(true);
		}
		let db_ctx = exec.database().context("a write requires a database context")?;
		let def =
			db_ctx.get_table_def(table, exec.version_stamp()).await.map_err(ControlFlow::Err)?;
		Ok(def.is_none_or(|def| !def.permissions.create.is_none()))
	}
}

/// What one record's run of the write program came to.
///
/// The two answers are independent, and a form that owes a guaranteed record
/// needs the first rather than the second: a record can be matched and show
/// nothing — under `RETURN NONE`, under `OPTION IMPORT`, or when the select
/// gate withholds a record already written — while a record the `WHERE`
/// condition rejected is neither matched nor shown.
struct Written {
	/// Whether the statement's condition accepted the record, as
	/// `check_where_condition` recorded on the row.
	matched: bool,
	/// What the statement's output clause produced for the record, and `None`
	/// whenever it produced nothing or a stage declined the record.
	output: Option<Value>,
}

/// Run one record through the statement's program, and report what the program
/// made of it.
async fn write_one(
	program: &WriteProgram,
	mut row: WriteRow,
	scope: &Arc<WriteScope>,
) -> Result<Written, ControlFlow> {
	let exec = scope.exec();
	// Polled per record as well as per batch: a statement whose stages are slow
	// pulls no fresh batch for as long as one record takes, and the buffer and
	// metric wrappers inject no cancellation of their own.
	check_cancelled(exec)?;
	exec.ctx().expect_not_timedout().await.map_err(ControlFlow::Err)?;
	let table = row
		.table()
		.cloned()
		.ok_or_else(|| {
			anyhow::anyhow!(ExecError::InvalidStatement(
				"A write subject names neither a record nor a table".to_string()
			))
		})
		.map_err(ControlFlow::Err)?;
	let write_ctx = scope.write_ctx_for(&table, WritePurpose::Statement).await?;
	// The record-type marker cannot travel in a batch of values, so a row a
	// row source produced arrives without one and picks it up here, before any
	// stage that dispatches on it runs. See `WriteRow::load_stored_metadata`.
	row.load_stored_metadata(&write_ctx).await?;
	let outcome = program.run_record(&mut row, &write_ctx).await;
	if counts_as_affected(row.mutated, &outcome)
		&& let Some(counters) = exec.ctx().statement_counters()
	{
		counters.record_affected();
	}
	let output = match outcome? {
		StageOutcome::SkipRow => None,
		StageOutcome::Continue => row.output.take(),
	};
	Ok(Written {
		matched: row.matched,
		output,
	})
}

/// Whether a record just run counts towards the statement's affected-row
/// total, which is counted before the row is dropped.
///
/// The total is the executor's source of truth for what a mutation affected,
/// and is deliberately not the number of rows returned: a `RETURN NONE` or a
/// bare `DELETE` emits nothing and still affects records, and a record skipped
/// *after* its write — an output gate refusing to show it — was still written.
///
/// `mutated` is set only once a KV write has succeeded, so a gate that refuses
/// a record before any storage write leaves it false and cannot inflate the
/// total. A hard error is excluded because the enclosing transaction is rolled
/// back. A loop or return signal is not excluded: a branching form rolls its
/// create attempt back and clears the flag itself, and on an order with no
/// savepoint the record's work stands.
///
/// The condition is the record's mutation flag read against the outcome the
/// stages returned.
pub(crate) fn counts_as_affected(
	mutated: bool,
	outcome: &Result<StageOutcome, ControlFlow>,
) -> bool {
	mutated && !matches!(outcome, Err(ControlFlow::Err(_)))
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::path::Path;

	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::{TestDb, collect, plan_statement, try_collect};
	use crate::exec::operators::write::plan::{WriteFacts, WriteForm, WritePlan};
	use crate::expr::paths::ID;
	use crate::kvs::TransactionType;

	/// Plan `sql` and run it on `exec`, returning the rows it emitted.
	async fn run(sql: &str, exec: &ExecutionContext) -> Vec<Value> {
		let plan = plan_statement(sql, exec).await;
		collect(&plan, exec).await
	}

	/// Plan `sql`, run it on `exec`, and return the error it raised.
	async fn refuse(sql: &str, exec: &ExecutionContext) -> String {
		let plan = plan_statement(sql, exec).await;
		match try_collect(&plan, exec).await {
			Err(ControlFlow::Err(err)) => err.to_string(),
			other => panic!("`{sql}` should have been refused, got {other:?}"),
		}
	}

	/// The record ids of `rows`, rendered.
	fn ids(rows: &[Value]) -> Vec<String> {
		rows.iter().map(|row| row.pick(&ID).to_sql()).collect()
	}

	/// The value at `path` on each of `rows`, rendered.
	fn field(rows: &[Value], path: &str) -> Vec<String> {
		let path = crate::exec::operators::test_util::field_idiom(path);
		rows.iter().map(|row| row.pick(&path).to_sql()).collect()
	}

	/// A database-level context over a write transaction, with `person`,
	/// `other` and the `likes` edge table defined.
	async fn insert_ctx() -> (TestDb, ExecutionContext) {
		let db = TestDb::new(
			"
			DEFINE TABLE person SCHEMALESS;
			DEFINE TABLE other SCHEMALESS;
			DEFINE TABLE likes SCHEMALESS TYPE RELATION;
			",
		)
		.await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		(db, exec)
	}

	/// Every `.rs` file of the write module, as source text.
	fn module_sources() -> Vec<(String, String)> {
		/// Every `.rs` file at or below `dir`, named relative to the module
		/// root. The stages live in a subdirectory, and a walk that stopped at
		/// the top level would police the seam and none of the bodies — which
		/// is the whole of what these two tests exist to police.
		fn collect(dir: &Path, prefix: &str, out: &mut Vec<(String, String)>) {
			for entry in std::fs::read_dir(dir).expect("a readable directory") {
				let path = entry.expect("a readable directory entry").path();
				let name = path.file_name().expect("a file name").to_string_lossy().into_owned();
				if path.is_dir() {
					collect(&path, &format!("{prefix}{name}/"), out);
					continue;
				}
				if path.extension().is_none_or(|e| e != "rs") {
					continue;
				}
				let src = std::fs::read_to_string(&path).expect("readable source");
				// The contract is about what the module does, not about what
				// its tests may name; a test that asserts a string is absent
				// would otherwise contain it.
				let src = match src.find("#[cfg(all(test").or_else(|| src.find("#[cfg(test)]")) {
					Some(at) => src[..at].to_string(),
					None => src,
				};
				out.push((format!("{prefix}{name}"), src));
			}
		}

		let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/exec/operators/write");
		let mut out = Vec::new();
		collect(&dir, "", &mut out);
		// Naming the subdirectory rather than counting alone, because the
		// count was satisfied by the seam's own files while every stage body
		// went unread.
		assert!(
			out.iter().filter(|(name, _)| name.starts_with("stages/")).count() >= 9,
			"the stage bodies were not reached; scanned {:?}",
			out.iter().map(|(name, _)| name).collect::<Vec<_>>()
		);
		out
	}

	/// R1: the operator writes through the statement's transaction and never
	/// ends one. A write that committed mid-statement would make a later
	/// failure unrollbackable, and one that cancelled would discard work the
	/// enclosing statement still owns. Savepoints are the only boundary this
	/// module may push.
	#[test]
	fn the_write_module_never_ends_a_transaction() {
		for (name, src) in module_sources() {
			for banned in ["commit(", "cancel(", "transaction("] {
				let found = src
					.lines()
					.filter(|line| !line.trim_start().starts_with("//"))
					.any(|line| line.contains(banned));
				assert!(!found, "`{banned}` appears in write/{name}; only savepoints are allowed");
			}
		}
	}

	/// The affected-row total counts a record that reached storage, once, and
	/// nothing else. A statement that returns no rows still reports what it
	/// wrote, and a record refused before any write — or one whose transaction
	/// is about to be rolled back — reports nothing.
	#[test]
	fn only_a_record_that_reached_storage_is_counted() {
		let err = || Err(ControlFlow::Err(anyhow::anyhow!("a stage failed")));
		// Written, then shown: counted.
		assert!(counts_as_affected(true, &Ok(StageOutcome::Continue)));
		// Written, then withheld by an output gate: still counted, which is
		// what separates this total from the rows the statement returns.
		assert!(counts_as_affected(true, &Ok(StageOutcome::SkipRow)));
		// Refused before any write: not counted, whichever way it ended.
		assert!(!counts_as_affected(false, &Ok(StageOutcome::SkipRow)));
		assert!(!counts_as_affected(false, &Ok(StageOutcome::Continue)));
		// A hard error rolls the transaction back, so nothing it wrote counts.
		assert!(!counts_as_affected(true, &err()));
		assert!(!counts_as_affected(false, &err()));
		// A loop or return signal leaves the record's work standing on an order
		// with no savepoint, and a branching form has already cleared the flag.
		assert!(counts_as_affected(true, &Err(ControlFlow::Break)));
		assert!(counts_as_affected(true, &Err(ControlFlow::Return(Value::None))));
		assert!(!counts_as_affected(false, &Err(ControlFlow::Continue)));
	}

	/// R2: a mutation refuses a read-only transaction before any of the
	/// statement's work begins. The transaction is selected from a syntactic
	/// predicate over the parsed statement, so a clause that predicate
	/// under-approximates must fail here rather than at the first storage
	/// write, half a record in.
	#[tokio::test]
	async fn a_write_refuses_a_read_only_transaction() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let plan = Arc::new(WritePlan::build(WriteFacts {
			form: WriteForm::Create,
			has_data: true,
			on_duplicate_key_update: false,
			ignore: false,
			repeatable: false,
		}));
		let op = WriteOperator::new(
			WriteSource::Targets(vec![]),
			plan,
			Arc::new(WriteClauses::default()),
			None,
			false,
		);
		let Err(err) = op.execute(&ctx) else {
			panic!("a write must refuse a read transaction");
		};
		let ControlFlow::Err(err) = err else {
			panic!("expected an error, got a loop or return signal");
		};
		assert!(
			err.to_string().contains("read only transaction"),
			"the refusal must name the transaction, got: {err}"
		);
	}

	/// A caller with no permission for the statement's clause writes nothing on
	/// that table, and is refused before the statement evaluates anything for
	/// the subject.
	///
	/// The per-record gates cannot stand in for this: they open with a test
	/// that the record has an id, and a target naming a table has none until
	/// `generate_record_id`, which every creating order runs after the data
	/// clause. A denied `CREATE person SET n = (…a write…)` would otherwise
	/// perform that write and only then be denied.
	#[tokio::test]
	async fn a_table_the_caller_may_not_write_admits_no_subject() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE denied SCHEMALESS PERMISSIONS FOR create NONE, FOR update FULL;
			DEFINE TABLE allowed SCHEMALESS PERMISSIONS FULL;
			DEFINE TABLE guarded SCHEMALESS PERMISSIONS FOR create WHERE n > 1;
			",
		)
		.await;
		let anonymous = Session::default().with_ns("test").with_db("test");
		let exec = db.exec_ctx_as(&anonymous, TransactionType::Write).await;

		let mut gate = WriteGate::resolve(WriteForm::Create, &exec).expect("the gate resolves");
		for (table, admitted, why) in [
			("denied", false, "a NONE create permission admits no subject"),
			("allowed", true, "a FULL permission admits every subject"),
			// A predicate is the order's own gate to judge, against a record
			// this one has not seen.
			("guarded", true, "a conditional permission is not judged here"),
			// Existence is the statement's question, answered later and
			// differently by each form.
			("absent", true, "a table with no definition carries no clause"),
		] {
			let name = TableName::from(table);
			assert_eq!(
				gate.admits(Some(&name), &exec).await.expect("the gate answers"),
				admitted,
				"{table}: {why}"
			);
		}

		// The same tables, for a caller whose writes are not checked at all.
		let owner = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let mut gate = WriteGate::resolve(WriteForm::Create, &owner).expect("the gate resolves");
		assert!(
			gate.admits(Some(&TableName::from("denied")), &owner).await.expect("the gate answers"),
			"an unchecked caller is gated by nothing"
		);
	}

	/// A `VALUES` tuple is one record. Each `(path, expression)` pair is set
	/// into a fresh object at the path it names, so a tuple's columns are
	/// places and not merely field names.
	#[tokio::test]
	async fn a_values_tuple_is_one_record() {
		let (_db, exec) = insert_ctx().await;
		let rows =
			run("INSERT INTO person (id, name, age) VALUES (1, 'ann', 30), (2, 'bo', 40)", &exec)
				.await;
		assert_eq!(ids(&rows), ["person:1", "person:2"]);
		assert_eq!(field(&rows, "name"), ["'ann'", "'bo'"]);
		assert_eq!(field(&rows, "age"), ["30", "40"]);

		// The record reached storage, not only the returned row.
		let stored = run("SELECT name FROM person ORDER BY id", &exec).await;
		assert_eq!(field(&stored, "name"), ["'ann'", "'bo'"]);

		// A column naming a path writes through it, building what the path
		// needs, exactly as the same path on the left of a `SET` does.
		let nested = run("INSERT INTO person (id, a.b) VALUES (3, 7)", &exec).await;
		assert_eq!(field(&nested, "a.b"), ["7"]);
	}

	/// A single object is one record, and an array is one record per element.
	/// A value that is neither names no record and is refused.
	#[tokio::test]
	async fn an_object_is_one_record_and_an_array_is_one_each() {
		let (_db, exec) = insert_ctx().await;
		let one = run("INSERT INTO person { id: 1, name: 'ann' }", &exec).await;
		assert_eq!(ids(&one), ["person:1"]);
		assert_eq!(field(&one, "name"), ["'ann'"]);

		let many =
			run("INSERT INTO person [{ id: 2, name: 'bo' }, { id: 3, name: 'cy' }]", &exec).await;
		assert_eq!(ids(&many), ["person:2", "person:3"]);
		assert_eq!(field(&many, "name"), ["'bo'", "'cy'"]);

		// A row with no id of its own still gets one derived in the `INTO`
		// table, and keeps its fields.
		let derived = run("INSERT INTO person { name: 'di' }", &exec).await;
		assert_eq!(field(&derived, "name"), ["'di'"]);
		assert_eq!(derived.len(), 1);

		let refused = refuse("INSERT INTO person 1", &exec).await;
		assert!(refused.contains("Cannot execute INSERT statement"), "got: {refused}");
	}

	/// Where a row's table comes from, which `INTO` decides.
	///
	/// With `INTO` the row's `id` supplies only the key, and a record id there
	/// has its own table **discarded** — which is what lets
	/// `INSERT INTO other (SELECT * FROM person)` migrate keys across tables.
	/// Without `INTO` the row's `id` must be a record id, and supplies both.
	#[tokio::test]
	async fn a_rows_id_names_its_key_and_into_names_its_table() {
		let (_db, exec) = insert_ctx().await;
		let migrated = run("INSERT INTO other { id: person:1, n: 1 }", &exec).await;
		assert_eq!(ids(&migrated), ["other:1"], "INTO wins over the record id's own table");
		assert_eq!(field(&migrated, "n"), ["1"]);

		let bare = run("INSERT { id: person:2, n: 2 }", &exec).await;
		assert_eq!(ids(&bare), ["person:2"], "without INTO the row names its own table");
		assert_eq!(field(&bare, "n"), ["2"]);

		// With no `INTO` and no record id there is no table to write to.
		let refused = refuse("INSERT { n: 3 }", &exec).await;
		assert!(refused.contains("property 'id' is"), "got: {refused}");
	}

	/// `INSERT RELATION` writes an edge: the `in` and `out` the row carries are
	/// the endpoints the edge joins, and must be record ids.
	#[tokio::test]
	async fn insert_relation_writes_an_edge_between_the_endpoints_the_row_names() {
		let (_db, exec) = insert_ctx().await;
		// The vertices, so the hop below has somewhere to start from.
		run("CREATE person:a, person:b", &exec).await;
		let rows = run(
			"INSERT RELATION INTO likes { id: 1, in: person:a, out: person:b, since: 2024 }",
			&exec,
		)
		.await;
		assert_eq!(ids(&rows), ["likes:1"]);
		assert_eq!(field(&rows, "in"), ["person:a"]);
		assert_eq!(field(&rows, "out"), ["person:b"]);
		assert_eq!(field(&rows, "since"), ["2024"]);

		// The edge is reachable from the vertex it points out of, which is the
		// adjacency the endpoints wrote.
		let hop = run("SELECT ->likes->person AS seen FROM person:a", &exec).await;
		assert_eq!(field(&hop, "seen"), ["[person:b]"]);

		// Each end names which one was wrong.
		let from = refuse("INSERT RELATION INTO likes { in: 1, out: person:b }", &exec).await;
		assert!(from.contains("property 'in' is"), "got: {from}");
		let to = refuse("INSERT RELATION INTO likes { in: person:a, out: 1 }", &exec).await;
		assert!(to.contains("property 'out' is"), "got: {to}");
	}

	/// The clause a form is gated by is the one its statement kind names, which
	/// is not the same as the order's own per-record gate: an
	/// `UPSERT` that creates is still gated by `update` here.
	#[test]
	fn a_form_is_gated_by_the_clause_its_statement_kind_names() {
		use crate::catalog::PermissionKind;
		for (form, clause) in [
			(WriteForm::Create, PermissionKind::Create),
			(WriteForm::Delete, PermissionKind::Delete),
			(WriteForm::Update, PermissionKind::Update),
			(WriteForm::Upsert, PermissionKind::Update),
			(WriteForm::Relate, PermissionKind::Update),
			(WriteForm::Insert, PermissionKind::Update),
		] {
			assert_eq!(form.permission_kind(), clause, "{form:?}");
		}
	}
}
