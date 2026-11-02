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

pub(crate) mod apply;
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

use futures::StreamExt;
use surrealdb_types::ToSql;

use self::row::WriteRow;
use self::stage::StageOutcome;
use self::subject::extract_table_and_rid_key;
use crate::catalog::{Generation, PermissionKind};
use crate::exec::{Error as ExecError, ExecutionContext, ValueBatchStream};
use crate::expr::mock::IntoIter as MockIds;
use crate::expr::paths::{IN, OUT};
use crate::expr::{ControlFlow, ControlFlowExt, Part};
use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange, TableName, Value};

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
				value
			},
			RelateSide::To => ExecError::RelateStatementId {
				value
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
		if let Value::RecordId(id) = value {
			Ok(id)
		} else if let Value::Object(object) = value {
			match object.rid() {
				Some(id) => Ok(id),
				None => Err(side.refuse(&Value::Object(object))),
			}
		} else {
			Err(side.refuse(&value))
		}
	}
	if let Value::Array(array) = value {
		array.into_iter().map(|value| one(value, side)).collect()
	} else {
		Ok(vec![one(value, side)?])
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
	let mut row = if matches!(at, Value::RecordId(_) | Value::Table(_)) {
		WriteRow::for_target(at)?
	} else {
		return Err(ControlFlow::Err(anyhow::anyhow!(ExecError::RelateStatementOut {
			value: at.to_sql()
		})));
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
/// `deferable` says whether a table target names a record to invent rather
/// than records to find — `CREATE`, and an `UPSERT` with no condition. A form
/// that is not deferable scans a table target instead.
///
/// A record-id target names a record to create without reading it only for
/// `CREATE`. Every other form reads the record it names: an updating form's
/// before-image is the stored record, never the value the target expression
/// happened to be written as. `UPSERT` reads it with or without a condition, so
/// a stored record takes the updating order and its data clause is evaluated
/// against the stored record, never against the empty record a creating
/// attempt starts from.
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
				Some(id) => record_subject(id, form, deferable).map(Some),
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
		Value::RecordId(id) => out.push(record_subject(id, form, deferable)?),
		Value::Object(object) => match object.rid() {
			Some(id) => out.push(record_subject(id, form, deferable)?),
			None => return Err(refuse_target(&Value::Object(object))),
		},
		other @ (Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => return Err(refuse_target(&other)),
	}
	Ok(())
}

/// The subject a record-id target names, classified as [`target_subjects`]
/// classifies every record id a target evaluates to.
fn record_subject(
	id: RecordId,
	form: plan::WriteForm,
	deferable: bool,
) -> Result<Subject, ControlFlow> {
	if id.key.is_range() {
		if matches!(form, plan::WriteForm::Create) {
			return Err(refuse_target(&Value::RecordId(id)));
		}
		let RecordId {
			table,
			key: RecordIdKey::Range(range),
		} = id
		else {
			unreachable!("the key was just tested for a range")
		};
		return Ok(Subject::Scan(table, Some(range)));
	}
	if deferable && matches!(form, plan::WriteForm::Create) {
		return Ok(Subject::Row(Box::new(WriteRow::for_target(Value::RecordId(id))?)));
	}
	Ok(Subject::Read(id))
}

/// The refusal of a value that names no subject a write can take.
fn refuse_target(value: &Value) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatementTarget {
		value: value.to_sql()
	}))
}

/// The row for a record-id subject an updating form named, with the stored
/// record as both of its images, brought through the migration steps of its
/// table that the record's stamp has not passed.
///
/// A record that is not there leaves both empty, which the order's existence
/// check turns into a subject the statement passes over.
pub(crate) async fn read_subject(
	exec: &ExecutionContext,
	id: RecordId,
) -> Result<WriteRow, ControlFlow> {
	#[cfg(test)]
	crate::exec::vm::note_txn_read_on_worker();
	let db = exec.database().context("a write requires a database context")?;
	let (stored, stamp) = crate::exec::migrate::stamped_record(
		&exec.txn(),
		db.ns_ctx.ns.namespace_id,
		db.db.database_id,
		&id.table,
		&id.key,
		exec.version_stamp(),
	)
	.await
	.map_err(ControlFlow::Err)?;
	let mut row = WriteRow::for_target(Value::RecordId(id))?;
	row.adopt_stored(&stored, stamp);
	row.replay_stored(exec).await?;
	Ok(row)
}

/// The records a [`Subject::Scan`] names, as a stream of batches of stored
/// values.
///
/// Streamed rather than collected: a target naming a table names every record
/// in it, and the statement writes them one at a time. Each row is checked
/// against the table's layouts at the statement's read version before it is
/// decoded.
pub(crate) async fn scan_subjects(
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
	let txn = exec.txn();
	let version = exec.version_stamp();
	let epoch = txn.layout_epoch();
	let layouts =
		txn.table_layouts(ns_id, db_id, table, version).await.map_err(ControlFlow::Err)?;
	// The batches carry each record's stamp: the statement replays the steps
	// the stored record has not passed as it takes the record up
	// ([`WriteRow::replay_stored`]), since the write stamps what it writes.
	let replay = crate::exec::migrate::Replay::of(&layouts, ns_id, db_id);
	let gate = crate::kvs::RowGate::new(layouts, ns_id, db_id, table, version, epoch)
		.map_err(ControlFlow::Err)?;
	Ok(crate::exec::operators::scan::pipeline::kv_scan_stream(
		txn,
		raw,
		gate,
		version,
		None,
		crate::kvs::Direction::Forward,
		0,
		None,
		None,
		None,
		replay,
		crate::exec::operators::scan::pipeline::ScalarRows::RecordIds,
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
	/// A batch of stored records and, for a table with migration steps, the
	/// generation each is stamped with.
	Batch(
		std::vec::IntoIter<Value>,
		Option<std::vec::IntoIter<Option<Generation>>>,
		ValueBatchStream,
	),
}

impl Rows {
	/// The next row. A record of a scan is brought through the migration steps
	/// of its table that its stamp has not passed, in `exec`, before it is
	/// handed on.
	pub(crate) async fn next(
		&mut self,
		exec: &ExecutionContext,
	) -> Result<Option<WriteRow>, ControlFlow> {
		loop {
			match self {
				Self::One(row) => return Ok(row.take().map(|row| *row)),
				Self::Scan(stream) => match stream.next().await {
					Some(batch) => {
						let (values, stamps) = batch?.into_stamped();
						let batch = values.into_iter();
						let stamps = stamps.map(Vec::into_iter);
						// Replaced rather than nested: the stream moves into
						// the batch state and back when the batch is spent, so
						// the two never both hold it.
						let Self::Scan(stream) = std::mem::replace(self, Self::One(None)) else {
							unreachable!("the arm was just matched")
						};
						*self = Self::Batch(batch, stamps, stream);
					}
					None => return Ok(None),
				},
				Self::Batch(batch, stamps, _) => match batch.next() {
					Some(value) => {
						let stamp = stamps.as_mut().and_then(Iterator::next);
						// A record whose content is not an object arrives as its record id,
						// and is read by that id as a record a statement names is.
						if let Value::RecordId(id) = value {
							return Ok(Some(read_subject(exec, id).await?));
						}
						let mut row = WriteRow::for_record(value)?;
						row.stored_stamp = stamp;
						row.replay_stored(exec).await?;
						return Ok(Some(row));
					}
					None => {
						let Self::Batch(_, _, stream) = std::mem::replace(self, Self::One(None))
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
	let picked = value.pick(path);
	if let Value::RecordId(id) = picked {
		Ok(id)
	} else {
		Err(ControlFlow::Err(anyhow::anyhow!(refuse(picked.to_sql()))))
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
		let def = db_ctx
			.current_table_def(table, exec.version_stamp())
			.await
			.map_err(ControlFlow::Err)?;
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
		let def = db_ctx
			.current_table_def(table, exec.version_stamp())
			.await
			.map_err(ControlFlow::Err)?;
		Ok(def.is_none_or(|def| !def.permissions.create.is_none()))
	}
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
	use crate::exec::operators::write::plan::WriteForm;
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
