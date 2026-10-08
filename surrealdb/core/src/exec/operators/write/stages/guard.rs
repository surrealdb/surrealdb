//! The structural guards.
//!
//! Every stage that asks whether the *table* admits this write at all — its storage mode, its
//! adjacency shape, its declared record type, and whether the record exists.
//!
//! A guard reads the catalog and the record and nothing the statement wrote,
//! which is what separates it from [`super::data`].
//!
//! # Where these sit, and why it matters
//!
//! [`CheckRecordExists`] heads the updating orders, ahead of the permission
//! gate, because a record that is not there is not a record the gate has an
//! opinion about; it skips the record rather than failing the statement.
//!
//! [`CheckTableNotView`] and [`CheckTableNotLightweight`] follow the order's
//! permission gate wherever they appear, so an actor without permission gets
//! the permission outcome — a silent skip — rather than a failure that
//! discloses the table's storage mode.
//!
//! [`CheckTableType`] does not share that property, and must not be read as if
//! it did. It *precedes* the permission gate on `UPSERT`'s two orders,
//! `INSERT`'s two and `RELATE`'s updating one, so a caller those gates would
//! have refused is told the table's declared record type instead; only
//! `CREATE`'s order and `RELATE`'s creating order place it after the gate.
//!
//! The positions are fixed in [`plan`](crate::exec::operators::write::plan);
//! nothing in this module may reorder them.
//!
//! A guard that refuses fails the statement. Only the existence check skips a
//! record, because only it is answering "is there anything here to write".

use std::sync::Arc;

use surrealdb_types::ToSql;

use crate::catalog::{TableDefinition, TableType};
use crate::doc::Error as DocError;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::{WriteExtras, WriteRow};
use crate::exec::operators::write::stage::{
	LightweightReason, StageId, StageOutcome, TableTypeFor, WriteStage,
};
use crate::exec::{BoxFut, Error as ExecError};
use crate::expr::ControlFlow;

/// This module's stages, by id.
///
/// `None` is a stage whose body has not landed:
/// [`WritePlan::resolve`](crate::exec::operators::write::plan::WritePlan)
/// refuses to bind a plan that names one, rather than running a partial order.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "a stage this module does not own is not this module's to name, and `super::lookup` \
	          routes none of them here"
)]
pub(crate) fn stage(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::CheckRecordExists => Some(Arc::new(CheckRecordExists)),
		StageId::CheckTableNotView => Some(Arc::new(CheckTableNotView)),
		StageId::CheckTableNotLightweight(reason) => {
			Some(Arc::new(CheckTableNotLightweight(reason)))
		}
		StageId::CheckTableType(subject) => Some(Arc::new(CheckTableType(subject))),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body. Answering
		// `None` leaves it to `resolve` to name, as it names any unbound stage.
		_ => None,
	}
}

/// The record a failure names.
///
/// A table-type guard that refuses names the record id unconditionally, and an
/// order whose guard precedes the id derivation — `UPSERT`'s and `INSERT`'s —
/// can arrive without one. That combination is internal-error territory; the
/// guard's own refusal is reached only where an id exists.
fn record_label(row: &WriteRow) -> Result<String, ControlFlow> {
	match &row.id {
		Some(id) => Ok(id.to_sql()),
		None => Err(ControlFlow::Err(anyhow::Error::new(crate::err::Error::unreachable(
			"Expected a document id to be present",
		)))),
	}
}

/// Whether the table's edges are vertex-side adjacency keys alone, synthesized
/// on read from their canonical `[in, out]` ids.
pub(crate) fn table_is_lightweight(table: &TableDefinition) -> bool {
	matches!(&table.table_type, TableType::Relation(rel) if rel.lightweight)
}

/// Refuse a mutation of a lightweight relation's edges, naming `action` as the
/// thing they cannot be.
///
/// Their whole state is the canonical id: there is nothing to set, and readers
/// synthesize the record, so a stored mutation could never be observed
/// consistently.
pub(crate) fn refuse_lightweight(table: &TableDefinition, action: &str) -> Result<(), ControlFlow> {
	if table_is_lightweight(table) {
		return Err(ControlFlow::Err(anyhow::anyhow!(ExecError::Thrown(format!(
			"a LIGHTWEIGHT relation's edges cannot be {action}: they carry no data beyond `id`, `in` and `out`"
		)))));
	}
	Ok(())
}

/// Refuse a write whose record type the table does not admit.
///
/// `relation` is what the write is, not what the table is: it selects the
/// phrasing of the failure and is paired with the predicate that was tested.
pub(crate) fn refuse_table_type(
	row: &WriteRow,
	table: &TableDefinition,
	relation: bool,
) -> Result<(), ControlFlow> {
	Err(ControlFlow::Err(anyhow::anyhow!(DocError::TableCheck {
		record: record_label(row)?,
		relation,
		target_type: table.table_type.to_sql(),
	})))
}

/// The record a statement named must already be there.
///
/// Present in the four updating orders and `DELETE`, absent from the four
/// creating orders. A record the statement named by id that storage does not
/// hold is skipped, not failed: the statement succeeds having written nothing
/// for it.
///
/// A subject with no id yet — a creating order's target that named a table — is
/// not judged here, because nothing has been read for it.
#[derive(Debug)]
struct CheckRecordExists;

impl WriteStage for CheckRecordExists {
	fn name(&self) -> &'static str {
		StageId::CheckRecordExists.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		_ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if row.id.is_some() && row.current.is_none() {
				return Ok(StageOutcome::SkipRow);
			}
			Ok(StageOutcome::Continue)
		})
	}
}

/// A view table (`DEFINE TABLE … AS SELECT`) is read-only.
///
/// Its records are computed from the source query and maintained
/// automatically, so a manual write is refused. Follows the permission gate on
/// every order that has one, so an actor without permission gets the permission
/// outcome rather than an error naming the table's storage mode.
///
/// Replaying an export is exempt: a database export emits the stored rows of a
/// materialised view as `INSERT` statements, and `process_table_views` skips
/// view maintenance while importing for the same reason.
#[derive(Debug)]
struct CheckTableNotView;

impl WriteStage for CheckTableNotView {
	fn name(&self) -> &'static str {
		StageId::CheckTableNotView.name()
	}

	fn apply<'a>(
		&'a self,
		_row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			// A context that carries no options carries no import flag
			// either, and the guard then applies: refusing a write to a view is
			// the conservative reading of an unknown frame.
			if ctx.exec().options().is_some_and(|options| options.import) {
				return Ok(StageOutcome::Continue);
			}
			if ctx.table.view.is_some() {
				return Err(ControlFlow::Err(anyhow::anyhow!(DocError::TableIsView {
					table: ctx.table.name.to_string(),
				})));
			}
			Ok(StageOutcome::Continue)
		})
	}
}

/// A lightweight relation's edges carry no data beyond `id`, `in` and `out`.
///
/// The payload is the phrase the refusal names: `UPDATE` refuses because such
/// an edge is immutable, and `RELATE` refuses only when the statement carries a
/// data clause — a `RELATE` without one is how a lightweight edge is written.
#[derive(Debug)]
struct CheckTableNotLightweight(LightweightReason);

impl WriteStage for CheckTableNotLightweight {
	fn name(&self) -> &'static str {
		StageId::CheckTableNotLightweight(self.0).name()
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("reason".to_string(), self.0.as_str().to_string())]
	}

	fn apply<'a>(
		&'a self,
		_row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			refuse_lightweight(&ctx.table, self.0.as_str())?;
			Ok(StageOutcome::Continue)
		})
	}
}

/// The table's declared record type admits this write.
///
/// Four checks occupy one slot and the payload says which: `CREATE`, `UPSERT`
/// and the plain form of `INSERT` require a table that admits normal records,
/// `RELATE` and `INSERT RELATION` one that admits relations.
///
/// `INSERT RELATION` carries the extra refusal: it is the data-bearing
/// edge-write form, and a lightweight edge carries no data, so `RELATE` is the
/// only way to write one.
#[derive(Debug)]
struct CheckTableType(TableTypeFor);

impl WriteStage for CheckTableType {
	fn name(&self) -> &'static str {
		StageId::CheckTableType(self.0).name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			let table = &ctx.table;
			match self.0 {
				TableTypeFor::Create | TableTypeFor::Upsert => {
					if !table.allows_normal() {
						refuse_table_type(row, table, false)?;
					}
				}
				TableTypeFor::Relate => {
					if !table.allows_relation() {
						refuse_table_type(row, table, true)?;
					}
				}
				// The statement's own payload decides which record type an
				// `INSERT` is writing: `INSERT RELATION` carries the endpoints,
				// every other `INSERT` carries a row.
				TableTypeFor::Insert => match &row.extras {
					WriteExtras::Relate(_, _, _) => {
						if !table.allows_relation() {
							refuse_table_type(row, table, true)?;
						}
						refuse_lightweight(table, "written with INSERT")?;
					}
					WriteExtras::Normal | WriteExtras::Insert(_) => {
						if !table.allows_normal() {
							refuse_table_type(row, table, false)?;
						}
					}
				},
			}
			Ok(StageOutcome::Continue)
		})
	}
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::operators::write::clauses::WriteClauses;
	use crate::exec::operators::write::ctx::TableExistence;
	use crate::kvs::TransactionType;
	use crate::val::{TableName, Value};

	/// A write context over `table`, as the operator resolves one per
	/// statement per table.
	async fn write_ctx(db: &TestDb, table: &str) -> WriteCtx {
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		WriteCtx::resolve(
			&exec,
			&TableName::from(table),
			TableExistence::Required,
			None,
			Arc::new(WriteClauses::default()),
		)
		.await
		.expect("the table resolves")
	}

	/// A row for a record that already exists, as a reading source yields one.
	async fn row_for(src: &str) -> WriteRow {
		WriteRow::for_record(crate::exec::operators::test_util::val(src).await)
			.expect("a record value")
	}

	/// The record id `src` denotes.
	async fn record_id(src: &str) -> crate::val::RecordId {
		let Value::RecordId(id) = crate::exec::operators::test_util::val(src).await else {
			panic!("{src} is not a record id");
		};
		id
	}

	/// A record the statement named and storage does not hold is skipped, not
	/// failed: the statement succeeds having written nothing for it.
	#[tokio::test]
	async fn a_missing_record_skips_rather_than_fails() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = write_ctx(&db, "t").await;
		let mut row = row_for("{ id: t:1 }").await;
		// The row source read nothing for this id.
		row.current = Value::None;
		let stage = stage(StageId::CheckRecordExists).expect("the stage is registered");
		assert_eq!(stage.apply(&mut row, &ctx).await.unwrap(), StageOutcome::SkipRow);
	}

	/// A subject with no id has had nothing read for it, so the existence
	/// check has nothing to judge and the creating orders can omit it safely.
	#[tokio::test]
	async fn a_subject_with_no_id_is_not_judged() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = write_ctx(&db, "t").await;
		let mut row = WriteRow::for_target(Value::Table(TableName::from("t"))).unwrap();
		let stage = stage(StageId::CheckRecordExists).expect("the stage is registered");
		assert_eq!(stage.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
	}

	/// A view table is read-only, and the refusal fails the statement rather
	/// than skipping the record: a caller who reached this guard already holds
	/// permission, so silence would hide a mistake rather than a secret.
	#[tokio::test]
	async fn a_view_table_refuses_the_write() {
		let db = TestDb::new(
			"DEFINE TABLE src SCHEMALESS; DEFINE TABLE v AS SELECT count() FROM src GROUP ALL;",
		)
		.await;
		let ctx = write_ctx(&db, "v").await;
		let mut row = row_for("{ id: v:1 }").await;
		let stage = stage(StageId::CheckTableNotView).expect("the stage is registered");
		let ControlFlow::Err(err) = stage.apply(&mut row, &ctx).await.unwrap_err() else {
			panic!("a view refusal is an error, not a loop signal");
		};
		assert!(err.to_string().contains("as it is a view"), "got: {err}");
	}

	/// An ordinary table passes the view guard.
	#[tokio::test]
	async fn an_ordinary_table_passes_the_view_guard() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = write_ctx(&db, "t").await;
		let mut row = row_for("{ id: t:1 }").await;
		let stage = stage(StageId::CheckTableNotView).expect("the stage is registered");
		assert_eq!(stage.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
	}

	/// The lightweight refusal names the phrase its payload carries, which is
	/// how one stage serves `UPDATE` and `RELATE` alike.
	#[tokio::test]
	async fn the_lightweight_refusal_names_its_reason() {
		let db = TestDb::new("DEFINE TABLE e TYPE RELATION IN a OUT b LIGHTWEIGHT;").await;
		let ctx = write_ctx(&db, "e").await;
		let mut row = row_for("{ id: e:1 }").await;
		for (reason, phrase) in [
			(LightweightReason::Updated, "cannot be updated"),
			(LightweightReason::GivenADataClause, "cannot be given a data clause"),
		] {
			let stage =
				stage(StageId::CheckTableNotLightweight(reason)).expect("the stage is registered");
			let ControlFlow::Err(err) = stage.apply(&mut row, &ctx).await.unwrap_err() else {
				panic!("a lightweight refusal is an error, not a loop signal");
			};
			assert!(err.to_string().contains(phrase), "got: {err}");
		}
	}

	/// A relation table admits `RELATE` and refuses `CREATE`, and the two
	/// refusals differ in the record type they name.
	#[tokio::test]
	async fn a_relation_table_admits_relate_and_refuses_create() {
		let db = TestDb::new("DEFINE TABLE e TYPE RELATION;").await;
		let ctx = write_ctx(&db, "e").await;
		let mut row = row_for("{ id: e:1 }").await;

		let relate = stage(StageId::CheckTableType(TableTypeFor::Relate)).unwrap();
		assert_eq!(relate.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);

		let create = stage(StageId::CheckTableType(TableTypeFor::Create)).unwrap();
		let ControlFlow::Err(err) = create.apply(&mut row, &ctx).await.unwrap_err() else {
			panic!("a table-type refusal is an error, not a loop signal");
		};
		assert!(err.to_string().contains("which is not a relation"), "got: {err}");
	}

	/// `INSERT` reads the record type off the statement's own payload: a plain
	/// row needs a table that admits normal records, and `INSERT RELATION` one
	/// that admits relations.
	#[tokio::test]
	async fn insert_judges_the_type_its_payload_declares() {
		let db = TestDb::new("DEFINE TABLE e TYPE RELATION; DEFINE TABLE t TYPE NORMAL;").await;
		let insert = stage(StageId::CheckTableType(TableTypeFor::Insert)).unwrap();

		// A plain row into a relation table is refused.
		let edge_ctx = write_ctx(&db, "e").await;
		let mut plain = row_for("{ id: e:1 }").await;
		assert!(insert.apply(&mut plain, &edge_ctx).await.is_err());

		// The same row into a normal table is admitted.
		let normal_ctx = write_ctx(&db, "t").await;
		let mut plain = row_for("{ id: t:1 }").await;
		assert_eq!(insert.apply(&mut plain, &normal_ctx).await.unwrap(), StageOutcome::Continue);
	}

	/// `INSERT RELATION` is the data-bearing edge-write form, so it is refused
	/// on a lightweight relation even though `RELATE` is not.
	#[tokio::test]
	async fn insert_relation_refuses_a_lightweight_edge() {
		let db = TestDb::new("DEFINE TABLE e TYPE RELATION IN a OUT b LIGHTWEIGHT;").await;
		let ctx = write_ctx(&db, "e").await;
		let mut row = row_for("{ id: e:1, in: a:1, out: b:1 }").await;
		row.extras = WriteExtras::Relate(record_id("a:1").await, record_id("b:1").await, None);
		let insert = stage(StageId::CheckTableType(TableTypeFor::Insert)).unwrap();
		let ControlFlow::Err(err) = insert.apply(&mut row, &ctx).await.unwrap_err() else {
			panic!("a lightweight refusal is an error, not a loop signal");
		};
		assert!(err.to_string().contains("written with INSERT"), "got: {err}");
	}
}
