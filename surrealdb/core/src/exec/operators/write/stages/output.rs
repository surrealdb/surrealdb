//! The row the statement returns for this record.
//!
//! The statement's `RETURN` clause, dispatched against the images the record's
//! stages built.
//!
//! Produces [`WriteRow::output`](super::super::row::WriteRow), which is `None`
//! when the clause returns nothing — which is why the `ONLY` cardinality rule
//! counts rows the statement returned rather than records it wrote.
//!
//! # What this stage is not
//!
//! It projects; it does not gate. The table's `PERMISSIONS FOR select` clause
//! is answered by `check_select_permissions`, which sits immediately before
//! this stage in all nine orders, and answering it again here would evaluate a
//! volatile guard twice. What the projection still owes is the *field* half:
//! an image shown to a caller carries only the fields that caller may select,
//! with the table's `COMPUTED` bodies derived on it and their own guards
//! applied. That is [`WriteCtx::view_checked`], the resolver for images the
//! caller is shown, as against `view_unchecked`, for images a stage derives in
//! order to evaluate something.

use std::sync::Arc;

use crate::exec::operators::project::evaluate_and_set_field;
use crate::exec::operators::scan::pipeline::materialise_fields_with_permissions;
use crate::exec::operators::write::clauses::WriteProjection;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::stage::{RecordImage, StageId, StageOutcome, WriteStage};
use crate::exec::{BoxFut, Error as ExecError, EvalContext};
use crate::expr::output::Output;
use crate::expr::{ControlFlow, Operation};
use crate::val::{Object, Value};

/// This module's stages, by id.
///
/// `None` is a stage whose body has not landed:
/// [`WritePlan::resolve`](crate::exec::operators::write::plan::WritePlan)
/// refuses to bind a plan that names one, rather than running a partial order.
// `stages::lookup` is the total router over `StageId` and holds no wildcard, so
// a new stage still has to be given an owner before it compiles. The wildcard
// below is the arm for a stage this module does not own; enumerating every
// other variant here would restate `lookup`'s table in nine places and go stale
// independently of it.
#[allow(clippy::wildcard_enum_match_arm)]
pub(crate) fn stage(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::OutputWrite => Some(Arc::new(OutputWrite)),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body. Answering
		// `None` leaves it to `resolve` to name, as it names any unbound stage.
		_ => None,
	}
}

/// Project the row the statement returns for this record, or drop it.
///
/// Last of every order, so the images it reads are final: the post-image is the
/// record as stored, the pre-image is the record as the statement read it, and
/// the `COMPUTED` values an observer needed are already on them.
///
/// The record is emitted through [`WriteRow::output`]. Leaving that `None` is
/// how a record the statement wrote returns no row — `RETURN NONE`, a bare
/// `DELETE`, and every write under `OPTION IMPORT` — and it is distinct from
/// [`StageOutcome::SkipRow`], which is a record the statement excluded. Both
/// emit nothing; only the latter says the record was not written, and the
/// affected-row total is read from the mutation flag rather than from either.
#[derive(Debug)]
struct OutputWrite;

impl WriteStage for OutputWrite {
	fn name(&self) -> &'static str {
		StageId::OutputWrite.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(project(row, ctx))
	}
}

/// Dispatch the statement's `RETURN` clause onto the record's images.
async fn project(row: &mut WriteRow, ctx: &WriteCtx) -> Result<StageOutcome, ControlFlow> {
	// Under `OPTION IMPORT` a writer surfaces no per-row output at all, so an
	// import stays idempotent rather than echoing every record it restored.
	if ctx.exec().options().is_some_and(|options| options.import) {
		return Ok(StageOutcome::Continue);
	}
	// The mutation stages rewrote the post-image after any reduced counterpart
	// of it was derived — the pre-mutation snapshot the data clause reads is
	// one — so that counterpart describes a record this stage must not show.
	// The pre-image is immutable once the row is built and keeps its own.
	row.current_reduced = None;
	row.output = match ctx.output.as_ref() {
		Some(Output::None) => None,
		Some(Output::Null) => Some(Value::Null),
		Some(Output::After) => Some(shown(row, ctx, RecordImage::Current).await?),
		Some(Output::Before) => Some(shown(row, ctx, RecordImage::Initial).await?),
		Some(Output::Diff) => Some(diff(row, ctx).await?),
		Some(Output::Fields(_)) => Some(fields(row, ctx).await?),
		// An unspecified `RETURN` is the post-image, on the orders that leave
		// one. `DELETE` empties its post-image, and returns nothing.
		None if row.current.is_none() => None,
		None => Some(shown(row, ctx, RecordImage::Current).await?),
	};
	Ok(StageOutcome::Continue)
}

/// `RETURN DIFF`: the JSON Patch from the pre-image to the post-image.
///
/// Both sides are the images the caller is shown, so a field the caller may not
/// select is absent from both and cannot reach a patch path. That is also why
/// no further field pass runs over the ops: they are keyed `op` / `path` /
/// `value` rather than by field name, and a name-keyed pass over them would
/// match a table's own `value` field against an op's payload.
async fn diff(row: &mut WriteRow, ctx: &WriteCtx) -> Result<Value, ControlFlow> {
	let before = shown(row, ctx, RecordImage::Initial).await?;
	let after = shown(row, ctx, RecordImage::Current).await?;
	Ok(Operation::operations_to_value(before.diff(&after)))
}

/// `RETURN <fields>`: the statement's own projection, over the record's two
/// images.
///
/// The projection was compiled with the statement's other clauses and is
/// shared by every record; nothing the user wrote is planned here. It is
/// evaluated against the post-image, with `$before` and `$after` naming the
/// two images — the same three values, so a field reached by any of the three
/// routes is the field the caller may see.
///
/// That is what discharges the field half of the permission contract, and why
/// no further pass runs over what the projection produced: every value it can
/// reach comes from an image [`shown`] has already reduced, so no alias, no
/// rename and no `$before` read can carry out a field the caller may not
/// select. A name-keyed pass over the projected object would judge output keys
/// rather than record fields, and cut a key that merely shares a guarded
/// field's name.
async fn fields(row: &mut WriteRow, ctx: &WriteCtx) -> Result<Value, ControlFlow> {
	let projection = ctx.clauses.output_fields.as_ref().ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(
			"a RETURN clause of fields whose projection was not compiled".to_string()
		)))
	})?;
	let before = shown(row, ctx, RecordImage::Initial).await?;
	let after = shown(row, ctx, RecordImage::Current).await?;
	// `RETURN *` and nothing else is the image itself, which is how it answers
	// for a record that no longer is one: `DELETE` leaves no post-image, and
	// there is no object to start an object projection from.
	if let WriteProjection::Fields {
		fields,
		include_all: true,
	} = projection
		&& fields.is_empty()
	{
		return Ok(after);
	}
	let frame = ctx.exec().with_params([("before", before), ("after", after.clone())]);
	let eval = EvalContext::from_exec_ctx(&frame).with_value_and_doc(&after);
	match projection {
		WriteProjection::Value(expr) => expr.evaluate(eval).await,
		WriteProjection::Fields {
			fields,
			include_all,
		} => {
			// `*` seeds the object with the record, so a named field written
			// beside it lands on top of the field it shares a name with. An
			// image that is not a record seeds nothing.
			let mut object = Object::default();
			if *include_all && let Value::Object(record) = &after {
				object = record.clone();
			}
			// A field at a time, in the order it was written: the projection
			// is one record's, so there is no batch to evaluate column-wise,
			// and a field that writes must write when the statement says.
			for field in fields {
				evaluate_and_set_field(&mut object, field, eval.clone()).await?;
			}
			Ok(Value::Object(object))
		}
	}
}

/// One of the record's two images, as the caller is allowed to see it: the
/// fields that caller may select, with the table's `COMPUTED` bodies derived on
/// them and each computed field's own guard applied to its final value.
///
/// Each field guard is answered once, against the image it is being applied to
/// — the pre-image's guards judge the pre-image, the post-image's the
/// post-image — so `RETURN DIFF`, which is shown both, evaluates each side's
/// guards exactly once. The result is left on the row's reduced counterpart of
/// that image, so the row carries the reduction the caller was shown.
///
/// A caller whose reads are not permission-checked has no reduced counterpart:
/// the image differs from the record only by the `COMPUTED` values derived onto
/// it, and for the post-image even those are already in place whenever an
/// observer asked for them.
async fn shown(
	row: &mut WriteRow,
	ctx: &WriteCtx,
	image: RecordImage,
) -> Result<Value, ControlFlow> {
	let source = match image {
		RecordImage::Current => &row.current,
		RecordImage::Initial => &row.initial,
	};
	// No record to show: a creating order has no pre-image, and `DELETE` has
	// emptied its post-image. Deriving a `COMPUTED` field from an absent record
	// raises rather than yielding an empty answer, so the absent image stands
	// as it is. A record with no id carries no field schema to project against.
	if !matches!(source, Value::Object(_)) || row.id.is_none() {
		return Ok(source.clone());
	}
	let mut shown = source.clone();
	let mut views = ctx.view_checked.lock().await;
	let view = views.view_for(ctx.exec(), &ctx.table.name).await?;
	let checked = view.checked();
	// Nothing left to derive: an unchecked caller's view cuts no field, and the
	// post-image already carries its `COMPUTED` values. Re-deriving them would
	// run every body a second time, which a non-deterministic body would answer
	// differently from the one this record's observers were given.
	if !checked && matches!(image, RecordImage::Current) && row.fields_computed {
		return Ok(shown);
	}
	materialise_fields_with_permissions(
		ctx.exec(),
		view.field_state(),
		&mut shown,
		ctx.exec().root().skip_fetch_perms,
		checked,
	)
	.await?;
	drop(views);
	if checked {
		match image {
			RecordImage::Current => row.current_reduced = Some(shown.clone()),
			RecordImage::Initial => row.initial_reduced = Some(shown.clone()),
		}
	}
	Ok(shown)
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::{TestDb, val};
	use crate::exec::operators::write::clauses::WriteClauses;
	use crate::exec::operators::write::ctx::TableExistence;
	use crate::exec::planner::Planner;
	use crate::expr::logical::LogicalOp;
	use crate::kvs::TransactionType;

	/// A write context over table `t`, carrying the statement's `RETURN`
	/// clause — compiled through the planner, as the operator's is — and
	/// nothing else.
	async fn write_ctx(db: &TestDb, session: &Session, output: Option<Output>) -> WriteCtx {
		let exec = db.exec_ctx_as(session, TransactionType::Write).await;
		let planner = Planner::new(exec.ctx(), exec.function_registry());
		let clauses = WriteClauses {
			output_fields: planner.write_output(output.as_ref()).await.unwrap(),
			..Default::default()
		};
		WriteCtx::resolve(&exec, &"t".into(), TableExistence::Required, output, Arc::new(clauses))
			.await
			.unwrap()
	}

	/// The `RETURN` clause of `src`, so a test states one as the text a user
	/// writes rather than as a hand-built clause.
	fn clause(src: &str) -> Output {
		let mut ast = crate::syn::parse(src).expect("the statement parses").expressions;
		assert_eq!(ast.len(), 1, "expected exactly one statement in {src:?}");
		let crate::sql::TopLevelExpr::Expr(expr) = ast.remove(0) else {
			panic!("expected a statement in {src:?}");
		};
		let LogicalOp::Write(write) =
			crate::expr::logical::lower(expr).expect("the statement lowers")
		else {
			panic!("expected a write statement in {src:?}");
		};
		write.statement.output().cloned().expect("the statement carries a RETURN clause")
	}

	/// A record that already existed and was then rewritten.
	async fn updated(initial: &str, current: &str) -> WriteRow {
		let mut row = WriteRow::for_record(val(current).await).unwrap();
		row.initial = val(initial).await;
		row
	}

	/// The anonymous session on `test`/`test`, which a datastore built with
	/// auth enabled subjects to permission checks.
	fn anonymous() -> Session {
		Session::default().with_ns("test").with_db("test")
	}

	/// `RETURN NONE` writes the record and returns no row for it. The record is
	/// not skipped: skipping says the statement excluded it, which the
	/// affected-row total would then be read against.
	#[tokio::test]
	async fn return_none_emits_no_row() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = write_ctx(&db, &TestDb::owner(), Some(Output::None)).await;
		let mut row = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 2 }").await;
		let outcome = OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(outcome, StageOutcome::Continue);
		assert!(row.output.is_none(), "RETURN NONE returns no row");
	}

	/// With no `RETURN` clause the statement returns its post-image — and
	/// nothing at all on an order that leaves none, which is what makes a bare
	/// `DELETE` answer with an empty result rather than a row of `NONE`.
	#[tokio::test]
	async fn an_unspecified_return_is_the_post_image_when_there_is_one() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = write_ctx(&db, &TestDb::owner(), None).await;

		let mut row = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 2 }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.output, Some(val("{ id: t:1, n: 2 }").await));

		let mut deleted = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 1 }").await;
		deleted.current = Value::None;
		OutputWrite.apply(&mut deleted, &ctx).await.unwrap();
		assert!(deleted.output.is_none(), "a cleared post-image returns no row");
	}

	/// `RETURN BEFORE` reads the pre-image and `RETURN AFTER` the post-image,
	/// including on an order that has emptied one of them.
	#[tokio::test]
	async fn before_and_after_read_the_two_images() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;

		let before = write_ctx(&db, &TestDb::owner(), Some(Output::Before)).await;
		let mut row = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 2 }").await;
		OutputWrite.apply(&mut row, &before).await.unwrap();
		assert_eq!(row.output, Some(val("{ id: t:1, n: 1 }").await));

		let after = write_ctx(&db, &TestDb::owner(), Some(Output::After)).await;
		let mut deleted = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 1 }").await;
		deleted.current = Value::None;
		OutputWrite.apply(&mut deleted, &after).await.unwrap();
		assert_eq!(
			deleted.output,
			Some(Value::None),
			"an explicit RETURN AFTER answers for a record that no longer exists"
		);
	}

	/// A field the caller may not select is absent from the record it is shown,
	/// on either image.
	#[tokio::test]
	async fn a_denied_field_is_cut_from_both_images() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			 DEFINE FIELD secret ON t PERMISSIONS FOR select NONE;",
		)
		.await;
		let ctx = write_ctx(&db, &anonymous(), Some(Output::After)).await;
		let mut row =
			updated("{ id: t:1, n: 1, secret: 'a' }", "{ id: t:1, n: 2, secret: 'b' }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.output, Some(val("{ id: t:1, n: 2 }").await));

		let ctx = write_ctx(&db, &anonymous(), Some(Output::Before)).await;
		let mut row =
			updated("{ id: t:1, n: 1, secret: 'a' }", "{ id: t:1, n: 2, secret: 'b' }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.output, Some(val("{ id: t:1, n: 1 }").await));
	}

	/// `RETURN DIFF` patches the pre-image into the post-image, and neither side
	/// carries a field the caller may not select — so no patch path can name
	/// one.
	#[tokio::test]
	async fn diff_patches_between_the_images_the_caller_may_see() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			 DEFINE FIELD secret ON t PERMISSIONS FOR select NONE;",
		)
		.await;
		let ctx = write_ctx(&db, &anonymous(), Some(Output::Diff)).await;
		let mut row =
			updated("{ id: t:1, n: 1, secret: 'a' }", "{ id: t:1, n: 2, secret: 'b' }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(
			row.output,
			Some(val("[{ op: 'replace', path: '/n', value: 2 }]").await),
			"the denied field changed too, and must not reach a patch path"
		);
	}

	/// `RETURN <fields>` answers with the object the clause names, and with
	/// nothing else: a field of the record the clause did not ask for is absent
	/// from it, and a field it asked for that the record does not carry is
	/// present as `NONE`.
	#[tokio::test]
	async fn a_projection_names_the_object_it_returns() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx =
			write_ctx(&db, &TestDb::owner(), Some(clause("UPDATE t SET n = 2 RETURN n, z"))).await;
		let mut row = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 2 }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.output, Some(val("{ n: 2, z: NONE }").await));
	}

	/// An alias is where the value lands, so a projected field is returned
	/// under the name the clause gave it and not under the one it was read
	/// from.
	#[tokio::test]
	async fn an_alias_names_where_the_value_lands() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx =
			write_ctx(&db, &TestDb::owner(), Some(clause("UPDATE t SET n = 2 RETURN n AS total")))
				.await;
		let mut row = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 2 }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.output, Some(val("{ total: 2 }").await));
	}

	/// `RETURN VALUE <expr>` makes the row the value itself, with no object
	/// around it.
	#[tokio::test]
	async fn a_value_projection_returns_the_bare_value() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx =
			write_ctx(&db, &TestDb::owner(), Some(clause("UPDATE t SET n = 2 RETURN VALUE id")))
				.await;
		let mut row = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 2 }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.output, Some(val("t:1").await));
	}

	/// The projection reads the record through the post-image, and `$before`
	/// and `$after` name the two images — so a clause can return what the
	/// statement changed as well as what it left.
	#[tokio::test]
	async fn a_projection_reads_both_images() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = write_ctx(
			&db,
			&TestDb::owner(),
			Some(clause("UPDATE t SET n = 2 RETURN VALUE $after.n - $before.n")),
		)
		.await;
		let mut row = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 2 }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.output, Some(val("1").await));
	}

	/// `RETURN *` is the post-image, which is how it answers for a record that
	/// no longer is one: a `DELETE` leaves no post-image to project.
	#[tokio::test]
	async fn a_wildcard_projection_is_the_post_image() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = write_ctx(&db, &TestDb::owner(), Some(clause("UPDATE t RETURN *"))).await;

		let mut row = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 2 }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.output, Some(val("{ id: t:1, n: 2 }").await));

		let mut deleted = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 1 }").await;
		deleted.current = Value::None;
		OutputWrite.apply(&mut deleted, &ctx).await.unwrap();
		assert_eq!(deleted.output, Some(Value::None));
	}

	/// A field the caller may not select cannot be projected out of the
	/// record, by any of the three routes the clause has to it: by name, under
	/// an alias, or through `$before`. Every one of them reads an image the
	/// caller is shown, and the field is absent from all of them.
	#[tokio::test]
	async fn a_denied_field_is_beyond_the_projection() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			 DEFINE FIELD secret ON t PERMISSIONS FOR select NONE;",
		)
		.await;
		let ctx = write_ctx(
			&db,
			&anonymous(),
			Some(clause("UPDATE t RETURN n, secret, secret AS leaked, $before.secret AS was")),
		)
		.await;
		let mut row =
			updated("{ id: t:1, n: 1, secret: 'a' }", "{ id: t:1, n: 2, secret: 'b' }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.output, Some(val("{ n: 2, secret: NONE, leaked: NONE, was: NONE }").await));
	}

	/// A `COMPUTED` field is derived onto the image the projection reads, so a
	/// clause can name one even though no record carries it.
	#[tokio::test]
	async fn a_projection_sees_the_computed_fields() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD n ON t TYPE number;
			 DEFINE FIELD dbl ON t COMPUTED n * 2;",
		)
		.await;
		let ctx = write_ctx(&db, &TestDb::owner(), Some(clause("UPDATE t RETURN dbl"))).await;
		let mut row = updated("{ id: t:1, n: 1 }", "{ id: t:1, n: 2 }").await;
		OutputWrite.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.output, Some(val("{ dbl: 4 }").await));
	}
}
