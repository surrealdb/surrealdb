//! `MATERIALIZE INTO`: the list of the records that reference each record, kept on the
//! referenced record.
//!
//! A `REFERENCE` field with the clause names a top-level field of the referenced table, its
//! destination. The destination holds the ids of the records that reference the record through the
//! field, in ascending id order, and the write of a referencing record keeps it in step in the
//! transaction of that write: the referencing record joins the list of the record it references
//! when it is created, moves from one list to another when the reference changes, enters or
//! leaves a list as its `WHERE` condition becomes true or false, and leaves the list when it is
//! deleted (`vm::write_cascade`, `ReferenceInstr::Materialize`).
//!
//! The list is written as an `UPDATE` of the referenced record, run as a cascade unit with
//! enforcement suppressed. The write carries a marker ([`mark`]) that the field kernel reads to
//! allow the change of a `READONLY` destination ([`is_marked`]) and that the observers read to
//! leave the referenced table's events alone ([`is_marked`]): the change is the engine's
//! bookkeeping and no write of the statement's actor. Live queries on the referenced table are
//! notified of it as of any other write, and its change feed records it: the list is a value of
//! the record that clients read and replicas hold, and no EVENT is written against it.
//!
//! # Contract
//!
//! - The destination is a top-level field the referenced table declares as a `READONLY` array of
//!   records of the referencing table, with no length limit. One clause fills a destination.
//! - A list holds each id once, in ascending order of the ids as values, which is the order the
//!   reverse lookup of the references returns them in.
//! - A list is kept for the records that exist when the referencing record is written. A record
//!   created after the references to it is not listed until `DEFINE FIELD OVERWRITE` fills the
//!   lists again from the references.
//! - Defining the clause fills every list ([`backfill`]); removing it, or the `REFERENCE` it
//!   belongs to, stops the lists being kept and leaves them as plain values.
//! - An import restores the lists with the records they belong to, so the maintenance of a record
//!   written by an import is skipped.
//!
//! # Hot parents
//!
//! A change to a list rewrites the whole record that holds it, so the cost of a write of a
//! referencing record grows linearly with the length of the list it changes, and writes that
//! change one list conflict with each other. Lists of up to about a thousand ids cost little
//! beyond the write itself; a list that grows past a few thousand is better read through the
//! reverse lookup (`<~`) than kept.

use std::sync::LazyLock;

use crate::exec::ExecutionContext;
use crate::expr::data::Assignment;
use crate::expr::field::Selector;
use crate::expr::logical::{LogicalOp, lower, lower_select};
use crate::expr::statements::{IfelseStatement, SelectStatement, UpdateStatement};
use crate::expr::{
	AssignOperator, BinaryOperator, ControlFlow, Data, Expr, Fields, Function, FunctionCall, Idiom,
	Literal, Param,
};
use crate::val::Value;

/// `$__materialize_parent`: the referenced record whose list a write changes.
pub(crate) const PARENT: &str = "__materialize_parent";
/// `$__materialize_child`: the referencing record that joins or leaves the list.
pub(crate) const CHILD: &str = "__materialize_child";
/// `$__materialize_add`: whether the write joins the list (`true`) or leaves it.
pub(crate) const ADD: &str = "__materialize_add";
/// `$__materialize_image`: the image of a referencing record its `WHERE` condition is judged on.
pub(crate) const IMAGE: &str = "__materialize_image";
/// `$__materialize_write`: the marker of a write that maintains a list.
pub(crate) const MARK: &str = "__materialize_write";

/// The value of the marker. It is drawn once per process, so a variable a request binds under
/// the marker's name does not carry it.
static TOKEN: LazyLock<Value> =
	LazyLock::new(|| Value::Uuid(crate::val::Uuid(uuid::Uuid::new_v4())));

/// The marker binding for the frame of a write that maintains a list. The rewrite of a renamed
/// enum's stored labels (`vm::schema_host::rewrite_enum_labels`) carries it too, being engine
/// bookkeeping of the same kind: it may change a `READONLY` field and fires no event.
pub(crate) fn mark() -> (&'static str, Value) {
	(MARK, TOKEN.clone())
}

/// Whether the frame `exec` is the frame of a write that maintains a list, or of another write
/// that carries its marker ([`mark`]).
pub(crate) fn is_marked(exec: &ExecutionContext) -> bool {
	exec.ctx().value(MARK).is_some_and(|value| value == &*TOKEN)
}

fn param(name: &str) -> Expr {
	Expr::Param(Param::from(name.to_owned()))
}

fn call(name: &str, arguments: Vec<Expr>) -> Expr {
	Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal(name.to_owned()),
		arguments,
	}))
}

/// `dest ?? []`: the list as it is held, or an empty one.
fn held(dest: &Idiom) -> Expr {
	Expr::Binary {
		left: Box::new(Expr::Idiom(dest.clone())),
		op: BinaryOperator::NullCoalescing,
		right: Box::new(Expr::Literal(Literal::Array(Vec::new()))),
	}
}

fn lowered(statement: Expr) -> Result<LogicalOp, ControlFlow> {
	lower(statement).map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))
}

fn update_list(dest: &Idiom, value: Expr) -> Result<LogicalOp, ControlFlow> {
	lowered(Expr::Update(Box::new(UpdateStatement {
		what: vec![param(PARENT)],
		data: Some(Data::SetExpression(vec![Assignment {
			place: dest.clone(),
			operator: AssignOperator::Assign,
			value,
		}])),
		..Default::default()
	})))
}

/// `UPDATE $__materialize_parent SET dest = IF $__materialize_add { array::sort(array::union(dest
/// ?? [], [$child])) } ELSE { array::complement(dest ?? [], [$child]) }`: the child joins the
/// list, which stays in ascending order and holds it once, or leaves it, the order kept.
pub(crate) fn write_statement(dest: &Idiom) -> Result<LogicalOp, ControlFlow> {
	let child = || Expr::Literal(Literal::Array(vec![param(CHILD)]));
	let join = call("array::sort", vec![call("array::union", vec![held(dest), child()])]);
	let leave = call("array::complement", vec![held(dest), child()]);
	update_list(
		dest,
		Expr::IfElse(Box::new(IfelseStatement {
			exprs: vec![(param(ADD), join)],
			close: Some(leave),
		})),
	)
}

/// `SELECT VALUE (filter) FROM [$__materialize_image]`: the condition of the clause, judged on an
/// image of a referencing record.
pub(crate) fn filter_statement(filter: &Expr) -> Result<LogicalOp, ControlFlow> {
	let statement = SelectStatement {
		fields: Fields::Value(Box::new(Selector {
			expr: filter.clone(),
			alias: None,
		})),
		what: vec![Expr::Literal(Literal::Array(vec![param(IMAGE)]))],
		omit: vec![],
		only: false,
		with: None,
		cond: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	};
	lower_select(statement).map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))
}

/// Fill the lists of the table `definition`'s `MATERIALIZE INTO` clause names from the records
/// that reference each record, as the clause keeps them from then on.
///
/// Every record of the referenced table is written: the list of the records that reference it and
/// meet the clause's condition, in ascending id order, or an empty one, so a list a previous
/// definition of the clause left is replaced rather than extended. The lists are those a record
/// holds by the reference keys the field's `REFERENCE` clause wrote, which the caller has brought
/// up to date. Nothing is written when the clause is unchanged from `old`, unless `rebuild`.
pub(crate) async fn backfill(
	exec: &ExecutionContext,
	old: Option<&crate::catalog::FieldDefinition>,
	definition: &crate::catalog::FieldDefinition,
	rebuild: bool,
) -> anyhow::Result<()> {
	use common::fmt::EscapeKwFreeIdent;
	use surrealdb_types::ToSql;

	let Some(materialize) =
		definition.reference.as_ref().and_then(|reference| reference.materialize.as_ref())
	else {
		return Ok(());
	};
	let previous = old
		.and_then(|old| old.reference.as_ref())
		.and_then(|reference| reference.materialize.as_ref());
	if !rebuild && previous == Some(materialize) {
		return Ok(());
	}
	let source = format!(
		"<~({} FIELD {})",
		EscapeKwFreeIdent(definition.table.as_str()).to_sql(),
		definition.name.to_sql()
	);
	let dest_table = EscapeKwFreeIdent(materialize.table.as_str()).to_sql();
	let dest_field = materialize.field.to_sql();
	match &materialize.filter {
		Some(filter) => {
			let select_text = format!(
				"SELECT VALUE id FROM (array::distinct(array::flatten(SELECT VALUE {source} FROM {dest_table}))) WHERE ({})",
				crate::expr::CoverStmts(filter).to_sql()
			);
			let select_stmt = crate::syn::expr(&select_text)?;
			let select_frame = exec
				.with_perms_suppressed(true)
				.refusing_writes(crate::dbs::NoWriteFrame::ComputedField);
			let matching = crate::exec::entry::statement(&select_frame, &select_stmt)
				.await
				.map_err(|err| match err.downcast::<crate::exec::Error>() {
					Ok(err) => {
						if let crate::exec::Error::SchemaClauseEffect {
							construct,
							..
						} = err
						{
							crate::exec::Error::SchemaClauseEffect {
								clause: "MATERIALIZE INTO".to_owned(),
								construct,
							}
							.into()
						} else {
							err.into()
						}
					}
					Err(err) => err,
				})?;
			let update_text = format!(
				"UPDATE {dest_table} SET {dest_field} = array::sort(array::intersect({source}, $__materialize_matching))"
			);
			let update_stmt = crate::syn::expr(&update_text)?;
			let update_frame = exec
				.with_perms_suppressed(true)
				.with_params([mark(), ("__materialize_matching", matching)]);
			crate::exec::entry::statement(&update_frame, &update_stmt).await?;
		}
		None => {
			let text = format!("UPDATE {dest_table} SET {dest_field} = array::sort({source})");
			let statement = crate::syn::expr(&text)?;
			let frame = exec.with_perms_suppressed(true).with_params([mark()]);
			crate::exec::entry::statement(&frame, &statement).await?;
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use crate::dbs::Session;
	use crate::kvs::Datastore;

	#[tokio::test]
	async fn materialize_into_where_filter_refuses_effects_at_runtime() {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().with_ns("test").with_db("test");

		ds.execute(
			"DEFINE FUNCTION fn::check($v: bool) -> bool { RETURN $v; };
			 DEFINE TABLE author SCHEMALESS;
			 DEFINE FIELD posts ON author TYPE array<record<post>> READONLY DEFAULT [];
			 DEFINE TABLE post SCHEMALESS;
			 DEFINE TABLE audit SCHEMALESS;
			 DEFINE FIELD author ON post TYPE option<record<author>> REFERENCE MATERIALIZE INTO author.posts WHERE fn::check(published = true);
			 DEFINE FUNCTION OVERWRITE fn::check($v: bool) -> bool { CREATE audit:1 SET seen = $v; RETURN $v; };",
			&ses,
			None,
		)
		.await
		.unwrap();

		let res = ds
			.execute(
				"CREATE author:a; CREATE post:1 SET author = author:a, published = true;",
				&ses,
				None,
			)
			.await
			.unwrap();
		let err = res.into_iter().nth(1).unwrap().result.unwrap_err().to_string();
		assert!(
			err.contains("The MATERIALIZE INTO clause must not have effects"),
			"unexpected error: {err}"
		);

		let check = ds.execute("SELECT * FROM audit;", &ses, None).await.unwrap();
		let val = check.into_iter().next().unwrap().result.unwrap();
		assert_eq!(val.to_sql(), "[]");
	}

	#[tokio::test]
	async fn replace_preserves_readonly_materialized_destination() {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().with_ns("test").with_db("test");

		let res = ds
			.execute(
				"DEFINE NAMESPACE test;
				 DEFINE DATABASE test;
				 DEFINE TABLE author SCHEMALESS;
				 DEFINE FIELD posts ON author TYPE array<record<post>> READONLY DEFAULT [];
				 DEFINE TABLE post SCHEMALESS;
				 DEFINE FIELD author ON post TYPE option<record<author>> REFERENCE MATERIALIZE INTO author.posts;
				 CREATE author:a SET name = 'Alice';
				 CREATE post:1 SET author = author:a;
				 UPDATE author:a REPLACE { name: 'Alicia' };
				 SELECT name, posts FROM author:a;",
				&ses,
				None,
			)
			.await
			.unwrap();
		for r in &res {
			assert!(r.result.is_ok(), "unexpected error: {:?}", r.result);
		}
		let last = res.into_iter().last().unwrap().result.unwrap();
		assert_eq!(last.to_sql(), "[{ name: 'Alicia', posts: [post:1] }]");
	}
}
