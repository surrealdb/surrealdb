//! Resolving the scalars a schema statement is written with.
//!
//! A schema statement names its subject with an expression: `DEFINE ANALYZER
//! foo`, `REMOVE INDEX $name ON t`, `DEFINE INDEX i ON t FIELDS a, b[$n]`. Each
//! helper here turns one of those expressions into the name, path or value the
//! catalog write is formed from, by lowering it to its logical form, compiling
//! it, and evaluating it in the frame the statement runs in.
//!
//! # A bare name is not a read of the row
//!
//! Two shapes are answered from the expression itself and never evaluated:
//!
//! - a single-[`Part::Field`] idiom, which is what the parser produces for a bare identifier in
//!   every DDL name position but the ones that name a table;
//! - [`Expr::Table`], which is what it produces in those — the `DEFINE`/`ALTER TABLE` name and the
//!   `ON <table>` of `EVENT`, `FIELD` and `INDEX`, all parsed with loose identifiers read as tables
//!   rather than as fields.
//!
//! The two sets are disjoint and neither stands in for the other: `DEFINE
//! TABLE t` names a table, while `REMOVE TABLE t` names the same thing with an
//! idiom.
//!
//! Both are load-bearing, and in opposite ways. A single-field idiom
//! *evaluates* to the value of that field on the row in scope, so evaluating
//! `DEFINE ANALYZER foo` under a row would name the analyzer after whatever
//! `foo` holds there, and under no row would name it `NONE` — which
//! [`optional_ident`] would then accept as "unset". [`Expr::Table`] evaluates
//! to a `Value::Table`, which coerces to no name at all. Answering both from
//! the expression is what makes a written name mean itself.
//!
//! The same distinction reaches one level further down, in [`raw_idiom`]: a
//! path whose head is a field keeps its structure and has only its
//! parameterised subscripts evaluated, so `addr[$i]` becomes `addr.city` rather
//! than a read of the current row.

use std::str::FromStr;

use anyhow::Result;

#[cfg(test)]
use crate::exec::EvalContext;
use crate::exec::ExecutionContext;
/// Evaluate one of a schema statement's expressions.
///
/// The general route, with no shortcut: lower, compile, evaluate. Every helper
/// here is this plus a way of reading the result, and the ones that take a name
/// check their shortcuts before reaching it. See [`crate::exec::entry`].
pub(crate) use crate::exec::entry::scalar;
#[cfg(test)]
use crate::exec::entry::{eval_frame, evaluate};
#[cfg(test)]
use crate::expr::Function;
use crate::expr::{Expr, Idiom, Literal, Part};
use crate::val::value::Coerce;
use crate::val::{Number, Value};

/// Resolve `expr` to the name it gives.
///
/// `into` names the clause in the error a non-string result raises — "analyzer
/// name", "index table" — so a refusal says which part of the statement was
/// unusable rather than only that a string was wanted.
pub(crate) async fn ident(exec: &ExecutionContext, expr: &Expr, into: &str) -> Result<String> {
	if let Some(name) = written_name(expr) {
		return Ok(name);
	}
	coerce_into(scalar(exec, expr).await?, into)
}

#[cfg(test)]
/// Resolve `expr` to the name it gives, where the clause is optional.
///
/// `NONE` is the absent clause, so an expression that evaluates to it leaves
/// the setting unset rather than failing. That is why the written-name
/// shortcuts matter more here than in [`ident`]: without them a bare name would
/// evaluate to `NONE` under no row and silently unset the very clause it was
/// written to set.
pub(crate) async fn optional_ident(
	exec: &ExecutionContext,
	expr: &Expr,
	into: &str,
) -> Result<Option<String>> {
	if let Some(name) = written_name(expr) {
		return Ok(Some(name));
	}
	coerce_into(scalar(exec, expr).await?, into)
}

#[cfg(test)]
/// Resolve `expr` to a field path, enforcing that every part of it is one a
/// stored schema key may hold.
///
/// This is the form `DEFINE`, `ALTER` and `REMOVE FIELD` take, where the path
/// becomes a catalog key. Use [`raw_idiom`] for the positions that accept more
/// than [`Idiom::validate_local`] does.
pub(crate) async fn idiom(exec: &ExecutionContext, expr: &Expr, into: &str) -> Result<Idiom> {
	let idiom = raw_idiom(exec, expr, into).await?;
	idiom.validate_local(into)?;
	Ok(idiom)
}

#[cfg(test)]
/// Resolve `expr` to a field path, accepting any part the path was written
/// with.
///
/// A path whose head is a field keeps the structure it was written with and has
/// only its parameterised subscripts resolved — see [`substitute_indices`].
/// Anything else (a parameter, a function call, a path starting from an
/// expression) is evaluated, coerced to a string, and re-parsed as a path.
pub(crate) async fn raw_idiom(exec: &ExecutionContext, expr: &Expr, into: &str) -> Result<Idiom> {
	if let Expr::Idiom(idiom) = expr
		&& matches!(idiom.0.first(), Some(Part::Field(_)))
	{
		substitute_indices(idiom.clone(), &eval_frame(exec)).await
	} else {
		parse_path(scalar(exec, expr).await?, into)
	}
}

/// Read an evaluated result as a field path: coerced to a string and parsed
/// as a path. The tail of [`raw_idiom`] for a path that is evaluated whole.
pub(crate) fn parse_path(value: Value, into: &str) -> Result<Idiom> {
	let raw: String = coerce_into(value, into)?;
	Idiom::from_str(&raw).map_err(|e| anyhow::anyhow!("Failed to parse {} from string: {e}", into))
}

/// Read an evaluated `type::fields` argument as the paths it names.
pub(crate) fn type_fields_paths(value: Value) -> Result<Vec<Idiom>> {
	let raws = value
		.coerce_to::<Vec<String>>()
		.map_err(|_| anyhow::anyhow!("Expected an array of strings"))?;
	raws.into_iter().map(|raw| crate::syn::idiom(&raw)).collect()
}

/// Read an evaluated `type::field` argument as the path it names.
pub(crate) fn type_field_path(value: Value) -> Result<Idiom> {
	let raw = value.coerce_to::<String>().map_err(|_| anyhow::anyhow!("Expected a string"))?;
	crate::syn::idiom(&raw)
}

#[cfg(test)]
/// Resolve a written list of field paths — `DEFINE INDEX … FIELDS`, `OMIT`.
///
/// `type::fields(x)` and `type::field(x)` are expanded here rather than
/// evaluated as paths: their argument is evaluated and its strings are parsed
/// as paths, so one written entry can contribute several. Every other entry
/// goes through [`raw_idiom`], which is to say the list accepts parts
/// [`Idiom::validate_local`] rejects — `OMIT obj.{ a, b }` and `FIELDS
/// id.id().val` are both written this way.
pub(crate) async fn fields(exec: &ExecutionContext, exprs: &[Expr]) -> Result<Vec<Idiom>> {
	let mut out = Vec::new();
	for expr in exprs {
		if let Expr::FunctionCall(call) = expr
			&& let Function::Normal(ref name) = call.receiver
			&& (name == "type::fields" || name == "type::field")
		{
			let Some(arg) = call.arguments.first() else {
				return Err(anyhow::anyhow!("Expected an argument for {name} function call"));
			};
			if name == "type::fields" {
				out.extend(type_fields_paths(scalar(exec, arg).await?)?);
			} else {
				out.push(type_field_path(scalar(exec, arg).await?)?);
			}
		} else {
			out.push(raw_idiom(exec, expr, "field name").await?);
		}
	}
	Ok(out)
}

/// The name an expression gives by being written, or `None` when it gives one
/// only by being evaluated.
///
/// The two shapes the parser produces for a written identifier; see the module
/// documentation for why neither may be evaluated.
pub(crate) fn written_name(expr: &Expr) -> Option<String> {
	if let Expr::Idiom(Idiom(parts)) = expr
		&& let [Part::Field(name)] = parts.as_slice()
	{
		return Some(name.as_str().to_owned());
	}
	if let Expr::Table(name) = expr {
		return Some(name.as_str().to_owned());
	}
	None
}

/// The name an evaluated operand gives, naming `into` in the refusal of a result that is no
/// string. The coercion [`ident`] applies to an operand that is not a written name.
pub(crate) fn name_of(value: Value, into: &str) -> Result<String> {
	coerce_into(value, into)
}

/// Read an evaluated result as `T`, naming the clause in the refusal
/// ([`Value::coerce_naming`]).
fn coerce_into<T: Coerce>(value: Value, into: &str) -> Result<T> {
	value.coerce_naming(into).map_err(anyhow::Error::from)
}

#[cfg(test)]
/// Resolve a written path's parameterised subscripts, keeping everything else
/// as written.
///
/// `foo[$n]` with `$n = 4` becomes `foo[4]`, and a string subscript is
/// canonicalised to a field, so `addr[$key]` with `$key = "city"` is stored as
/// `addr.city`. A literal subscript is canonicalised the same way without being
/// evaluated — `addr['city']` is `addr.city` — which is what makes the stored
/// path independent of how it was spelled.
///
/// Fails when a subscript resolves to neither an integer nor a string, since
/// there is no path part for anything else.
async fn substitute_indices(this: Idiom, frame: &EvalContext<'_>) -> Result<Idiom> {
	let mut out = Vec::with_capacity(this.0.len());
	for part in this.0 {
		let part = match part {
			Part::Value(Expr::Literal(Literal::String(s))) => Part::Field(s),
			Part::Value(Expr::Literal(lit)) => Part::Value(Expr::Literal(lit)),
			Part::Value(expr) => subscript(evaluate(&expr, frame).await?)?,
			other @ (Part::All
			| Part::Flatten
			| Part::Last
			| Part::First
			| Part::Field(_)
			| Part::Where(_)
			| Part::Lookup(_)
			| Part::Start(_)
			| Part::Method(..)
			| Part::Destructure(_)
			| Part::Optional
			| Part::Recurse(..)
			| Part::Doc
			| Part::RepeatRecurse) => other,
		};
		out.push(part);
	}
	Ok(Idiom(out))
}

/// The path part a parameterised subscript's value stands for.
fn subscript(value: Value) -> Result<Part> {
	if let Value::Number(Number::Int(i)) = value {
		Ok(Part::Value(Expr::Literal(Literal::Integer(i))))
	} else if let Value::String(s) = value {
		Ok(Part::Field(s))
	} else {
		Err(anyhow::anyhow!(
			"Field path index must evaluate to an integer or string, found {}",
			value.kind_of()
		))
	}
}

/// [`raw_idiom`]'s resolution of a written path whose subscripts are already
/// evaluated: `evaluated` holds the value of each subscript that is not a
/// literal, in the order the path holds them.
pub(crate) fn substitute_evaluated(this: Idiom, evaluated: Vec<Value>) -> Result<Idiom> {
	let mut values = evaluated.into_iter();
	let mut out = Vec::with_capacity(this.0.len());
	for part in this.0 {
		let part = match part {
			Part::Value(Expr::Literal(Literal::String(s))) => Part::Field(s),
			Part::Value(Expr::Literal(lit)) => Part::Value(Expr::Literal(lit)),
			Part::Value(_) => subscript(values.next().ok_or_else(|| {
				anyhow::anyhow!("a path holds a subscript with no evaluated value")
			})?)?,
			other @ (Part::All
			| Part::Flatten
			| Part::Last
			| Part::First
			| Part::Field(_)
			| Part::Where(_)
			| Part::Lookup(_)
			| Part::Start(_)
			| Part::Method(..)
			| Part::Destructure(_)
			| Part::Optional
			| Part::Recurse(..)
			| Part::Doc
			| Part::RepeatRecurse) => other,
		};
		out.push(part);
	}
	Ok(Idiom(out))
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_types::ToSql;

	use super::*;
	use crate::exec::operators::test_util::TestDb;

	/// A context over a transaction, which an expression evaluated from
	/// outside a plan is planned in.
	async fn root_ctx() -> ExecutionContext {
		TestDb::new("").await.exec_ctx().await
	}
	use crate::expr::statements::{DefineStatement, RemoveStatement};

	/// Parse exactly one statement and hand back its surface form.
	fn parse_one(src: &str) -> crate::expr::Expr {
		let ast = crate::syn::parse(src).expect("statement should parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected exactly one statement in {src:?}");
		let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = exprs.remove(0) else {
			panic!("expected a plain statement for {src:?}");
		};
		expr
	}

	/// Parse a `DEFINE`, the form a schema statement is carried and executed
	/// as.
	fn define(src: &str) -> DefineStatement {
		let crate::expr::Expr::Define(stmt) = parse_one(src) else {
			panic!("expected a DEFINE for {src:?}");
		};
		*stmt
	}

	/// Parse a `REMOVE`, as [`define`] does.
	fn remove(src: &str) -> RemoveStatement {
		let crate::expr::Expr::Remove(stmt) = parse_one(src) else {
			panic!("expected a REMOVE for {src:?}");
		};
		*stmt
	}

	/// The `name` of a `DEFINE ANALYZER`, the shortest statement whose name
	/// takes the field-shaped parse.
	fn analyzer_name(src: &str) -> Expr {
		let DefineStatement::Analyzer(stmt) = define(src) else {
			panic!("expected a DEFINE ANALYZER");
		};
		stmt.name
	}

	/// The `name` of a `DEFINE TABLE`, the statement whose name takes the
	/// table-shaped parse.
	fn define_table_name(src: &str) -> Expr {
		let DefineStatement::Table(stmt) = define(src) else {
			panic!("expected a DEFINE TABLE");
		};
		stmt.name
	}

	/// The `name` of a `REMOVE TABLE`.
	fn remove_table_name(src: &str) -> Expr {
		let RemoveStatement::Table(stmt) = remove(src) else {
			panic!("expected a REMOVE TABLE");
		};
		stmt.name
	}

	/// The `name` and `what` of a `DEFINE FIELD`.
	fn define_field_parts(src: &str) -> (Expr, Expr) {
		let DefineStatement::Field(stmt) = define(src) else {
			panic!("expected a DEFINE FIELD");
		};
		(stmt.name, stmt.what)
	}

	/// The `FIELDS` list of a `DEFINE INDEX`.
	fn define_index_cols(src: &str) -> Vec<Expr> {
		let DefineStatement::Index(stmt) = define(src) else {
			panic!("expected a DEFINE INDEX");
		};
		stmt.cols
	}

	/// The `NAMESPACE` and `DATABASE` of a `DEFINE CONFIG DEFAULT`.
	fn default_config_parts(src: &str) -> (Expr, Expr) {
		let DefineStatement::Config(stmt) = define(src) else {
			panic!("expected a DEFINE CONFIG");
		};
		let crate::expr::statements::define::config::ConfigInner::Default(cfg) = stmt.inner else {
			panic!("expected a DEFINE CONFIG DEFAULT");
		};
		(cfg.namespace, cfg.database)
	}

	/// A context with `row` as the row in scope, so that an expression
	/// evaluated under it can read the row's fields.
	async fn ctx_with_row(row: Value) -> ExecutionContext {
		root_ctx().await.with_current_row(Arc::new(row))
	}

	/// An object row, built from SurrealQL so the fixture reads as the row it
	/// stands for.
	fn row(src: &str) -> Value {
		crate::syn::value(src)
			.map(crate::val::convert_public::convert_public_value_to_internal)
			.expect("row fixture should parse")
	}

	// ========================================================================
	// The written-name shortcuts
	// ========================================================================

	#[tokio::test]
	async fn a_field_shaped_name_is_the_name_written() {
		let name = analyzer_name("DEFINE ANALYZER foo");
		assert!(
			matches!(&name, Expr::Idiom(Idiom(parts)) if matches!(parts.as_slice(), [Part::Field(_)])),
			"a bare DDL name parses as a single-field idiom, got {name:?}"
		);
		let ctx = root_ctx().await;
		assert_eq!(ident(&ctx, &name, "analyzer name").await.unwrap(), "foo");
	}

	#[tokio::test]
	async fn a_field_shaped_name_is_not_a_read_of_the_row() {
		// With a row in scope, evaluating the name would read the row's `foo`
		// and name the analyzer `surprise`. A wrong answer here is silent: the
		// statement succeeds, against the wrong subject.
		let name = analyzer_name("DEFINE ANALYZER foo");
		let ctx = ctx_with_row(row("{ foo: 'surprise' }")).await;
		assert_eq!(ident(&ctx, &name, "analyzer name").await.unwrap(), "foo");
	}

	#[tokio::test]
	async fn a_field_shaped_name_with_no_row_is_not_none() {
		// With no row, evaluating reaches `Value::None`. `ident` would refuse
		// loudly, but `optional_ident` reads `NONE` as "clause absent", so the
		// clause would silently go unset.
		let (namespace, database) =
			default_config_parts("DEFINE CONFIG DEFAULT NAMESPACE ns DATABASE db");
		let ctx = root_ctx().await;
		assert_eq!(
			optional_ident(&ctx, &namespace, "namespace").await.unwrap(),
			Some("ns".to_owned())
		);
		assert_eq!(
			optional_ident(&ctx, &database, "database").await.unwrap(),
			Some("db".to_owned())
		);
	}

	#[tokio::test]
	async fn an_optional_name_is_not_a_read_of_the_row_either() {
		let (namespace, _) = default_config_parts("DEFINE CONFIG DEFAULT NAMESPACE ns DATABASE db");
		let ctx = ctx_with_row(row("{ ns: 'surprise' }")).await;
		assert_eq!(
			optional_ident(&ctx, &namespace, "namespace").await.unwrap(),
			Some("ns".to_owned())
		);
	}

	#[tokio::test]
	async fn a_table_shaped_name_is_the_name_written() {
		let name = define_table_name("DEFINE TABLE foo");
		assert!(
			matches!(&name, Expr::Table(_)),
			"a DEFINE TABLE name parses as a table, got {name:?}"
		);
		let ctx = root_ctx().await;
		assert_eq!(ident(&ctx, &name, "table name").await.unwrap(), "foo");
	}

	#[tokio::test]
	async fn a_table_shaped_name_is_not_a_read_of_the_row() {
		// A `Value::Table` coerces to no string at all. Without this shortcut
		// every `DEFINE TABLE` would be refused, row or no row.
		let name = define_table_name("DEFINE TABLE foo");
		let ctx = ctx_with_row(row("{ foo: 'surprise' }")).await;
		assert_eq!(ident(&ctx, &name, "table name").await.unwrap(), "foo");
	}

	#[tokio::test]
	async fn the_two_shortcuts_cover_disjoint_statements() {
		// `DEFINE TABLE t` and `REMOVE TABLE t` name the same thing and take
		// opposite parses, so neither shortcut stands in for the other.
		let defined = define_table_name("DEFINE TABLE foo");
		let removed = remove_table_name("REMOVE TABLE foo");
		assert!(matches!(&defined, Expr::Table(_)), "got {defined:?}");
		assert!(
			matches!(&removed, Expr::Idiom(Idiom(parts)) if matches!(parts.as_slice(), [Part::Field(_)])),
			"got {removed:?}"
		);

		let ctx = ctx_with_row(row("{ foo: 'surprise' }")).await;
		assert_eq!(ident(&ctx, &defined, "table name").await.unwrap(), "foo");
		assert_eq!(ident(&ctx, &removed, "table name").await.unwrap(), "foo");
	}

	#[tokio::test]
	async fn a_field_statement_names_both_its_field_and_its_table() {
		// One statement, both parses: the field name is field-shaped and the
		// `ON` target is table-shaped.
		let (name, what) = define_field_parts("DEFINE FIELD email ON person");
		let ctx = ctx_with_row(row("{ email: 'surprise', person: 'surprise' }")).await;
		assert_eq!(idiom(&ctx, &name, "field name").await.unwrap().to_sql(), "email");
		assert_eq!(ident(&ctx, &what, "table name").await.unwrap(), "person");
	}

	// ========================================================================
	// The evaluated route
	// ========================================================================

	#[tokio::test]
	async fn a_computed_name_is_evaluated() {
		let name = analyzer_name("DEFINE ANALYZER type::string($x)");
		let ctx = root_ctx().await.with_param("x", Value::from("foo"));
		assert_eq!(ident(&ctx, &name, "analyzer name").await.unwrap(), "foo");
	}

	#[tokio::test]
	async fn a_computed_name_reads_the_row_in_scope() {
		// The other side of the shortcut: an expression that is not a written
		// name does evaluate, and the row in scope is what a path in it reads.
		// `addr.city` is two parts, so no shortcut applies to it.
		let name = analyzer_name("DEFINE ANALYZER type::string(addr.city)");
		let ctx = ctx_with_row(row("{ addr: { city: 'from the row' } }")).await;
		assert_eq!(ident(&ctx, &name, "analyzer name").await.unwrap(), "from the row");
	}

	#[tokio::test]
	async fn a_refused_name_says_which_clause_wanted_it() {
		let name = analyzer_name("DEFINE ANALYZER type::int(1)");
		let ctx = root_ctx().await;
		let err = ident(&ctx, &name, "analyzer name")
			.await
			.expect_err("an integer is not a name")
			.to_string();
		assert!(err.contains("analyzer name"), "the refusal names the clause, got {err:?}");
	}

	#[tokio::test]
	async fn an_optional_name_that_evaluates_to_none_is_absent() {
		let name = analyzer_name("DEFINE ANALYZER $x");
		let ctx = root_ctx().await.with_param("x", Value::None);
		assert_eq!(optional_ident(&ctx, &name, "namespace").await.unwrap(), None);
	}

	#[tokio::test]
	async fn scalar_evaluates_with_no_shortcut() {
		// `scalar` is the general route, so the shape `ident` shortcuts is
		// evaluated here — a bare name reads the row.
		let name = analyzer_name("DEFINE ANALYZER foo");
		let ctx = ctx_with_row(row("{ foo: 'from the row' }")).await;
		assert_eq!(scalar(&ctx, &name).await.unwrap(), Value::from("from the row"));
	}

	// ========================================================================
	// Field paths
	// ========================================================================

	#[tokio::test]
	async fn a_written_path_keeps_its_structure() {
		let (name, _) = define_field_parts("DEFINE FIELD addr.city ON person");
		let ctx = ctx_with_row(row("{ addr: { city: 'surprise' } }")).await;
		assert_eq!(idiom(&ctx, &name, "field name").await.unwrap().to_sql(), "addr.city");
	}

	#[tokio::test]
	async fn a_parameterised_subscript_is_resolved_in_place() {
		let (name, _) = define_field_parts("DEFINE FIELD addr[$i] ON person");
		let ctx = root_ctx().await.with_param("i", Value::from(2));
		assert_eq!(idiom(&ctx, &name, "field name").await.unwrap().to_sql(), "addr[2]");
	}

	#[tokio::test]
	async fn a_string_subscript_becomes_a_field() {
		let (name, _) = define_field_parts("DEFINE FIELD addr[$k] ON person");
		let ctx = root_ctx().await.with_param("k", Value::from("city"));
		assert_eq!(idiom(&ctx, &name, "field name").await.unwrap().to_sql(), "addr.city");
	}

	#[tokio::test]
	async fn a_literal_subscript_is_canonicalised_without_evaluating() {
		// Literal subscripts are canonicalised without evaluation:
		// `addr['city']` and `addr.city` must store the same path, and no row
		// is consulted to decide that.
		let (name, _) = define_field_parts("DEFINE FIELD addr['city'] ON person");
		let ctx = ctx_with_row(row("{ addr: { city: 'surprise' } }")).await;
		assert_eq!(idiom(&ctx, &name, "field name").await.unwrap().to_sql(), "addr.city");
	}

	#[tokio::test]
	async fn a_subscript_that_is_neither_an_index_nor_a_key_is_refused() {
		let (name, _) = define_field_parts("DEFINE FIELD addr[$i] ON person");
		let ctx = root_ctx().await.with_param("i", Value::Bool(true));
		let err = idiom(&ctx, &name, "field name")
			.await
			.expect_err("a boolean addresses nothing")
			.to_string();
		assert!(err.contains("integer or string"), "got {err:?}");
	}

	#[tokio::test]
	async fn a_path_that_is_not_field_headed_is_evaluated_and_reparsed() {
		let (name, _) = define_field_parts("DEFINE FIELD $p ON person");
		let ctx = root_ctx().await.with_param("p", Value::from("addr.city"));
		assert_eq!(idiom(&ctx, &name, "field name").await.unwrap().to_sql(), "addr.city");
	}

	#[tokio::test]
	async fn a_local_path_refuses_what_a_raw_path_accepts() {
		// The two path helpers differ only here, and the difference is why
		// there are two: an index column may be a method call, a stored field
		// name may not.
		let cols = define_index_cols("DEFINE INDEX i ON person FIELDS id.id()");
		let expr = cols.first().expect("one column").clone();
		let ctx = root_ctx().await;
		assert_eq!(raw_idiom(&ctx, &expr, "field name").await.unwrap().to_sql(), "id.id()");
		idiom(&ctx, &expr, "field name").await.expect_err("a method call is not a local path");
	}

	// ========================================================================
	// Field lists
	// ========================================================================

	#[tokio::test]
	async fn a_written_field_list_keeps_each_path() {
		let cols = define_index_cols("DEFINE INDEX i ON person FIELDS first, last");
		let ctx = ctx_with_row(row("{ first: 'surprise', last: 'surprise' }")).await;
		let resolved = fields(&ctx, &cols).await.unwrap();
		let rendered: Vec<String> = resolved.iter().map(|i| i.to_sql()).collect();
		assert_eq!(rendered, vec!["first".to_owned(), "last".to_owned()]);
	}

	#[tokio::test]
	async fn type_fields_expands_to_several_paths() {
		let cols = define_index_cols("DEFINE INDEX i ON person FIELDS type::fields($f)");
		let ctx = root_ctx()
			.await
			.with_param("f", Value::from(vec![Value::from("first"), Value::from("addr.city")]));
		let resolved = fields(&ctx, &cols).await.unwrap();
		let rendered: Vec<String> = resolved.iter().map(|i| i.to_sql()).collect();
		assert_eq!(rendered, vec!["first".to_owned(), "addr.city".to_owned()]);
	}

	#[tokio::test]
	async fn type_field_expands_to_one_path() {
		let cols = define_index_cols("DEFINE INDEX i ON person FIELDS type::field($f)");
		let ctx = root_ctx().await.with_param("f", Value::from("addr.city"));
		let resolved = fields(&ctx, &cols).await.unwrap();
		let rendered: Vec<String> = resolved.iter().map(|i| i.to_sql()).collect();
		assert_eq!(rendered, vec!["addr.city".to_owned()]);
	}

	#[tokio::test]
	async fn a_field_list_accepts_a_path_a_stored_field_name_could_not_be() {
		let cols = define_index_cols("DEFINE INDEX i ON person FIELDS id.id()");
		let ctx = root_ctx().await;
		let resolved = fields(&ctx, &cols).await.unwrap();
		let rendered: Vec<String> = resolved.iter().map(|i| i.to_sql()).collect();
		assert_eq!(rendered, vec!["id.id()".to_owned()]);
	}
}
