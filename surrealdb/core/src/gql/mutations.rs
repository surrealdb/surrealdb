//! GraphQL mutation generation.
//!
//! Generates CREATE, UPDATE, UPSERT, DELETE mutations (single and bulk) for each
//! table in the schema, along with the corresponding input types.
//!
//! - Single mutations: `create{Table}`, `update{Table}`, `upsert{Table}`, `delete{Table}`
//! - Bulk mutations: `createMany{Table}`, `updateMany{Table}`, `upsertMany{Table}`,
//!   `deleteMany{Table}`
//! - For relation tables, `create{Table}` uses RELATE instead of CREATE

use std::collections::BTreeMap;
use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	Field, FieldFuture, FieldValue, InputObject, InputValue, Object, Type, TypeRef,
};
use async_graphql::{Name, Value as GqlValue};
use surrealdb_types::ToSql;

use super::error::{GqlError, resolver_error};
use super::schema::{SchemaContext, gql_to_sql_kind, kind_to_type_with_enum_prefix, unwrap_type};
use super::tables::{CachedRecord, cond_from_filter};
use super::utils::{GqlValueUtils, execute_plan};
use crate::catalog::providers::TableProvider;
use crate::catalog::{FieldDefinition, TableDefinition, TableType};
use crate::dbs::Session;
use crate::expr::part::Part;
use crate::expr::statements::{
	CreateStatement, DeleteStatement, RelateStatement, UpdateStatement, UpsertStatement,
};
use crate::expr::{Cond, Data, Expr, Kind, Literal, LogicalPlan, Output, TopLevelExpr};
use crate::kvs::Datastore;
use crate::val::{Object as SurObject, RecordId, TableName, Value};

/// Capitalize the first character of a string.
fn capitalize_first(s: &str) -> String {
	let mut chars = s.chars();
	match chars.next() {
		None => String::new(),
		Some(c) => c.to_uppercase().to_string() + chars.as_str(),
	}
}

/// Parse a record ID from a table name and user-provided ID string.
///
/// Attempts to parse the full `table:id` string as a proper record ID (handling
/// numeric keys, etc.), falling back to a plain string key.
fn parse_record_id(table_name: &TableName, id: &str) -> Result<RecordId, GqlError> {
	let rid_str = format!("{table_name}:{id}");
	match crate::syn::record_id(&rid_str) {
		Ok(x) => Ok(x.into()),
		Err(_) => Ok(RecordId::new(table_name.clone(), id.to_string())),
	}
}

/// Parse a full record ID string (e.g., "person:alice").
fn parse_full_record_id(id_str: &str) -> Result<RecordId, GqlError> {
	crate::syn::record_id(id_str)
		.map(|x| x.into())
		.map_err(|e| resolver_error(format!("Invalid record ID: {id_str}: {e}")))
}

/// Convert a GraphQL input object to a SurrealDB Object for use as CONTENT/MERGE data.
///
/// Iterates over the input fields, finds each field's Kind from the field definitions,
/// and converts the GraphQL value to the corresponding SurrealDB value.
fn gql_input_to_sql_object(
	input: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	skip_fields: &[&str],
) -> Result<SurObject, GqlError> {
	let mut map = BTreeMap::new();
	for (key, val) in input {
		let key_str = key.as_str();
		if skip_fields.contains(&key_str) {
			continue;
		}
		// Skip null values
		if matches!(val, GqlValue::Null) {
			continue;
		}
		// Find the field kind from definitions
		let kind = fds
			.iter()
			.find(|fd| {
				fd.name.0.len() == 1 && matches!(&fd.name.0[0], Part::Field(n) if n == key_str)
			})
			.and_then(|fd| fd.field_kind.clone())
			.unwrap_or(Kind::Any);
		let sql_val = gql_to_sql_kind(val, kind)?;
		map.insert(key_str.to_string(), sql_val);
	}
	Ok(SurObject(map))
}

/// Map a field's `Kind` to a GraphQL `TypeRef` suitable for mutation input types.
///
/// This differs from `kind_to_type(kind, types, true)` in that `record<target>`
/// fields are mapped to `ID` (the user passes a record ID string) rather than
/// the target table's output Object type (which is not a valid input type).
fn kind_to_input_type_ref(
	kind: Kind,
	types: &mut Vec<Type>,
	enum_scope: Option<&str>,
) -> Result<TypeRef, GqlError> {
	let optional = kind.can_be_none();

	// Check if the kind (after stripping option) is a record type.
	// Record fields should be represented as ID in input types (the user passes
	// a record ID string, not a nested object).
	match &kind {
		Kind::Record(_) => {
			let ty = TypeRef::named(TypeRef::ID);
			return Ok(if optional {
				ty
			} else {
				TypeRef::NonNull(Box::new(ty))
			});
		}
		Kind::Either(ks) => {
			// Strip None/Null variants to see what's underneath
			let non_none: Vec<&Kind> =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect();
			if non_none.len() == 1
				&& let Kind::Record(_) = non_none[0]
			{
				// option<record<T>> -> nullable ID
				return Ok(TypeRef::named(TypeRef::ID));
			}
		}
		_ => {}
	}

	// For all other kinds, delegate to the standard kind_to_type with is_input=true
	kind_to_type_with_enum_prefix(kind, types, true, enum_scope)
}

/// Generate Create/Update/Upsert input types for a table and return their names.
fn generate_input_types(
	tb_name: &str,
	fds: &[FieldDefinition],
	is_relation: bool,
	types: &mut Vec<Type>,
) -> Result<(String, String, String), GqlError> {
	let cap_name = capitalize_first(tb_name);
	let create_name = format!("Create{cap_name}Input");
	let update_name = format!("Update{cap_name}Input");
	let upsert_name = format!("Upsert{cap_name}Input");

	let mut create_input = InputObject::new(&create_name)
		.description(format!("Input for creating a `{tb_name}` record"));
	let mut update_input = InputObject::new(&update_name)
		.description(format!("Input for updating a `{tb_name}` record"));
	let mut upsert_input = InputObject::new(&upsert_name)
		.description(format!("Input for upserting a `{tb_name}` record"));

	// Add optional `id` field for create and upsert
	create_input = create_input.field(InputValue::new("id", TypeRef::named(TypeRef::ID)));
	upsert_input = upsert_input.field(InputValue::new("id", TypeRef::named(TypeRef::ID)));

	// For relation tables, add required `in` and `out` ID fields
	if is_relation {
		create_input = create_input
			.field(InputValue::new("in", TypeRef::named_nn(TypeRef::ID)))
			.field(InputValue::new("out", TypeRef::named_nn(TypeRef::ID)));
		upsert_input = upsert_input
			.field(InputValue::new("in", TypeRef::named_nn(TypeRef::ID)))
			.field(InputValue::new("out", TypeRef::named_nn(TypeRef::ID)));
	}

	// Add fields from field definitions
	for fd in fds.iter() {
		let Some(ref kind) = fd.field_kind else {
			continue;
		};
		if fd.name.is_id() {
			continue;
		}
		// Skip nested fields (multi-part idioms)
		if fd.name.0.len() > 1 {
			continue;
		}
		let fd_name = fd.name.to_sql();
		// Skip `in` and `out` for relation tables (already added above)
		if is_relation && (fd_name == "in" || fd_name == "out") {
			continue;
		}

		// For create and upsert: use the field's input type (preserving non-null)
		let enum_scope = format!("{}_{}", tb_name, fd_name);
		let create_type = kind_to_input_type_ref(kind.clone(), types, Some(&enum_scope))?;
		create_input = create_input.field(InputValue::new(&fd_name, create_type.clone()));
		upsert_input = upsert_input.field(InputValue::new(&fd_name, create_type));

		// For update: all fields are optional (strip NonNull)
		let update_type =
			unwrap_type(kind_to_input_type_ref(kind.clone(), types, Some(&enum_scope))?);
		update_input = update_input.field(InputValue::new(&fd_name, update_type));
	}

	types.push(Type::InputObject(create_input));
	types.push(Type::InputObject(update_input));
	types.push(Type::InputObject(upsert_input));

	Ok((create_name, update_name, upsert_name))
}

/// Shared context for generating mutation fields for a single table.
///
/// Groups the per-table parameters that are passed to every mutation field
/// builder, replacing scattered arguments with a single reference.
struct MutationTableContext {
	/// The capitalized table name (e.g., "Person").
	cap_name: String,
	/// The table name as a string (e.g., "person").
	tb_name_str: String,
	/// The table name.
	tb_name: TableName,
	/// Whether the table is a relation table.
	is_relation: bool,
	/// Field definitions for this table.
	fds: Arc<[FieldDefinition]>,
	/// The datastore.
	kvs: Arc<Datastore>,
	/// The filter type name for this table (e.g., "_filter_person").
	table_filter_name: String,
}

/// Process all tables and generate the Mutation root object with mutation fields.
pub async fn process_mutations(
	tbs: Arc<[TableDefinition]>,
	types: &mut Vec<Type>,
	schema_ctx: &SchemaContext<'_>,
) -> Result<Object, GqlError> {
	let mut mutation = Object::new("Mutation");

	for tb in tbs.iter() {
		let tb_name = tb.name.clone();
		let tb_name_str = tb_name.clone().into_string();
		let is_relation = matches!(tb.table_type, TableType::Relation(_));

		let fds = schema_ctx.tx.all_tb_fields(schema_ctx.ns, schema_ctx.db, &tb.name, None).await?;

		// Generate input types
		let (create_input_name, update_input_name, upsert_input_name) =
			generate_input_types(&tb_name_str, &fds, is_relation, types)?;

		let ctx = MutationTableContext {
			cap_name: capitalize_first(&tb_name_str),
			table_filter_name: format!("_filter_{tb_name_str}"),
			tb_name_str,
			tb_name,
			is_relation,
			fds,
			kvs: schema_ctx.datastore.clone(),
		};

		// --- Single-record mutations ---
		mutation = add_create_field(mutation, &ctx, &create_input_name);
		mutation = add_update_field(mutation, &ctx, &update_input_name);
		mutation = add_upsert_field(mutation, &ctx, &upsert_input_name);
		mutation = add_delete_field(mutation, &ctx);

		// --- Bulk mutations ---
		mutation = add_create_many_field(mutation, &ctx, &create_input_name);
		mutation = add_update_many_field(mutation, &ctx, &update_input_name);
		mutation = add_upsert_many_field(mutation, &ctx, &upsert_input_name);
		mutation = add_delete_many_field(mutation, &ctx);
	}

	Ok(mutation)
}

// ---------------------------------------------------------------------------
// Single-record mutation field builders
// ---------------------------------------------------------------------------

fn add_create_field(mutation: Object, tc: &MutationTableContext, input_name: &str) -> Object {
	let fds = tc.fds.clone();
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	let is_relation = tc.is_relation;
	mutation.field(
		Field::new(
			format!("create{}", tc.cap_name),
			TypeRef::named(tc.tb_name_str.as_str()),
			move |ctx| {
				let fds = fds.clone();
				let kvs = kvs.clone();
				let tb_name = tb_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();
					let data_obj = get_data_object(args)?;
					let id_opt = data_obj.get("id").and_then(GqlValueUtils::as_string);

					if is_relation {
						execute_relate_create(&kvs, sess, &tb_name, data_obj, &fds, id_opt).await
					} else {
						execute_normal_create(&kvs, sess, &tb_name, data_obj, &fds, id_opt).await
					}
				})
			},
		)
		.description(format!("Create a new `{}` record", tc.tb_name_str))
		.argument(InputValue::new("data", TypeRef::named_nn(input_name))),
	)
}

fn add_update_field(mutation: Object, tc: &MutationTableContext, input_name: &str) -> Object {
	let fds = tc.fds.clone();
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	mutation.field(
		Field::new(
			format!("update{}", tc.cap_name),
			TypeRef::named(tc.tb_name_str.as_str()),
			move |ctx| {
				let fds = fds.clone();
				let kvs = kvs.clone();
				let tb_name = tb_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();
					let id_str = get_required_id(args)?;
					let data_obj = get_data_object(args)?;

					let rid = parse_record_id(&tb_name, &id_str)?;
					let content = gql_input_to_sql_object(data_obj, &fds, &["id"])?;

					let data = if content.0.is_empty() {
						None
					} else {
						Some(Data::MergeExpression(Value::Object(content).into_literal()))
					};

					let stmt = UpdateStatement {
						only: true,
						what: vec![Value::RecordId(rid).into_literal()],
						data,
						cond: None,
						output: None,
						timeout: Expr::Literal(Literal::None),
						..Default::default()
					};

					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(Expr::Update(Box::new(stmt)))],
					};

					let res = execute_plan(&kvs, sess, plan).await?;
					extract_single_record(res)
				})
			},
		)
		.description(format!("Update an existing `{}` record", tc.tb_name_str))
		.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID)))
		.argument(InputValue::new("data", TypeRef::named_nn(input_name))),
	)
}

fn add_upsert_field(mutation: Object, tc: &MutationTableContext, input_name: &str) -> Object {
	let fds = tc.fds.clone();
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	mutation.field(
		Field::new(
			format!("upsert{}", tc.cap_name),
			TypeRef::named(tc.tb_name_str.as_str()),
			move |ctx| {
				let fds = fds.clone();
				let kvs = kvs.clone();
				let tb_name = tb_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();
					let id_str = get_required_id(args)?;
					let data_obj = get_data_object(args)?;

					let rid = parse_record_id(&tb_name, &id_str)?;
					let content = gql_input_to_sql_object(data_obj, &fds, &["id"])?;

					let data = if content.0.is_empty() {
						None
					} else {
						Some(Data::ContentExpression(Value::Object(content).into_literal()))
					};

					let stmt = UpsertStatement {
						only: true,
						what: vec![Value::RecordId(rid).into_literal()],
						data,
						cond: None,
						output: None,
						timeout: Expr::Literal(Literal::None),
						..Default::default()
					};

					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(Expr::Upsert(Box::new(stmt)))],
					};

					let res = execute_plan(&kvs, sess, plan).await?;
					extract_single_record(res)
				})
			},
		)
		.description(format!("Upsert a `{}` record (create or update)", tc.tb_name_str))
		.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID)))
		.argument(InputValue::new("data", TypeRef::named_nn(input_name))),
	)
}

fn add_delete_field(mutation: Object, tc: &MutationTableContext) -> Object {
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	mutation.field(
		Field::new(
			format!("delete{}", tc.cap_name),
			TypeRef::named_nn(TypeRef::BOOLEAN),
			move |ctx| {
				let kvs = kvs.clone();
				let tb_name = tb_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();
					let id_str = get_required_id(args)?;

					let rid = parse_record_id(&tb_name, &id_str)?;

					let stmt = DeleteStatement {
						only: false,
						what: vec![Value::RecordId(rid).into_literal()],
						cond: None,
						output: None,
						timeout: Expr::Literal(Literal::None),
						..Default::default()
					};

					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(Expr::Delete(Box::new(stmt)))],
					};

					let _res = execute_plan(&kvs, sess, plan).await?;
					Ok(Some(FieldValue::value(GqlValue::Boolean(true))))
				})
			},
		)
		.description(format!("Delete a `{}` record by ID", tc.tb_name_str))
		.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID))),
	)
}

// ---------------------------------------------------------------------------
// Bulk mutation field builders
// ---------------------------------------------------------------------------

fn add_create_many_field(mutation: Object, tc: &MutationTableContext, input_name: &str) -> Object {
	let fds = tc.fds.clone();
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	let is_relation = tc.is_relation;
	mutation.field(
		Field::new(
			format!("createMany{}", tc.cap_name),
			TypeRef::named_nn_list_nn(tc.tb_name_str.as_str()),
			move |ctx| {
				let fds = fds.clone();
				let kvs = kvs.clone();
				let tb_name = tb_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();
					let data_list =
						args.get("data").and_then(GqlValueUtils::as_list).ok_or_else(|| {
							resolver_error("Missing required 'data' argument (must be a list)")
						})?;

					let mut results = Vec::with_capacity(data_list.len());

					for item in data_list {
						let data_obj = item.as_object().ok_or_else(|| {
							resolver_error("Each item in 'data' must be an object")
						})?;
						let id_opt = data_obj.get("id").and_then(GqlValueUtils::as_string);

						let res = if is_relation {
							execute_relate_create(&kvs, sess, &tb_name, data_obj, &fds, id_opt)
								.await
						} else {
							execute_normal_create(&kvs, sess, &tb_name, data_obj, &fds, id_opt)
								.await
						};

						match res? {
							Some(fv) => results.push(fv),
							None => {
								return Err(resolver_error(
									"Create returned no result for bulk create",
								)
								.into());
							}
						}
					}

					Ok(Some(FieldValue::list(results)))
				})
			},
		)
		.description(format!("Create multiple `{}` records", tc.tb_name_str))
		.argument(InputValue::new("data", TypeRef::named_nn_list_nn(input_name))),
	)
}

fn add_update_many_field(mutation: Object, tc: &MutationTableContext, input_name: &str) -> Object {
	let fds = tc.fds.clone();
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	mutation.field(
		Field::new(
			format!("updateMany{}", tc.cap_name),
			TypeRef::named_nn_list_nn(tc.tb_name_str.as_str()),
			move |ctx| {
				let fds = fds.clone();
				let kvs = kvs.clone();
				let tb_name = tb_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();

					let data_obj = get_data_object(args)?;
					let content = gql_input_to_sql_object(data_obj, &fds, &["id"])?;
					let data = if content.0.is_empty() {
						None
					} else {
						Some(Data::MergeExpression(Value::Object(content).into_literal()))
					};

					// Parse WHERE condition
					let cond = parse_where_arg(args, &fds)?;

					let stmt = UpdateStatement {
						only: false,
						what: vec![Expr::Table(tb_name)],
						data,
						cond,
						output: None,
						timeout: Expr::Literal(Literal::None),
						..Default::default()
					};

					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(Expr::Update(Box::new(stmt)))],
					};

					let res = execute_plan(&kvs, sess, plan).await?;
					extract_record_list(res)
				})
			},
		)
		.description(format!("Update multiple `{}` records matching a filter", tc.tb_name_str))
		.argument(InputValue::new("where", TypeRef::named(tc.table_filter_name.as_str())))
		.argument(InputValue::new("data", TypeRef::named_nn(input_name))),
	)
}

fn add_upsert_many_field(mutation: Object, tc: &MutationTableContext, input_name: &str) -> Object {
	let fds = tc.fds.clone();
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	mutation.field(
		Field::new(
			format!("upsertMany{}", tc.cap_name),
			TypeRef::named_nn_list_nn(tc.tb_name_str.as_str()),
			move |ctx| {
				let fds = fds.clone();
				let kvs = kvs.clone();
				let tb_name = tb_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();

					let data_obj = get_data_object(args)?;
					let content = gql_input_to_sql_object(data_obj, &fds, &["id"])?;
					let data = if content.0.is_empty() {
						None
					} else {
						Some(Data::ContentExpression(Value::Object(content).into_literal()))
					};

					// Parse WHERE condition
					let cond = parse_where_arg(args, &fds)?;

					let stmt = UpsertStatement {
						only: false,
						what: vec![Expr::Table(tb_name)],
						data,
						cond,
						output: None,
						timeout: Expr::Literal(Literal::None),
						..Default::default()
					};

					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(Expr::Upsert(Box::new(stmt)))],
					};

					let res = execute_plan(&kvs, sess, plan).await?;
					extract_record_list(res)
				})
			},
		)
		.description(format!("Upsert multiple `{}` records matching a filter", tc.tb_name_str))
		.argument(InputValue::new("where", TypeRef::named(tc.table_filter_name.as_str())))
		.argument(InputValue::new("data", TypeRef::named_nn(input_name))),
	)
}

fn add_delete_many_field(mutation: Object, tc: &MutationTableContext) -> Object {
	let fds = tc.fds.clone();
	let kvs = tc.kvs.clone();
	let tb_name = tc.tb_name.clone();
	mutation.field(
		Field::new(
			format!("deleteMany{}", tc.cap_name),
			TypeRef::named_nn(TypeRef::INT),
			move |ctx| {
				let fds = fds.clone();
				let kvs = kvs.clone();
				let tb_name = tb_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();

					// Parse WHERE condition
					let cond = parse_where_arg(args, &fds)?;

					// Use RETURN BEFORE to count deleted records
					let stmt = DeleteStatement {
						only: false,
						what: vec![Expr::Table(tb_name)],
						cond,
						output: Some(Output::Before),
						timeout: Expr::Literal(Literal::None),
						..Default::default()
					};

					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(Expr::Delete(Box::new(stmt)))],
					};

					let res = execute_plan(&kvs, sess, plan).await?;
					let count = match res {
						Value::Array(a) => a.len() as i64,
						_ => 0,
					};
					Ok(Some(FieldValue::value(GqlValue::Number(count.into()))))
				})
			},
		)
		.description(format!(
			"Delete multiple `{}` records matching a filter, returns count",
			tc.tb_name_str
		))
		.argument(InputValue::new("where", TypeRef::named(tc.table_filter_name.as_str()))),
	)
}

// ---------------------------------------------------------------------------
// Shared helpers for mutation resolvers
// ---------------------------------------------------------------------------

/// Extract the required `data` argument as an object from the args map.
fn get_data_object(args: &IndexMap<Name, GqlValue>) -> Result<&IndexMap<Name, GqlValue>, GqlError> {
	args.get("data")
		.ok_or_else(|| resolver_error("Missing required 'data' argument"))
		.and_then(|v| v.as_object().ok_or_else(|| resolver_error("'data' must be an object")))
}

/// Extract the required `id` argument as a string from the args map.
fn get_required_id(args: &IndexMap<Name, GqlValue>) -> Result<String, GqlError> {
	args.get("id")
		.and_then(GqlValueUtils::as_string)
		.ok_or_else(|| resolver_error("Missing required 'id' argument"))
}

/// Parse the optional `where` argument into a `Cond`.
fn parse_where_arg(
	args: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
) -> Result<Option<Cond>, GqlError> {
	match args.get("where") {
		Some(GqlValue::Object(o)) if !o.is_empty() => Ok(Some(cond_from_filter(o, fds)?)),
		_ => Ok(None),
	}
}

/// Execute a CREATE for a normal (non-relation) table.
async fn execute_normal_create(
	kvs: &Arc<Datastore>,
	sess: &Arc<Session>,
	tb_name: &TableName,
	data_obj: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	id_opt: Option<String>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let content = gql_input_to_sql_object(data_obj, fds, &["id"])?;

	let what = match id_opt {
		Some(id_str) => {
			let rid = parse_record_id(tb_name, &id_str)?;
			vec![Value::RecordId(rid).into_literal()]
		}
		None => vec![Expr::Table(tb_name.clone())],
	};

	let data = if content.0.is_empty() {
		None
	} else {
		Some(Data::ContentExpression(Value::Object(content).into_literal()))
	};

	let stmt = CreateStatement {
		only: true,
		what,
		data,
		output: None,
		timeout: Expr::Literal(Literal::None),
	};

	let plan = LogicalPlan {
		expressions: vec![TopLevelExpr::Expr(Expr::Create(Box::new(stmt)))],
	};

	let res = execute_plan(kvs, sess, plan).await?;
	extract_single_record(res)
}

/// Execute a RELATE for a relation table creation.
async fn execute_relate_create(
	kvs: &Arc<Datastore>,
	sess: &Arc<Session>,
	tb_name: &TableName,
	data_obj: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	id_opt: Option<String>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let in_str = data_obj
		.get("in")
		.and_then(GqlValueUtils::as_string)
		.ok_or_else(|| resolver_error("Relation create requires 'in' field"))?;
	let out_str = data_obj
		.get("out")
		.and_then(GqlValueUtils::as_string)
		.ok_or_else(|| resolver_error("Relation create requires 'out' field"))?;

	let from_rid = parse_full_record_id(&in_str)?;
	let to_rid = parse_full_record_id(&out_str)?;

	// Build content data (excluding id, in, out)
	let content = gql_input_to_sql_object(data_obj, fds, &["id", "in", "out"])?;

	let through = match id_opt {
		Some(id_str) => {
			let rid = parse_record_id(tb_name, &id_str)?;
			Value::RecordId(rid).into_literal()
		}
		None => Expr::Table(tb_name.clone()),
	};

	let data = if content.0.is_empty() {
		None
	} else {
		Some(Data::ContentExpression(Value::Object(content).into_literal()))
	};

	let stmt = RelateStatement {
		only: true,
		through,
		from: Value::RecordId(from_rid).into_literal(),
		to: Value::RecordId(to_rid).into_literal(),
		data,
		output: None,
		timeout: Expr::Literal(Literal::None),
	};

	let plan = LogicalPlan {
		expressions: vec![TopLevelExpr::Expr(Expr::Relate(Box::new(stmt)))],
	};

	let res = execute_plan(kvs, sess, plan).await?;
	extract_single_record(res)
}

/// Extract a single record from a mutation result and return it as a FieldValue.
///
/// The full result object is cached in a [`CachedRecord`] so that field
/// resolvers can extract values directly without additional database queries.
fn extract_single_record(val: Value) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	match val {
		Value::Object(obj) => {
			let rid = match obj.get("id") {
				Some(Value::RecordId(rid)) => rid.clone(),
				_ => return Err(resolver_error("Mutation result missing 'id' field").into()),
			};
			Ok(Some(FieldValue::owned_any(CachedRecord {
				rid,
				version: None,
				data: obj,
			})))
		}
		Value::None | Value::Null => Ok(None),
		_ => {
			error!("Unexpected mutation result type: {val:?}");
			Err(resolver_error("Unexpected mutation result").into())
		}
	}
}

/// Extract a list of records from a bulk mutation result.
///
/// Each result object is cached in a [`CachedRecord`] for efficient field
/// resolution.
fn extract_record_list(val: Value) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	match val {
		Value::Array(arr) => {
			let items: Result<Vec<FieldValue>, GqlError> = arr
				.0
				.into_iter()
				.map(|v| match v {
					Value::Object(obj) => {
						let rid = match obj.get("id") {
							Some(Value::RecordId(rid)) => rid.clone(),
							_ => return Err(resolver_error("Mutation result missing 'id' field")),
						};
						Ok(FieldValue::owned_any(CachedRecord {
							rid,
							version: None,
							data: obj,
						}))
					}
					_ => {
						error!("Expected object in mutation result, found: {v:?}");
						Err(resolver_error("Unexpected mutation result format"))
					}
				})
				.collect();
			Ok(Some(FieldValue::list(items?)))
		}
		_ => {
			error!("Expected array result for bulk mutation, found: {val:?}");
			Err(resolver_error("Unexpected bulk mutation result format").into())
		}
	}
}
