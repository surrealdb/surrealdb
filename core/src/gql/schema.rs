use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;

use crate::dbs::Session;
use crate::kvs::Datastore;
use crate::iam;
use crate::sql::kind::Literal;
use crate::sql::statements::define::config::graphql::TablesConfig;
use crate::sql::statements::{DefineFieldStatement, SelectStatement, CreateStatement, UpdateStatement, DeleteStatement};
use crate::sql::{self, Table};
use crate::sql::{Cond, Data, Fields};
use crate::sql::{Expression, Geometry, Operator};
use crate::sql::{Ident, Idiom, Kind, Part};
use crate::sql::{Statement, Thing, Values, Object as SqlObject, TableType};
use async_graphql::dynamic::{Enum, FieldValue, ResolverContext, Type, Union};
use async_graphql::dynamic::{Field, Interface};
use async_graphql::dynamic::{FieldFuture, InterfaceField};
use async_graphql::dynamic::{InputObject, Object};
use async_graphql::dynamic::{InputValue, Schema};
use async_graphql::dynamic::{Scalar, TypeRef};
use async_graphql::indexmap::IndexMap;
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde_json::Number;

use super::error::{resolver_error, GqlError};
use super::ext::IntoExt;
#[cfg(debug_assertions)]
use super::ext::ValidatorExt;
use crate::gql::error::{internal_error, schema_error, type_error};
use crate::gql::ext::{NamedContainer, TryAsExt};
use crate::gql::utils::{GQLTx, GqlValueUtils};
use crate::kvs::LockType;
use reblessive::TreeStack;
use crate::kvs::TransactionType;
use crate::sql::Value as SqlValue;

#[derive(Debug, Clone)]
struct RelationInfo {
    field_name: String,
    target_table: String,
    relation_type: RelationType,
    is_list: bool,
}

#[derive(Debug, Clone)]
enum RelationType {
    /// Direct field reference (field is record<table>)
    Direct,
    /// Incoming relation through a relation table (this table is referenced by out field)
    IncomingRelation { relation_table: String },
    /// Outgoing relation through a relation table (this table is referenced by in field)
    OutgoingRelation { relation_table: String },
}

type ErasedRecord = (GQLTx, Thing);

fn field_val_erase_owned(val: ErasedRecord) -> FieldValue<'static> {
	FieldValue::owned_any(val)
}

macro_rules! limit_input {
	() => {
		InputValue::new("limit", TypeRef::named(TypeRef::INT))
	};
}

macro_rules! start_input {
	() => {
		InputValue::new("start", TypeRef::named(TypeRef::INT))
	};
}

macro_rules! id_input {
	() => {
		InputValue::new("id", TypeRef::named_nn(TypeRef::ID))
	};
}

macro_rules! version_input {
	() => {
		InputValue::new("version", TypeRef::named(TypeRef::STRING))
	};
}

macro_rules! order {
	(asc, $field:expr) => {{
		let mut tmp = sql::Order::default();
		tmp.order = $field.into();
		tmp.direction = true;
		tmp
	}};
	(desc, $field:expr) => {{
		let mut tmp = sql::Order::default();
		tmp.order = $field.into();
		tmp
	}};
}

fn where_name_from_table(tb_name: impl Display) -> String {
	format!("_where_{tb_name}")
}

fn uppercase(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

pub async fn generate_schema(
	datastore: &Arc<Datastore>,
	session: &Session,
) -> Result<Schema, GqlError> {
	let kvs = datastore;
	let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let ns = session.ns.as_ref().ok_or(GqlError::UnspecifiedNamespace)?;
	let db = session.db.as_ref().ok_or(GqlError::UnspecifiedDatabase)?;

	let cg = tx.get_db_config(ns, db, "graphql").await.map_err(|e| match e {
		crate::err::Error::CgNotFound {
			..
		} => GqlError::NotConfigured,
		e => e.into(),
	})?;
	let config = cg.inner.clone().try_into_graphql()?;

	let tbs = tx.all_tb(ns, db, None).await?;

	let tbs = match config.tables {
		TablesConfig::None => return Err(GqlError::NotConfigured),
		TablesConfig::Auto => tbs,
		TablesConfig::Include(inc) => {
			tbs.iter().filter(|t| inc.contains_name(&t.name)).cloned().collect()
		}
		TablesConfig::Exclude(exc) => {
			tbs.iter().filter(|t| !exc.contains_name(&t.name)).cloned().collect()
		}
	};

	let mut query = Object::new("Query");
	let mut mutation = Object::new("Mutation");
	let mut types: Vec<Type> = Vec::new();

	let accesses = tx.all_db_accesses(ns, db).await?;
	let mut all_signin_vars = std::collections::HashSet::new();
	let mut all_signup_vars = std::collections::HashSet::new();

	// Add root authentication fields (only for signin, not signup)
	all_signin_vars.insert("username".to_string());
	all_signin_vars.insert("password".to_string());

	for access in accesses.iter() {
		if let crate::sql::AccessType::Record(record_access) = &access.kind {
			// Collect signin variables from SIGNIN clauses only
			if let Some(signin_clause) = &record_access.signin {
				let signin_vars = extract_signin_variables(signin_clause);
				for var in signin_vars {
					all_signin_vars.insert(var);
				}
			}

			// Collect signup variables from SIGNUP clauses only
			if let Some(signup_clause) = &record_access.signup {
				let signup_vars = extract_signin_variables(signup_clause);
				for var in signup_vars {
					all_signup_vars.insert(var);
				}
			}
		}
	}

	let mut signin_input = InputObject::new("SignInInput")
		.description("Input for authentication - contains all possible authentication parameters");

	for var_name in &all_signin_vars {
		signin_input = signin_input.field(
			InputValue::new(var_name, TypeRef::named(TypeRef::STRING))
				.description(format!("Authentication parameter: {}", var_name))
		);
	}
	types.push(Type::InputObject(signin_input));

	// Create SignUpInput only if there are signup variables from SIGNUP clauses (from ACCESSES)
	if !all_signup_vars.is_empty() {
		let mut signup_input = InputObject::new("SignUpInput")
			.description("Input for registration - contains all possible registration parameters");

		for var_name in &all_signup_vars {
			signup_input = signup_input.field(
				InputValue::new(var_name, TypeRef::named(TypeRef::STRING))
					.description(format!("Registration parameter: {}", var_name))
			);
		}
		types.push(Type::InputObject(signup_input));
	}

	trace!(ns, db, ?tbs, "generating schema");

	if tbs.len() == 0 {
		return Err(schema_error("no tables found in database"));
	}

	// Collect relation information for all tables
	let mut table_relations: HashMap<String, Vec<RelationInfo>> = HashMap::new();

	// First pass: collect all direct field relations and relation tables
	for tb in tbs.iter() {
		let tb_name = tb.name.to_string();
		let mut relations = Vec::new();

		trace!("Processing table: {} with type: {:?}", tb_name, tb.kind);
		println!("🔍 Processing table: {} with type: {:?}", tb_name, tb.kind);

		// Get all fields for this table
		let fds = tx.all_tb_fields(ns, db, &tb.name.0, None).await?;

		// Check for direct record field relations
		for fd in fds.iter() {
			if let Some(Kind::Record(target_tables)) = &fd.kind {
				if !target_tables.is_empty() && fd.name.to_string() != "id" {
					// For now, handle single target table (most common case)
					// TODO: Handle multiple target tables properly
					if target_tables.len() == 1 {
						relations.push(RelationInfo {
							field_name: fd.name.to_string(),
							target_table: target_tables[0].0.clone(),
							relation_type: RelationType::Direct,
							is_list: false, // Direct field references are typically single records
						});
					}
				}
			}
		}

		// Check if this table is a relation table
		if let TableType::Relation(rel) = &tb.kind {
			trace!("Found relation table: {} with from: {:?}, to: {:?}", tb_name, rel.from, rel.to);
			println!("🔗 Found relation table: {} with from: {:?}, to: {:?}", tb_name, rel.from, rel.to);
			// For relation tables, we'll add reverse relations to connected tables
			if let (Some(Kind::Record(from_tables)), Some(Kind::Record(to_tables))) = (&rel.from, &rel.to) {
				trace!("Processing relation table {} with from_tables: {:?}, to_tables: {:?}", tb_name, from_tables, to_tables);
				println!("📊 Processing relation table {} with from_tables: {:?}, to_tables: {:?}", tb_name, from_tables, to_tables);
				// Add outgoing relations from 'from' tables
				for from_table in from_tables {
					let from_table_name = from_table.0.clone();
					let relation_info = RelationInfo {
						field_name: tb_name.clone(),
						target_table: tb_name.clone(), // Return the relation table records
						relation_type: RelationType::OutgoingRelation { relation_table: tb_name.clone() },
						is_list: true, // Relation tables typically return lists
					};
					trace!("Adding outgoing relation to {}: {:?}", from_table_name, relation_info);
					println!("➡️  Adding outgoing relation to {}: {:?}", from_table_name, relation_info);
					table_relations.entry(from_table_name.clone()).or_default().push(relation_info);
				}

				// Add incoming relations to 'to' tables
				for to_table in to_tables {
					let to_table_name = to_table.0.clone();
					let relation_info = RelationInfo {
						field_name: format!("{}_in", tb_name),
						target_table: tb_name.clone(), // Return the relation table records
						relation_type: RelationType::IncomingRelation { relation_table: tb_name.clone() },
						is_list: true, // Relation tables typically return lists
					};
					trace!("Adding incoming relation to {}: {:?}", to_table_name, relation_info);
					println!("⬅️  Adding incoming relation to {}: {:?}", to_table_name, relation_info);
					table_relations.entry(to_table_name.clone()).or_default().push(relation_info);
				}
			} else {
				trace!("Relation table {} missing from/to table definitions", tb_name);
			}
		} else {
			trace!("Table {} is not a relation table, type: {:?}", tb_name, tb.kind);
		}

		trace!("Final relations for table {}: {:?}", tb_name, relations);
		println!("📋 Final relations for table {}: {:?}", tb_name, relations);
		table_relations.entry(tb_name.clone()).or_default().extend(relations);
	}

	trace!("All table relations collected: {:?}", table_relations);
	println!("🗂️  All table relations collected: {:?}", table_relations);

	for tb in tbs.iter() {
		trace!("Adding table: {}", tb.name);
		let tb_name = tb.name.to_string();
		let first_tb_name = tb_name.clone();
		let second_tb_name = tb_name.clone();

		let table_orderable_name = format!("_orderable_{tb_name}");
		let mut table_orderable = Enum::new(&table_orderable_name).item("id");
		table_orderable = table_orderable.description(format!(
			"Generated from `{}` the fields which a query can be ordered by",
			tb.name
		));
		let table_order_name = format!("_order_{tb_name}");
		let table_order = InputObject::new(&table_order_name)
			.description(format!(
				"Generated from `{}` an object representing a query ordering",
				tb.name
			))
			.field(InputValue::new("asc", TypeRef::named(&table_orderable_name)))
			.field(InputValue::new("desc", TypeRef::named(&table_orderable_name)))
			.field(InputValue::new("then", TypeRef::named(&table_order_name)));

		let table_where_name = where_name_from_table(&tb_name);
		let mut table_where = InputObject::new(&table_where_name);
		table_where = table_where
			.field(InputValue::new("id", TypeRef::named("_where_id")))
			.field(InputValue::new("and", TypeRef::named_nn_list(&table_where_name)))
			.field(InputValue::new("or", TypeRef::named_nn_list(&table_where_name)))
			.field(InputValue::new("not", TypeRef::named(&table_where_name)));
		types.push(Type::InputObject(where_id()));

		let sess1 = session.to_owned();
		let fds = tx.all_tb_fields(ns, db, &tb.name.0, None).await?;
		let fds1 = fds.clone();
		let kvs1 = datastore.clone();

		// Get relations for this table
		let table_relations_for_tb = table_relations.get(&tb_name).cloned().unwrap_or_default();
		trace!("Relations for table {}: {:?}", tb_name, table_relations_for_tb);
		println!("🎯 Relations for table {}: {:?}", tb_name, table_relations_for_tb);

		query = query.field(
			Field::new(
				tb.name.to_string(),
				TypeRef::named_nn_list_nn(tb.name.to_string()),
				move |ctx| {
					let tb_name = first_tb_name.clone();
					let sess1 = sess1.clone();
					let fds1 = fds1.clone();
					let kvs1 = kvs1.clone();
					FieldFuture::new(async move {
						let args = ctx.args.as_index_map();
						trace!("received request with args: {args:?}");

						let start = args.get("start").and_then(|v| v.as_i64()).map(|s| s.intox());

						let limit = args.get("limit").and_then(|v| v.as_i64()).map(|l| l.intox());

						let order = args.get("order");

						let where_clause = args.get("where");

						let version = args.get("version").and_then(|v| v.as_string()).and_then(|s| {
							s.parse::<crate::sql::datetime::Datetime>().ok().map(crate::sql::Version)
						});

						// Create version-aware GQLTx for field resolution
						let gtx = if let Some(ref v) = version {
							GQLTx::new_with_version(&kvs1, &sess1, Some(v.to_u64())).await?
						} else {
							GQLTx::new(&kvs1, &sess1).await?
						};

						let orders = match order {
							Some(GqlValue::Object(o)) => {
								let mut orders = vec![];
								let mut current = o;
								loop {
									let asc = current.get("asc");
									let desc = current.get("desc");
									match (asc, desc) {
										(Some(_), Some(_)) => {
											return Err("Found both asc and desc in order".into());
										}
										(Some(GqlValue::Enum(a)), None) => {
											orders.push(order!(asc, a.as_str()))
										}
										(None, Some(GqlValue::Enum(d))) => {
											orders.push(order!(desc, d.as_str()))
										}
										(_, _) => {
											break;
										}
									}
									if let Some(GqlValue::Object(next)) = current.get("then") {
										current = next;
									} else {
										break;
									}
								}
								Some(orders)
							}
							_ => None,
						};
						trace!("parsed orders: {orders:?}");

						let cond = match where_clause {
							Some(w) => {
								let o = match w {
									GqlValue::Object(o) => o,
									w => {
										error!("Found where clause {w}, which should be object and should have been rejected by async graphql.");
										return Err("Value in cond doesn't fit schema".into());
									}
								};

								let cond = cond_from_where(o, &fds1)?;

								Some(cond)
							}
							None => None,
						};

						trace!("parsed where clause: {cond:?}");

						// SELECT VALUE id FROM ...
						let ast = Statement::Select({
							SelectStatement {
								what: vec![SqlValue::Table(tb_name.intox())].into(),
								expr: Fields(
									vec![sql::Field::Single {
										expr: SqlValue::Idiom(Idiom::from("id")),
										alias: None,
									}],
									// this means the `value` keyword
									true,
								),
								order: orders.map(IntoExt::intox),
								cond,
								limit,
								start,
								version: version.clone(),
								..Default::default()
							}
						});

						let query = crate::sql::Query(crate::sql::Statements(vec![ast]));
						let mut results = kvs1.process(query, &sess1, None).await?;
						let res = if let Some(response) = results.pop() {
							response.result.map_err(|e| -> crate::gql::error::GqlError { e.into() })?
						} else {
							return Err("No response from query".into());
						};

						let res_vec =
							match res {
								SqlValue::Array(a) => a,
								v => {
									error!("Found top level value, in result which should be array: {v:?}");
									return Err("Internal Error".into());
								}
							};

						let out: Result<Vec<FieldValue>, SqlValue> = res_vec
							.0
							.into_iter()
							.map(|v| {
								v.try_as_thing().map(|t| {
									let erased: ErasedRecord = (gtx.clone(), t);
									field_val_erase_owned(erased)
								})
							})
							.collect();

						match out {
							Ok(l) => Ok(Some(FieldValue::list(l))),
							Err(v) => {
								Err(internal_error(format!("expected thing, found: {v:?}")).into())
							}
						}
					})
				},
			)
			.description(format!(
				"{}. Table: `{}`",
				if let Some(ref c) = &tb.comment {
					let comment = c.trim_matches('\'');
					comment.to_string()
				} else {
					"".to_string()
				},
				tb.name
			))
			.argument(limit_input!())
			.argument(start_input!())
			.argument(InputValue::new("order", TypeRef::named(&table_order_name)))
			.argument(InputValue::new("where", TypeRef::named(&table_where_name)))
			.argument(version_input!()),
		);

		let create_input_name = format!("Create{}Input", uppercase(&tb.name));
		let update_input_name = format!("Update{}Input", uppercase(&tb.name));

		let create_tb_name = tb.name.to_string();
		let create_sess = session.to_owned();
		let create_fds = fds.clone();
		let create_kvs = datastore.clone();
		let create_input_name_ref = create_input_name.clone();

		mutation = mutation.field(
			Field::new(
				format!("create{}", uppercase(&tb.name)),
				TypeRef::named(tb.name.to_string()),
				move |ctx| {
					let tb_name = create_tb_name.clone();
					let sess = create_sess.clone();
					let fds = create_fds.clone();
					let kvs = create_kvs.clone();

					FieldFuture::new(async move {
						let args = ctx.args.as_index_map();
						let input = args.get("data").ok_or_else(|| resolver_error("data argument is required"))?;

						let version = args.get("version").and_then(|v| v.as_string()).and_then(|s| {
							s.parse::<crate::sql::datetime::Datetime>().ok().map(crate::sql::Version)
						});

						let mut data_fields = Vec::new();
						let mut record_id = None;

						if let GqlValue::Object(obj) = input {
							for (key, value) in obj {
								// Handle id field specially for versioned record creation
								if key.as_str() == "id" {
									if let Some(id_str) = value.as_string() {
										let thing = match id_str.clone().try_into() {
											Ok(t) => t,
											Err(_) => Thing::from((tb_name.clone(), id_str)),
										};
										record_id = Some(thing);
									}
									continue; // Skip adding id to data fields
								}

								let field_def = fds.iter().find(|fd| fd.name.to_string() == key.as_str());

								if let Some(fd) = field_def {
									if let Some(ref kind) = fd.kind {
										let sql_value = gql_to_sql_kind(value, kind.clone())?;
										data_fields.push((
											Idiom(vec![Part::Field(Ident(key.to_string()))]),
											Operator::Equal,
											sql_value
										));
									}
								}
							}
						} else {
							return Err(resolver_error("data must be an object").into());
						}

						let what_value = if let Some(thing) = record_id {
							// If ID is provided in data, create a specific record
							SqlValue::Thing(thing)
						} else {
							// If no ID provided, create from table
							SqlValue::Table(tb_name.intox())
						};

						let ast = Statement::Create(CreateStatement {
							only: true,
							what: Values(vec![what_value]),
							data: Some(Data::SetExpression(data_fields)),
							output: Some(crate::sql::Output::After),
							timeout: None,
							parallel: false,
							version,
						});

						let res = GQLTx::execute_mutation(&kvs, &sess, ast).await?;

						// Create a read-only GQLTx for field resolution
						let gtx = GQLTx::new(&kvs, &sess).await?;

						match res {
							SqlValue::Thing(t) => {
								let erased: ErasedRecord = (gtx, t);
								Ok(Some(field_val_erase_owned(erased)))
							}
							SqlValue::Object(ref obj) => {
								// Extract the ID from the object (CREATE with OUTPUT AFTER returns full record)
								if let Some(SqlValue::Thing(t)) = obj.get("id") {
									let erased: ErasedRecord = (gtx, t.clone());
									Ok(Some(field_val_erase_owned(erased)))
								} else {
									Ok(None)
								}
							}
							SqlValue::Array(mut a) if a.len() == 1 => {
								match a.0.pop() {
									Some(SqlValue::Thing(t)) => {
										let erased: ErasedRecord = (gtx, t);
										Ok(Some(field_val_erase_owned(erased)))
									}
									Some(SqlValue::Object(ref obj)) => {
										// Handle array containing object
										if let Some(SqlValue::Thing(t)) = obj.get("id") {
											let erased: ErasedRecord = (gtx, t.clone());
											Ok(Some(field_val_erase_owned(erased)))
										} else {
											Ok(None)
										}
									}
									_ => Ok(None)
								}
							}
							_ => Ok(None),
						}
					})
				},
			)
			.description(format!("Create a new {} record", tb.name))
			.argument(InputValue::new("data", TypeRef::named_nn(&create_input_name_ref)))
			.argument(version_input!()),
		);

		let update_tb_name = tb.name.to_string();
		let update_sess = session.to_owned();
		let update_fds = fds.clone();
		let update_kvs = datastore.clone();
		let update_input_name_ref = update_input_name.clone();

		mutation = mutation.field(
			Field::new(
				format!("update{}", uppercase(&tb.name)),
				TypeRef::named(tb.name.to_string()),
				move |ctx| {
					let tb_name = update_tb_name.clone();
					let sess = update_sess.clone();
					let fds = update_fds.clone();
					let kvs = update_kvs.clone();

					FieldFuture::new(async move {
						let args = ctx.args.as_index_map();
						let id = args.get("id").and_then(GqlValueUtils::as_string)
							.ok_or_else(|| resolver_error("id argument is required"))?;
						let input = args.get("data").ok_or_else(|| resolver_error("data argument is required"))?;

						let version = args.get("version").and_then(|v| v.as_string()).and_then(|s| {
							s.parse::<crate::sql::datetime::Datetime>().ok().map(crate::sql::Version)
						});

						let thing = match id.clone().try_into() {
							Ok(t) => t,
							Err(_) => Thing::from((tb_name.clone(), id)),
						};

						let mut data_fields = Vec::new();

						if let GqlValue::Object(obj) = input {
							for (key, value) in obj {
								// Find the field definition to get its kind
								let field_def = fds.iter().find(|fd| fd.name.to_string() == key.as_str());

								if let Some(fd) = field_def {
									if let Some(ref kind) = fd.kind {
										let sql_value = gql_to_sql_kind(value, kind.clone())?;
										data_fields.push((
											Idiom(vec![Part::Field(Ident(key.to_string()))]),
											Operator::Equal,
											sql_value
										));
									}
								}
							}
						} else {
							return Err(resolver_error("data must be an object").into());
						}

						let ast = Statement::Update(UpdateStatement {
							only: true,
							what: Values(vec![SqlValue::Thing(thing)]),
							data: Some(Data::SetExpression(data_fields)),
							cond: None,
							output: Some(crate::sql::Output::After),
							timeout: None,
							parallel: false,
						});

						let res = GQLTx::execute_mutation(&kvs, &sess, ast).await?;

						// Create a read-only GQLTx for field resolution
						let gtx = GQLTx::new(&kvs, &sess).await?;

						match res {
							SqlValue::Thing(t) => {
								let erased: ErasedRecord = (gtx, t);
								Ok(Some(field_val_erase_owned(erased)))
							}
							SqlValue::Object(ref obj) => {
								// Extract the ID from the object (UPDATE with OUTPUT AFTER returns full record)
								if let Some(SqlValue::Thing(t)) = obj.get("id") {
									let erased: ErasedRecord = (gtx, t.clone());
									Ok(Some(field_val_erase_owned(erased)))
								} else {
									Ok(None)
								}
							}
							SqlValue::Array(mut a) if a.len() == 1 => {
								match a.0.pop() {
									Some(SqlValue::Thing(t)) => {
										let erased: ErasedRecord = (gtx, t);
										Ok(Some(field_val_erase_owned(erased)))
									}
									Some(SqlValue::Object(ref obj)) => {
										// Handle array containing object
										if let Some(SqlValue::Thing(t)) = obj.get("id") {
											let erased: ErasedRecord = (gtx, t.clone());
											Ok(Some(field_val_erase_owned(erased)))
										} else {
											Ok(None)
										}
									}
									_ => Ok(None)
								}
							}
							_ => Ok(None),
						}
					})
				},
			)
			.description(format!("Update a {} record by ID", tb.name))
			.argument(id_input!())
			.argument(InputValue::new("data", TypeRef::named_nn(&update_input_name_ref)))
			.argument(version_input!()),
		);

		let delete_tb_name = tb.name.to_string();
		let delete_sess = session.to_owned();
		let delete_kvs = datastore.clone();

		mutation = mutation.field(
			Field::new(
				format!("delete{}", uppercase(&tb.name)),
				TypeRef::named_nn(TypeRef::BOOLEAN),
				move |ctx| {
					let tb_name = delete_tb_name.clone();
					let sess = delete_sess.clone();
					let kvs = delete_kvs.clone();

					FieldFuture::new(async move {
						let args = ctx.args.as_index_map();
						let id = args.get("id").and_then(GqlValueUtils::as_string)
							.ok_or_else(|| resolver_error("id argument is required"))?;

						let version = args.get("version").and_then(|v| v.as_string()).and_then(|s| {
							s.parse::<crate::sql::datetime::Datetime>().ok().map(crate::sql::Version)
						});

						let thing = match id.clone().try_into() {
							Ok(t) => t,
							Err(_) => Thing::from((tb_name, id)),
						};

						let ast = Statement::Delete(DeleteStatement {
							only: true,
							what: Values(vec![SqlValue::Thing(thing)]),
							cond: None,
							output: Some(crate::sql::Output::Before),
							timeout: None,
							parallel: false,
						});

						let res = GQLTx::execute_mutation(&kvs, &sess, ast).await?;

						// Return true if something was deleted, false otherwise
						match res {
							SqlValue::None => Ok(Some(FieldValue::value(GqlValue::Boolean(false)))),
							SqlValue::Thing(_) => Ok(Some(FieldValue::value(GqlValue::Boolean(true)))),
							SqlValue::Array(a) => Ok(Some(FieldValue::value(GqlValue::Boolean(!a.is_empty())))),
							_ => Ok(Some(FieldValue::value(GqlValue::Boolean(true)))),
						}
					})
				},
			)
			.description(format!("Delete a {} record by ID", tb.name))
			.argument(id_input!())
			.argument(version_input!()),
		);

		let sess2 = session.to_owned();
		let kvs2 = datastore.to_owned();
		query = query.field(
			Field::new(
				format!("_get_{}", tb.name),
				TypeRef::named(tb.name.to_string()),
				move |ctx| {
					let tb_name = second_tb_name.clone();
					let kvs2 = kvs2.clone();
					FieldFuture::new({
						let sess2 = sess2.clone();
						async move {
							let args = ctx.args.as_index_map();

							let version = args.get("version").and_then(|v| v.as_string()).and_then(|s| {
								s.parse::<crate::sql::datetime::Datetime>().ok().map(crate::sql::Version)
							});

							// Create version-aware GQLTx for field resolution in _get_ queries
							let gtx = if let Some(ref v) = version {
								GQLTx::new_with_version(&kvs2, &sess2, Some(v.to_u64())).await?
							} else {
								GQLTx::new(&kvs2, &sess2).await?
							};

							let id = match args.get("id").and_then(GqlValueUtils::as_string) {
								Some(i) => i,
								None => {
									return Err(internal_error(
										"Schema validation failed: No id found in _get_",
									)
									.into());
								}
							};
							let thing = match id.clone().try_into() {
								Ok(t) => t,
								Err(_) => Thing::from((tb_name, id)),
							};

							// Create a SELECT statement for the specific record with version
							let ast = Statement::Select(SelectStatement {
								what: vec![SqlValue::Thing(thing.clone())].into(),
								expr: Fields(
									vec![sql::Field::All],
									false,
								),
								version: version.clone(),
								..Default::default()
							});

							// Use kvs.process() for proper version handling
							let query = crate::sql::Query(crate::sql::Statements(vec![ast]));
							let mut results = kvs2.process(query, &sess2, None).await?;
							let res = if let Some(response) = results.pop() {
								response.result.map_err(|e| -> crate::gql::error::GqlError { e.into() })?
							} else {
								return Err("No response from _get_ query".into());
							};

							match res {
								SqlValue::Array(mut a) if a.len() == 1 => {
									match a.0.pop() {
										Some(SqlValue::Object(obj)) => {
											if let Some(SqlValue::Thing(t)) = obj.get("id") {
												let erased: ErasedRecord = (gtx, t.clone());
												Ok(Some(field_val_erase_owned(erased)))
											} else {
												Ok(None)
											}
										}
										Some(SqlValue::Thing(t)) => {
											let erased: ErasedRecord = (gtx, t);
											Ok(Some(field_val_erase_owned(erased)))
										}
										_ => Ok(None)
									}
								}
								_ => Ok(None)
							}
						}
					})
				},
			)
			.description(format!(
				"{}. Table: `{}`",
				if let Some(ref c) = &tb.comment {
					let comment = c.trim_matches('\'');
					comment.to_string()
				} else {
					"".to_string()
				},
				tb.name
			))
			.argument(id_input!())
			.argument(version_input!()),
		);

		let mut table_ty_obj = Object::new(tb.name.to_string())
			.field(Field::new(
				"id",
				TypeRef::named_nn(TypeRef::ID),
				make_table_field_resolver(
					"id",
					Some(Kind::Record(vec![Table::from(tb.name.to_string())])),
				),
			))
			.implement("record");

		for fd in fds.iter() {
			let Some(ref kind) = fd.kind else {
				continue;
			};
			let fd_name = Name::new(fd.name.to_string());
			let fd_type = kind_to_type(kind.clone(), &mut types)?;
			table_orderable = table_orderable.item(fd_name.to_string());
			let type_where_name = format!("_where_{}", unwrap_type(fd_type.clone()));

			let type_where = Type::InputObject(where_from_type(
				kind.clone(),
				type_where_name.clone(),
				&mut types,
			)?);
			trace!("\n{type_where:?}\n");
			types.push(type_where);

			table_where = table_where
				.field(InputValue::new(fd.name.to_string(), TypeRef::named(type_where_name)));

			table_ty_obj = table_ty_obj.field(Field::new(
				fd.name.to_string(),
				fd_type,
				make_table_field_resolver(fd_name.as_str(), fd.kind.clone()),
			));
		}

		// Add relation fields to the table object (excluding direct field relations which are already handled)
		for relation in table_relations_for_tb.iter() {
			// Skip direct field relations as they're already added by the regular field processing loop
			if matches!(relation.relation_type, RelationType::Direct) {
				trace!("Skipping direct relation field: {}", relation.field_name);
				continue;
			}

			trace!("Adding relation field {} to table {}", relation.field_name, tb_name);
			println!("✅ Adding relation field {} to table {}", relation.field_name, tb_name);

			let field_type = if relation.is_list {
				TypeRef::named_nn_list(relation.target_table.clone())
			} else {
				TypeRef::named(relation.target_table.clone())
			};

			let relation_clone = relation.clone();
			let kvs_clone = datastore.clone();
			let sess_clone = session.clone();
			let current_table = tb_name.clone();

			table_ty_obj = table_ty_obj.field(
				Field::new(
					&relation.field_name,
					field_type,
					move |ctx| {
						let relation = relation_clone.clone();
						let kvs = kvs_clone.clone();
						let sess = sess_clone.clone();
						let table_name = current_table.clone();

						FieldFuture::new(async move {
							make_relation_field_resolver(
								ctx,
								relation,
								kvs,
								sess,
								table_name,
							).await
						})
					},
				)
				.description(format!("Relation to {}", relation.target_table))
				.argument(limit_input!())
				.argument(start_input!())
				.argument(InputValue::new("where", TypeRef::named(where_name_from_table(&relation.target_table))))
				.argument(InputValue::new("order", TypeRef::named(format!("_order_{}", relation.target_table))))
				.argument(version_input!()),
			);
		}

		types.push(Type::Object(table_ty_obj));
		types.push(table_order.into());
		types.push(Type::Enum(table_orderable));
		types.push(Type::InputObject(table_where));

		let mut create_input = InputObject::new(&create_input_name)
			.description(format!("Input type for creating a new {} record", tb.name));

		let mut update_input = InputObject::new(&update_input_name)
			.description(format!("Input type for updating a {} record", tb.name));

		// Explicitly add optional id field to create input
		create_input = create_input.field(InputValue::new("id", TypeRef::named(TypeRef::ID)));

		for fd in fds.iter() {
			let Some(ref kind) = fd.kind else {
				continue;
			};

			// Skip ID field since we explicitly added it above
			if fd.name.to_string() == "id" {
				continue;
			}

			let fd_type = kind_to_input_type(kind.clone(), &mut types)?;
			create_input = create_input.field(InputValue::new(fd.name.to_string(), fd_type.clone()));
			// Make all fields optional for update
			update_input = update_input.field(InputValue::new(fd.name.to_string(), unwrap_type(fd_type)));
		}

		types.push(Type::InputObject(create_input));
		types.push(Type::InputObject(update_input));
	}

	let sess3 = session.to_owned();
	let kvs3 = datastore.to_owned();
	query = query.field(
		Field::new("_get", TypeRef::named("record"), move |ctx| {
			FieldFuture::new({
				let sess3 = sess3.clone();
				let kvs3 = kvs3.clone();
				async move {
					let gtx = GQLTx::new(&kvs3, &sess3).await?;

					let args = ctx.args.as_index_map();
					let id = match args.get("id").and_then(GqlValueUtils::as_string) {
						Some(i) => i,
						None => {
							return Err(internal_error(
								"Schema validation failed: No id found in _get",
							)
							.into());
						}
					};

					let thing: Thing = match id.clone().try_into() {
						Ok(t) => t,
						Err(_) => return Err(resolver_error(format!("invalid id: {id}")).into()),
					};

					match gtx.get_record_field(thing, "id").await? {
						SqlValue::Thing(t) => {
							let ty = t.tb.to_string();
							let out = field_val_erase_owned((gtx, t)).with_type(ty);
							Ok(Some(out))
						}
						_ => Ok(None),
					}
				}
			})
		})
		.description("allows fetching arbitrary records".to_string())
		.argument(id_input!()),
	);

	// Add _param_{PARAM} fields for DEFINE PARAM statements
	let params = tx.all_db_params(ns, db).await?;
	for param in params.iter() {
		let param_name = param.name.to_string();
		let field_name = format!("_param_{}", param_name);
		let param_value = param.value.clone();
		let datastore_clone = datastore.clone();
		let session_clone = session.clone();

		query = query.field(
			Field::new(&field_name, TypeRef::named("String"), move |_ctx| {
				let param_val = param_value.clone();
				let kvs = datastore_clone.clone();
				let sess = session_clone.clone();
				FieldFuture::new(async move {
					// Create a minimal context to evaluate the param value, todo: Possibly refactor without TreeStack
					let gtx = GQLTx::new(&kvs, &sess).await?;
					let mut stack = TreeStack::new();
					let computed_value = stack.enter(|stk| param_val.compute(stk, gtx.get_context(), gtx.get_options(), None)).finish().await?;
					let gql_value = sql_value_to_gql_value(computed_value)?;
					Ok(Some(FieldValue::value(gql_value)))
				})
			})
			.description(format!("Global parameter: {}", param_name))
		);
	}

	// Create list of valid access names for validation
	let valid_access_names: Vec<String> = accesses.iter().map(|access| access.name.0.clone()).collect();

	// Create description with available access methods
	let access_description = if valid_access_names.is_empty() {
		"Access method name (Optional for root authentication)".to_string()
	} else {
		format!("Access method name (Optional for root authentication). Available access methods: {}", valid_access_names.join(", "))
	};

	let has_signup_access = !all_signup_vars.is_empty();

	let signin_kvs = datastore.clone();
	let signin_session = session.clone();
	let signin_valid_accesses = valid_access_names.clone();
	mutation = mutation.field(
		Field::new("signIn", TypeRef::named_nn(TypeRef::STRING), move |ctx| {
			let kvs = signin_kvs.clone();
			let session = signin_session.clone();
			let valid_accesses = signin_valid_accesses.clone();

			FieldFuture::new(async move {
				let args = ctx.args.as_index_map();
				let access = args.get("access").and_then(|v| v.as_string());
				let variables = args.get("variables").ok_or_else(|| resolver_error("variables argument is required"))?;

				// Validate access name if provided
				if let Some(access_name) = &access {
					if !valid_accesses.contains(access_name) {
						return Err(resolver_error(format!("Invalid access method: '{}'. Valid access methods are: {}", access_name, valid_accesses.join(", "))).into());
					}
				}

				if let GqlValue::Object(obj) = variables {
					// Convert GraphQL input to SQL Object for signin
					let mut vars = SqlObject::default();

					// Add NS and DB from GraphQL context
					if let Some(ns) = &session.ns {
						vars.insert("NS".to_string(), SqlValue::from(ns.to_string()));
					}
					if let Some(db) = &session.db {
						vars.insert("DB".to_string(), SqlValue::from(db.to_string()));
					}

					// Add access method if specified
					if let Some(access_name) = access {
						vars.insert("AC".to_string(), SqlValue::from(access_name));
					}

					// Add all variable fields to vars
					for (key, value) in obj {
						let sql_val = match value {
							GqlValue::String(s) => SqlValue::from(s.clone()),
							GqlValue::Number(n) => {
								if let Some(i) = n.as_i64() {
									SqlValue::from(i)
								} else if let Some(f) = n.as_f64() {
									SqlValue::from(f)
								} else {
									SqlValue::from(n.to_string())
								}
							},
							GqlValue::Boolean(b) => SqlValue::from(*b),
							GqlValue::Null => SqlValue::Null,
							_ => SqlValue::from(value.to_string()),
						};
						vars.insert(key.to_string(), sql_val);
					}

					// Call signin function
					let mut signin_session = session.clone();
					match iam::signin::signin(&kvs, &mut signin_session, vars).await {
						Ok(token) => Ok(Some(FieldValue::value(GqlValue::String(token)))),
						Err(e) => Err(resolver_error(format!("Authentication failed: {}", e)).into()),
					}
				} else {
					Err(resolver_error("variables must be an object").into())
				}
			})
		})
		.description("Sign in and receive a JWT token")
		.argument(InputValue::new("access", TypeRef::named(TypeRef::STRING)).description(&access_description))
		.argument(InputValue::new("variables", TypeRef::named_nn("SignInInput")).description("Authentication variables"))
	);

	if has_signup_access {
		let signup_kvs = datastore.clone();
		let signup_session = session.clone();
		let signup_valid_accesses = valid_access_names.clone();
		mutation = mutation.field(
			Field::new("signUp", TypeRef::named_nn(TypeRef::STRING), move |ctx| {
				let kvs = signup_kvs.clone();
				let session = signup_session.clone();
				let valid_accesses = signup_valid_accesses.clone();

				FieldFuture::new(async move {
					let args = ctx.args.as_index_map();
					let access = args.get("access").and_then(|v| v.as_string()).ok_or_else(|| resolver_error("access argument is required"))?;
					let variables = args.get("variables").ok_or_else(|| resolver_error("variables argument is required"))?;

					// Validate access name (required)
					if !valid_accesses.contains(&access) {
						return Err(resolver_error(format!("Invalid access method: '{}'. Valid access methods are: {}", access, valid_accesses.join(", "))).into());
					}

					if let GqlValue::Object(obj) = variables {
						// Convert GraphQL input to SQL Object for signup
						let mut vars = SqlObject::default();

						// Add NS and DB from GraphQL context
						if let Some(ns) = &session.ns {
							vars.insert("NS".to_string(), SqlValue::from(ns.to_string()));
						}
						if let Some(db) = &session.db {
							vars.insert("DB".to_string(), SqlValue::from(db.to_string()));
						}

						// Add access method (required)
						vars.insert("AC".to_string(), SqlValue::from(access));

						// Add all variable fields to vars
						for (key, value) in obj {
							let sql_val = match value {
								GqlValue::String(s) => SqlValue::from(s.clone()),
								GqlValue::Number(n) => {
									if let Some(i) = n.as_i64() {
										SqlValue::from(i)
									} else if let Some(f) = n.as_f64() {
										SqlValue::from(f)
									} else {
										SqlValue::from(n.to_string())
									}
								},
								GqlValue::Boolean(b) => SqlValue::from(*b),
								GqlValue::Null => SqlValue::Null,
								_ => SqlValue::from(value.to_string()),
							};
							vars.insert(key.to_string(), sql_val);
						}

						// Call signup function
						let mut signup_session = session.clone();
						match iam::signup::signup(&kvs, &mut signup_session, vars).await {
							Ok(Some(token)) => Ok(Some(FieldValue::value(GqlValue::String(token)))),
							Ok(None) => Err(resolver_error("Registration completed but no token returned").into()),
							Err(e) => Err(resolver_error(format!("Registration failed: {}", e)).into()),
						}
					} else {
						Err(resolver_error("variables must be an object").into())
					}
				})
			})
			.description("Sign up and receive a JWT token")
			.argument(InputValue::new("access", TypeRef::named_nn(TypeRef::STRING)).description("Access method name (required for registration)"))
			.argument(InputValue::new("variables", TypeRef::named_nn("SignUpInput")).description("Registration variables"))
		);
	}

	trace!("current Query object for schema: {:?}", query);

	let mut schema = Schema::build("Query", Some("Mutation"), None).register(query).register(mutation);
	for ty in types {
		trace!("adding type: {ty:?}");
		schema = schema.register(ty);
	}

	macro_rules! scalar_debug_validated {
		($schema:ident, $name:expr, $kind:expr) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				::std::option::Option::<&str>::None,
				::std::option::Option::<&str>::None
			)
		};
		($schema:ident, $name:expr, $kind:expr, $desc:literal) => {
			scalar_debug_validated!($schema, $name, $kind, std::option::Option::Some($desc), None)
		};
		($schema:ident, $name:expr, $kind:expr, $desc:literal, $url:literal) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				std::option::Option::Some($desc),
				Some($url)
			)
		};
		($schema:ident, $name:expr, $kind:expr, $desc:expr, $url:expr) => {{
			let new_type = Type::Scalar({
				let mut tmp = Scalar::new($name);
				if let Some(desc) = $desc {
					tmp = tmp.description(desc);
				}
				if let Some(url) = $url {
					tmp = tmp.specified_by_url(url);
				}
				#[cfg(debug_assertions)]
				tmp.add_validator(|v| gql_to_sql_kind(v, $kind).is_ok());
				tmp
			});
			$schema = $schema.register(new_type);
		}};
	}

	scalar_debug_validated!(
		schema,
		"uuid",
		Kind::Uuid,
		"a string encoded uuid",
		"https://datatracker.ietf.org/doc/html/rfc4122"
	);

	scalar_debug_validated!(schema, "Decimal", Kind::Decimal);
	scalar_debug_validated!(schema, "Number", Kind::Number);
	scalar_debug_validated!(schema, "Null", Kind::Null);
	scalar_debug_validated!(schema, "Datetime", Kind::Datetime);
	scalar_debug_validated!(schema, "Duration", Kind::Duration);
	scalar_debug_validated!(schema, "Object", Kind::Object);
	scalar_debug_validated!(schema, "Any", Kind::Any);

	scalar_debug_validated!(schema, "geometry", Kind::Geometry(vec![]));
	scalar_debug_validated!(schema, "point", Kind::Geometry(vec!["point".to_string()]));
	scalar_debug_validated!(schema, "line", Kind::Geometry(vec!["line".to_string()]));
	scalar_debug_validated!(schema, "polygon", Kind::Geometry(vec!["polygon".to_string()]));
	scalar_debug_validated!(schema, "multipoint", Kind::Geometry(vec!["multipoint".to_string()]));
	scalar_debug_validated!(schema, "multiline", Kind::Geometry(vec!["multiline".to_string()]));
	scalar_debug_validated!(schema, "multipolygon", Kind::Geometry(vec!["multipolygon".to_string()]));
	scalar_debug_validated!(schema, "collection", Kind::Geometry(vec!["collection".to_string()]));

	let id_interface =
		Interface::new("record").field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)));
	schema = schema.register(id_interface);

	// TODO: when used get: `Result::unwrap()` on an `Err` value: SchemaError("Field \"like.in\" is not sub-type of \"relation.in\"")
	let relation_interface = Interface::new("relation")
		.field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)))
		.field(InterfaceField::new("in", TypeRef::named_nn("record")))
		.field(InterfaceField::new("out", TypeRef::named_nn("record")))
		.implement("record");
	schema = schema.register(relation_interface);

	schema
		.finish()
		.map_err(|e| schema_error(format!("there was an error generating schema: {e:?}")))
}

// Helper function to extract variable names from SIGNIN clause
fn extract_signin_variables(signin_value: &SqlValue) -> Vec<String> {
	let mut variables = HashSet::new();
	extract_variables_recursive(signin_value, &mut variables);

	// Filter out system parameters that are automatically provided
	let filtered_vars: Vec<String> = variables
		.into_iter()
		.filter(|var| {
			// Exclude system parameters that are automatically set by the GraphQL context
			!matches!(var.to_uppercase().as_str(), "NS" | "DB" | "AC")
		})
		.collect();

	let mut sorted_vars = filtered_vars;
	sorted_vars.sort();
	sorted_vars
}

// Recursively extract parameter variables from SQL Value
fn extract_variables_recursive(value: &SqlValue, variables: &mut HashSet<String>) {
	match value {
		SqlValue::Param(param) => {
			variables.insert(param.0.to_string());
		}
		SqlValue::Expression(expr) => {
			match &**expr {
				crate::sql::Expression::Binary { l, r, .. } => {
					extract_variables_recursive(l, variables);
					extract_variables_recursive(r, variables);
				}
				crate::sql::Expression::Unary { v, .. } => {
					extract_variables_recursive(v, variables);
				}
			}
		}
		SqlValue::Function(func) => {
			for arg in func.args() {
				extract_variables_recursive(arg, variables);
			}
		}
		SqlValue::Subquery(subquery) => {
			match &**subquery {
				crate::sql::Subquery::Select(select) => {
					if let Some(cond) = &select.cond {
						extract_variables_from_cond(cond, variables);
					}
					// Also check fields and other parts of SELECT
					for field in &select.expr.0 {
						extract_variables_from_field(field, variables);
					}
				}
				crate::sql::Subquery::Create(create) => {
					// Handle CREATE statements (common in SIGNUP clauses)
					if let Some(data) = &create.data {
						extract_variables_from_data(data, variables);
					}
				}
				crate::sql::Subquery::Update(update) => {
					// Handle UPDATE statements
					if let Some(data) = &update.data {
						extract_variables_from_data(data, variables);
					}
					if let Some(cond) = &update.cond {
						extract_variables_from_cond(cond, variables);
					}
				}
				_ => {}
			}
		}
		SqlValue::Array(array) => {
			for item in &array.0 {
				extract_variables_recursive(item, variables);
			}
		}
		SqlValue::Object(object) => {
			for (_, val) in &object.0 {
				extract_variables_recursive(val, variables);
			}
		}
		_ => {}
	}
}

// Extract variables from condition expressions
fn extract_variables_from_cond(cond: &Cond, variables: &mut HashSet<String>) {
	extract_variables_recursive(&cond.0, variables);
}

// Extract variables from field expressions
fn extract_variables_from_field(field: &crate::sql::Field, variables: &mut HashSet<String>) {
	match field {
		crate::sql::Field::Single { expr, .. } => {
			extract_variables_recursive(expr, variables);
		}
		crate::sql::Field::All => {}
	}
}

// Extract variables from data expressions (SET clauses in CREATE/UPDATE)
fn extract_variables_from_data(data: &crate::sql::Data, variables: &mut HashSet<String>) {
	match data {
		crate::sql::Data::SetExpression(obj) => {
			for (_, _, val) in obj {
				extract_variables_recursive(val, variables);
			}
		}
		crate::sql::Data::UpdateExpression(ops) => {
			for (_, _, val) in ops {
				extract_variables_recursive(val, variables);
			}
		}
		crate::sql::Data::PatchExpression(val) => {
			extract_variables_recursive(val, variables);
		}
		crate::sql::Data::MergeExpression(val) => {
			extract_variables_recursive(val, variables);
		}
		crate::sql::Data::ReplaceExpression(val) => {
			extract_variables_recursive(val, variables);
		}
		crate::sql::Data::ContentExpression(val) => {
			extract_variables_recursive(val, variables);
		}
		crate::sql::Data::SingleExpression(val) => {
			extract_variables_recursive(val, variables);
		}
		crate::sql::Data::ValuesExpression(vals) => {
			for row in vals {
				for (_, val) in row {
					extract_variables_recursive(val, variables);
				}
			}
		}
		crate::sql::Data::EmptyExpression => {}
		crate::sql::Data::UnsetExpression(_) => {}
	}
}

fn make_table_field_resolver(
	fd_name: impl Into<String>,
	kind: Option<Kind>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		let field_kind = kind.clone();
		FieldFuture::new({
			async move {
				let (ref gtx, ref rid) = ctx
					.parent_value
					.downcast_ref::<ErasedRecord>()
					.ok_or_else(|| internal_error("failed to downcast"))?;

				// Use SELECT statement with version for versioned queries, get_record_field for non-versioned
				let val = if let Some(version_nanos) = gtx.get_version() {
					// Use SELECT statement with version for proper version handling
					let version = {
						let dt = chrono::DateTime::from_timestamp_nanos(version_nanos as i64);
						Some(crate::sql::Version(crate::sql::datetime::Datetime::from(dt)))
					};

					let ast = Statement::Select(SelectStatement {
						what: vec![SqlValue::Thing(rid.clone())].into(),
						expr: Fields(
							vec![sql::Field::Single {
								expr: SqlValue::Idiom(Idiom::from(fd_name.as_str())),
								alias: None,
							}],
							true, // VALUE keyword
						),
						version,
						..Default::default()
					});

					gtx.process_stmt(ast).await?
				} else {
					// Use get_record_field for non-versioned queries (better type handling)
					gtx.get_record_field(rid.clone(), fd_name.as_str()).await?
				};

				let out = match val {
					SqlValue::Thing(rid) if fd_name != "id" => {
						let mut tmp = field_val_erase_owned((gtx.clone(), rid.clone()));
						match field_kind {
							Some(Kind::Record(ts)) if ts.len() != 1 => {
								tmp = tmp.with_type(rid.tb.clone())
							}
							_ => {}
						}
						Ok(Some(tmp))
					}
					SqlValue::None | SqlValue::Null => Ok(None),
					v => {
						match field_kind {
							Some(Kind::Either(ks)) if ks.len() != 1 => {}
							_ => {}
						}
						let out = sql_value_to_gql_value(v.to_owned())
							.map_err(|_| "SQL to GQL translation failed")?;
						Ok(Some(FieldValue::value(out)))
					}
				};
				out
			}
		})
	}
}

async fn make_relation_field_resolver(
	ctx: ResolverContext<'_>,
	relation: RelationInfo,
	kvs: Arc<Datastore>,
	sess: Session,
	_current_table: String,
) -> Result<Option<FieldValue>, async_graphql::Error> {
	let (ref gtx, ref rid) = ctx
		.parent_value
		.downcast_ref::<ErasedRecord>()
		.ok_or_else(|| internal_error("failed to downcast"))?;

	let args = ctx.args.as_index_map();
	let start = args.get("start").and_then(|v| v.as_i64()).map(|s| s.intox());
	let limit = args.get("limit").and_then(|v| v.as_i64()).map(|l| l.intox());
	let _where_clause = args.get("where");
	let _order = args.get("order");
	let version = args.get("version").and_then(|v| v.as_string()).and_then(|s| {
		s.parse::<crate::sql::datetime::Datetime>().ok().map(crate::sql::Version)
	});

	// Build the appropriate query based on relation type
	let query_stmt = match &relation.relation_type {
		RelationType::Direct => {
			// Direct field reference - get the field value and resolve it as a record
			let field_value = gtx.get_record_field(rid.clone(), relation.field_name.as_str()).await?;

			match field_value {
				SqlValue::Thing(thing) => {
					// Single record reference
					let gtx_clone = if let Some(ref v) = version {
						GQLTx::new_with_version(&kvs, &sess, Some(v.to_u64())).await?
					} else {
						GQLTx::new(&kvs, &sess).await?
					};

					let erased: ErasedRecord = (gtx_clone, thing);
					return Ok(Some(field_val_erase_owned(erased)));
				}
				SqlValue::Array(arr) => {
					// Multiple record references
					let gtx_clone = if let Some(ref v) = version {
						GQLTx::new_with_version(&kvs, &sess, Some(v.to_u64())).await?
					} else {
						GQLTx::new(&kvs, &sess).await?
					};

					let mut results = Vec::new();
					for val in arr.0 {
						if let SqlValue::Thing(thing) = val {
							let erased: ErasedRecord = (gtx_clone.clone(), thing);
							results.push(field_val_erase_owned(erased));
						}
					}
					return Ok(Some(FieldValue::list(results)));
				}
				_ => return Ok(None),
			}
		}
		RelationType::OutgoingRelation { relation_table } => {
			// Query: SELECT * FROM relation_table WHERE in = current_record
			Statement::Select(SelectStatement {
				what: vec![SqlValue::Table(relation_table.clone().intox())].into(),
				expr: Fields(
					vec![sql::Field::All],
					false, // Don't use VALUE keyword to get full records
				),
				cond: Some(Cond(SqlValue::Expression(Box::new(Expression::Binary {
					l: SqlValue::Idiom(Idiom::from("in")),
					o: Operator::Equal,
					r: SqlValue::Thing(rid.clone()),
				})))),
				order: None, // We'll handle ordering later
				limit,
				start,
				version: version.clone(),
				..Default::default()
			})
		}
		RelationType::IncomingRelation { relation_table } => {
			// Query: SELECT * FROM relation_table WHERE out = current_record
			Statement::Select(SelectStatement {
				what: vec![SqlValue::Table(relation_table.clone().intox())].into(),
				expr: Fields(
					vec![sql::Field::All],
					false, // Don't use VALUE keyword to get full records
				),
				cond: Some(Cond(SqlValue::Expression(Box::new(Expression::Binary {
					l: SqlValue::Idiom(Idiom::from("out")),
					o: Operator::Equal,
					r: SqlValue::Thing(rid.clone()),
				})))),
				order: None, // We'll handle ordering later
				limit,
				start,
				version: version.clone(),
				..Default::default()
			})
		}
	};

	// Execute the query
	let query = crate::sql::Query(crate::sql::Statements(vec![query_stmt]));
	let mut results = kvs.process(query, &sess, None).await?;
	let res = if let Some(response) = results.pop() {
		response.result.map_err(|e| -> crate::gql::error::GqlError { e.into() })?
	} else {
		return Err("No response from relation query".into());
	};

	// Process the results
	let res_vec = match res {
		SqlValue::Array(a) => a,
		v => {
			// Single result, wrap in array
			crate::sql::Array(vec![v])
		}
	};

	// Create the appropriate GQLTx for field resolution
	let gtx_clone = if let Some(ref v) = version {
		GQLTx::new_with_version(&kvs, &sess, Some(v.to_u64())).await?
	} else {
		GQLTx::new(&kvs, &sess).await?
	};

	let out: Result<Vec<FieldValue>, String> = res_vec
		.0
		.into_iter()
		.map(|v| {
			match v {
				SqlValue::Object(obj) => {
					if let Some(SqlValue::Thing(t)) = obj.get("id") {
						let erased: ErasedRecord = (gtx_clone.clone(), t.clone());
						Ok(field_val_erase_owned(erased))
					} else {
						Err("Relation record missing id field".to_string())
					}
				}
				SqlValue::Thing(t) => {
					let erased: ErasedRecord = (gtx_clone.clone(), t);
					Ok(field_val_erase_owned(erased))
				}
				_ => Err(format!("Expected object or thing, found: {:?}", v))
			}
		})
		.collect();

	match out {
		Ok(l) => {
			if relation.is_list {
				Ok(Some(FieldValue::list(l)))
			} else {
				// For single relations, return the first item or None
				Ok(l.into_iter().next().map(|v| v))
			}
		}
		Err(v) => {
			Err(internal_error(format!("expected thing, found: {v:?}")).into())
		}
	}
}

pub fn sql_value_to_gql_value(v: SqlValue) -> Result<GqlValue, GqlError> {
	let out = match v {
		SqlValue::None => GqlValue::Null,
		SqlValue::Null => GqlValue::Null,
		SqlValue::Bool(b) => GqlValue::Boolean(b),
		SqlValue::Number(n) => match n {
			crate::sql::Number::Int(i) => GqlValue::Number(i.into()),
			crate::sql::Number::Float(f) => GqlValue::Number(
				Number::from_f64(f)
					.ok_or(resolver_error("unimplemented: graceful NaN and Inf handling"))?,
			),
			num @ crate::sql::Number::Decimal(_) => GqlValue::String(num.to_string()),
		},
		SqlValue::Strand(s) => GqlValue::String(s.0),
		d @ SqlValue::Duration(_) => GqlValue::String(d.to_string()),
		SqlValue::Datetime(d) => GqlValue::String(d.to_rfc3339()),
		SqlValue::Uuid(uuid) => GqlValue::String(uuid.to_string()),
		SqlValue::Array(a) => {
			GqlValue::List(a.into_iter().map(|v| sql_value_to_gql_value(v).unwrap()).collect())
		}
		SqlValue::Object(o) => GqlValue::Object(
			o.0.into_iter()
				.map(|(k, v)| (Name::new(k), sql_value_to_gql_value(v).unwrap()))
				.collect(),
		),
		SqlValue::Geometry(geom) => {
			// Convert geometry to GeoJSON format
			match geom {
				crate::sql::Geometry::Point(point) => {
					let coords = vec![
						GqlValue::Number(Number::from_f64(point.x()).unwrap()),
						GqlValue::Number(Number::from_f64(point.y()).unwrap()),
					];
					{
						let mut map = IndexMap::new();
						map.insert(Name::new("type"), GqlValue::String("Point".to_string()));
						map.insert(Name::new("coordinates"), GqlValue::List(coords));
						GqlValue::Object(map)
					}
				}
				crate::sql::Geometry::Line(line) => {
					let coords: Vec<GqlValue> = line.coords()
						.map(|coord| GqlValue::List(vec![
							GqlValue::Number(Number::from_f64(coord.x).unwrap()),
							GqlValue::Number(Number::from_f64(coord.y).unwrap()),
						]))
						.collect();
					{
						let mut map = IndexMap::new();
						map.insert(Name::new("type"), GqlValue::String("LineString".to_string()));
						map.insert(Name::new("coordinates"), GqlValue::List(coords));
						GqlValue::Object(map)
					}
				}
				crate::sql::Geometry::Polygon(polygon) => {
					let exterior_coords: Vec<GqlValue> = polygon.exterior()
						.coords()
						.map(|coord| GqlValue::List(vec![
							GqlValue::Number(Number::from_f64(coord.x).unwrap()),
							GqlValue::Number(Number::from_f64(coord.y).unwrap()),
						]))
						.collect();

					let mut rings = vec![GqlValue::List(exterior_coords)];
					for interior in polygon.interiors() {
						let interior_coords: Vec<GqlValue> = interior
							.coords()
							.map(|coord| GqlValue::List(vec![
								GqlValue::Number(Number::from_f64(coord.x).unwrap()),
								GqlValue::Number(Number::from_f64(coord.y).unwrap()),
							]))
							.collect();
						rings.push(GqlValue::List(interior_coords));
					}

					{
						let mut map = IndexMap::new();
						map.insert(Name::new("type"), GqlValue::String("Polygon".to_string()));
						map.insert(Name::new("coordinates"), GqlValue::List(rings));
						GqlValue::Object(map)
					}
				}
				crate::sql::Geometry::MultiPoint(multipoint) => {
					let coords: Vec<GqlValue> = multipoint.iter()
						.map(|point| GqlValue::List(vec![
							GqlValue::Number(Number::from_f64(point.x()).unwrap()),
							GqlValue::Number(Number::from_f64(point.y()).unwrap()),
						]))
						.collect();
					{
						let mut map = IndexMap::new();
						map.insert(Name::new("type"), GqlValue::String("MultiPoint".to_string()));
						map.insert(Name::new("coordinates"), GqlValue::List(coords));
						GqlValue::Object(map)
					}
				}
				crate::sql::Geometry::MultiLine(multiline) => {
					let coords: Vec<GqlValue> = multiline.iter()
						.map(|line| {
							let line_coords: Vec<GqlValue> = line.coords()
								.map(|coord| GqlValue::List(vec![
									GqlValue::Number(Number::from_f64(coord.x).unwrap()),
									GqlValue::Number(Number::from_f64(coord.y).unwrap()),
								]))
								.collect();
							GqlValue::List(line_coords)
						})
						.collect();
					{
						let mut map = IndexMap::new();
						map.insert(Name::new("type"), GqlValue::String("MultiLineString".to_string()));
						map.insert(Name::new("coordinates"), GqlValue::List(coords));
						GqlValue::Object(map)
					}
				}
				crate::sql::Geometry::MultiPolygon(multipolygon) => {
					let coords: Vec<GqlValue> = multipolygon.iter()
						.map(|polygon| {
							let exterior_coords: Vec<GqlValue> = polygon.exterior()
								.coords()
								.map(|coord| GqlValue::List(vec![
									GqlValue::Number(Number::from_f64(coord.x).unwrap()),
									GqlValue::Number(Number::from_f64(coord.y).unwrap()),
								]))
								.collect();

							let mut rings = vec![GqlValue::List(exterior_coords)];
							for interior in polygon.interiors() {
								let interior_coords: Vec<GqlValue> = interior
									.coords()
									.map(|coord| GqlValue::List(vec![
										GqlValue::Number(Number::from_f64(coord.x).unwrap()),
										GqlValue::Number(Number::from_f64(coord.y).unwrap()),
									]))
									.collect();
								rings.push(GqlValue::List(interior_coords));
							}
							GqlValue::List(rings)
						})
						.collect();
					{
						let mut map = IndexMap::new();
						map.insert(Name::new("type"), GqlValue::String("MultiPolygon".to_string()));
						map.insert(Name::new("coordinates"), GqlValue::List(coords));
						GqlValue::Object(map)
					}
				}
				crate::sql::Geometry::Collection(geometries) => {
					let geoms: Result<Vec<GqlValue>, GqlError> = geometries.iter()
						.map(|g| sql_value_to_gql_value(SqlValue::Geometry(g.clone())))
						.collect();
					{
						let mut map = IndexMap::new();
						map.insert(Name::new("type"), GqlValue::String("GeometryCollection".to_string()));
						map.insert(Name::new("geometries"), GqlValue::List(geoms?));
						GqlValue::Object(map)
					}
				}
			}
		},
		SqlValue::Bytes(b) => GqlValue::Binary(b.into_inner().into()),
		SqlValue::Thing(t) => GqlValue::String(t.to_string()),
		v => return Err(internal_error(format!("found unsupported value variant: {v:?}"))),
	};
	Ok(out)
}

fn kind_to_input_type(kind: Kind, types: &mut Vec<Type>) -> Result<TypeRef, GqlError> {
	let (optional, match_kind) = match kind {
		Kind::Option(op_ty) => (true, *op_ty),
		_ => (false, kind),
	};
	let out_ty = match match_kind {
		Kind::Any => TypeRef::named("any"),
		Kind::Null => TypeRef::named("null"),
		Kind::Bool => TypeRef::named(TypeRef::BOOLEAN),
		Kind::Bytes => TypeRef::named("bytes"),
		Kind::Datetime => TypeRef::named("Datetime"),
		Kind::Decimal => TypeRef::named("decimal"),
		Kind::Duration => TypeRef::named("Duration"),
		Kind::Float => TypeRef::named(TypeRef::FLOAT),
		Kind::Int => TypeRef::named(TypeRef::INT),
		Kind::Number => TypeRef::named("number"),
		Kind::Object => TypeRef::named("object"),
		Kind::Point => return Err(schema_error("Kind::Point is not yet supported")),
		Kind::String => TypeRef::named(TypeRef::STRING),
		Kind::Uuid => TypeRef::named("uuid"),
		Kind::Record(mut r) => match r.len() {
			0 => TypeRef::named("record"),
			1 => {
				let table_name = r.pop().unwrap().0;
				TypeRef::named(format!("Create{}Input", uppercase(&table_name)))
			},
			_ => {
				let names: Vec<String> = r.into_iter().map(|t| format!("Create{}Input", uppercase(&t.0))).collect();
				let ty_name = names.join("_or_");

				let mut tmp_union = Union::new(ty_name.clone())
					.description(format!("An input which is one of: {}", names.join(", ")));
				for n in names {
					tmp_union = tmp_union.possible_type(n);
				}

				types.push(Type::Union(tmp_union));
				TypeRef::named(ty_name)
			}
		},
		Kind::Geometry(ref geom_types) => {
			if geom_types.is_empty() {
				TypeRef::named("geometry")
			} else if geom_types.len() == 1 {
				TypeRef::named(geom_types[0].clone())
			} else {
				// Create a union type for multiple geometry types
				let union_name = format!("geometry_{}", geom_types.join("_or_"));
				let mut geom_union = Union::new(union_name.clone())
					.description(format!("A geometry which is one of: {}", geom_types.join(", ")));

				for geom_type in geom_types {
					geom_union = geom_union.possible_type(geom_type.clone());
				}

				types.push(Type::Union(geom_union));
				TypeRef::named(union_name)
			}
		},
		Kind::Option(t) => {
			let mut non_op_ty = *t;
			while let Kind::Option(inner) = non_op_ty {
				non_op_ty = *inner;
			}
			kind_to_input_type(non_op_ty, types)?
		}
		Kind::Either(ks) => {
			let (ls, others): (Vec<Kind>, Vec<Kind>) =
				ks.into_iter().partition(|k| matches!(k, Kind::Literal(Literal::String(_))));

			let enum_ty = if ls.len() > 0 {
				let vals: Vec<String> = ls
					.into_iter()
					.map(|l| {
						let Kind::Literal(Literal::String(out)) = l else {
							unreachable!(
								"just checked that this is a Kind::Literal(Literal::String(_))"
							);
						};
						out.0
					})
					.collect();

				let mut tmp = Enum::new(vals.join("_or_"));
				tmp = tmp.items(vals);

				let enum_ty = tmp.type_name().to_string();

				types.push(Type::Enum(tmp));
				if others.len() == 0 {
					return Ok(TypeRef::named(enum_ty));
				}
				Some(enum_ty)
			} else {
				None
			};

			let pos_names: Result<Vec<TypeRef>, GqlError> =
				others.into_iter().map(|k| kind_to_input_type(k, types)).collect();
			let pos_names: Vec<String> = pos_names?.into_iter().map(|tr| tr.to_string()).collect();
			let ty_name = pos_names.join("_or_");

			let mut tmp_union = Union::new(ty_name.clone());
			for n in pos_names {
				tmp_union = tmp_union.possible_type(n);
			}

			if let Some(ty) = enum_ty {
				tmp_union = tmp_union.possible_type(ty);
			}

			types.push(Type::Union(tmp_union));
			TypeRef::named(ty_name)
		}
		Kind::Set(_, _) => return Err(schema_error("Kind::Set is not yet supported")),
		Kind::Array(k, _) => TypeRef::List(Box::new(kind_to_input_type(*k, types)?)),
		Kind::Function(_, _) => return Err(schema_error("Kind::Function is not yet supported")),
		Kind::Range => return Err(schema_error("Kind::Range is not yet supported")),
		// TODO(raphaeldarley): check if union is of literals and generate enum
		// generate custom scalar from other literals?
		Kind::Literal(_) => return Err(schema_error("Kind::Literal is not yet supported")),
	};

	let out = match optional {
		true => out_ty,
		false => TypeRef::NonNull(Box::new(out_ty)),
	};
	Ok(out)
}

fn kind_to_type(kind: Kind, types: &mut Vec<Type>) -> Result<TypeRef, GqlError> {
	let (optional, match_kind) = match kind {
		Kind::Option(op_ty) => (true, *op_ty),
		_ => (false, kind),
	};
	let out_ty = match match_kind {
		Kind::Any => TypeRef::named("any"),
		Kind::Null => TypeRef::named("null"),
		Kind::Bool => TypeRef::named(TypeRef::BOOLEAN),
		Kind::Bytes => TypeRef::named("bytes"),
		Kind::Datetime => TypeRef::named("Datetime"),
		Kind::Decimal => TypeRef::named("decimal"),
		Kind::Duration => TypeRef::named("Duration"),
		Kind::Float => TypeRef::named(TypeRef::FLOAT),
		Kind::Int => TypeRef::named(TypeRef::INT),
		Kind::Number => TypeRef::named("number"),
		Kind::Object => TypeRef::named("object"),
		Kind::Point => return Err(schema_error("Kind::Point is not yet supported")),
		Kind::String => TypeRef::named(TypeRef::STRING),
		Kind::Uuid => TypeRef::named("uuid"),
		Kind::Record(mut r) => match r.len() {
			0 => TypeRef::named("record"),
			1 => TypeRef::named(r.pop().unwrap().0),
			_ => {
				let names: Vec<String> = r.into_iter().map(|t| t.0).collect();
				let ty_name = names.join("_or_");

				let mut tmp_union = Union::new(ty_name.clone())
					.description(format!("A record which is one of: {}", names.join(", ")));
				for n in names {
					tmp_union = tmp_union.possible_type(n);
				}

				types.push(Type::Union(tmp_union));
				TypeRef::named(ty_name)
			}
		},
		Kind::Geometry(ref geom_types) => {
			if geom_types.is_empty() {
				TypeRef::named("geometry")
			} else if geom_types.len() == 1 {
				TypeRef::named(geom_types[0].clone())
			} else {
				// Create a union type for multiple geometry types
				let union_name = format!("geometry_{}", geom_types.join("_or_"));
				let mut geom_union = Union::new(union_name.clone())
					.description(format!("A geometry which is one of: {}", geom_types.join(", ")));

				for geom_type in geom_types {
					geom_union = geom_union.possible_type(geom_type.clone());
				}

				types.push(Type::Union(geom_union));
				TypeRef::named(union_name)
			}
		},
		Kind::Option(t) => {
			let mut non_op_ty = *t;
			while let Kind::Option(inner) = non_op_ty {
				non_op_ty = *inner;
			}
			kind_to_type(non_op_ty, types)?
		}
		Kind::Either(ks) => {
			let (ls, others): (Vec<Kind>, Vec<Kind>) =
				ks.into_iter().partition(|k| matches!(k, Kind::Literal(Literal::String(_))));

			let enum_ty = if ls.len() > 0 {
				let vals: Vec<String> = ls
					.into_iter()
					.map(|l| {
						let Kind::Literal(Literal::String(out)) = l else {
							unreachable!(
								"just checked that this is a Kind::Literal(Literal::String(_))"
							);
						};
						out.0
					})
					.collect();

				let mut tmp = Enum::new(vals.join("_or_"));
				tmp = tmp.items(vals);

				let enum_ty = tmp.type_name().to_string();

				types.push(Type::Enum(tmp));
				if others.len() == 0 {
					return Ok(TypeRef::named(enum_ty));
				}
				Some(enum_ty)
			} else {
				None
			};

			let pos_names: Result<Vec<TypeRef>, GqlError> =
				others.into_iter().map(|k| kind_to_type(k, types)).collect();
			let pos_names: Vec<String> = pos_names?.into_iter().map(|tr| tr.to_string()).collect();
			let ty_name = pos_names.join("_or_");

			let mut tmp_union = Union::new(ty_name.clone());
			for n in pos_names {
				tmp_union = tmp_union.possible_type(n);
			}

			if let Some(ty) = enum_ty {
				tmp_union = tmp_union.possible_type(ty);
			}

			types.push(Type::Union(tmp_union));
			TypeRef::named(ty_name)
		}
		Kind::Set(_, _) => return Err(schema_error("Kind::Set is not yet supported")),
		Kind::Array(k, _) => TypeRef::List(Box::new(kind_to_type(*k, types)?)),
		Kind::Function(_, _) => return Err(schema_error("Kind::Function is not yet supported")),
		Kind::Range => return Err(schema_error("Kind::Range is not yet supported")),
		// TODO(raphaeldarley): check if union is of literals and generate enum
		// generate custom scalar from other literals?
		Kind::Literal(_) => return Err(schema_error("Kind::Literal is not yet supported")),
	};

	let out = match optional {
		true => out_ty,
		false => TypeRef::NonNull(Box::new(out_ty)),
	};
	Ok(out)
}

macro_rules! where_impl {
	($where_obj:ident, $ty:ident, $name:expr) => {
		$where_obj = $where_obj.field(InputValue::new($name, $ty.clone()));
	};
}

fn where_id() -> InputObject {
	let mut where_obj = InputObject::new("_where_id");
	let ty = TypeRef::named(TypeRef::ID);
	where_impl!(where_obj, ty, "eq");
	where_impl!(where_obj, ty, "ne");
	where_obj
}

fn where_from_type(
	kind: Kind,
	where_name: String,
	types: &mut Vec<Type>,
) -> Result<InputObject, GqlError> {
	let inner_kind = match &kind {
		Kind::Option(inner) => inner.as_ref().clone(),
		_ => kind.clone(),
	};

	let ty = match &inner_kind {
		Kind::Record(ts) => match ts.len() {
			1 => TypeRef::named(where_name_from_table(
				ts.first().expect("ts should have exactly one element").as_str(),
			)),
			_ => TypeRef::named(TypeRef::ID),
		},
		k => unwrap_type(kind_to_type(k.clone(), types)?),
	};

	let mut where_obj = InputObject::new(where_name);
	where_impl!(where_obj, ty, "eq");
	where_impl!(where_obj, ty, "ne");

	match inner_kind {
		Kind::String => {
			let string_ty = TypeRef::named(TypeRef::STRING);
			let string_list_ty = TypeRef::named_nn_list(TypeRef::STRING);
			where_impl!(where_obj, string_ty, "contains");
			where_impl!(where_obj, string_ty, "startsWith");
			where_impl!(where_obj, string_ty, "endsWith");
			where_impl!(where_obj, string_ty, "regex");
			where_impl!(where_obj, string_list_ty, "in");
		}
		Kind::Int => {
			where_impl!(where_obj, ty, "gt");
			where_impl!(where_obj, ty, "gte");
			where_impl!(where_obj, ty, "lt");
			where_impl!(where_obj, ty, "lte");
			let list_ty = TypeRef::named_nn_list(TypeRef::INT);
			where_impl!(where_obj, list_ty, "in");
		}
		Kind::Float => {
			where_impl!(where_obj, ty, "gt");
			where_impl!(where_obj, ty, "gte");
			where_impl!(where_obj, ty, "lt");
			where_impl!(where_obj, ty, "lte");
			let list_ty = TypeRef::named_nn_list(TypeRef::FLOAT);
			where_impl!(where_obj, list_ty, "in");
		}
		Kind::Number => {
			where_impl!(where_obj, ty, "gt");
			where_impl!(where_obj, ty, "gte");
			where_impl!(where_obj, ty, "lt");
			where_impl!(where_obj, ty, "lte");
			let list_ty = TypeRef::named_nn_list("Number");
			where_impl!(where_obj, list_ty, "in");
		}
		Kind::Decimal => {
			where_impl!(where_obj, ty, "gt");
			where_impl!(where_obj, ty, "gte");
			where_impl!(where_obj, ty, "lt");
			where_impl!(where_obj, ty, "lte");
			let list_ty = TypeRef::named_nn_list("Decimal");
			where_impl!(where_obj, list_ty, "in");
		}
		Kind::Bool => {}
		Kind::Datetime => {
			where_impl!(where_obj, ty, "gt");
			where_impl!(where_obj, ty, "gte");
			where_impl!(where_obj, ty, "lt");
			where_impl!(where_obj, ty, "lte");
		}
		Kind::Any => {}
		Kind::Null => {}
		Kind::Bytes => {}
		Kind::Duration => {}
		Kind::Object => {}
		Kind::Point => {}
		Kind::Uuid => {}
		Kind::Record(_) => {}
		Kind::Geometry(_) => {}
		Kind::Option(_) => {}
		Kind::Either(_) => {}
		Kind::Set(_, _) => {}
		Kind::Array(_, _) => {}
		Kind::Function(_, _) => {}
		Kind::Range => {}
		Kind::Literal(_) => {}
	};
	Ok(where_obj)
}

fn unwrap_type(ty: TypeRef) -> TypeRef {
	match ty {
		TypeRef::NonNull(t) => unwrap_type(*t),
		_ => ty,
	}
}

fn cond_from_where(
	where_clause: &IndexMap<Name, GqlValue>,
	fds: &[DefineFieldStatement],
) -> Result<Cond, GqlError> {
	val_from_where(where_clause, fds).map(IntoExt::intox)
}

fn val_from_where(
	where_clause: &IndexMap<Name, GqlValue>,
	fds: &[DefineFieldStatement],
) -> Result<SqlValue, GqlError> {
	if where_clause.len() != 1 {
		return Err(resolver_error("Table Where clause must have one item"));
	}

	let (k, v) = where_clause.iter().next().unwrap();

	let cond = match k.as_str().to_lowercase().as_str() {
		"or" => aggregate(v, AggregateOp::Or, fds),
		"and" => aggregate(v, AggregateOp::And, fds),
		"not" => negate(v, fds),
		_ => binop(k.as_str(), v, fds),
	};

	cond
}

fn parse_op(name: impl AsRef<str>) -> Result<sql::Operator, GqlError> {
	match name.as_ref() {
		"eq" => Ok(sql::Operator::Equal),
		"ne" => Ok(sql::Operator::NotEqual),
		"gt" => Ok(sql::Operator::MoreThan),
		"gte" => Ok(sql::Operator::MoreThanOrEqual),
		"lt" => Ok(sql::Operator::LessThan),
		"lte" => Ok(sql::Operator::LessThanOrEqual),
		"contains" => Ok(sql::Operator::Contain),
		"startsWith" => Ok(sql::Operator::Equal),
		"endsWith" => Ok(sql::Operator::Equal),
		"regex" => Ok(sql::Operator::Matches(None)),
		"in" => Ok(sql::Operator::Inside),
		op => Err(resolver_error(format!("Unsupported op: {op}"))),
	}
}

fn negate(where_clause: &GqlValue, fds: &[DefineFieldStatement]) -> Result<SqlValue, GqlError> {
	let obj = where_clause.as_object().ok_or(resolver_error("Value of NOT must be object"))?;
	let inner_cond = val_from_where(obj, fds)?;

	Ok(Expression::Unary {
		o: sql::Operator::Not,
		v: inner_cond,
	}
	.into())
}

enum AggregateOp {
	And,
	Or,
}

fn aggregate(
	filter: &GqlValue,
	op: AggregateOp,
	fds: &[DefineFieldStatement],
) -> Result<SqlValue, GqlError> {
	let op_str = match op {
		AggregateOp::And => "AND",
		AggregateOp::Or => "OR",
	};
	let op = match op {
		AggregateOp::And => sql::Operator::And,
		AggregateOp::Or => sql::Operator::Or,
	};
	let list =
		filter.as_list().ok_or(resolver_error(format!("Value of {op_str} should be a list")))?;
	let where_arr = list
		.iter()
		.map(|v| v.as_object().map(|o| val_from_where(o, fds)))
		.collect::<Option<Result<Vec<SqlValue>, GqlError>>>()
		.ok_or(resolver_error(format!("List of {op_str} should contain objects")))??;

	let mut iter = where_arr.into_iter();

	let mut cond = iter
		.next()
		.ok_or(resolver_error(format!("List of {op_str} should contain at least one object")))?;

	for clause in iter {
		cond = Expression::Binary {
			l: clause,
			o: op.clone(),
			r: cond,
		}
		.into();
	}

	Ok(cond)
}

fn binop(
	field_name: &str,
	val: &GqlValue,
	fds: &[DefineFieldStatement],
) -> Result<SqlValue, GqlError> {
	let obj = val.as_object().ok_or(resolver_error("Field where condition should be object"))?;

	let Some(fd) = fds.iter().find(|fd| fd.name.to_string() == field_name) else {
		return Err(resolver_error(format!("Field `{field_name}` not found")));
	};

	if obj.len() != 1 {
		return Err(resolver_error("Field Where condition must have one item"));
	}

	let lhs = sql::Value::Idiom(field_name.intox());

	let (k, v) = obj.iter().next().unwrap();
	let op = parse_op(k)?;

	// Handle special cases that need function calls instead of binary operators
	if k.as_str() == "startsWith" {
		let value = gql_to_sql_kind(v, fd.kind.clone().unwrap_or_default())?;
		let func_val = sql::Value::Function(Box::new(sql::Function::Normal(
			"string::starts_with".to_string(),
			vec![lhs, value]
		)));
		return Ok(func_val);
	} else if k.as_str() == "endsWith" {
		let value = gql_to_sql_kind(v, fd.kind.clone().unwrap_or_default())?;
		let func_val = sql::Value::Function(Box::new(sql::Function::Normal(
			"string::ends_with".to_string(),
			vec![lhs, value]
		)));
		return Ok(func_val);
	}

	let rhs = if k.as_str() == "in" {
		// For 'in' operator, expect an array of values
		let list = v.as_list().ok_or(resolver_error("Value for 'in' operator must be a list"))?;
		let field_kind = fd.kind.clone().unwrap_or_default();
		let sql_values: Result<Vec<_>, _> = list
			.iter()
			.map(|item| gql_to_sql_kind(item, field_kind.clone()))
			.collect();
		sql::Value::Array(sql_values?.into())
	} else {
		gql_to_sql_kind(v, fd.kind.clone().unwrap_or_default())?
	};

	let expr = sql::Expression::Binary {
		l: lhs,
		o: op,
		r: rhs,
	};

	Ok(expr.into())
}

macro_rules! either_try_kind {
	($ks:ident, $val:expr, Kind::Array) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr, Array) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr, Record) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr, AllNumbers) => {
		either_try_kind!($ks, $val, Kind::Int);
		either_try_kind!($ks, $val, Kind::Float);
		either_try_kind!($ks, $val, Kind::Decimal);
		either_try_kind!($ks, $val, Kind::Number);
	};
	($ks:ident, $val:expr, $kind:expr) => {
		if $ks.contains(&$kind) {
			if let Ok(out) = gql_to_sql_kind($val, $kind) {
				return Ok(out);
			}
		}
	};
}

macro_rules! either_try_kinds {
	($ks:ident, $val:expr, $($kind:tt),+) => {
		$(either_try_kind!($ks, $val, $kind));+
	};
}

macro_rules! any_try_kind {
	($val:expr, $kind:expr) => {
		if let Ok(out) = gql_to_sql_kind($val, $kind) {
			return Ok(out);
		}
	};
}
macro_rules! any_try_kinds {
	($val:expr, $($kind:tt),+) => {
		$(any_try_kind!($val, $kind));+
	};
}

fn gql_to_sql_kind(val: &GqlValue, kind: Kind) -> Result<SqlValue, GqlError> {
	use crate::syn;
	match kind {
		Kind::Any => match val {
			GqlValue::String(s) => {
				use Kind::*;
				any_try_kinds!(val, Datetime, Duration, Uuid);
				syn::value_legacy_strand(s.as_str()).map_err(|_| type_error(kind, val))
			}
			GqlValue::Null => Ok(SqlValue::Null),
			obj @ GqlValue::Object(_) => gql_to_sql_kind(obj, Kind::Object),
			num @ GqlValue::Number(_) => gql_to_sql_kind(num, Kind::Number),
			GqlValue::Boolean(b) => Ok(SqlValue::Bool(*b)),
			bin @ GqlValue::Binary(_) => gql_to_sql_kind(bin, Kind::Bytes),
			GqlValue::Enum(s) => Ok(SqlValue::Strand(s.as_str().into())),
			arr @ GqlValue::List(_) => gql_to_sql_kind(arr, Kind::Array(Box::new(Kind::Any), None)),
		},
		Kind::Null => match val {
			GqlValue::Null => Ok(SqlValue::Null),
			_ => Err(type_error(kind, val)),
		},
		Kind::Bool => match val {
			GqlValue::Boolean(b) => Ok(SqlValue::Bool(*b)),
			_ => Err(type_error(kind, val)),
		},
		Kind::Bytes => match val {
			GqlValue::Binary(b) => Ok(SqlValue::Bytes(b.to_owned().to_vec().into())),
			_ => Err(type_error(kind, val)),
		},
		Kind::Datetime => match val {
			GqlValue::String(s) => match syn::datetime(s) {
				Ok(dt) => Ok(dt.into()),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Decimal => match val {
			GqlValue::Number(n) => {
				if let Some(int) = n.as_i64() {
					Ok(SqlValue::Number(sql::Number::Decimal(int.into())))
				} else if let Some(d) = n.as_f64().and_then(Decimal::from_f64) {
					Ok(SqlValue::Number(sql::Number::Decimal(d)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SqlValue::Number(sql::Number::Decimal(uint.into())))
				} else {
					Err(type_error(kind, val))
				}
			}
			GqlValue::String(s) => match syn::value(s) {
				Ok(SqlValue::Number(n)) => match n {
					sql::Number::Int(i) => Ok(SqlValue::Number(sql::Number::Decimal(i.into()))),
					sql::Number::Float(f) => match Decimal::from_f64(f) {
						Some(d) => Ok(SqlValue::Number(sql::Number::Decimal(d))),
						None => Err(type_error(kind, val)),
					},
					sql::Number::Decimal(d) => Ok(SqlValue::Number(sql::Number::Decimal(d))),
				},
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Duration => match val {
			GqlValue::String(s) => match syn::duration(s) {
				Ok(d) => Ok(d.into()),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Float => match val {
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SqlValue::Number(sql::Number::Float(i as f64)))
				} else if let Some(f) = n.as_f64() {
					Ok(SqlValue::Number(sql::Number::Float(f)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SqlValue::Number(sql::Number::Float(uint as f64)))
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				}
			}
			GqlValue::String(s) => match syn::value(s) {
				Ok(SqlValue::Number(n)) => match n {
					sql::Number::Int(int) => Ok(SqlValue::Number(sql::Number::Float(int as f64))),
					sql::Number::Float(float) => Ok(SqlValue::Number(sql::Number::Float(float))),
					sql::Number::Decimal(d) => match d.try_into() {
						Ok(f) => Ok(SqlValue::Number(sql::Number::Float(f))),
						_ => Err(type_error(kind, val)),
					},
				},
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Int => match val {
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SqlValue::Number(sql::Number::Int(i)))
				} else {
					Err(type_error(kind, val))
				}
			}
			GqlValue::String(s) => match syn::value(s) {
				Ok(SqlValue::Number(n)) => match n {
					sql::Number::Int(int) => Ok(SqlValue::Number(sql::Number::Int(int))),
					sql::Number::Float(float) => {
						if float.fract() == 0.0 {
							Ok(SqlValue::Number(sql::Number::Int(float as i64)))
						} else {
							Err(type_error(kind, val))
						}
					}
					sql::Number::Decimal(d) => match d.try_into() {
						Ok(i) => Ok(SqlValue::Number(sql::Number::Int(i))),
						_ => Err(type_error(kind, val)),
					},
				},
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Number => match val {
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SqlValue::Number(sql::Number::Int(i)))
				} else if let Some(f) = n.as_f64() {
					Ok(SqlValue::Number(sql::Number::Float(f)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SqlValue::Number(sql::Number::Decimal(uint.into())))
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				}
			}
			GqlValue::String(s) => match syn::value(s) {
				Ok(SqlValue::Number(n)) => Ok(SqlValue::Number(n.clone())),
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Object => match val {
			GqlValue::Object(o) => {
				let out: Result<BTreeMap<String, SqlValue>, GqlError> = o
					.iter()
					.map(|(k, v)| gql_to_sql_kind(v, Kind::Any).map(|sqlv| (k.to_string(), sqlv)))
					.collect();
				Ok(SqlValue::Object(out?.into()))
			}
			GqlValue::String(s) => match syn::value_legacy_strand(s.as_str()) {
				Ok(obj @ SqlValue::Object(_)) => Ok(obj),
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Point => match val {
			GqlValue::List(l) => match l.as_slice() {
				[GqlValue::Number(x), GqlValue::Number(y)] => match (x.as_f64(), y.as_f64()) {
					(Some(x), Some(y)) => Ok(SqlValue::Geometry(Geometry::Point((x, y).into()))),
					_ => Err(type_error(kind, val)),
				},
				_ => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::String => match val {
			GqlValue::String(s) => Ok(SqlValue::Strand(s.to_owned().into())),
			GqlValue::Enum(s) => Ok(SqlValue::Strand(s.as_str().into())),
			_ => Err(type_error(kind, val)),
		},
		Kind::Uuid => match val {
			GqlValue::String(s) => match s.parse::<uuid::Uuid>() {
				Ok(u) => Ok(SqlValue::Uuid(u.into())),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Record(ref ts) => match val {
			GqlValue::String(s) => match syn::thing(s) {
				Ok(t) => match ts.contains(&t.tb.as_str().into()) {
					true => Ok(SqlValue::Thing(t)),
					false => Err(type_error(kind, val)),
				},
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		// GeoJSON / Geometries
		Kind::Geometry(ref geom_types) => match val {
			GqlValue::String(s) => {
				use crate::syn;
				match syn::value(s) {
					Ok(SqlValue::Geometry(geom)) => {
						if !geom_types.is_empty() {
							let geom_type_name = match geom {
								crate::sql::Geometry::Point(_) => "point",
								crate::sql::Geometry::Line(_) => "line",
								crate::sql::Geometry::Polygon(_) => "polygon",
								crate::sql::Geometry::MultiPoint(_) => "multipoint",
								crate::sql::Geometry::MultiLine(_) => "multiline",
								crate::sql::Geometry::MultiPolygon(_) => "multipolygon",
								crate::sql::Geometry::Collection(_) => "collection",
							};

							if !geom_types.contains(&geom_type_name.to_string()) {
								return Err(type_error(kind, val));
							}
						}
						Ok(SqlValue::Geometry(geom))
					}
					_ => Err(type_error(kind, val)),
				}
			}
			GqlValue::Object(obj) => {
				if let (Some(GqlValue::String(geom_type)), Some(coordinates)) =
					(obj.get("type"), obj.get("coordinates")) {

					match geom_type.to_lowercase().as_str() {
						"point" => {
							if let GqlValue::List(coords) = coordinates {
								if coords.len() == 2 {
									if let (Some(GqlValue::Number(x)), Some(GqlValue::Number(y))) =
										(coords.get(0), coords.get(1)) {
										if let (Some(x_val), Some(y_val)) = (x.as_f64(), y.as_f64()) {
											let geom = crate::sql::Geometry::Point((x_val, y_val).into());
											if !geom_types.is_empty() && !geom_types.contains(&"point".to_string()) {
												return Err(type_error(kind, val));
											}
											return Ok(SqlValue::Geometry(geom));
										}
									}
								}
							}
						}
						"linestring" => {
							if let GqlValue::List(coords) = coordinates {
								let mut line_coords = Vec::new();
								for coord in coords {
									if let GqlValue::List(point) = coord {
										if point.len() == 2 {
											if let (Some(GqlValue::Number(x)), Some(GqlValue::Number(y))) =
												(point.get(0), point.get(1)) {
												if let (Some(x_val), Some(y_val)) = (x.as_f64(), y.as_f64()) {
													line_coords.push((x_val, y_val));
												} else {
													return Err(type_error(kind, val));
												}
											} else {
												return Err(type_error(kind, val));
											}
										} else {
											return Err(type_error(kind, val));
										}
									} else {
										return Err(type_error(kind, val));
									}
								}
								if line_coords.len() >= 2 {
									let geom = crate::sql::Geometry::Line(line_coords.into());
									if !geom_types.is_empty() && !geom_types.contains(&"line".to_string()) {
										return Err(type_error(kind, val));
									}
									return Ok(SqlValue::Geometry(geom));
								}
							}
						}
						_ => return Err(type_error(kind, val)),
					}
				}
				Err(type_error(kind, val))
			}
			GqlValue::List(coords) => {
				// Handle simple point format [x, y]
				if coords.len() == 2 {
					if let (Some(GqlValue::Number(x)), Some(GqlValue::Number(y))) =
						(coords.get(0), coords.get(1)) {
						if let (Some(x_val), Some(y_val)) = (x.as_f64(), y.as_f64()) {
							let geom = crate::sql::Geometry::Point((x_val, y_val).into());
							if !geom_types.is_empty() && !geom_types.contains(&"point".to_string()) {
								return Err(type_error(kind, val));
							}
							return Ok(SqlValue::Geometry(geom));
						}
					}
				}
				Err(type_error(kind, val))
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Option(k) => match val {
			GqlValue::Null => Ok(SqlValue::None),
			v => gql_to_sql_kind(v, *k),
		},
		// TODO: handle nested eithers
		Kind::Either(ref ks) => {
			use Kind::*;

			match val {
				GqlValue::Null => {
					if ks.iter().any(|k| matches!(k, Kind::Option(_))) {
						Ok(SqlValue::None)
					} else if ks.contains(&Kind::Null) {
						Ok(SqlValue::Null)
					} else {
						Err(type_error(kind, val))
					}
				}
				num @ GqlValue::Number(_) => {
					either_try_kind!(ks, num, AllNumbers);
					Err(type_error(kind, val))
				}
				string @ GqlValue::String(_) => {
					either_try_kinds!(
						ks, string, Datetime, Duration, AllNumbers, Object, Uuid, Array, Any,
						String
					);
					Err(type_error(kind, val))
				}
				bool @ GqlValue::Boolean(_) => {
					either_try_kind!(ks, bool, Kind::Bool);
					Err(type_error(kind, val))
				}
				GqlValue::Binary(_) => {
					Err(resolver_error("binary input for Either is not yet supported"))
				}
				GqlValue::Enum(n) => {
					either_try_kind!(ks, &GqlValue::String(n.to_string()), Kind::String);
					Err(type_error(kind, val))
				}
				list @ GqlValue::List(_) => {
					either_try_kind!(ks, list, Kind::Array);
					Err(type_error(kind, val))
				}
				// TODO: consider geometry and other types that can come from objects
				obj @ GqlValue::Object(_) => {
					either_try_kind!(ks, obj, Object);
					Err(type_error(kind, val))
				}
			}
		}
		Kind::Set(_k, _n) => Err(resolver_error("Sets are not yet supported")),
		Kind::Array(ref k, n) => match val {
			GqlValue::List(l) => {
				let list_iter = l.iter().map(|v| gql_to_sql_kind(v, *k.to_owned()));
				let list: Result<Vec<SqlValue>, GqlError> = list_iter.collect();

				match (list, n) {
					(Ok(l), Some(n)) => {
						if l.len() as u64 == n {
							Ok(l.into())
						} else {
							Err(type_error(kind, val))
						}
					}
					(Ok(l), None) => Ok(l.into()),
					(Err(e), _) => Err(e),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Function(_, _) => Err(resolver_error("Sets are not yet supported")),
		Kind::Range => Err(resolver_error("Ranges are not yet supported")),
		Kind::Literal(_) => Err(resolver_error("Literals are not yet supported")),
	}
}
