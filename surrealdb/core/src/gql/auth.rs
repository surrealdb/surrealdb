//! GraphQL authentication mutations.
//!
//! Generates `signIn` and `signUp` mutation fields by inspecting database
//! access definitions. Each Record access method that has a SIGNIN clause
//! contributes to the `signIn` mutation, and each that has a SIGNUP clause
//! contributes to the `signUp` mutation.
//!
//! The mutations accept an `access` name and a `variables` object (JSON scalar),
//! and return a JWT access token string on success.

use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, Object, TypeRef};
use async_graphql::{Name, Value as GqlValue};

use super::error::{GqlError, auth_error, resolver_error};
use super::utils::GqlValueUtils;
use crate::catalog::{AccessDefinition, AccessType};
use crate::dbs::Session;
use crate::iam::token::Token;
use crate::iam::{signin, signup};
use crate::kvs::Datastore;
use crate::types::PublicVariables;

/// Inspect all database access definitions and add `signIn` / `signUp`
/// mutation fields to the provided Mutation object.
///
/// - `signIn(access: String!, variables: JSON!): String!` is added when at least one Record access
///   method with a SIGNIN clause exists.
/// - `signUp(access: String!, variables: JSON!): String!` is added when at least one Record access
///   method with a SIGNUP clause exists.
///
/// The `variables` argument accepts an arbitrary JSON object containing the
/// authentication variables (e.g., `{ email: "user@example.com", pass: "secret" }`).
///
/// Returns the (possibly unchanged) mutation object.
pub fn add_auth_mutations(
	mutation: Object,
	accesses: &[AccessDefinition],
	ns: &str,
	db: &str,
	datastore: &Arc<Datastore>,
) -> Object {
	let has_signin = accesses.iter().any(|ac| match &ac.access_type {
		AccessType::Record(rec) => rec.signin.is_some(),
		_ => false,
	});

	let has_signup = accesses.iter().any(|ac| match &ac.access_type {
		AccessType::Record(rec) => rec.signup.is_some(),
		_ => false,
	});

	let mut mutation = mutation;

	if has_signin {
		let kvs = datastore.clone();
		let ns_name = ns.to_string();
		let db_name = db.to_string();
		mutation = mutation.field(
			Field::new("signIn", TypeRef::named_nn(TypeRef::STRING), move |ctx| {
				let kvs = kvs.clone();
				let ns_name = ns_name.clone();
				let db_name = db_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();

					let access = args
						.get("access")
						.and_then(GqlValueUtils::as_string)
						.ok_or_else(|| resolver_error("Missing required 'access' argument"))?;

					let variables = args
						.get("variables")
						.and_then(GqlValueUtils::as_object)
						.ok_or_else(|| {
							resolver_error(
								"Missing required 'variables' argument (must be an object)",
							)
						})?;

					let vars = build_public_variables(&ns_name, &db_name, &access, variables)?;

					// Create a fresh session for the signin operation
					let mut auth_sess = Session::default();
					auth_sess.ns = Some(ns_name.clone());
					auth_sess.db = Some(db_name.clone());
					auth_sess.ip.clone_from(&sess.ip);
					auth_sess.or.clone_from(&sess.or);

					let token = signin::signin(&kvs, &mut auth_sess, vars).await.map_err(|e| {
						warn!("GraphQL signIn failed: {e}");
						auth_error("There was a problem with authentication")
					})?;

					let access_token = match token {
						Token::Access(t) => t,
						Token::WithRefresh {
							access,
							..
						} => access,
					};

					Ok(Some(FieldValue::value(GqlValue::String(access_token))))
				})
			})
			.description("Sign in using a database access method and return a JWT token")
			.argument(InputValue::new("access", TypeRef::named_nn(TypeRef::STRING)))
			.argument(InputValue::new("variables", TypeRef::named_nn("JSON"))),
		);
	}

	if has_signup {
		let kvs = datastore.clone();
		let ns_name = ns.to_string();
		let db_name = db.to_string();
		mutation = mutation.field(
			Field::new("signUp", TypeRef::named_nn(TypeRef::STRING), move |ctx| {
				let kvs = kvs.clone();
				let ns_name = ns_name.clone();
				let db_name = db_name.clone();
				FieldFuture::new(async move {
					let sess = ctx.data::<Arc<Session>>()?;
					let args = ctx.args.as_index_map();

					let access = args
						.get("access")
						.and_then(GqlValueUtils::as_string)
						.ok_or_else(|| resolver_error("Missing required 'access' argument"))?;

					let variables = args
						.get("variables")
						.and_then(GqlValueUtils::as_object)
						.ok_or_else(|| {
							resolver_error(
								"Missing required 'variables' argument (must be an object)",
							)
						})?;

					let vars = build_public_variables(&ns_name, &db_name, &access, variables)?;

					// Create a fresh session for the signup operation
					let mut auth_sess = Session::default();
					auth_sess.ns = Some(ns_name.clone());
					auth_sess.db = Some(db_name.clone());
					auth_sess.ip.clone_from(&sess.ip);
					auth_sess.or.clone_from(&sess.or);

					let token = signup::signup(&kvs, &mut auth_sess, vars).await.map_err(|e| {
						warn!("GraphQL signUp failed: {e}");
						auth_error("There was a problem with authentication")
					})?;

					let access_token = match token {
						Token::Access(t) => t,
						Token::WithRefresh {
							access,
							..
						} => access,
					};

					Ok(Some(FieldValue::value(GqlValue::String(access_token))))
				})
			})
			.description("Sign up using a database access method and return a JWT token")
			.argument(InputValue::new("access", TypeRef::named_nn(TypeRef::STRING)))
			.argument(InputValue::new("variables", TypeRef::named_nn("JSON"))),
		);
	}

	mutation
}

/// Build a `PublicVariables` map from a GraphQL object value.
///
/// Sets the system variables `NS`, `DB`, and `AC` from the provided
/// namespace, database, and access method name, then converts each
/// key-value pair from the GraphQL object to a public variable.
///
/// Values are converted directly to their natural types (strings stay
/// as strings, numbers as numbers, booleans as booleans) without going
/// through the SurrealQL parser, since auth variables are user-provided
/// credentials, not SurrealQL expressions.
fn build_public_variables(
	ns: &str,
	db: &str,
	access: &str,
	variables: &IndexMap<Name, GqlValue>,
) -> Result<PublicVariables, GqlError> {
	let mut vars = PublicVariables::new();

	// Set the system-level routing variables
	vars.insert("NS", ns.to_string());
	vars.insert("DB", db.to_string());
	vars.insert("AC", access.to_string());

	// Convert user-provided variables directly to PublicVariables
	for (key, val) in variables {
		let key_str = key.as_str();
		// Skip system variables the user might have accidentally included
		if matches!(key_str, "NS" | "ns" | "DB" | "db" | "AC" | "ac") {
			continue;
		}

		match val {
			GqlValue::Null => continue,
			GqlValue::String(s) => vars.insert(key_str.to_string(), s.clone()),
			GqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					vars.insert(key_str.to_string(), i);
				} else if let Some(f) = n.as_f64() {
					vars.insert(key_str.to_string(), f);
				} else {
					vars.insert(key_str.to_string(), n.to_string());
				}
			}
			GqlValue::Boolean(b) => vars.insert(key_str.to_string(), *b),
			GqlValue::Enum(s) => vars.insert(key_str.to_string(), s.as_str().to_string()),
			// For complex types (lists, nested objects), convert to string
			other => vars.insert(key_str.to_string(), other.to_string()),
		}
	}

	Ok(vars)
}
