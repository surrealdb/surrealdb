use crate::sql::json;
use crate::sql::Object;
use crate::sql::Value;
use jsonwebtoken::{Algorithm, Header};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub static HEADER: Lazy<Header> = Lazy::new(|| Header::new(Algorithm::HS512));

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[non_exhaustive]
pub struct Claims {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub iat: Option<i64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub nbf: Option<i64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub exp: Option<i64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub iss: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub jti: Option<String>,
	#[serde(alias = "ns")]
	#[serde(alias = "NS")]
	#[serde(rename = "NS")]
	#[serde(alias = "https://surrealdb.com/ns")]
	#[serde(alias = "https://surrealdb.com/namespace")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ns: Option<String>,
	#[serde(alias = "db")]
	#[serde(alias = "DB")]
	#[serde(rename = "DB")]
	#[serde(alias = "https://surrealdb.com/db")]
	#[serde(alias = "https://surrealdb.com/database")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub db: Option<String>,
	#[serde(alias = "sc")]
	#[serde(alias = "SC")]
	#[serde(rename = "SC")]
	#[serde(alias = "https://surrealdb.com/sc")]
	#[serde(alias = "https://surrealdb.com/scope")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub sc: Option<String>,
	#[serde(alias = "tk")]
	#[serde(alias = "TK")]
	#[serde(rename = "TK")]
	#[serde(alias = "https://surrealdb.com/tk")]
	#[serde(alias = "https://surrealdb.com/token")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tk: Option<String>,
	#[serde(alias = "id")]
	#[serde(alias = "ID")]
	#[serde(rename = "ID")]
	#[serde(alias = "https://surrealdb.com/id")]
	#[serde(alias = "https://surrealdb.com/record")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
	#[serde(alias = "rl")]
	#[serde(alias = "RL")]
	#[serde(rename = "RL")]
	#[serde(alias = "https://surrealdb.com/rl")]
	#[serde(alias = "https://surrealdb.com/roles")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub roles: Option<Vec<String>>,

	#[serde(flatten)]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub custom_claims: Option<HashMap<String, serde_json::Value>>,
}

impl From<Claims> for Value {
	fn from(v: Claims) -> Value {
		// Set default value
		let mut out = Object::default();
		// Add iss field if set
		if let Some(iss) = v.iss {
			out.insert("iss".to_string(), iss.into());
		}
		// Add iat field if set
		if let Some(iat) = v.iat {
			out.insert("iat".to_string(), iat.into());
		}
		// Add nbf field if set
		if let Some(nbf) = v.nbf {
			out.insert("nbf".to_string(), nbf.into());
		}
		// Add exp field if set
		if let Some(exp) = v.exp {
			out.insert("exp".to_string(), exp.into());
		}
		// Add jti field if set
		if let Some(jti) = v.jti {
			out.insert("jti".to_string(), jti.into());
		}
		// Add NS field if set
		if let Some(ns) = v.ns {
			out.insert("NS".to_string(), ns.into());
		}
		// Add DB field if set
		if let Some(db) = v.db {
			out.insert("DB".to_string(), db.into());
		}
		// Add SC field if set
		if let Some(sc) = v.sc {
			out.insert("SC".to_string(), sc.into());
		}
		// Add TK field if set
		if let Some(tk) = v.tk {
			out.insert("TK".to_string(), tk.into());
		}
		// Add ID field if set
		if let Some(id) = v.id {
			out.insert("ID".to_string(), id.into());
		}
		// Add RL field if set
		if let Some(role) = v.roles {
			out.insert("RL".to_string(), role.into());
		}
		// Add custom claims if set
		if let Some(custom_claims) = v.custom_claims {
			for (claim, value) in custom_claims {
				// Serialize the raw JSON string representing the claim value
				let claim_json = match serde_json::to_string(&value) {
					Ok(claim_json) => claim_json,
					Err(err) => {
						debug!("Failed to serialize token claim '{}': {}", claim, err);
						continue;
					}
				};
				// Parse that JSON string into the corresponding SurrealQL value
				let claim_value = match json(&claim_json) {
					Ok(claim_value) => claim_value,
					Err(err) => {
						debug!("Failed to parse token claim '{}': {}", claim, err);
						continue;
					}
				};
				// Strip namespace prefix to store only canonical claim names
				if let Some(canonical_claim) = claim.strip_prefix("https://surrealdb.com/") {
					// TODO(gguillemas): Kept for backward compatibility. Remove in 2.0.0.
					out.insert(claim.to_owned(), claim_value.clone());
					// Avoid replacing a public claim with a private one
					if !out.contains_key(canonical_claim) {
						out.insert(canonical_claim.to_owned(), claim_value);
					}
				} else {
					out.insert(claim.to_owned(), claim_value);
				}
			}
		}
		// Return value
		out.into()
	}
}
