use jsonwebtoken::{Algorithm, Header};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use surrealdb::sql::Object;
use surrealdb::sql::Value;

pub static HEADER: Lazy<Header> = Lazy::new(|| Header::new(Algorithm::HS512));

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Claims {
	pub iat: i64,
	pub exp: i64,
	pub iss: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub nbf: Option<i64>,
	#[serde(alias = "ns")]
	#[serde(alias = "NS")]
	#[serde(rename = "NS")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ns: Option<String>,
	#[serde(alias = "db")]
	#[serde(alias = "DB")]
	#[serde(rename = "DB")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub db: Option<String>,
	#[serde(alias = "sc")]
	#[serde(alias = "SC")]
	#[serde(rename = "SC")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub sc: Option<String>,
	#[serde(alias = "tk")]
	#[serde(alias = "TK")]
	#[serde(rename = "TK")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tk: Option<String>,
	#[serde(alias = "id")]
	#[serde(alias = "ID")]
	#[serde(rename = "ID")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
}

impl From<Claims> for Value {
	fn from(v: Claims) -> Value {
		// Set default value
		let mut out = Object::default();
		// Add default fields
		out.insert("iat".to_string(), v.iat.into());
		out.insert("exp".to_string(), v.exp.into());
		out.insert("iss".to_string(), v.iss.into());
		// Add nbf field if set
		if let Some(nbf) = v.nbf {
			out.insert("nbf".to_string(), nbf.into());
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
		// Add NS field if set
		if let Some(id) = v.id {
			out.insert("ID".to_string(), id.into());
		}
		// Return value
		out.into()
	}
}
