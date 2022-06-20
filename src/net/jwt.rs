use jsonwebtoken::{Algorithm, Header};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

pub static HEADER: Lazy<Header> = Lazy::new(|| Header::new(Algorithm::HS512));

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Claims {
	pub iat: i64,
	pub nbf: i64,
	pub exp: i64,
	pub iss: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ns: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub db: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub sc: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tk: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
}
