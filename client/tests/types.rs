#![cfg(any(feature = "ws", feature = "http"))]

use serde::Deserialize;
use serde::Serialize;

pub const NS: &str = "test-ns";
pub const DB: &str = "test-db";
pub const ROOT_USER: &str = "root";
pub const ROOT_PASS: &str = "root";
pub const DB_ENDPOINT: &str = "localhost:8000";

#[derive(Debug, Serialize)]
pub struct Record<'a> {
	pub name: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct RecordId {
	pub id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthParams<'a> {
	pub email: &'a str,
	pub pass: &'a str,
}
