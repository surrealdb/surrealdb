pub mod base;
pub mod clear;
pub mod parse;
pub mod signin;
pub mod signup;
pub mod token;
pub mod verify;

pub const LOG: &str = "surrealdb::iam";
pub const TOKEN: &str = "Bearer ";
pub const DEFAULT_ROOT_USER: &str = "root";
pub const DEFAULT_ROOT_PASS: &str = "surrealdb";
