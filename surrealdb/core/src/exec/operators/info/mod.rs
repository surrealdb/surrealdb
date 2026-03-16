//! INFO statement operators.
//!
//! These operators implement various INFO statements that return metadata
//! about the database schema and configuration:
//!
//! - `RootInfoPlan`: INFO FOR ROOT - returns root-level metadata
//! - `NamespaceInfoPlan`: INFO FOR NS - returns namespace metadata
//! - `DatabaseInfoPlan`: INFO FOR DB - returns database metadata
//! - `TableInfoPlan`: INFO FOR TABLE - returns table metadata
//! - `UserInfoPlan`: INFO FOR USER - returns user information
//! - `IndexInfoPlan`: INFO FOR INDEX - returns index building status

mod database;
mod index;
mod namespace;
mod root;
mod table;
mod user;

pub use database::DatabaseInfoPlan;
pub use index::IndexInfoPlan;
pub use namespace::NamespaceInfoPlan;
pub use root::RootInfoPlan;
pub use table::TableInfoPlan;
pub use user::UserInfoPlan;
