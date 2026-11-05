//! Schema and database namespace resolution per SQL dialect (Decision D4).
//!
//! - **PostgreSQL**:
//!   - `table` -> `table`
//!   - `public.table` -> `table` (`public` schema is elided to plain table name)
//!   - `schema.table` -> `schema.table` (dotted table name preserving cross-schema joins/FKs)
//!   - `db.schema.table` -> rejected if `db` differs from current database, otherwise resolves
//!     `schema.table`
//!   - 4+ parts -> rejected
//!
//! - **MySQL**:
//!   - `table` -> `table`
//!   - `db.table` -> `table` if `db` matches current database, otherwise rejected as cross-database
//!     query
//!   - 3+ parts -> rejected

use crate::catalog::BinderCatalog;
use crate::error::PlanError;

/// Resolves a PostgreSQL table name from qualified name parts per Decision D4.
///
/// The parts are parser-produced names: unquoted names are already lowercase, and the remaining
/// spelling is significant. Returns `(Option<schema>, resolved_table_name)`.
pub fn resolve_pgsql_table_name(
	parts: &[String],
	catalog: Option<&dyn BinderCatalog>,
) -> Result<(Option<String>, String), PlanError> {
	match parts.len() {
		0 => Err(PlanError::Custom("empty table name".into())),
		1 => {
			let tbl = parts[0].clone();
			Ok((None, tbl))
		}
		2 => {
			let schema = parts[0].clone();
			let table = parts[1].clone();
			if schema == "public" {
				// PostgreSQL public schema is elided to plain table name
				Ok((Some("public".to_string()), table))
			} else {
				// Other schemas are dotted table names: schema.table
				let dotted = format!("{schema}.{table}");
				Ok((Some(schema), dotted))
			}
		}
		3 => {
			let db = parts[0].clone();
			let schema = parts[1].clone();
			let table = parts[2].clone();

			if let Some(cat) = catalog
				&& let Some(cur_db) = cat.current_database()
				&& db != cur_db
			{
				return Err(PlanError::Custom(format!(
					"cross-database queries not supported: {}",
					parts.join(".")
				)));
			}

			if schema == "public" {
				Ok((Some("public".to_string()), table))
			} else {
				let dotted = format!("{schema}.{table}");
				Ok((Some(schema), dotted))
			}
		}
		_ => Err(PlanError::Custom(format!(
			"unsupported table qualification depth: {}",
			parts.join(".")
		))),
	}
}

/// Resolves a MySQL table name from qualified name parts per Decision D4.
///
/// Returns `(Option<database>, resolved_table_name)`.
pub fn resolve_mysql_table_name(
	parts: &[String],
	catalog: Option<&dyn BinderCatalog>,
) -> Result<(Option<String>, String), PlanError> {
	match parts.len() {
		0 => Err(PlanError::Custom("empty table name".into())),
		1 => {
			let tbl = parts[0].clone();
			Ok((None, tbl))
		}
		2 => {
			let db = parts[0].clone();
			let table = parts[1].clone();

			if let Some(cat) = catalog
				&& let Some(cur_db) = cat.current_database()
				&& !db.eq_ignore_ascii_case(cur_db)
			{
				return Err(PlanError::Custom(format!(
					"cross-database queries not supported: {}",
					parts.join(".")
				)));
			}

			Ok((Some(db), table))
		}
		_ => Err(PlanError::Custom(format!(
			"database qualification depth not supported: {}",
			parts.join(".")
		))),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::MemoryCatalog;

	#[test]
	fn test_pgsql_namespace_resolution() {
		let mut cat = MemoryCatalog::new();
		cat.set_database(Some("mydb".to_string()));

		// Bare table
		let parts = vec!["users".to_string()];
		assert_eq!(
			resolve_pgsql_table_name(&parts, Some(&cat)).unwrap(),
			(None, "users".to_string())
		);

		// Public schema is elided
		let parts = vec!["public".to_string(), "users".to_string()];
		assert_eq!(
			resolve_pgsql_table_name(&parts, Some(&cat)).unwrap(),
			(Some("public".to_string()), "users".to_string())
		);

		// Custom schema becomes dotted table name
		let parts = vec!["auth".to_string(), "users".to_string()];
		assert_eq!(
			resolve_pgsql_table_name(&parts, Some(&cat)).unwrap(),
			(Some("auth".to_string()), "auth.users".to_string())
		);

		// 3-part matching database
		let parts = vec!["mydb".to_string(), "public".to_string(), "users".to_string()];
		assert_eq!(
			resolve_pgsql_table_name(&parts, Some(&cat)).unwrap(),
			(Some("public".to_string()), "users".to_string())
		);

		let parts = vec!["mydb".to_string(), "auth".to_string(), "users".to_string()];
		assert_eq!(
			resolve_pgsql_table_name(&parts, Some(&cat)).unwrap(),
			(Some("auth".to_string()), "auth.users".to_string())
		);

		// 3-part cross-database rejected
		let parts = vec!["otherdb".to_string(), "public".to_string(), "users".to_string()];
		assert!(resolve_pgsql_table_name(&parts, Some(&cat)).is_err());

		// 4-part rejected
		let parts = vec!["a".into(), "b".into(), "c".into(), "d".into()];
		assert!(resolve_pgsql_table_name(&parts, Some(&cat)).is_err());
	}

	#[test]
	fn test_mysql_namespace_resolution() {
		let mut cat = MemoryCatalog::new();
		cat.set_database(Some("mydb".to_string()));

		// Bare table
		let parts = vec!["users".to_string()];
		assert_eq!(
			resolve_mysql_table_name(&parts, Some(&cat)).unwrap(),
			(None, "users".to_string())
		);

		// 2-part matching database
		let parts = vec!["mydb".to_string(), "users".to_string()];
		assert_eq!(
			resolve_mysql_table_name(&parts, Some(&cat)).unwrap(),
			(Some("mydb".to_string()), "users".to_string())
		);

		// 2-part cross-database rejected
		let parts = vec!["otherdb".to_string(), "users".to_string()];
		assert!(resolve_mysql_table_name(&parts, Some(&cat)).is_err());

		// 3-part rejected
		let parts = vec!["a".into(), "b".into(), "c".into()];
		assert!(resolve_mysql_table_name(&parts, Some(&cat)).is_err());
	}
}
