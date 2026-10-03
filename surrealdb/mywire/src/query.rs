//! MySQL query translation and execution helper.

use anyhow::{Result, bail};

/// Translate a MySQL query string into SurrealQL.
pub fn translate_query(sql: &str) -> Result<String> {
	match surrealdb_mysql::translate_sql(sql) {
		Ok(surql) => Ok(surql),
		Err(e) => bail!("Failed to translate MySQL query: {e}"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_translate_query() {
		let res = translate_query("SELECT 1").unwrap();
		assert_eq!(res, "SELECT 1;");
	}
}
