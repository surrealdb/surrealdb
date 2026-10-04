//! Dialect policies for SQL binding and execution semantics.

/// SQL dialect targeted by the binder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlDialect {
	Postgres,
	MySql,
}

/// Policy for case-folding and matching identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NamePolicy {
	/// PostgreSQL: unquoted identifiers are case-folded to lowercase.
	/// Quoted identifiers preserve exact case.
	PgLowercaseUnquoted,

	/// MySQL: identifier case sensitivity depends on system variables.
	/// Default is case-insensitive for table & column names on case-insensitive filesystems.
	MySqlCaseInsensitive,
}

/// Default ordering of NULL values in `ORDER BY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullOrderPolicy {
	/// PostgreSQL: ASC defaults to NULLS LAST, DESC defaults to NULLS FIRST.
	PgDefault,

	/// MySQL: ASC defaults to NULLS FIRST, DESC defaults to NULLS LAST.
	MySqlDefault,
}

/// Dialect-specific policy settings configured for the binder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DialectPolicy {
	pub dialect: SqlDialect,
	pub name_policy: NamePolicy,
	pub null_ordering: NullOrderPolicy,
	/// Whether `GROUP BY` allows target column aliases (e.g. `SELECT a + 1 AS b ... GROUP BY b`).
	/// MySQL permits this; standard PostgreSQL requires the full expression or ordinal.
	pub group_by_alias: bool,
	/// Whether `GROUP BY` allows 1-based positional numbers (e.g. `GROUP BY 1`).
	pub group_by_ordinal: bool,
	/// Whether `ORDER BY` allows 1-based positional numbers (e.g. `ORDER BY 1`).
	pub order_by_ordinal: bool,
	/// Default schema namespace (e.g. `public` for PostgreSQL).
	pub default_schema: Option<&'static str>,
}

impl DialectPolicy {
	/// Standard policy for PostgreSQL.
	pub const fn postgres() -> Self {
		Self {
			dialect: SqlDialect::Postgres,
			name_policy: NamePolicy::PgLowercaseUnquoted,
			null_ordering: NullOrderPolicy::PgDefault,
			group_by_alias: false,
			group_by_ordinal: true,
			order_by_ordinal: true,
			default_schema: Some("public"),
		}
	}

	/// Standard policy for MySQL.
	pub const fn mysql() -> Self {
		Self {
			dialect: SqlDialect::MySql,
			name_policy: NamePolicy::MySqlCaseInsensitive,
			null_ordering: NullOrderPolicy::MySqlDefault,
			group_by_alias: true,
			group_by_ordinal: true,
			order_by_ordinal: true,
			default_schema: None,
		}
	}

	/// Fold an unquoted identifier according to the dialect's name policy.
	pub fn fold_identifier<'a>(&self, name: &'a str) -> std::borrow::Cow<'a, str> {
		match self.name_policy {
			NamePolicy::PgLowercaseUnquoted => {
				if name.chars().any(|c| c.is_ascii_uppercase()) {
					std::borrow::Cow::Owned(name.to_ascii_lowercase())
				} else {
					std::borrow::Cow::Borrowed(name)
				}
			}
			NamePolicy::MySqlCaseInsensitive => {
				// Normalize to lowercase for internal matching
				std::borrow::Cow::Owned(name.to_ascii_lowercase())
			}
		}
	}

	/// Compare two identifiers under the dialect's policy.
	pub fn idents_match(&self, a: &str, b: &str) -> bool {
		match self.name_policy {
			NamePolicy::PgLowercaseUnquoted => a == b,
			NamePolicy::MySqlCaseInsensitive => a.eq_ignore_ascii_case(b),
		}
	}
}
