/// Converts some text into a new line byte string
macro_rules! bytes {
	($expression:expr) => {
		format!("{}\n", $expression).into_bytes()
	};
}

/// Creates a new b-tree map of key-value pairs
macro_rules! map {
    ($($k:expr => $v:expr),* $(,)?) => {{
        ::std::collections::BTreeMap::from([
            $(($k, $v),)+
        ])
    }};
}

/// Matches on a specific config environment
macro_rules! get_cfg {
	($i:ident : $($s:expr),+) => (
		let $i = || { $( if cfg!($i=$s) { return $s; } );+ "unknown"};
	)
}

/// Parses a set of SurrealQL statements at compile time
///
/// # Examples
///
/// ```rust,no_run
/// # use surrealdb::Datastore;
/// # use surrealdb::Error;
/// # #[tokio::main]
/// # fn main() -> Result<(), Error> {
/// let query = sql!("LET $name = 'Tobie'; SELECT * FROM user WHERE name = $name;");
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! sql {
	($expression:expr) => {
		match $crate::sql::parse($expression) {
			Ok(v) => v,
			Err(e) => compile_error!(e.to_string()),
		}
	};
}
