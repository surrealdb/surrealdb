/// A macro that allows lazily parsing a value from the environment variable,
/// with a fallback default value if the variable is not set or parsing fails.
///
/// # Parameters
///
/// - `$key`: An expression representing the name of the environment variable.
/// - `$t`: The type of the value to be parsed.
/// - `$default`: The default value to fall back to if the environment variable
///   is not set or parsing fails.
///
/// # Return Value
///
/// A lazy static variable of type `std::sync::LazyLock`, which holds the parsed value
/// from the environment variable or the default value.
#[macro_export]
macro_rules! lazy_env_parse {
	// With no default specified
	($key:expr, Option<String>) => {
		std::sync::LazyLock::new(|| std::env::var($key).ok())
	};
	// With no default specified
	($key:expr, $t:ty) => {
		std::sync::LazyLock::new(|| {
			std::env::var($key).ok().and_then(|s| s.parse::<$t>().ok()).unwrap_or_default()
		})
	};
	// With a closure for the default value
	($key:expr, $t:ty, || $default:expr) => {
		std::sync::LazyLock::new(|| {
			std::env::var($key).ok().and_then(|s| s.parse::<$t>().ok()).unwrap_or_else(|| $default)
		})
	};
	// With a static expression for the default value
	($key:expr, $t:ty, $default:expr) => {
		std::sync::LazyLock::new(|| {
			std::env::var($key).ok().and_then(|s| s.parse::<$t>().ok()).unwrap_or($default)
		})
	};
	// With a closure for the default value, allowing for byte suffixes
	(bytes, $key:expr, $t:ty, || $default:expr) => {
		std::sync::LazyLock::new(|| {
			std::env::var($key)
				.ok()
				.and_then(|s| {
					use $crate::str::ParseBytes;
					s.parse_bytes::<$t>().ok()
				})
				.unwrap_or_else(|| $default)
		})
	};
	// With a static expression for the default value, allowing for byte suffixes
	(bytes, $key:expr, $t:ty, $default:expr) => {
		std::sync::LazyLock::new(|| {
			std::env::var($key)
				.ok()
				.and_then(|s| {
					use $crate::str::ParseBytes;
					s.parse_bytes::<$t>().ok()
				})
				.unwrap_or($default)
		})
	};
}

/// Creates a new b-tree map of key-value pairs.
///
/// This macro creates a new map, clones the items
/// from the secondary map, and inserts additional
/// items to the new map.
#[macro_export]
macro_rules! map {
    ($($k:expr $(, if let $grant:pat = $check:expr)? $(, if $guard:expr)? => $v:expr),* $(,)? $( => $x:expr )?) => {{
        let mut m = ::std::collections::BTreeMap::new();
    	$(m.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)?
		$( $(if let $grant = $check)? $(if $guard)? { m.insert($k, $v); };)+
        m
    }};
}

/// Extends a b-tree map of key-value pairs.
///
/// This macro extends the supplied map, by cloning
/// the items from the secondary map into it.
#[macro_export]
macro_rules! mrg {
	($($m:expr, $x:expr)+) => {{
		$($m.extend($x.iter().map(|(k, v)| (k.clone(), v.clone())));)+
		$($m)+
	}};
}

/// Throws an unreachable error with location details
macro_rules! fail {
	($($arg:tt)+) => {
		return Err(::anyhow::Error::new($crate::err::Error::unreachable(format_args!($($arg)*))))
	};
}

/// Converts some text into a new line byte string
macro_rules! bytes {
	($expression:expr) => {
		format!("{}\n", $expression).into_bytes()
	};
}

/// Pauses and yields execution to the tokio runtime
macro_rules! yield_now {
	() => {
		if tokio::runtime::Handle::try_current().is_ok() {
			tokio::task::consume_budget().await;
		}
	};
}

/// Matches on a specific config environment
macro_rules! get_cfg {
	($i:ident : $($s:expr),+) => (
		let $i = || { $( if cfg!($i=$s) { return $s; } );+ "unknown"};
	)
}

/// Runs a method on a transaction, ensuring that the transaction
/// is cancelled and rolled back if the initial function fails.
/// This can be used to ensure that the use of the `?` operator to
/// fail fast and return an error from a function does not leave
/// a transaction in an uncommitted state without rolling back.
macro_rules! catch {
	($txn:ident, $default:expr) => {
		match $default {
			Err(e) => {
				let _ = $txn.cancel().await;
				return Err(e);
			}
			Ok(v) => v,
		}
	};
}

/// Runs a method on a transaction, ensuring that the transaction
/// is cancelled and rolled back if the initial function fails, or
/// committed successfully if the initial function succeeds. This
/// can be used to ensure that the use of the `?` operator to fail
/// fast and return an error from a function does not leave a
/// transaction in an uncommitted state without rolling back.
macro_rules! run {
	($txn:ident, $default:expr) => {
		match $default {
			Err(e) => {
				let _ = $txn.cancel().await;
				Err(e)
			}
			Ok(v) => match $txn.commit().await {
				Err(e) => {
					let _ = $txn.cancel().await;
					Err(e)
				}
				Ok(_) => Ok(v),
			},
		}
	};
}

#[cfg(test)]
mod test {
	use crate::err::Error;

	fn fail_func() -> Result<(), anyhow::Error> {
		fail!("Reached unreachable code");
	}

	fn fail_func_args() -> Result<(), anyhow::Error> {
		fail!("Found {} but expected {}", "test", "other");
	}

	#[test]
	fn fail_literal() {
		let Ok(Error::Unreachable(msg)) = fail_func().unwrap_err().downcast() else {
			panic!()
		};
		assert_eq!("crates/core/src/mac/mod.rs:168: Reached unreachable code", msg);
	}

	#[test]
	fn fail_call() {
		let Error::Unreachable(msg) = Error::unreachable("Reached unreachable code") else {
			panic!()
		};
		assert_eq!("crates/core/src/mac/mod.rs:185: Reached unreachable code", msg);
	}

	#[test]
	fn fail_arguments() {
		let Ok(Error::Unreachable(msg)) = fail_func_args().unwrap_err().downcast() else {
			panic!()
		};
		assert_eq!("crates/core/src/mac/mod.rs:172: Found test but expected other", msg);
	}
}
