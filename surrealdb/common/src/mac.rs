/// A macro that allows lazily parsing a value from the environment variable,
/// with a fallback default value if the variable is not set or parsing fails.
///
/// # Parameters
///
/// - `$key`: An expression representing the name of the environment variable.
/// - `$t`: The type of the value to be parsed.
/// - `$default`: The default value to fall back to if the environment variable is not set or
///   parsing fails.
///
/// # Return Value
///
/// A lazy static variable of type `std::sync::LazyLock`, which holds the parsed
/// value from the environment variable or the default value.
#[macro_export]
macro_rules! lazy_env_parse {
	// With no default specified
	($key:expr_2021, Option<String>) => {
		std::sync::LazyLock::new(|| std::env::var($key).ok())
	};
	// With no default specified
	($key:expr_2021, $t:ty) => {
		std::sync::LazyLock::new(|| {
			std::env::var($key).ok().and_then(|s| s.parse::<$t>().ok()).unwrap_or_default()
		})
	};
	// With a closure for the default value
	($key:expr_2021, $t:ty, || $default:expr_2021) => {
		std::sync::LazyLock::new(|| {
			std::env::var($key).ok().and_then(|s| s.parse::<$t>().ok()).unwrap_or_else(|| $default)
		})
	};
	// With a static expression for the default value
	($key:expr_2021, $t:ty, $default:expr_2021) => {
		std::sync::LazyLock::new(|| {
			std::env::var($key).ok().and_then(|s| s.parse::<$t>().ok()).unwrap_or($default)
		})
	};
	// With a closure for the default value, allowing for byte suffixes
	(bytes, $key:expr_2021, $t:ty, || $default:expr_2021) => {
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
	(bytes, $key:expr_2021, $t:ty, $default:expr_2021) => {
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
	// With a closure for the default value, allowing for byte suffixes
	(duration, $key:expr_2021, $t:ty, || $default:expr_2021) => {
		std::sync::LazyLock::new(|| {
			std::env::var($key)
				.ok()
				.and_then(|s| {
					use $crate::str::ParseDuration;
					s.parse_duration::<$t>().ok()
				})
				.unwrap_or_else(|| $default)
		})
	};
	// With a static expression for the default value, allowing for byte suffixes
	(duration, $key:expr_2021, $t:ty, $default:expr_2021) => {
		std::sync::LazyLock::new(|| {
			std::env::var($key)
				.ok()
				.and_then(|s| {
					use $crate::str::ParseDuration;
					s.parse_duration::<$t>().ok()
				})
				.unwrap_or($default)
		})
	};
}
