//! Generic string-pair configuration map used to configure subsystems from
//! environment variables, connection-string query parameters, or code.

use std::collections::HashMap;
use std::str::FromStr;

use tracing::warn;

mod duration;
pub use duration::{format_duration, parse_duration};

/// A map with a set of configuration values stored as pairs of strings.
#[derive(Clone, Debug)]
pub struct ConfigMap {
	values: HashMap<String, String>,
}

impl Default for ConfigMap {
	fn default() -> Self {
		Self::empty()
	}
}

impl ConfigMap {
	/// Returns an empty map.
	pub fn empty() -> Self {
		ConfigMap {
			values: HashMap::new(),
		}
	}

	/// Adds a new key and value into the config map, will overwrite existing values.
	pub fn with_key_value<K, V>(mut self, key: K, value: V) -> Self
	where
		String: From<K>,
		String: From<V>,
	{
		self.values.insert(key.into(), value.into());
		self
	}

	/// Creates a config map from all the environment variables prefixed with `SURREAL_`
	pub fn from_env() -> Self {
		Self::from_env_prefix("SURREAL_")
	}

	/// Creates a config map from all the environment variables prefixed with the specific prefix.
	pub fn from_env_prefix(prefix: &str) -> Self {
		let mut values = HashMap::new();
		for (k, v) in std::env::vars() {
			let Some(x) = k.strip_prefix(prefix) else {
				continue;
			};

			let key_name = x.to_lowercase();
			values.insert(key_name, v);
		}
		ConfigMap {
			values,
		}
	}

	/// Map all the keys in the config map with the given closure.
	pub fn map_keys<F: FnMut(String) -> String>(self, mut f: F) -> Self {
		Self {
			values: self.values.into_iter().map(|(k, v)| (f(k), v)).collect(),
		}
	}

	/// Creates a config map from all the environment variables
	pub fn from_config_string(s: &str) -> Self {
		let values = s
			.split('&')
			.filter_map(|x| {
				let (k, v) = x.split_once('=')?;
				Some((k.to_lowercase(), v.to_string()))
			})
			.collect();

		ConfigMap {
			values,
		}
	}

	/// Join two config maps together prefering the values not in self.
	pub fn join(mut self, other: ConfigMap) -> Self {
		for (k, v) in other.values {
			self.values.insert(k, v);
		}
		self
	}

	/// Load a config type from the map.
	pub fn load<C: Config>(&self) -> C {
		let mut def = C::default();
		def.parse(self);
		def
	}

	/// Parse a value out of the map if it exists.
	///
	/// If either the key does not exist or the parsing values the value is unaltered.
	pub fn parse_key<S: FromStr>(&self, key: &str, value: &mut S) -> &Self {
		self.parse_key_with(key, value, |x| S::from_str(x).ok())
	}

	pub fn parse_key_option<S: FromStr>(&self, key: &str, value: &mut Option<S>) -> &Self {
		self.parse_key_with(key, value, |x| S::from_str(x).ok().map(Some))
	}

	/// Parse a boolean out of the map if it exists.
	///
	/// If either the key does not exist or the parsing values the value is unaltered.
	pub fn parse_key_bool(&self, key: &str, value: &mut bool) -> &Self {
		self.parse_key_with(key, value, |x| {
			if x.eq_ignore_ascii_case("true") || x == "1" {
				Some(true)
			} else if x.eq_ignore_ascii_case("false") || x == "0" {
				Some(false)
			} else {
				None
			}
		})
	}

	/// Parse a value out of the map if it exists.
	/// Takes a closure which can be used to define how to parse the string
	///
	/// If either the key does not exist or the parsing closure returns `None` the value is
	/// unaltered.
	pub fn parse_key_with<R, F: FnOnce(&str) -> Option<R>>(
		&self,
		key: &str,
		value: &mut R,
		f: F,
	) -> &Self {
		let Some(v) = self.values.get(key) else {
			return self;
		};

		let Some(v) = f(v) else {
			warn!("Could not parse configuration value for key `{}`", key.to_uppercase());
			return self;
		};

		*value = v;
		self
	}

	pub fn has_key(&self, key: &str) -> bool {
		self.values.contains_key(key)
	}
}

/// Trait for types which contain configureation information.
pub trait Config: Default {
	fn parse(&mut self, map: &ConfigMap);
}
