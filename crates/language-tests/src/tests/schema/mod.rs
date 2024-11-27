//! Module defining the configuration schema.

mod bytes_hack;

use std::{collections::BTreeMap, fmt, str::FromStr};

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use surrealdb_core::{
	dbs::capabilities::{FuncTarget, MethodTarget, NetTarget, RouteTarget},
	sql::Value as CoreValue,
	syn,
};

/// Root test config struct.
#[derive(Default, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TestConfig {
	pub env: Option<TestEnv>,
	pub test: Option<TestDetails>,
	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

impl TestConfig {
	/// Returns the namespace if the environement specifies one, none otherwise
	pub fn namespace(&self) -> Option<&str> {
		self.env.as_ref().map(|x| x.namespace()).unwrap_or(Some("test"))
	}

	/// Returns the namespace if the environement specifies one, none otherwise
	pub fn database(&self) -> Option<&str> {
		self.env.as_ref().map(|x| x.database()).unwrap_or(Some("test"))
	}

	/// Returns true if the test should be run.
	/// returns false if the test is configured to be skipped.
	pub fn should_run(&self) -> bool {
		self.test.as_ref().map(|x| x.should_run()).unwrap_or(true)
	}

	pub fn is_wip(&self) -> bool {
		self.test.as_ref().map(|x| x.is_wip()).unwrap_or(false)
	}

	pub fn issue(&self) -> Option<u64> {
		self.test.as_ref().and_then(|x| x.issue())
	}

	/// Returns the imports for this file, empty if no imports are defined.
	pub fn imports(&self) -> &[Utf8PathBuf] {
		self.env.as_ref().and_then(|x| x.imports.as_ref()).map(|x| x.as_slice()).unwrap_or(&[])
	}

	/// Returns if this test must be run without other test running.
	pub fn should_run_sequentially(&self) -> bool {
		self.env.as_ref().map(|x| x.sequential).unwrap_or(false)
	}

	/// Whether this test can use one of the datastorage struct which are reused between tests.
	pub fn can_use_reusable_ds(&self) -> bool {
		self.env.as_ref().map(|x| !x.clean).unwrap_or(true)
	}

	pub fn unused_keys(&self) -> Vec<String> {
		let mut res: Vec<_> = self._unused_keys.keys().map(|x| x.clone()).collect();

		if let Some(x) = self.env.as_ref() {
			res.append(&mut x.unused_keys())
		}

		if let Some(x) = self.test.as_ref() {
			res.append(&mut x.unused_keys())
		}

		res
	}
}

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TestEnv {
	#[serde(default)]
	pub sequential: bool,
	#[serde(default)]
	pub clean: bool,
	pub namespace: Option<BoolOr<String>>,
	pub database: Option<BoolOr<String>>,
	pub imports: Option<Vec<Utf8PathBuf>>,
	pub timeout: Option<BoolOr<u64>>,
	pub capabilities: Option<BoolOr<Capabilities>>,

	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

impl TestEnv {
	/// Returns the namespace if the environement specifies one, none otherwise
	///
	/// Defaults to "test"
	pub fn namespace(&self) -> Option<&str> {
		if let Some(x) = &self.namespace {
			x.as_ref().map(|x| x.as_str()).to_value("test")
		} else {
			Some("test")
		}
	}

	/// Returns the namespace if the environement specifies one, none otherwise
	///
	/// Defaults to "test"
	pub fn database(&self) -> Option<&str> {
		if let Some(x) = &self.database {
			x.as_ref().map(|x| x.as_str()).to_value("test")
		} else {
			Some("test")
		}
	}

	pub fn timeout(&self) -> Option<u64> {
		self.timeout.map(|x| x.to_value(1000)).unwrap_or(Some(1000))
	}

	pub fn unused_keys(&self) -> Vec<String> {
		let mut res: Vec<_> = self._unused_keys.keys().map(|x| format!("env.{x}")).collect();

		if let Some(x) = self.capabilities.as_ref() {
			if let BoolOr::Value(x) = x {
				res.append(&mut x.unused_keys());
			}
		}

		res
	}
}

pub enum TestResultFlat {
	Value(SurrealValue),
	Error(BoolOr<String>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum TestResult {
	/// The result is a nomral value
	Plain(SurrealValue),
	/// The result should be an error.
	Error(ErrorTestResult),
	/// The result is a value but specified as a table.
	Value(ValueTestResult),
}

impl TestResult {
	pub fn rough_match(&self) -> bool {
		match self {
			TestResult::Value(x) => x.rough.unwrap_or(false),
			_ => false,
		}
	}

	pub fn flatten(self) -> TestResultFlat {
		match self {
			TestResult::Plain(x) => TestResultFlat::Value(x),
			TestResult::Error(e) => TestResultFlat::Error(e.error),
			TestResult::Value(x) => TestResultFlat::Value(x.value),
		}
	}
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ErrorTestResult {
	pub error: BoolOr<String>,

	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ValueTestResult {
	pub value: SurrealValue,
	pub rough: Option<bool>,

	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

/// A enum for when configuration which can be disabled, enabled or configured to have a specific
/// value.
///
/// # Example
/// ```toml
/// # Sets the timeout enabled to the default value
/// [env]
/// timeout = true
///
/// # Set the timeout as enabeled with the value of 1000ms
/// [env]
/// timeout = 1000
/// ```
#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum BoolOr<T> {
	Bool(bool),
	Value(T),
}

impl<T> BoolOr<T> {
	pub fn as_ref(&self) -> BoolOr<&T> {
		match *self {
			BoolOr::Bool(x) => BoolOr::Bool(x),
			BoolOr::Value(ref x) => BoolOr::Value(x),
		}
	}

	pub fn map<R, F: FnOnce(T) -> R>(self, m: F) -> BoolOr<R> {
		match self {
			BoolOr::Bool(x) => BoolOr::Bool(x),
			BoolOr::Value(v) => BoolOr::Value(m(v)),
		}
	}

	/// Returns the value of this bool/or returning the default in case of BoolOr::Bool(true), the value in
	/// case of BoolOr::Value(_) or None in case of BoolOr::Bool(false)
	pub fn to_value(self, default: T) -> Option<T> {
		match self {
			BoolOr::Bool(false) => None,
			BoolOr::Bool(true) => Some(default),
			BoolOr::Value(x) => Some(x),
		}
	}

	/// Returns the value of this bool/or returning the default in case of BoolOr::Bool(true), the value in
	/// case of BoolOr::Value(_) or None in case of BoolOr::Bool(false)
	pub fn with_to_value<F: FnOnce() -> T>(self, default: F) -> Option<T> {
		match self {
			BoolOr::Bool(false) => None,
			BoolOr::Bool(true) => Some(default()),
			BoolOr::Value(x) => Some(x),
		}
	}
}

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TestDetails {
	pub results: Option<TestDetailsResults>,
	pub reason: Option<String>,
	run: Option<bool>,
	issue: Option<u64>,
	wip: Option<bool>,
	pub fuzzing_reproduction: Option<String>,

	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

impl TestDetails {
	/// Returns whether this test should be run.
	pub fn should_run(&self) -> bool {
		self.run.unwrap_or(true)
	}

	/// Returns the whether this test is tests a work in progress feature.
	pub fn is_wip(&self) -> bool {
		self.wip.unwrap_or(false)
	}

	/// Returns the issue number for this test if any exists.
	pub fn issue(&self) -> Option<u64> {
		self.issue
	}

	pub fn unused_keys(&self) -> Vec<String> {
		let mut res: Vec<_> = self._unused_keys.keys().map(|x| format!("test.{x}")).collect();

		if let Some(results) = self.results.as_ref() {
			match results {
				TestDetailsResults::QueryResult(x) => {
					for (idx, r) in x.iter().enumerate() {
						match r {
							TestResult::Plain(_) => {}
							TestResult::Error(e) => res.append(
								&mut e
									._unused_keys
									.keys()
									.map(|x| format!("test.results[{idx}].{x}"))
									.collect(),
							),
							TestResult::Value(e) => res.append(
								&mut e
									._unused_keys
									.keys()
									.map(|x| format!("test.results[{idx}].{x}"))
									.collect(),
							),
						}
					}
				}
				TestDetailsResults::ParserError(x) => res.append(
					&mut x._unused_keys.keys().map(|x| format!("test.results.{x}")).collect(),
				),
			}
		}
		res
	}
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
#[serde(rename_all = "kebab-case")]
pub enum TestDetailsResults {
	QueryResult(Vec<TestResult>),
	ParserError(ParsingTestResult),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ParsingTestResult {
	pub parsing_error: BoolOr<String>,
	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

#[derive(Clone, Debug)]
pub struct SurrealValue(pub CoreValue);

impl Serialize for SurrealValue {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let v = self.0.to_string();
		v.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for SurrealValue {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let source = String::deserialize(deserializer)?;
		let mut v = syn::value(&source).map_err(|x| <D::Error as serde::de::Error>::custom(x))?;
		bytes_hack::compute_bytes_inplace(&mut v);
		Ok(SurrealValue(v))
	}
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Capabilities {
	pub scripting: Option<bool>,
	pub quest_access: Option<bool>,
	pub live_query_notifications: Option<bool>,

	pub allow_functions: Option<BoolOr<Vec<SchemaTarget<FuncTarget>>>>,
	pub deny_functions: Option<BoolOr<Vec<SchemaTarget<FuncTarget>>>>,

	pub allow_net: Option<BoolOr<Vec<SchemaTarget<NetTarget>>>>,
	pub deny_net: Option<BoolOr<Vec<SchemaTarget<NetTarget>>>>,

	pub allow_rpc: Option<BoolOr<Vec<SchemaTarget<MethodTarget>>>>,
	pub deny_rpc: Option<BoolOr<Vec<SchemaTarget<MethodTarget>>>>,

	pub allow_http: Option<BoolOr<Vec<SchemaTarget<RouteTarget>>>>,
	pub deny_http: Option<BoolOr<Vec<SchemaTarget<RouteTarget>>>>,

	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

#[derive(Clone, Debug)]
pub struct SchemaTarget<T>(pub T);

impl<'de, T: FromStr> Deserialize<'de> for SchemaTarget<T>
where
	T: FromStr,
	<T as FromStr>::Err: fmt::Display,
{
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let v = String::deserialize(deserializer)?;
		v.parse().map(SchemaTarget).map_err(|x| <D::Error as serde::de::Error>::custom(x))
	}
}

impl<T: fmt::Display> Serialize for SchemaTarget<T> {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		self.0.to_string().serialize(serializer)
	}
}

impl Capabilities {
	pub fn unused_keys(&self) -> Vec<String> {
		self._unused_keys.keys().map(|x| format!("env.capabilities.{x}")).collect()
	}
}
