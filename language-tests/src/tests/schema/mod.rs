use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use semver::VersionReq;
use serde::{Deserialize, Serialize, de};
use surrealdb_core::syn::{self};
use surrealdb_rpc::capabilities::ArbitraryQueryTarget;
use surrealdb_rpc::capabilities::EvalQueryTarget;
use surrealdb_rpc::capabilities::ExperimentalTarget;
use surrealdb_rpc::capabilities::FuncTarget;
use surrealdb_rpc::capabilities::MethodTarget;
use surrealdb_rpc::capabilities::NetTarget;
use surrealdb_rpc::capabilities::NewPlannerStrategy;
use surrealdb_rpc::capabilities::RouteTarget;
use surrealdb_syn::parser::ParserSettings;
use surrealdb_types::{Object, RecordId, ToSql, Value};

use crate::cli::Backend;

fn bool_or_f<T>() -> BoolOr<T> {
	BoolOr::Bool(false)
}

fn default_planner_strategy() -> Vec<NewPlannerStrategyConfig> {
	vec![
		NewPlannerStrategyConfig::ComputeOnly,
		NewPlannerStrategyConfig::AllRo,
		NewPlannerStrategyConfig::BestEffortRo,
	]
}

fn t() -> bool {
	true
}

fn default_duration<const V: u64>() -> TestDuration {
	TestDuration(Duration::from_millis(V))
}

fn default_usize<const V: usize>() -> usize {
	V
}

pub const ENV_DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);
pub const ENV_DEFAULT_NAMESPACE: &str = "test";
pub const ENV_DEFAULT_DATABASE: &str = "test";

/// Root test config struct.
#[derive(Default, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TestConfig {
	#[serde(default)]
	pub env: TestEnv,
	#[serde(default)]
	pub test: TestDetails,
	#[serde(default)]
	pub bench: BenchDetails,
	#[serde(default)]
	pub graphql: GraphQlDetails,
	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

impl TestConfig {
	/// Returns a list of keys which are not in the schema but still defined.
	pub fn unused_keys(&self) -> Vec<String> {
		let mut res: Vec<_> = self._unused_keys.keys().cloned().collect();
		res.append(&mut self.env.unused_keys());
		res.append(&mut self.test.unused_keys());
		res.extend(self.bench._unused_keys.keys().cloned());
		res.extend(self.graphql._unused_keys.keys().map(|x| format!("graphql.{x}")));
		res
	}
}

/// Request options for GraphQL (`.graphql`) test cases.
///
/// Only consulted when the test's dialect is GraphQL; for SurrealQL tests the
/// section is unused (and flagged by the unused-key warning if present).
#[derive(Default, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct GraphQlDetails {
	/// Variables sent with the GraphQL request, e.g.
	/// `variables = { id = "person:1", min = 2 }`.
	#[serde(default)]
	pub variables: Option<toml::Table>,
	/// The operation to execute when the document defines multiple named
	/// operations (the GraphQL `operationName` request field).
	#[serde(default)]
	pub operation: Option<String>,
	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TestEnv {
	/// Should the test be run sequentially
	#[serde(default)]
	pub sequential: bool,
	/// Does the test keep values around in the datastore which can't be removed with a REMOVE NS
	#[serde(default)]
	pub clean: bool,
	/// Does the test make no modification to the datastore itself.
	#[serde(default)]
	pub readonly: bool,

	#[serde(default)]
	pub namespace: BoolOr<String>,
	#[serde(default)]
	pub database: BoolOr<String>,

	pub auth: Option<TestAuth>,
	pub signup: Option<SurrealObject>,
	pub signin: Option<SurrealObject>,

	#[serde(default)]
	pub imports: Vec<String>,
	#[serde(default)]
	pub timeout: BoolOr<TestDuration>,
	#[serde(default)]
	pub capabilities: BoolOr<Capabilities>,

	/// Specifies which backends this test should run on.
	/// If empty, the test runs on all backends.
	/// If specified, the test only runs when the selected backend is in this list.
	/// Valid values: "mem", "rocksdb", "surrealkv", "tikv"
	#[serde(default)]
	pub backend: Vec<Backend>,

	/// Whether the test requires MVCC versioning to be enabled on the datastore.
	/// When true, the datastore is created with `?versioned=true` in the connection string.
	#[serde(default)]
	pub versioned: bool,
	/// Planner strategies to run this test under.
	/// Defaults to `["compute-only", "all-ro"]` when omitted; the test is
	/// executed once per listed strategy.
	#[serde(default = "default_planner_strategy")]
	pub planner_strategy: Vec<NewPlannerStrategyConfig>,

	/// Whether EXPLAIN ANALYZE output omits elapsed durations, making
	/// output deterministic for test assertions. Defaults to true in the
	/// language test framework. Set to `false` explicitly if you need
	/// actual elapsed times.
	pub redact_volatile_explain_attrs: Option<bool>,

	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

impl Default for TestEnv {
	fn default() -> Self {
		Self {
			sequential: Default::default(),
			clean: Default::default(),
			readonly: Default::default(),
			namespace: Default::default(),
			database: Default::default(),
			auth: Default::default(),
			signup: None,
			signin: None,
			imports: Default::default(),
			timeout: Default::default(),
			capabilities: Default::default(),
			backend: Default::default(),
			versioned: Default::default(),
			planner_strategy: default_planner_strategy(),
			redact_volatile_explain_attrs: Default::default(),
			_unused_keys: Default::default(),
		}
	}
}

impl TestEnv {
	/// Returns the namespace if the environment specifies one, none otherwise
	///
	/// Defaults to "test"
	pub fn namespace(&self) -> Option<&str> {
		self.namespace.as_ref().map(|x| x.as_str()).into_value(ENV_DEFAULT_NAMESPACE)
	}

	/// Returns the namespace if the environment specifies one, none otherwise
	///
	/// Defaults to "test"
	pub fn database(&self) -> Option<&str> {
		self.database.as_ref().map(|x| x.as_str()).into_value(ENV_DEFAULT_DATABASE)
	}

	pub fn unused_keys(&self) -> Vec<String> {
		let mut res: Vec<_> = self._unused_keys.keys().map(|x| format!("env.{x}")).collect();

		if let BoolOr::Value(x) = self.capabilities.as_ref() {
			res.append(&mut x.unused_keys());
		}

		res
	}
}

/// Strategy for the new streaming planner/executor in language tests.
///
/// Maps to `surrealdb_rpc::capabilities::NewPlannerStrategy` but uses shorter
/// kebab-case names for TOML configuration.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum NewPlannerStrategyConfig {
	/// Try new planner, fall back on Unimplemented.
	BestEffortRo,
	/// Require new planner for all read-only statements (hard fail on Unimplemented).
	AllRo,
	/// Skip new planner entirely; always use legacy compute.
	ComputeOnly,
}

impl NewPlannerStrategyConfig {
	pub const DEFAULT_STRATEGIES: &[NewPlannerStrategyConfig] =
		&[NewPlannerStrategyConfig::ComputeOnly, NewPlannerStrategyConfig::AllRo];
}

impl fmt::Display for NewPlannerStrategyConfig {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::BestEffortRo => f.write_str("best-effort-ro"),
			Self::AllRo => f.write_str("all-ro"),
			Self::ComputeOnly => f.write_str("compute-only"),
		}
	}
}

impl From<NewPlannerStrategy> for NewPlannerStrategyConfig {
	fn from(strategy: NewPlannerStrategy) -> Self {
		match strategy {
			NewPlannerStrategy::BestEffortReadOnlyStatements => Self::BestEffortRo,
			NewPlannerStrategy::ComputeOnly => Self::ComputeOnly,
			NewPlannerStrategy::AllReadOnlyStatements => Self::AllRo,
		}
	}
}

impl From<NewPlannerStrategyConfig> for NewPlannerStrategy {
	fn from(strategy: NewPlannerStrategyConfig) -> Self {
		match strategy {
			NewPlannerStrategyConfig::BestEffortRo => {
				NewPlannerStrategy::BestEffortReadOnlyStatements
			}
			NewPlannerStrategyConfig::ComputeOnly => NewPlannerStrategy::ComputeOnly,
			NewPlannerStrategyConfig::AllRo => NewPlannerStrategy::AllReadOnlyStatements,
		}
	}
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum TestExpectation {
	// NOTE! Ordering of variants here is important.
	// Match must come before Error so that they are deserialized correctly.
	// Swapping match with error causes the error variant to be chosen when
	// match specifies if it expects an error.
	/// The result is a normal value
	Plain(SurrealConfigValue),
	/// The result is a value but specified as a table.
	Match(MatchTestResult),
	/// The result should be an error.
	Error(ErrorTestResult),
	/// The result is a value but specified as a table.
	Value(ValueTestResult),
}

fn to_deser_error<T: std::fmt::Display, D: serde::de::Error>(e: T) -> D {
	D::custom(e)
}

impl<'de> Deserialize<'de> for TestExpectation {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: de::Deserializer<'de>,
	{
		let v = toml::Value::deserialize(deserializer)?;
		if v.is_str() {
			SurrealConfigValue::deserialize(v).map_err(to_deser_error).map(TestExpectation::Plain)
		} else if let Some(x) = v.as_table() {
			if x.contains_key("match") {
				MatchTestResult::deserialize(v).map_err(to_deser_error).map(TestExpectation::Match)
			} else if x.contains_key("value") {
				ValueTestResult::deserialize(v).map_err(to_deser_error).map(TestExpectation::Value)
			} else if x.contains_key("error") {
				ErrorTestResult::deserialize(v).map_err(to_deser_error).map(TestExpectation::Error)
			} else {
				Err(to_deser_error(
					"Table does not match any the options, expected table to contain at least one `match`, `value` or `error` field",
				))
			}
		} else {
			Err(to_deser_error("Expected a string or a table"))
		}
	}
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ErrorTestResult {
	pub error: BoolOr<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ValueTestResult {
	pub value: SurrealConfigValue,
	#[serde(default)]
	pub skip_datetime: Option<bool>,
	#[serde(default)]
	pub skip_record_id_key: Option<bool>,
	#[serde(default)]
	pub skip_uuid: Option<bool>,
	#[serde(default)]
	pub skip_api_request_id: Option<bool>,
	#[serde(default)]
	pub float_roughly_eq: Option<bool>,
	#[serde(default)]
	pub decimal_roughly_eq: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct MatchTestResult {
	#[serde(rename = "match")]
	pub _match: SurrealExpr,
	#[serde(default)]
	pub error: Option<bool>,
}

/// Duration which deserializes from both a string as well as a number.
#[derive(Clone, Copy, Debug, Default)]
pub struct TestDuration(pub Duration);

impl<'de> Deserialize<'de> for TestDuration {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		#[derive(Deserialize)]
		#[serde(untagged)]
		enum NumberOrString {
			String(String),
			Number(u64),
		}

		match NumberOrString::deserialize(deserializer)? {
			NumberOrString::Number(x) => Ok(Self(Duration::from_millis(x))),
			NumberOrString::String(x) => {
				let settings = ParserSettings {
					object_recursion_limit: 100,
					query_recursion_limit: 100,
					expr_recursion_limit: 100,
					legacy_strands: false,
					flexible_record_id: true,
					files_enabled: true,
					surrealism_enabled: true,
					json_string_escapes: false,
				};

				let v = syn::parse_with_settings(x.as_bytes(), settings, async |parser, stk| {
					parser.parse_value(stk).await
				})
				.map_err(<D::Error as serde::de::Error>::custom)?;
				let Value::Duration(x) = v else {
					return Err(<D::Error as serde::de::Error>::custom("Invalid duration"));
				};
				Ok(Self(x.into_inner()))
			}
		}
	}
}

impl Serialize for TestDuration {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		format!("{:?}", self.0).serialize(serializer)
	}
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
/// # Set the timeout as enabled with the value of 1000ms
/// [env]
/// timeout = 1000
/// ```
#[derive(Copy, Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum BoolOr<T> {
	Bool(bool),
	Value(T),
}

impl<T> Default for BoolOr<T> {
	fn default() -> Self {
		BoolOr::Bool(true)
	}
}

impl<'d, T: Deserialize<'d>> Deserialize<'d> for BoolOr<T> {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: de::Deserializer<'d>,
	{
		let v = toml::Value::deserialize(deserializer)?;
		if v.is_bool() {
			bool::deserialize(v).map(BoolOr::Bool).map_err(to_deser_error)
		} else {
			T::deserialize(v).map(BoolOr::Value).map_err(to_deser_error)
		}
	}
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

	/// Returns the value of this bool/or returning the default in case of BoolOr::Bool(true), the
	/// value in case of BoolOr::Value(_) or None in case of BoolOr::Bool(false)
	pub fn into_value(self, default: T) -> Option<T> {
		match self {
			BoolOr::Bool(false) => None,
			BoolOr::Bool(true) => Some(default),
			BoolOr::Value(x) => Some(x),
		}
	}
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TestDetails {
	#[serde(default)]
	pub reason: Option<String>,
	#[serde(default = "t")]
	pub run: bool,
	#[serde(default)]
	pub issue: Option<u64>,
	#[serde(default)]
	pub wip: bool,

	#[serde(default)]
	pub upgrade: bool,

	#[serde(default)]
	pub version: Option<VersionReq>,
	#[serde(default)]
	pub importing_version: Option<VersionReq>,

	#[serde(default)]
	pub results: Option<TestDetailsResults>,

	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

impl Default for TestDetails {
	fn default() -> Self {
		Self {
			reason: Default::default(),
			run: true,
			issue: Default::default(),
			wip: false,
			upgrade: Default::default(),
			version: Default::default(),
			importing_version: Default::default(),
			results: Default::default(),
			_unused_keys: Default::default(),
		}
	}
}

impl TestDetails {
	pub fn unused_keys(&self) -> Vec<String> {
		let mut res: Vec<_> = self._unused_keys.keys().map(|x| format!("test.{x}")).collect();

		if let Some(results) = self.results.as_ref() {
			match results {
				TestDetailsResults::QueryResult(_) => {}
				TestDetailsResults::ParserError(x) => res.append(
					&mut x._unused_keys.keys().map(|x| format!("test.results.{x}")).collect(),
				),
				TestDetailsResults::SignupError(x) => res.append(
					&mut x._unused_keys.keys().map(|x| format!("test.results.{x}")).collect(),
				),
				TestDetailsResults::SigninError(x) => res.append(
					&mut x._unused_keys.keys().map(|x| format!("test.results.{x}")).collect(),
				),
			}
		}
		res
	}
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
#[serde(rename_all = "kebab-case")]
pub enum TestDetailsResults {
	QueryResult(Vec<TestExpectation>),
	ParserError(ParsingTestResult),
	SigninError(SigninErrorResult),
	SignupError(SignupErrorResult),
}

impl<'de> Deserialize<'de> for TestDetailsResults {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: de::Deserializer<'de>,
	{
		let value = toml::Value::deserialize(deserializer)?;
		if value.is_array() {
			Deserialize::deserialize(value)
				.map_err(to_deser_error)
				.map(TestDetailsResults::QueryResult)
		} else if let Some(t) = value.as_table() {
			if t.contains_key("signin-error") {
				Deserialize::deserialize(value)
					.map_err(to_deser_error)
					.map(TestDetailsResults::SigninError)
			} else if t.contains_key("signup-error") {
				Deserialize::deserialize(value)
					.map_err(to_deser_error)
					.map(TestDetailsResults::SignupError)
			} else {
				Deserialize::deserialize(value)
					.map_err(to_deser_error)
					.map(TestDetailsResults::ParserError)
			}
		} else {
			Err(to_deser_error("Expected table or array"))
		}
	}
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ParsingTestResult {
	pub parsing_error: BoolOr<String>,
	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SigninErrorResult {
	pub signin_error: BoolOr<String>,
	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SignupErrorResult {
	pub signup_error: BoolOr<String>,
	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

/// A wrapper around the `Value` type for SurrealDB in order to support parsing from toml.
#[derive(Clone, Debug)]
pub struct SurrealConfigValue(pub Value);

impl Serialize for SurrealConfigValue {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let v = self.0.to_sql();
		v.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for SurrealConfigValue {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let source = String::deserialize(deserializer)?;
		let settings = ParserSettings {
			object_recursion_limit: 100,
			query_recursion_limit: 100,
			expr_recursion_limit: 100,
			legacy_strands: false,
			flexible_record_id: true,
			files_enabled: true,
			surrealism_enabled: true,
			json_string_escapes: false,
		};

		let v = syn::parse_with_settings(source.as_bytes(), settings, async |parser, stk| {
			parser.parse_value(stk).await
		})
		.map_err(<D::Error as serde::de::Error>::custom)?;

		Ok(SurrealConfigValue(v))
	}
}

#[derive(Clone, Debug)]
pub struct SurrealExpr(pub String);

impl Serialize for SurrealExpr {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		self.0.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for SurrealExpr {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let source = String::deserialize(deserializer)?;
		// We can't validate the expression anymore since parse_expr_start is private
		// and parse_value doesn't handle variables like $error
		// We'll rely on runtime validation when the expression is executed
		Ok(SurrealExpr(source))
	}
}

#[derive(Clone, Debug)]
pub struct SurrealRecordId(pub RecordId);

impl Serialize for SurrealRecordId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let v = self.0.to_sql();
		v.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for SurrealRecordId {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let source = String::deserialize(deserializer)?;
		let settings = ParserSettings {
			object_recursion_limit: 100,
			query_recursion_limit: 100,
			expr_recursion_limit: 100,
			legacy_strands: false,
			flexible_record_id: true,
			files_enabled: true,
			surrealism_enabled: true,
			json_string_escapes: false,
		};

		let v = syn::parse_with_settings(source.as_bytes(), settings, async |parser, stk| {
			parser.parse_value(stk).await
		})
		.map_err(<D::Error as serde::de::Error>::custom)?;
		if let Value::RecordId(x) = v {
			Ok(SurrealRecordId(x))
		} else {
			Err(<D::Error as serde::de::Error>::custom(format_args!(
				"Expected a record-id, found '{source}'"
			)))
		}
	}
}

#[derive(Clone, Debug)]
pub struct SurrealObject(pub Object);

impl Serialize for SurrealObject {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let v = self.0.to_sql();
		v.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for SurrealObject {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let source = String::deserialize(deserializer)?;
		let settings = ParserSettings {
			object_recursion_limit: 100,
			query_recursion_limit: 100,
			expr_recursion_limit: 100,
			legacy_strands: false,
			flexible_record_id: true,
			files_enabled: true,
			surrealism_enabled: true,
			json_string_escapes: false,
		};

		let v = syn::parse_with_settings(source.as_bytes(), settings, async |parser, stk| {
			parser.parse_value(stk).await
		})
		.map_err(<D::Error as serde::de::Error>::custom)?;

		v.into_object().map(SurrealObject).map_err(|err| {
			<D::Error as serde::de::Error>::custom(format_args!(
				"Expected a object, found '{source}': {err}"
			))
		})
	}
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum AuthLevel {
	#[default]
	Owner,
	Editor,
	Viewer,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", untagged)]
pub enum TestAuth {
	Record {
		namespace: String,
		database: String,
		access: String,
		rid: SurrealRecordId,
	},
	Database {
		namespace: String,
		database: String,
		#[serde(default)]
		level: AuthLevel,
	},
	Namespace {
		namespace: String,
		#[serde(default)]
		level: AuthLevel,
	},
	Root {
		level: AuthLevel,
	},
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Capabilities {
	#[serde(default = "t")]
	pub scripting: bool,
	#[serde(default = "t")]
	pub quest_access: bool,
	#[serde(default = "t")]
	pub live_query_notifications: bool,

	#[serde(default)]
	pub allow_functions: BoolOr<Vec<SchemaTarget<FuncTarget>>>,
	#[serde(default = "bool_or_f")]
	pub deny_functions: BoolOr<Vec<SchemaTarget<FuncTarget>>>,

	#[serde(default)]
	pub allow_net: BoolOr<Vec<SchemaTarget<NetTarget>>>,
	#[serde(default = "bool_or_f")]
	pub deny_net: BoolOr<Vec<SchemaTarget<NetTarget>>>,

	#[serde(default)]
	pub allow_rpc: BoolOr<Vec<SchemaTarget<MethodTarget>>>,
	#[serde(default = "bool_or_f")]
	pub deny_rpc: BoolOr<Vec<SchemaTarget<MethodTarget>>>,

	#[serde(default)]
	pub allow_http: BoolOr<Vec<SchemaTarget<RouteTarget>>>,
	#[serde(default = "bool_or_f")]
	pub deny_http: BoolOr<Vec<SchemaTarget<RouteTarget>>>,

	#[serde(default)]
	pub allow_experimental: BoolOr<Vec<SchemaTarget<ExperimentalTarget>>>,
	#[serde(default = "bool_or_f")]
	pub deny_experimental: BoolOr<Vec<SchemaTarget<ExperimentalTarget>>>,

	#[serde(default)]
	pub allow_arbitrary_query: BoolOr<Vec<SchemaTarget<ArbitraryQueryTarget>>>,
	#[serde(default = "bool_or_f")]
	pub deny_arbitrary_query: BoolOr<Vec<SchemaTarget<ArbitraryQueryTarget>>>,

	#[serde(default = "bool_or_f")]
	pub allow_eval_query: BoolOr<Vec<SchemaTarget<EvalQueryTarget>>>,
	#[serde(default = "bool_or_f")]
	pub deny_eval_query: BoolOr<Vec<SchemaTarget<EvalQueryTarget>>>,

	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}

impl Default for Capabilities {
	fn default() -> Self {
		Self {
			scripting: true,
			quest_access: true,
			live_query_notifications: true,

			allow_functions: Default::default(),
			deny_functions: BoolOr::Bool(false),

			allow_net: Default::default(),
			deny_net: BoolOr::Bool(false),
			allow_rpc: Default::default(),
			deny_rpc: BoolOr::Bool(false),
			allow_http: Default::default(),
			deny_http: BoolOr::Bool(false),
			allow_experimental: Default::default(),
			deny_experimental: BoolOr::Bool(false),
			allow_arbitrary_query: BoolOr::Bool(true),
			deny_arbitrary_query: BoolOr::Bool(false),
			allow_eval_query: BoolOr::Bool(false),
			deny_eval_query: BoolOr::Bool(false),
			_unused_keys: Default::default(),
		}
	}
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
		v.parse().map(SchemaTarget).map_err(<D::Error as serde::de::Error>::custom)
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

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct BenchDetails {
	/// Whether to include this file in the benchmark suite (`bench run`). The
	/// bench equivalent of `[test].run`; set to `false` for files that exist
	/// only to be imported by other benches. Defaults to `true`.
	#[serde(default = "t")]
	pub run: bool,
	/// Whether to rebuild the datastore (and rerun imports) before every
	/// measured iteration.
	///
	/// Defaults to `false`, which builds the datastore and runs imports once
	/// before the warmup/measurement loops, so a read-only bench measures only
	/// the timed statement against a stable, pre-populated dataset. Set to
	/// `true` for mutating benches (CREATE/UPDATE/DELETE) which require a clean
	/// slate per iteration.
	#[serde(default)]
	pub rebuild: bool,
	/// Run this bench once per named dataset, each resolved (transitively) as the
	/// bench's import for that run. Lets a single read-only scan be measured
	/// against multiple datasets — e.g. one without indexes and one with — from a
	/// single file. The key is a short variant name (shown in the run label and
	/// selectable with `--dataset <name>`); the value is the import path. When
	/// empty (default) the bench runs once using `[env].imports`.
	///
	/// ```toml
	/// datasets = { unindexed = "../_dataset.surql", indexed = "../_dataset_indexed.surql" }
	/// ```
	#[serde(default)]
	pub datasets: BTreeMap<String, String>,
	/// How long to warm up before measuring — the statement is run in a loop for
	/// this duration to stabilise caches before timing begins. Defaults to 3s.
	#[serde(default = "default_duration::<3000>")]
	pub warmup: TestDuration,
	/// Number of timed samples to collect; the reported mean / median / std-dev /
	/// MAD are computed over these. Defaults to 100.
	#[serde(default = "default_usize::<100>")]
	pub sample_size: usize,
	/// Target total time to spend collecting the samples. The harness sizes the
	/// per-sample iteration count from the warmup estimate so the measurement run
	/// takes roughly this long. Defaults to 100s.
	#[serde(default = "default_duration::<100000>")]
	pub measurement_time: TestDuration,
	/// Hard wall-clock backstop on the sample-collection loop. `measurement_time`
	/// only sizes the iteration count from the warmup estimate; it does not cap
	/// the loop, so a mis-scoped bench whose per-iteration cost dwarfs the warmup
	/// estimate (e.g. an O(n²) query) collects all `sample_size` samples no matter
	/// how long that takes. This caps the collection so no single bench can consume
	/// the whole run: the harness always keeps at least one sample, then stops once
	/// cumulative measured time exceeds this. Defaults to 600s.
	#[serde(default = "default_duration::<600000>")]
	pub max_time: TestDuration,

	#[serde(skip_serializing)]
	#[serde(flatten)]
	_unused_keys: BTreeMap<String, toml::Value>,
}
