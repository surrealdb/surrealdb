use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use parse_common::error::RenderedError;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{Object, Value as SurValue, Variables};

use super::cmp::{RoughlyEq, RoughlyEqConfig};
use super::unordered::{same_rows, unordered_statements};
use crate::tests::TestRun;
use crate::tests::case::Dialect;
use crate::tests::run::{CaseImports, RunConfig, TestRunId};
use crate::tests::schema::{self, BoolOr, TestConfig, TestDetailsResults};

mod display;
mod update;

/// Strips `request_id` and `x-surreal-request-id` header from `got` when `expected` does not
/// include `request_id`. Used when the result has `skip_api_request_id = true` so tests can
/// omit traceability fields and still match.
fn strip_api_request_id_for_comparison(expected: &SurValue, got: SurValue) -> SurValue {
	match (expected, &got) {
		(SurValue::Object(exp), SurValue::Object(got_obj))
			if !exp.contains_key("request_id") && got_obj.contains_key("request_id") =>
		{
			let mut new_map: BTreeMap<String, SurValue> = BTreeMap::new();
			for (k, v) in got_obj.iter() {
				if k == "request_id" {
					continue;
				}
				if k == "headers" {
					if let SurValue::Object(headers) = v {
						let mut new_headers: BTreeMap<String, SurValue> = BTreeMap::new();
						for (hk, hv) in headers.iter() {
							if hk != "x-surreal-request-id" {
								new_headers.insert(hk.clone(), hv.clone());
							}
						}
						new_map.insert(k.clone(), SurValue::Object(Object::from(new_headers)));
					} else {
						new_map.insert(k.clone(), v.clone());
					}
				} else {
					new_map.insert(k.clone(), v.clone());
				}
			}
			SurValue::Object(Object::from(new_map))
		}
		_ => got,
	}
}

/// Everything a matcher expression is evaluated against.
///
/// `vars` carries the bindings the expression may reference in addition to
/// `$result`/`$error`. For a chain run this is the walk position, which is what
/// lets one body assert exactly at every hop even though `mutate` grows the
/// data underneath it.
pub struct MatchContext<'a> {
	pub datastore: &'a Datastore,
	pub vars: Variables,
	/// For each result of a statement run, whether its rows may be graded in any order. Empty
	/// when no result may be.
	pub unordered: Vec<bool>,
}

impl<'a> MatchContext<'a> {
	pub fn new(datastore: &'a Datastore) -> Self {
		MatchContext {
			datastore,
			vars: Variables::default(),
			unordered: Vec::new(),
		}
	}

	/// Grade the rows of the results `unordered` marks in any order.
	pub fn with_unordered(mut self, unordered: Vec<bool>) -> Self {
		self.unordered = unordered;
		self
	}

	pub fn with_vars(datastore: &'a Datastore, vars: Variables) -> Self {
		MatchContext {
			datastore,
			vars,
			unordered: Vec::new(),
		}
	}
}

/// One version's turn in a cumulative chain walk.
#[derive(Debug)]
pub struct ChainHopResult {
	/// Version whose binary produced this result, as displayed to the user.
	pub version: String,
	/// 1-based position among the hops that actually ran the body.
	pub hop: usize,
	/// Total number of hops that were expected to run the body.
	pub hops: usize,
	/// Chain bindings offered to matcher expressions for this hop.
	pub vars: Variables,
	pub result: TestTaskResult,
}

#[derive(Debug)]
pub enum TestTaskResult {
	ParserError(RenderedError),
	RunningError(anyhow::Error),
	SignupError(anyhow::Error),
	SigninError(anyhow::Error),
	Import(String, String),
	BadCleanup(Vec<Vec<u8>>),
	/// The test created root-level state without declaring `remote-exclusive`.
	/// Carries a description of each entity it left behind.
	#[cfg_attr(not(feature = "remote"), allow(dead_code))]
	LeakedRootState(Vec<String>),
	Results {
		did_timeout: bool,
		res: Vec<Result<SurValue, String>>,
		/// Each statement's trace, canonical, for a test that asks for one.
		trace: Option<Vec<Option<SurValue>>>,
	},
	/// A cumulative chain walk: the body's outcome at every version that ran it,
	/// in walk order. Graded hop by hop, and the first hop that does not match
	/// is the one reported.
	///
	/// Only the upgrade runner constructs this, so without that feature the
	/// variant is dead — the grading arms below still compile either way, which
	/// is what keeps them from drifting out of sync with the runner.
	#[cfg_attr(not(feature = "upgrade"), allow(dead_code))]
	Chain(Vec<ChainHopResult>),
	/// A bracketed `[[test.repair]]`: the body's outcome before the repair ran
	/// and after it. The declaration holds only if `before` shows the damage it
	/// claims; `after` is then graded as an ordinary result.
	///
	/// Constructed only by the upgrade runner; see [`TestTaskResult::Chain`].
	#[cfg_attr(not(feature = "upgrade"), allow(dead_code))]
	Repair {
		index: usize,
		since: String,
		reason: String,
		unrepaired: schema::Unrepaired,
		target: String,
		before: Box<TestTaskResult>,
		after: Box<TestTaskResult>,
	},
}

/// Enum with the outcome of a test
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum TestGrade {
	/// Test succeeded
	Success,
	/// Test failed to match the expected output.
	Failed,
	/// Test produced unexpected results but does not cause the test run overall to fail.
	/// Happens when for example a test is about a work in progress issue.
	Warning,
	/// Test was not run because the executor can't faithfully reproduce it (e.g.
	/// a feature unsupported over a remote `--endpoint`). Does not fail the run.
	Skipped,
}

pub enum TestError {
	Running(String),
	Panicked(String),
	Import(String, String),
	BadCleanup(Vec<Vec<u8>>),
	#[cfg_attr(not(feature = "remote"), allow(dead_code))]
	LeakedRootState(Vec<String>),
}

pub enum TestOutputs {
	Values(Vec<Result<SurValue, String>>),
	ParsingError(String),
	SigninError(String),
	SignupError(String),
	/// The error the whole request was refused with.
	RequestError(String),
}

pub struct ResultTypeMismatchReport {
	pub got: TestOutputs,
	pub expected: TestExpectation,
}

pub struct Mismatch {
	/// The index of the mismatch in the list of expected values.
	pub index: usize,
	pub kind: MismatchKind,
}

pub enum MismatchKind {
	/// The value was not specified by the test but returned as an extra result..
	Unexpected {
		got: Result<SurValue, String>,
	},
	/// The value was expected by the test but missing from the results.
	Missing {
		expected: TestValueExpectation,
	},
	/// The test specified a value to match against
	Value(ValueMismatchKind),
	/// The test specified a matcher expression to match against
	Matcher(MatcherMismatch),
	/// The statement sent a trace other than the one the test states
	/// ([`crate::tests::trace`]); `None` for none.
	Trace {
		expected: Option<SurValue>,
		got: Option<SurValue>,
	},
}

pub enum ValueMismatchKind {
	/// Value was a wrong error.
	InvalidError {
		expected: String,
		got: String,
	},
	/// Value returned a wrong value
	InvalidValue {
		expected: SurValue,
		got: SurValue,
	},
	/// Value returned a wrong value
	ExpectedError {
		expected: Option<String>,
		got: SurValue,
	},
	/// Value returned a wrong value
	ExpectedValue {
		expected: Option<SurValue>,
		got: String,
	},
}

pub enum MatcherMismatch {
	/// Running the matcher produced an error.
	Error {
		error: String,
		got: Result<SurValue, String>,
	},
	/// Running the matcher returned false
	Failed {
		matcher: String,
		value: Result<SurValue, String>,
	},
	/// The test returned a value when an error was expected
	UnexpectedValue {
		got: SurValue,
	},
	/// The test returned an error when a value was expected
	UnexpectedError {
		got: String,
	},
	/// Running the matcher returned a non-boolean value
	OutputType {
		got: SurValue,
	},
}

pub enum TestReportKind {
	/// An error happend while running the test.
	Error(TestError),
	/// Test completed, but no results were specified.
	NoExpectation {
		output: TestOutputs,
	},
	/// Test completed and results matched,
	Valid,
	/// Test return the wrong type of output.
	MismatchedType(ResultTypeMismatchReport),
	/// Test returned an invalid parsing output.
	MismatchedParsing {
		got: String,
		expected: String,
	},
	MismatchedSignin {
		got: String,
		expected: String,
	},
	MismatchedSignup {
		got: String,
		expected: String,
	},
	/// The request was refused with an error other than the expected one.
	MismatchedRequest {
		got: String,
		expected: String,
	},
	MismatchedValues(Vec<Mismatch>),
	/// A chain walk failed at one specific version. The inner kind is that
	/// hop's own grade; the fields locate it in the walk.
	ChainHop {
		version: String,
		hop: usize,
		hops: usize,
		/// The reported hop's own values, so `--results accept` rewrites the
		/// expectations from the hop that failed rather than from the first one
		/// that happened to complete.
		outputs: Option<TestOutputs>,
		inner: Box<TestReportKind>,
	},
	/// A `[[test.repair]]` declaration is no longer true of the code.
	RepairContract(String),
	/// The test was skipped (e.g. it relies on a feature the remote executor
	/// can't reproduce). `reason` is shown in the summary.
	#[cfg_attr(not(feature = "remote"), allow(dead_code))]
	Skipped {
		reason: String,
	},
}

#[derive(Clone)]
pub struct ValueExpectation {
	expected: SurValue,
	equality: RoughlyEqConfig,
	/// When true, strip request_id and x-surreal-request-id header from actual before comparison.
	skip_api_request_id: bool,
}

#[derive(Clone)]
pub enum MatchValueType {
	Both,
	Error,
	Value,
}

#[derive(Clone)]
pub struct MatcherExpectation {
	matcher_value_type: MatchValueType,
	value_str: String,
}

#[derive(Clone)]
pub enum TestValueExpectation {
	Error(Option<String>),
	Value(Option<ValueExpectation>),
	Matcher(MatcherExpectation),
}

pub enum TestExpectation {
	Parsing(Option<String>),
	Values(Option<Vec<TestValueExpectation>>),
	Signin(Option<String>),
	Signup(Option<String>),
	/// The whole request is refused, with the given error text when one is named.
	Request(Option<String>),
}

impl TestExpectation {
	pub fn from_test_config(config: &TestConfig) -> Option<Self> {
		let results = config.test.results.as_ref()?;

		let res = match results {
			TestDetailsResults::QueryResult(r) => {
				let v = r
					.iter()
					.map(|x| match x {
						schema::TestExpectation::Plain(x) => {
							TestValueExpectation::Value(Some(ValueExpectation {
								expected: x.0.clone(),
								equality: RoughlyEqConfig::all(),
								skip_api_request_id: false,
							}))
						}
						schema::TestExpectation::Error(e) => match e.error {
							BoolOr::Value(ref x) => TestValueExpectation::Error(Some(x.clone())),
							BoolOr::Bool(true) => TestValueExpectation::Error(None),
							BoolOr::Bool(false) => TestValueExpectation::Value(None),
						},
						schema::TestExpectation::Value(x) => {
							let eq_config = RoughlyEqConfig {
								uuid: x.skip_uuid.map(|x| !x).unwrap_or(true),
								datetime: x.skip_datetime.map(|x| !x).unwrap_or(true),
								record_id_keys: x.skip_record_id_key.map(|x| !x).unwrap_or(true),
								float: x.float_roughly_eq.unwrap_or_default(),
								decimal: x.decimal_roughly_eq.unwrap_or_default(),
							};
							TestValueExpectation::Value(Some(ValueExpectation {
								expected: x.value.0.clone(),
								equality: eq_config,
								skip_api_request_id: x.skip_api_request_id.unwrap_or(false),
							}))
						}
						schema::TestExpectation::Match(x) => {
							let ty = match x.error {
								Some(true) => MatchValueType::Error,
								Some(false) => MatchValueType::Value,
								None => MatchValueType::Both,
							};
							TestValueExpectation::Matcher(MatcherExpectation {
								matcher_value_type: ty,
								value_str: x._match.0.clone(),
							})
						}
					})
					.collect();
				TestExpectation::Values(Some(v))
			}
			TestDetailsResults::ParserError(x) => match x.parsing_error {
				BoolOr::Value(ref x) => TestExpectation::Parsing(Some(x.clone())),
				BoolOr::Bool(true) => TestExpectation::Parsing(None),
				BoolOr::Bool(false) => TestExpectation::Values(None),
			},
			TestDetailsResults::SigninError(x) => match x.signin_error {
				BoolOr::Value(ref x) => TestExpectation::Signin(Some(x.clone())),
				BoolOr::Bool(true) => TestExpectation::Signin(None),
				BoolOr::Bool(false) => TestExpectation::Values(None),
			},
			TestDetailsResults::SignupError(x) => match x.signup_error {
				BoolOr::Value(ref x) => TestExpectation::Signup(Some(x.clone())),
				BoolOr::Bool(true) => TestExpectation::Signup(None),
				BoolOr::Bool(false) => TestExpectation::Values(None),
			},
			TestDetailsResults::RequestError(x) => match x.request_error {
				BoolOr::Value(ref x) => TestExpectation::Request(Some(x.clone())),
				BoolOr::Bool(true) => TestExpectation::Request(None),
				BoolOr::Bool(false) => TestExpectation::Values(None),
			},
		};
		Some(res)
	}
}

/// For each result of the run of `run` that `job_result` holds, whether its rows are graded in any
/// order: the run asks for it, and the statement is one whose rows nothing orders. Empty when the
/// statements of the source cannot be matched up with its results, which are then compared
/// exactly.
fn unordered_results<T: RunConfig>(run: &TestRun<T>, job_result: &TestTaskResult) -> Vec<bool> {
	if !run.config.unordered_rows() || run.case.test.dialect != Dialect::SurrealQl {
		return Vec::new();
	}
	let TestTaskResult::Results {
		res,
		..
	} = job_result
	else {
		return Vec::new();
	};
	unordered_statements(&run.case.test.source)
		.filter(|bits| bits.len() == res.len())
		.unwrap_or_default()
}

/// The output `--results accept` writes for a refused request: the error text, and only when the
/// test expects a request error. Any other running error is not gradable and is never written.
fn request_error_output(config: &TestConfig, error: &anyhow::Error) -> Option<TestOutputs> {
	matches!(TestExpectation::from_test_config(config), Some(TestExpectation::Request(_)))
		.then(|| TestOutputs::RequestError(error.to_string()))
}

/// Grades the error the whole request was refused with (the `dbs.process` `Err`). It is valid
/// only against a `request-error` expectation that names no text or names this error's text;
/// without that expectation the refusal is an ungradable running error.
fn grade_running_error(config: &TestConfig, error: &anyhow::Error) -> TestReportKind {
	let Some(TestExpectation::Request(expected)) = TestExpectation::from_test_config(config) else {
		return TestReportKind::Error(TestError::Running(format!("{error:?}")));
	};
	let got = error.to_string();
	match expected {
		Some(expected) if expected != got => TestReportKind::MismatchedRequest {
			got,
			expected,
		},
		_ => TestReportKind::Valid,
	}
}

/// The message the engine answers a statement with when it declines to run
/// it (the core error `PlannerUnsupported`): a statement the
/// language accepts that the engine has no plan for. No test may meet one,
/// whatever it expects, so a test that does fails however its expectations
/// read.
const ENGINE_DECLINE: &str = "The engine cannot run this statement:";

/// The statements of `res` the engine declined to run, as 1-based statement
/// numbers paired with the error message.
fn declined_statements(res: &[Result<SurValue, String>]) -> impl Iterator<Item = (usize, &str)> {
	res.iter().enumerate().filter_map(|(at, result)| match result {
		Err(message) if message.contains(ENGINE_DECLINE) => Some((at + 1, message.as_str())),
		_ => None,
	})
}

/// Every statement of `res` the engine declined to run, as the report names
/// them; `None` where it declined none.
fn engine_decline(res: &[Result<SurValue, String>]) -> Option<String> {
	let declined: Vec<String> = declined_statements(res)
		.map(|(statement, message)| {
			format!("statement {statement} was declined by the engine: {message}")
		})
		.collect();
	(!declined.is_empty()).then(|| declined.join("\n"))
}

/// A statement the engine declined to run, for the reporting lane. The
/// statement is its 1-based position in the test body.
pub struct Decline {
	pub statement: usize,
	pub message: String,
}

pub struct TestReport {
	pub id: TestRunId,
	pub case: Arc<CaseImports>,
	name: String,
	timeout: bool,
	kind: TestReportKind,
	outputs: Option<TestOutputs>,
	/// Each statement's trace, for a test that asks for one: what
	/// `--results accept` writes beside each value.
	traces: Option<Vec<Option<SurValue>>>,
}

impl TestReport {
	pub fn grade(&self) -> TestGrade {
		match &self.kind {
			TestReportKind::Valid => {
				if self.timeout {
					TestGrade::Warning
				} else {
					TestGrade::Success
				}
			}
			TestReportKind::MismatchedType(_)
			| TestReportKind::MismatchedParsing {
				..
			}
			| TestReportKind::MismatchedSignup {
				..
			}
			| TestReportKind::MismatchedSignin {
				..
			}
			| TestReportKind::MismatchedRequest {
				..
			}
			| TestReportKind::MismatchedValues(_)
			| TestReportKind::ChainHop {
				..
			} => {
				if self.is_wip() {
					TestGrade::Warning
				} else {
					TestGrade::Failed
				}
			}
			// A repair declaration that no longer describes the code is a hard
			// failure even for a work-in-progress test: the whole point is that
			// it cannot rot into a mask.
			TestReportKind::RepairContract(_) => TestGrade::Failed,
			TestReportKind::Error(_) => {
				if self.is_wip() {
					TestGrade::Warning
				} else {
					TestGrade::Failed
				}
			}
			TestReportKind::NoExpectation {
				..
			} => TestGrade::Warning,
			TestReportKind::Skipped {
				..
			} => TestGrade::Skipped,
		}
	}

	pub fn is_wip(&self) -> bool {
		self.case.test.config.parsed.test.wip
	}

	/// The statements the engine declined to run, whatever the test's grade.
	pub fn declines(&self) -> Vec<Decline> {
		let Some(TestOutputs::Values(res)) = &self.outputs else {
			return Vec::new();
		};
		declined_statements(res)
			.map(|(statement, message)| Decline {
				statement,
				message: message.to_owned(),
			})
			.collect()
	}

	pub fn is_unspecified_test(&self) -> bool {
		matches!(self.kind, TestReportKind::NoExpectation { .. })
	}

	/// The reason this run was skipped, if it was.
	pub fn skip_reason(&self) -> Option<&str> {
		match &self.kind {
			TestReportKind::Skipped {
				reason,
			} => Some(reason),
			_ => None,
		}
	}

	pub async fn from_test_result<T: RunConfig>(
		run: TestRun<T>,
		job_result: TestTaskResult,
		matching_datastore: &Datastore,
	) -> Self {
		let (timeout, outputs) = match &job_result {
			TestTaskResult::ParserError(e) => {
				(false, Some(TestOutputs::ParsingError(e.to_string())))
			}
			TestTaskResult::SignupError(e) => {
				(false, Some(TestOutputs::SignupError(e.to_string())))
			}
			TestTaskResult::SigninError(e) => {
				(false, Some(TestOutputs::SigninError(e.to_string())))
			}
			TestTaskResult::RunningError(e) => {
				(false, request_error_output(&run.case.test.config.parsed, e))
			}
			TestTaskResult::Import(_, _) => (false, None),
			TestTaskResult::BadCleanup(_) => (false, None),
			TestTaskResult::LeakedRootState(_) => (false, None),
			TestTaskResult::Results {
				did_timeout,
				res, ..
			} => (*did_timeout, Some(TestOutputs::Values(res.clone()))),
			// The outputs of the hop the grader will report: the first that does
			// not match, or the last if they all do. Surfacing them is what lets
			// `--results accept` bootstrap a chain test's expectations the way it
			// does a pairwise one.
			// Filled in from the graded result below: which hop is reported is
			// only known once the hops have been graded.
			TestTaskResult::Chain(_) => (false, None),
			// The post-repair run is the one graded, so it is the one whose
			// values an author would accept.
			TestTaskResult::Repair {
				after,
				..
			} => (
				false,
				match &**after {
					TestTaskResult::Results {
						res,
						..
					} => Some(TestOutputs::Values(res.clone())),
					_ => None,
				},
			),
		};

		let traces = match &job_result {
			TestTaskResult::Results {
				trace,
				..
			} => trace.clone(),
			TestTaskResult::Repair {
				after,
				..
			} => match &**after {
				TestTaskResult::Results {
					trace,
					..
				} => trace.clone(),
				_ => None,
			},
			_ => None,
		};
		let cfg = &run.case.test.config.parsed;
		let match_ctx = MatchContext::new(matching_datastore)
			.with_unordered(unordered_results(&run, &job_result));
		let kind = Self::grade_result(cfg, job_result, &match_ctx).await;
		// A chain reports one hop; its values are the ones worth showing and the
		// ones `--results accept` should write.
		let outputs = match &kind {
			TestReportKind::ChainHop {
				outputs: Some(TestOutputs::Values(values)),
				..
			} => Some(TestOutputs::Values(values.clone())),
			_ => outputs,
		};

		TestReport {
			id: run.id,
			name: run.name(),
			case: run.case,
			kind,
			timeout,
			outputs,
			traces,
		}
	}

	/// Each statement's trace, for a test that asks for one.
	pub fn traces(&self) -> Option<&[Option<SurValue>]> {
		self.traces.as_deref()
	}

	pub fn from_panic<T: RunConfig>(
		run: TestRun<T>,
		panic_error: Box<dyn Any + Send + 'static>,
	) -> Self {
		let error = panic_error
			.downcast::<String>()
			.map(|x| *x)
			.or_else(|e| e.downcast::<&'static str>().map(|x| (*x).to_owned()))
			.unwrap_or_else(|_| "Could not retrieve panic payload".to_owned());

		let kind = TestReportKind::Error(TestError::Panicked(error));

		TestReport {
			id: run.id,
			name: run.name(),
			case: run.case,
			kind,
			timeout: false,
			outputs: None,
			traces: None,
		}
	}

	/// Builds a report for a test that was skipped without running (e.g. it uses
	/// a feature the remote executor can't reproduce, or the run is without).
	pub fn skipped<T: RunConfig>(run: TestRun<T>, reason: impl Into<String>) -> Self {
		TestReport {
			id: run.id,
			name: run.name(),
			case: run.case,
			kind: TestReportKind::Skipped {
				reason: reason.into(),
			},
			timeout: false,
			outputs: None,
			traces: None,
		}
	}

	async fn grade_result(
		config: &TestConfig,
		job_result: TestTaskResult,
		match_ctx: &MatchContext<'_>,
	) -> TestReportKind {
		match job_result {
			TestTaskResult::RunningError(e) => grade_running_error(config, &e),
			TestTaskResult::Import(a, b) => TestReportKind::Error(TestError::Import(a, b)),
			TestTaskResult::BadCleanup(x) => TestReportKind::Error(TestError::BadCleanup(x)),
			TestTaskResult::LeakedRootState(x) => {
				TestReportKind::Error(TestError::LeakedRootState(x))
			}
			TestTaskResult::SignupError(err) => {
				let expectation = TestExpectation::from_test_config(config);

				// ensure we expect a parsing error.
				let expected_error = match expectation {
					None => {
						return TestReportKind::NoExpectation {
							output: TestOutputs::SignupError(err.to_string()),
						};
					}
					Some(TestExpectation::Signup(x)) => x,
					Some(x) => {
						return TestReportKind::MismatchedType(ResultTypeMismatchReport {
							got: TestOutputs::SignupError(err.to_string()),
							expected: x,
						});
					}
				};

				let Some(expected_error) = expected_error else {
					// No specified parsing error, results is valid.
					return TestReportKind::Valid;
				};

				let results = err.to_string();

				if expected_error == results {
					return TestReportKind::Valid;
				}

				TestReportKind::MismatchedSignup {
					got: results,
					expected: expected_error,
				}
			}

			TestTaskResult::SigninError(err) => {
				let expectation = TestExpectation::from_test_config(config);

				// ensure we expect a parsing error.
				let expected_parsing_error = match expectation {
					None => {
						return TestReportKind::NoExpectation {
							output: TestOutputs::SigninError(err.to_string()),
						};
					}
					Some(TestExpectation::Signin(x)) => x,
					Some(x) => {
						return TestReportKind::MismatchedType(ResultTypeMismatchReport {
							got: TestOutputs::SigninError(err.to_string()),
							expected: x,
						});
					}
				};

				let Some(expected_error) = expected_parsing_error else {
					// No specified parsing error, results is valid.
					return TestReportKind::Valid;
				};

				let results = err.to_string();

				if expected_error == results {
					return TestReportKind::Valid;
				}

				TestReportKind::MismatchedSignin {
					got: results,
					expected: expected_error,
				}
			}
			TestTaskResult::ParserError(results) => {
				let expectation = TestExpectation::from_test_config(config);

				// ensure we expect a parsing error.
				let expected_parsing_error = match expectation {
					None => {
						return TestReportKind::NoExpectation {
							output: TestOutputs::ParsingError(results.to_string()),
						};
					}
					Some(TestExpectation::Parsing(x)) => x,
					Some(x) => {
						return TestReportKind::MismatchedType(ResultTypeMismatchReport {
							got: TestOutputs::ParsingError(results.to_string()),
							expected: x,
						});
					}
				};

				let Some(expected_error) = expected_parsing_error else {
					// No specified parsing error, results is valid.
					return TestReportKind::Valid;
				};

				let results = results.to_string();

				if expected_error == results {
					return TestReportKind::Valid;
				}

				TestReportKind::MismatchedParsing {
					got: results,
					expected: expected_error,
				}
			}
			TestTaskResult::Results {
				res,
				trace,
				..
			} => {
				if let Some(declined) = engine_decline(&res) {
					return TestReportKind::Error(TestError::Running(declined));
				}
				let expectation = TestExpectation::from_test_config(config);
				let statements = res.len();
				let kind = Self::grade_value_results(expectation, res, match_ctx).await;
				Self::grade_traces(config, statements, trace, kind)
			}
			TestTaskResult::Repair {
				index,
				since,
				reason,
				unrepaired,
				target,
				before,
				after,
			} => {
				let before_kind = Box::pin(Self::grade_result(config, *before, match_ctx)).await;
				let before_passed = matches!(before_kind, TestReportKind::Valid);

				// Both arms are derived from the graded outcome rather than from
				// raw statement results. A test may legitimately expect an error
				// somewhere — a duplicate insert against a unique index, say — so
				// keying on "any statement errored" would make `degraded`
				// unsatisfiable for it, and would leave `broken` true forever
				// once the repair stopped being needed.
				//
				// `broken` is an error the expectations do not allow; `degraded`
				// is every statement landing where it should and a value still
				// coming back wrong.
				let unexpected_error = matches!(
					&before_kind,
					TestReportKind::MismatchedValues(m)
						if m.iter().any(|x| matches!(
							x.kind,
							MismatchKind::Value(ValueMismatchKind::ExpectedValue { .. })
								| MismatchKind::Value(ValueMismatchKind::InvalidError { .. })
						))
				) || matches!(before_kind, TestReportKind::Error(_));
				let holds = match unrepaired {
					schema::Unrepaired::Broken => !before_passed && unexpected_error,
					schema::Unrepaired::Degraded => !before_passed && !unexpected_error,
				};

				if !holds {
					let seen = if before_passed {
						"the body passed unchanged"
					} else if unexpected_error {
						"a statement errored in a way the expectations do not allow"
					} else {
						"every statement landed as expected, but a value came back wrong"
					};
					return TestReportKind::RepairContract(format!(
						"`[[test.repair]]` #{index} declares `unrepaired = \"{unrepaired:?}\"` \
						 for targets matching `{since}`, but on {target} {seen}.\n\
						 Declared reason: {reason}\n\
						 If the requirement is gone, delete the declaration and its script and \
						 assert the un-repaired read directly — leaving it in place means the \
						 repair, not the upgrade, is what the test proves."
					));
				}

				Box::pin(Self::grade_result(config, *after, match_ctx)).await
			}
			TestTaskResult::Chain(hops) => {
				// Every hop is graded against the same expectations. The first
				// one that does not hold is the report: a walk that breaks at
				// 3.1.4 and stays broken afterwards should name 3.1.4, not the
				// last version in the chain.
				let total = hops.len();
				for hop in hops {
					let hop_ctx = MatchContext::with_vars(match_ctx.datastore, hop.vars);
					let values = match &hop.result {
						TestTaskResult::Results {
							res,
							..
						} => Some(TestOutputs::Values(res.clone())),
						_ => None,
					};
					let kind = Box::pin(Self::grade_result(config, hop.result, &hop_ctx)).await;
					if !matches!(kind, TestReportKind::Valid) {
						return TestReportKind::ChainHop {
							version: hop.version,
							hop: hop.hop,
							outputs: values,
							hops: if hop.hops == 0 {
								total
							} else {
								hop.hops
							},
							inner: Box::new(kind),
						};
					}
				}
				TestReportKind::Valid
			}
		}
	}

	/// `kind`, the grade of a test's values, with each statement's trace graded
	/// against the one its result states, for a test that asks for a trace. A
	/// statement whose result states no trace is expected to send none.
	fn grade_traces(
		config: &TestConfig,
		statements: usize,
		got: Option<Vec<Option<SurValue>>>,
		kind: TestReportKind,
	) -> TestReportKind {
		let Some(expected) = crate::tests::trace::expected(config) else {
			return kind;
		};
		// A statement the values already report as missing or unexpected has no
		// trace to grade.
		let got = got.unwrap_or_default();
		let mut mismatches = Vec::new();
		for index in 0..expected.len().min(statements) {
			let expected = expected[index].clone();
			let got = got.get(index).cloned().flatten();
			if !crate::tests::trace::agrees(expected.as_ref(), got.as_ref()) {
				mismatches.push(Mismatch {
					index,
					kind: MismatchKind::Trace {
						expected,
						got,
					},
				});
			}
		}
		if mismatches.is_empty() {
			return kind;
		}
		match kind {
			TestReportKind::Valid => TestReportKind::MismatchedValues(mismatches),
			TestReportKind::MismatchedValues(mut values) => {
				values.extend(mismatches);
				values.sort_by_key(|mismatch| mismatch.index);
				TestReportKind::MismatchedValues(values)
			}
			other => other,
		}
	}

	async fn grade_value_results(
		expectation: Option<TestExpectation>,
		results: Vec<Result<SurValue, String>>,
		match_ctx: &MatchContext<'_>,
	) -> TestReportKind {
		// Ensure we expect a value.
		let expected_values = match expectation {
			None => {
				return TestReportKind::NoExpectation {
					output: TestOutputs::Values(results),
				};
			}
			Some(TestExpectation::Values(None)) => return TestReportKind::Valid,
			Some(TestExpectation::Values(Some(ref x))) => x,
			Some(x) => {
				return TestReportKind::MismatchedType(ResultTypeMismatchReport {
					got: TestOutputs::Values(results),
					expected: x,
				});
			}
		};

		let mut expected = expected_values.iter();
		let mut results = results.into_iter();

		// types line up, check for value equality
		let mut mismatches = Vec::new();
		let mut index = 0;
		loop {
			let mismatch = match (expected.next(), results.next()) {
				(None, None) => break,
				(Some(x), None) => Mismatch {
					index,
					kind: MismatchKind::Missing {
						expected: x.clone(),
					},
				},
				(None, Some(x)) => Mismatch {
					index,
					kind: MismatchKind::Unexpected {
						got: x,
					},
				},
				(Some(e), Some(v)) => {
					let unordered = match_ctx.unordered.get(index).copied().unwrap_or(false);
					match Self::match_value_to_expectation(v, e, match_ctx, unordered).await {
						Some(kind) => Mismatch {
							index,
							kind,
						},
						None => {
							index += 1;
							continue;
						}
					}
				}
			};
			mismatches.push(mismatch);
			index += 1;
		}

		// If non of the values mismatched then the test is valid.
		if mismatches.is_empty() {
			TestReportKind::Valid
		} else {
			TestReportKind::MismatchedValues(mismatches)
		}
	}

	async fn match_value_to_expectation(
		value: Result<SurValue, String>,
		expectation: &TestValueExpectation,
		match_ctx: &MatchContext<'_>,
		unordered: bool,
	) -> Option<MismatchKind> {
		match expectation {
			TestValueExpectation::Error(expected) => match value {
				Ok(got) => Some(MismatchKind::Value(ValueMismatchKind::ExpectedError {
					expected: expected.clone(),
					got,
				})),
				Err(error) => {
					let Some(expected) = expected else {
						// No specific expectation, result is valid.
						return None;
					};

					if error == *expected {
						// value matched, result is valid.
						return None;
					}

					// invalid
					Some(MismatchKind::Value(ValueMismatchKind::InvalidError {
						expected: expected.clone(),
						got: error,
					}))
				}
			},
			TestValueExpectation::Value(expected) => match value {
				Err(got) => Some(MismatchKind::Value(ValueMismatchKind::ExpectedValue {
					expected: expected.clone().map(|x| x.expected),
					got,
				})),
				Ok(x) => {
					let Some(expected) = expected else {
						// No specific expectation, result is valid.
						return None;
					};

					// When skip_api_request_id is set, strip request_id and x-surreal-request-id
					// header before comparison (same idea as skip_record_id_key for record ids).
					let x_normalized = if expected.skip_api_request_id {
						strip_api_request_id_for_comparison(&expected.expected, x.clone())
					} else {
						x.clone()
					};

					if expected.expected.roughly_equal(&x_normalized, &expected.equality) {
						// value matched, result is valid.
						return None;
					}

					// The rows of a result nothing orders match in any order.
					if unordered && same_rows(&expected.expected, &x_normalized, &expected.equality)
					{
						return None;
					}

					// invalid
					Some(MismatchKind::Value(ValueMismatchKind::InvalidValue {
						expected: expected.expected.clone(),
						got: x,
					}))
				}
			},
			TestValueExpectation::Matcher(x) => {
				Self::match_value_to_matcher(value, x, match_ctx).await.map(MismatchKind::Matcher)
			}
		}
	}

	async fn match_value_to_matcher(
		value: Result<SurValue, String>,
		expectation: &MatcherExpectation,
		match_ctx: &MatchContext<'_>,
	) -> Option<MatcherMismatch> {
		match expectation.matcher_value_type {
			MatchValueType::Both => {}
			MatchValueType::Error => {
				if let Ok(got) = value {
					return Some(MatcherMismatch::UnexpectedValue {
						got,
					});
				}
			}
			MatchValueType::Value => {
				if let Err(got) = value {
					return Some(MatcherMismatch::UnexpectedError {
						got,
					});
				}
			}
		};

		// Context bindings first, so a `$result`/`$error` supplied by the run
		// itself always wins over anything the context happens to carry.
		let mut run_vars = match_ctx.vars.clone();
		match value {
			Ok(ref x) => run_vars.insert("result".to_string(), x.clone()),
			Err(ref e) => run_vars.insert("error".to_string(), SurValue::String(e.clone())),
		};

		let session = Session::viewer().with_ns("match").with_db("match");

		// Instead of creating an Ast directly, we'll execute the expression as a query string
		let query = format!("RETURN {}", expectation.value_str);
		let res = match_ctx.datastore.execute(&query, &session, Some(run_vars)).await;

		let x = match res {
			Err(e) => {
				return Some(MatcherMismatch::Error {
					error: e.to_string(),
					got: value,
				});
			}
			Ok(x) => x,
		};
		let x = match x.into_iter().next().unwrap().result {
			Err(e) => {
				return Some(MatcherMismatch::Error {
					error: e.to_string(),
					got: value,
				});
			}
			Ok(x) => x,
		};

		let res = match x {
			SurValue::Bool(x) => x,
			got => {
				return Some(MatcherMismatch::OutputType {
					got,
				});
			}
		};

		if res {
			return None;
		}

		Some(MatcherMismatch::Failed {
			matcher: expectation.value_str.clone(),
			value,
		})
	}
}

#[cfg(test)]
mod request_error_tests {
	use toml_edit::DocumentMut;

	use super::*;

	fn config(results: &str) -> TestConfig {
		toml::from_str(&format!("[test]\n{results}")).expect("test config parses")
	}

	fn refusal() -> anyhow::Error {
		anyhow::anyhow!("The request was refused")
	}

	#[test]
	fn request_error_text_deserialises_as_a_table_result() {
		let cfg = config("[test.results]\nrequest-error = \"The request was refused\"");
		assert!(cfg.unused_keys().is_empty());
		assert!(matches!(
			TestExpectation::from_test_config(&cfg),
			Some(TestExpectation::Request(Some(text))) if text == "The request was refused"
		));
	}

	#[test]
	fn request_error_true_deserialises_without_text() {
		let cfg = config("[test.results]\nrequest-error = true");
		assert!(matches!(
			TestExpectation::from_test_config(&cfg),
			Some(TestExpectation::Request(None))
		));
	}

	#[test]
	fn parsing_error_table_is_unchanged() {
		let cfg = config("[test.results]\nparsing-error = true");
		assert!(matches!(
			TestExpectation::from_test_config(&cfg),
			Some(TestExpectation::Parsing(None))
		));
	}

	#[test]
	fn matching_request_error_is_valid() {
		let cfg = config("[test.results]\nrequest-error = \"The request was refused\"");
		assert!(matches!(grade_running_error(&cfg, &refusal()), TestReportKind::Valid));
		let any = config("[test.results]\nrequest-error = true");
		assert!(matches!(grade_running_error(&any, &refusal()), TestReportKind::Valid));
	}

	#[test]
	fn mismatching_request_error_is_reported() {
		let cfg = config("[test.results]\nrequest-error = \"Something else\"");
		match grade_running_error(&cfg, &refusal()) {
			TestReportKind::MismatchedRequest {
				got,
				expected,
			} => {
				assert_eq!(got, "The request was refused");
				assert_eq!(expected, "Something else");
			}
			_ => panic!("expected a request-error mismatch"),
		}
	}

	#[test]
	fn running_error_without_a_request_error_expectation_stays_ungradable() {
		for results in
			["", "[test.results]\nparsing-error = true", "[[test.results]]\nvalue = \"1\""]
		{
			let cfg = config(results);
			assert!(matches!(
				grade_running_error(&cfg, &refusal()),
				TestReportKind::Error(TestError::Running(_))
			));
			assert!(request_error_output(&cfg, &refusal()).is_none());
		}
	}

	#[test]
	fn accept_writes_the_refusal_as_a_request_error_expectation() {
		let cfg = config("[test.results]\nrequest-error = \"Something else\"");
		let Some(TestOutputs::RequestError(text)) = request_error_output(&cfg, &refusal()) else {
			panic!("a request-error test carries the refusal as its output");
		};

		let mut doc = "[test]\nreason = \"r\"\n".parse::<DocumentMut>().unwrap();
		update::apply_error(&mut doc, "request-error", &text);
		let parsed: TestConfig = toml::from_str(&doc.to_string()).expect("rewritten config parses");
		assert!(matches!(
			TestExpectation::from_test_config(&parsed),
			Some(TestExpectation::Request(Some(t))) if t == "The request was refused"
		));
	}
}

#[cfg(test)]
mod unordered_rows_tests {
	use super::*;

	/// The grade of `got` against the expected results `expected`, with the statements `unordered`
	/// marks graded in any order.
	async fn grade(
		expected: &str,
		got: Vec<Result<SurValue, String>>,
		unordered: Vec<bool>,
	) -> TestReportKind {
		let datastore = Datastore::new("memory").await.expect("a datastore");
		let config: TestConfig =
			toml::from_str(&format!("[test]\n{expected}")).expect("test config parses");
		let context = MatchContext::new(&datastore).with_unordered(unordered);
		TestReport::grade_value_results(TestExpectation::from_test_config(&config), got, &context)
			.await
	}

	fn rows(numbers: &[i64]) -> Result<SurValue, String> {
		Ok(SurValue::Array(numbers.to_vec().into()))
	}

	const EXPECTED: &str = "[[test.results]]\nvalue = \"[1, 2, 3]\"";

	#[tokio::test]
	async fn rows_come_in_any_order_where_the_statement_is_unordered() {
		let kind = grade(EXPECTED, vec![rows(&[3, 1, 2])], vec![true]).await;
		assert!(matches!(kind, TestReportKind::Valid));
	}

	#[tokio::test]
	async fn rows_come_in_the_order_expected_where_the_statement_is_ordered() {
		for unordered in [vec![false], Vec::new()] {
			let kind = grade(EXPECTED, vec![rows(&[3, 1, 2])], unordered).await;
			assert!(matches!(kind, TestReportKind::MismatchedValues(_)));
		}
	}

	#[tokio::test]
	async fn other_rows_fail_in_any_order() {
		let kind = grade(EXPECTED, vec![rows(&[3, 1, 1])], vec![true]).await;
		assert!(matches!(kind, TestReportKind::MismatchedValues(_)));
	}
}
