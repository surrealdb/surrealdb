use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_syn::error::RenderedError;
use surrealdb_types::{Object, Value as SurValue, Variables};

use super::cmp::{RoughlyEq, RoughlyEqConfig};
use crate::tests::TestRun;
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
}

impl<'a> MatchContext<'a> {
	pub fn new(datastore: &'a Datastore) -> Self {
		MatchContext {
			datastore,
			vars: Variables::default(),
		}
	}

	pub fn with_vars(datastore: &'a Datastore, vars: Variables) -> Self {
		MatchContext {
			datastore,
			vars,
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
	Results {
		did_timeout: bool,
		res: Vec<Result<SurValue, String>>,
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
}

pub enum TestError {
	Running(String),
	Panicked(String),
	Import(String, String),
	BadCleanup(Vec<Vec<u8>>),
}

pub enum TestOutputs {
	Values(Vec<Result<SurValue, String>>),
	ParsingError(String),
	SigninError(String),
	SignupError(String),
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
		};
		Some(res)
	}
}

pub struct TestReport {
	pub id: TestRunId,
	pub case: Arc<CaseImports>,
	name: String,
	timeout: bool,
	kind: TestReportKind,
	outputs: Option<TestOutputs>,
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
		}
	}

	pub fn is_wip(&self) -> bool {
		self.case.test.config.parsed.test.wip
	}

	pub fn is_unspecified_test(&self) -> bool {
		matches!(self.kind, TestReportKind::NoExpectation { .. })
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
			TestTaskResult::RunningError(_) => (false, None),
			TestTaskResult::Import(_, _) => (false, None),
			TestTaskResult::BadCleanup(_) => (false, None),
			TestTaskResult::Results {
				did_timeout,
				res,
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

		let cfg = &run.case.test.config.parsed;
		let kind = Self::grade_result(cfg, job_result, &MatchContext::new(matching_datastore)).await;
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
		}
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
		}
	}

	async fn grade_result(
		config: &TestConfig,
		job_result: TestTaskResult,
		match_ctx: &MatchContext<'_>,
	) -> TestReportKind {
		match job_result {
			TestTaskResult::RunningError(e) => {
				TestReportKind::Error(TestError::Running(format!("{:?}", e)))
			}
			TestTaskResult::Import(a, b) => TestReportKind::Error(TestError::Import(a, b)),
			TestTaskResult::BadCleanup(x) => TestReportKind::Error(TestError::BadCleanup(x)),
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
				..
			} => {
				let expectation = TestExpectation::from_test_config(config);
				Self::grade_value_results(expectation, res, match_ctx).await
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
					let kind =
						Box::pin(Self::grade_result(config, hop.result, &hop_ctx)).await;
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
					match Self::match_value_to_expectation(v, e, match_ctx).await {
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

					// invalid
					Some(MismatchKind::Value(ValueMismatchKind::InvalidValue {
						expected: expected.expected.clone(),
						got: x,
					}))
				}
			},
			TestValueExpectation::Matcher(x) => {
				Self::match_value_to_matcher(value, x, match_ctx)
					.await
					.map(MismatchKind::Matcher)
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
