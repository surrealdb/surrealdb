use std::any::Any;

use super::cmp::{RoughlyEq, RoughlyEqConfig};
use crate::tests::schema::{self, TestConfig};
use crate::tests::{
	TestSet,
	schema::{BoolOr, TestDetailsResults},
	set::TestId,
};
use surrealdb_core::dbs::{Session, Variables};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::sql::{Ast, Expr, TopLevelExpr};
use surrealdb_core::syn::error::RenderedError;
use surrealdb_core::val::Value as SurValue;

mod display;
mod update;

#[derive(Debug)]
pub enum TestTaskResult {
	ParserError(RenderedError),
	RunningError(anyhow::Error),
	SignupError(anyhow::Error),
	SigninError(anyhow::Error),
	Import(String, String),
	Timeout,
	Results(Vec<Result<SurValue, String>>),
	Paniced(Box<dyn Any + Send + 'static>),
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
	Timeout,
	Running(String),
	Paniced(String),
	Import(String, String),
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
		matcher: Expr,
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
}

#[derive(Clone)]
pub struct ValueExpectation {
	expected: SurValue,
	equality: RoughlyEqConfig,
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
	value: Expr,
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
		let Some(details) = config.test.as_ref() else {
			return None;
		};

		let Some(results) = details.results.as_ref() else {
			return None;
		};

		let res = match results {
			TestDetailsResults::QueryResult(r) => {
				let v = r
					.iter()
					.map(|x| match x {
						schema::TestExpectation::Plain(x) => {
							TestValueExpectation::Value(Some(ValueExpectation {
								expected: x.0.clone(),
								equality: RoughlyEqConfig::all(),
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
								value: x._match.0.clone(),
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
	id: TestId,
	is_wip: bool,
	kind: TestReportKind,
	outputs: Option<TestOutputs>,
	extra_name: Option<String>,
}

impl TestReport {
	pub fn grade(&self) -> TestGrade {
		match self.kind {
			TestReportKind::Valid => TestGrade::Success,
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
			| TestReportKind::MismatchedValues(_) => {
				if self.is_wip {
					TestGrade::Warning
				} else {
					TestGrade::Failed
				}
			}
			TestReportKind::Error(_) => TestGrade::Failed,
			TestReportKind::NoExpectation {
				..
			} => TestGrade::Warning,
		}
	}

	pub fn is_wip(&self) -> bool {
		self.is_wip
	}

	pub fn is_unspecified_test(&self) -> bool {
		matches!(self.kind, TestReportKind::NoExpectation { .. })
	}

	pub fn test_id(&self) -> TestId {
		self.id
	}

	pub async fn from_test_result(
		id: TestId,
		set: &TestSet,
		job_result: TestTaskResult,
		matching_datastore: &Datastore,
		extra_name: Option<String>,
	) -> Self {
		let outputs = match job_result {
			TestTaskResult::ParserError(ref e) => Some(TestOutputs::ParsingError(e.to_string())),
			TestTaskResult::SignupError(ref e) => Some(TestOutputs::SignupError(e.to_string())),
			TestTaskResult::SigninError(ref e) => Some(TestOutputs::SigninError(e.to_string())),
			TestTaskResult::RunningError(_) => None,
			TestTaskResult::Timeout => None,
			TestTaskResult::Import(_, _) => None,
			TestTaskResult::Results(ref e) => Some(TestOutputs::Values(e.clone())),
			TestTaskResult::Paniced(_) => None,
		};

		let kind = Self::grade_result(&set[id].config, job_result, matching_datastore).await;

		TestReport {
			id,
			is_wip: set[id].config.is_wip(),
			kind,
			outputs,
			extra_name,
		}
	}

	async fn grade_result(
		config: &TestConfig,
		job_result: TestTaskResult,
		matcher_datastore: &Datastore,
	) -> TestReportKind {
		match job_result {
			TestTaskResult::RunningError(e) => {
				TestReportKind::Error(TestError::Running(format!("{:?}", e)))
			}
			TestTaskResult::Timeout => TestReportKind::Error(TestError::Timeout),
			TestTaskResult::Import(a, b) => TestReportKind::Error(TestError::Import(a, b)),
			TestTaskResult::Paniced(e) => {
				let error = e
					.downcast::<String>()
					.map(|x| *x)
					.or_else(|e| e.downcast::<&'static str>().map(|x| (*x).to_owned()))
					.unwrap_or_else(|_| "Could not retrieve panic payload".to_owned());

				TestReportKind::Error(TestError::Paniced(error))
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
			TestTaskResult::Results(results) => {
				let expectation = TestExpectation::from_test_config(config);
				Self::grade_value_results(expectation, results, matcher_datastore).await
			}
		}
	}

	async fn grade_value_results(
		expectation: Option<TestExpectation>,
		results: Vec<Result<SurValue, String>>,
		matcher_datastore: &Datastore,
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
					match Self::match_value_to_expectation(v, e, matcher_datastore).await {
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
		matcher_datastore: &Datastore,
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

					if expected.expected.roughly_equal(&x, &expected.equality) {
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
				Self::match_value_to_matcher(value, x, matcher_datastore)
					.await
					.map(MismatchKind::Matcher)
			}
		}
	}

	async fn match_value_to_matcher(
		value: Result<SurValue, String>,
		expectation: &MatcherExpectation,
		matcher_datastore: &Datastore,
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

		let run_vars = match value {
			Ok(ref x) => Variables::from_iter([("result".to_string(), x.clone())]),
			Err(ref e) => Variables::from_iter([(
				"error".to_string(),
				SurValue::Strand(e.clone().into()).clone(),
			)]),
		};

		let session = Session::viewer().with_ns("match").with_db("match");

		let ast = Ast {
			expressions: vec![TopLevelExpr::Expr(expectation.value.clone())],
		};
		let res = matcher_datastore.process(ast, &session, Some(run_vars)).await;

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
			matcher: expectation.value.clone(),
			value,
		})
	}
}
