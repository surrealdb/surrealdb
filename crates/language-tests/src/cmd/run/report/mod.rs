use std::collections::BTreeMap;

use super::cmp::{RoughlyEq, RoughlyEqConfig};
use super::TestTaskResult;
use crate::tests::schema::{self, TestConfig};
use crate::tests::{
	schema::{BoolOr, TestDetailsResults},
	testset::TestId,
	TestSet,
};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::sql::Value as SurValue;

mod display;
mod update;

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
}

pub enum TestOutputs {
	Values(Vec<Result<SurValue, String>>),
	ParsingError(String),
}

pub enum TypeMismatchReport {
	/// Expected a parsing error but got values
	ExpectedParsingError {
		got: Vec<Result<SurValue, String>>,
		expected: Option<String>,
	},
	/// Expected a value but got a parsing error.
	ExpectedValues {
		got: String,
		expected: Option<Vec<TestValueExpectation>>,
	},
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
		matcher: SurValue,
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
	MismatchedType(TypeMismatchReport),
	/// Test returned an invalid parsing output.
	MismatchedParsing {
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
	value: SurValue,
}

#[derive(Clone)]
pub enum TestValueExpectation {
	Error(Option<String>),
	Value(Option<ValueExpectation>),
	Matcher(MatcherExpectation),
}

pub enum TestExpectation {
	Unspecified,
	Parsing(Option<String>),
	Values(Option<Vec<TestValueExpectation>>),
}

impl TestExpectation {
	pub fn from_test_config(config: &TestConfig) -> Self {
		let Some(details) = config.test.as_ref() else {
			return TestExpectation::Unspecified;
		};

		let Some(results) = details.results.as_ref() else {
			return TestExpectation::Unspecified;
		};

		match results {
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
		}
	}
}

pub struct TestReport {
	id: TestId,
	is_wip: bool,
	kind: TestReportKind,
	outputs: Option<TestOutputs>,
}

impl TestReport {
	pub fn grade(&self) -> TestGrade {
		match self.kind {
			TestReportKind::Valid => TestGrade::Success,
			TestReportKind::MismatchedType(_)
			| TestReportKind::MismatchedParsing {
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
	) -> Self {
		let outputs = match job_result {
			TestTaskResult::ParserError(ref e) => Some(TestOutputs::ParsingError(e.to_string())),
			TestTaskResult::RunningError(_) => None,
			TestTaskResult::Timeout => None,
			TestTaskResult::Results(ref e) => Some(TestOutputs::Values(
				e.iter()
					.map(|x| x.result.as_ref().map_err(|e| e.to_string()).map(|x| x.clone()))
					.collect(),
			)),
		};

		let kind = Self::grade_result(&set[id].config, job_result, matching_datastore).await;

		TestReport {
			id,
			is_wip: set[id].config.is_wip(),
			kind,
			outputs,
		}
	}

	async fn grade_result(
		config: &TestConfig,
		job_result: TestTaskResult,
		matcher_datastore: &Datastore,
	) -> TestReportKind {
		match job_result {
			TestTaskResult::RunningError(e) => {
				TestReportKind::Error(TestError::Running(e.to_string()))
			}
			TestTaskResult::Timeout => TestReportKind::Error(TestError::Timeout),
			TestTaskResult::ParserError(results) => {
				let expectation = TestExpectation::from_test_config(config);

				// ensure we expect a parsing error.
				let expected_parsing_error = match expectation {
					TestExpectation::Unspecified => {
						return TestReportKind::NoExpectation {
							output: TestOutputs::ParsingError(results.to_string()),
						}
					}
					TestExpectation::Parsing(x) => x,
					TestExpectation::Values(x) => {
						return TestReportKind::MismatchedType(TypeMismatchReport::ExpectedValues {
							got: results.to_string(),
							expected: x,
						})
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
				let results =
					results.into_iter().map(|x| x.result.map_err(|x| x.to_string())).collect();
				Self::grade_value_results(expectation, results, matcher_datastore).await
			}
		}
	}

	async fn grade_value_results(
		expectation: TestExpectation,
		results: Vec<Result<SurValue, String>>,
		matcher_datastore: &Datastore,
	) -> TestReportKind {
		// Ensure we expect a value.
		let expected_values = match expectation {
			TestExpectation::Unspecified => {
				return TestReportKind::NoExpectation {
					output: TestOutputs::Values(results),
				}
			}
			TestExpectation::Parsing(expected) => {
				return TestReportKind::MismatchedType(TypeMismatchReport::ExpectedParsingError {
					got: results,
					expected,
				})
			}
			TestExpectation::Values(None) => return TestReportKind::Valid,
			TestExpectation::Values(Some(ref x)) => x,
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
						None => continue,
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
			Ok(ref x) => BTreeMap::from([("result".to_string(), x.clone())]),
			Err(ref e) => {
				BTreeMap::from([("error".to_string(), SurValue::Strand(e.clone().into()).clone())])
			}
		};

		let session = Session::viewer().with_ns("match").with_db("match");

		let res =
			matcher_datastore.compute(expectation.value.clone(), &session, Some(run_vars)).await;

		let x = match res {
			Err(e) => {
				return Some(MatcherMismatch::Error {
					error: e.to_string(),
					got: value,
				})
			}
			Ok(x) => x,
		};

		let res = match x {
			SurValue::Bool(x) => x,
			got => {
				return Some(MatcherMismatch::OutputType {
					got,
				})
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
