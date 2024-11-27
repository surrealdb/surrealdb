use std::time::{Duration, Instant};

use super::cmp::RoughlyEq;
use super::TestJobResult;
use crate::{
	format::ansi,
	tests::{
		schema::{BoolOr, TestDetailsResults, TestResultFlat},
		testset::TestId,
		TestSet,
	},
};
use similar::{Algorithm, TextDiff};
use surrealdb_core::sql::Value as SurValue;
use tracing::{error, info, warn};

mod display;
mod update;

/// Enum with the outcome of a test
#[derive(Clone, Copy)]
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

impl TestOutputs {
	pub fn as_parsing_error(&self) -> Option<&str> {
		match self {
			TestOutputs::ParsingError(x) => Some(x.as_str()),
			_ => None,
		}
	}
	pub fn as_results(&self) -> Option<&[Result<SurValue, String>]> {
		match self {
			TestOutputs::Values(x) => Some(x.as_slice()),
			_ => None,
		}
	}
}

pub enum TestOutputValidity {
	/// Test did not specify any results.
	Unspecified,
	/// Got a parsing error, expected some values
	UnexpectParsingError {
		expected: Option<Vec<TestResultFlat>>,
	},
	/// Got output, expected a parsing error.
	UnexpectedValues {
		expected: Option<String>,
	},
	/// The parsing error we got does not line up with the expected error.
	MismatchedParsingError {
		expected: String,
	},
	/// The values produced by the test are not the expected values.
	MismatchedValues {
		expected: Vec<TestResultFlat>,
		kind: MismatchedValuesKind,
	},
}

pub enum MismatchedValuesKind {
	ResultCount,
	ValueMismatch(usize),
	InvalidError(usize),
	InvalidValue(usize),
}

pub struct TestReport {
	id: TestId,
	is_wip: bool,
	grade: TestGrade,
	error: Option<TestError>,
	outputs: Option<TestOutputs>,
	output_validity: Option<TestOutputValidity>,
}

impl TestReport {
	pub fn new(id: TestId, set: &TestSet, job_result: TestJobResult) -> Self {
		let mut res = TestReport {
			id,
			is_wip: set[id].config.is_wip(),
			grade: TestGrade::Success,
			error: None,
			outputs: None,
			output_validity: None,
		};

		res.grade_result(set, job_result);

		if res.is_wip && res.has_failed() {
			res.grade = TestGrade::Warning;
		}

		res
	}

	fn grade_result(&mut self, set: &TestSet, job_result: TestJobResult) {
		match job_result {
			TestJobResult::RunningError(e) => {
				self.grade = TestGrade::Failed;
				self.error = Some(TestError::Running(e.to_string()));
			}
			TestJobResult::Timeout => {
				self.grade = TestGrade::Failed;
				self.error = Some(TestError::Timeout);
			}
			TestJobResult::ParserError(results) => {
				self.outputs = Some(TestOutputs::ParsingError(results.to_string()));

				let Some(expected) =
					set[self.id].config.test.as_ref().and_then(|x| x.results.as_ref())
				else {
					self.grade = TestGrade::Warning;
					self.output_validity = Some(TestOutputValidity::Unspecified);
					return;
				};

				match expected {
					TestDetailsResults::QueryResult(ref e) => {
						self.grade = TestGrade::Failed;
						let expected = e.iter().map(|x| x.clone().flatten()).collect();
						self.output_validity = Some(TestOutputValidity::UnexpectParsingError {
							expected: Some(expected),
						});
					}
					TestDetailsResults::ParserError(ref e) => match e.parsing_error {
						BoolOr::Bool(true) => {}
						BoolOr::Bool(false) => {
							self.output_validity = Some(TestOutputValidity::UnexpectParsingError {
								expected: None,
							});
						}
						BoolOr::Value(ref e) => {
							if e.to_string() == results.to_string() {
								return;
							}

							self.grade = TestGrade::Failed;
							self.output_validity =
								Some(TestOutputValidity::MismatchedParsingError {
									expected: e.clone(),
								});
						}
					},
				}
			}
			TestJobResult::Results(results) => {
				let outputs = self.outputs.insert(TestOutputs::Values(
					results.into_iter().map(|x| x.result.map_err(|e| e.to_string())).collect(),
				));

				let Some(expected) =
					set[self.id].config.test.as_ref().and_then(|x| x.results.as_ref())
				else {
					self.grade = TestGrade::Warning;
					self.output_validity = Some(TestOutputValidity::Unspecified);
					return;
				};

				let TestOutputs::Values(outputs) = outputs else {
					unreachable!();
				};

				match expected {
					TestDetailsResults::QueryResult(expected) => {
						let (rough_match, expected): (Vec<_>, Vec<_>) = expected
							.iter()
							.map(|x| (x.rough_match(), x.clone().flatten()))
							.collect();

						if expected.len() != outputs.len() {
							self.grade = TestGrade::Failed;
							self.output_validity = Some(TestOutputValidity::MismatchedValues {
								expected,
								kind: MismatchedValuesKind::ResultCount,
							});
							return;
						}

						for (idx, out) in outputs.iter().enumerate() {
							match (out, &expected[idx]) {
								(Ok(_), TestResultFlat::Error(BoolOr::Bool(false))) => {}
								(
									Ok(_),
									TestResultFlat::Error(BoolOr::Bool(true) | BoolOr::Value(_)),
								) => {
									self.grade = TestGrade::Failed;
									self.output_validity =
										Some(TestOutputValidity::MismatchedValues {
											expected,
											kind: MismatchedValuesKind::InvalidValue(idx),
										});
									return;
								}
								(Ok(r), TestResultFlat::Value(ref e)) => {
									if rough_match[idx] {
										if !r.roughly_equal(&e.0) {
											self.grade = TestGrade::Failed;
											self.output_validity =
												Some(TestOutputValidity::MismatchedValues {
													expected,
													kind: MismatchedValuesKind::ValueMismatch(idx),
												});
											return;
										}
									} else {
										if *r != e.0 {
											self.grade = TestGrade::Failed;
											self.output_validity =
												Some(TestOutputValidity::MismatchedValues {
													expected,
													kind: MismatchedValuesKind::ValueMismatch(idx),
												});
											return;
										}
									}
								}
								(
									Err(_),
									TestResultFlat::Error(BoolOr::Bool(false))
									| TestResultFlat::Value(_),
								) => {
									self.grade = TestGrade::Failed;
									self.output_validity =
										Some(TestOutputValidity::MismatchedValues {
											expected,
											kind: MismatchedValuesKind::InvalidError(idx),
										});
									return;
								}
								(Err(_), TestResultFlat::Error(BoolOr::Bool(true))) => {}
								(Err(ref e), TestResultFlat::Error(BoolOr::Value(ref v))) => {
									if e != v {
										self.grade = TestGrade::Failed;
										self.output_validity =
											Some(TestOutputValidity::MismatchedValues {
												expected,
												kind: MismatchedValuesKind::ValueMismatch(idx),
											});
										return;
									}
								}
							}
						}
					}
					TestDetailsResults::ParserError(e) => match e.parsing_error {
						BoolOr::Bool(false) => {}
						BoolOr::Bool(true) => {
							self.grade = TestGrade::Failed;
							self.output_validity = Some(TestOutputValidity::UnexpectedValues {
								expected: None,
							});
						}
						BoolOr::Value(ref expected) => {
							self.grade = TestGrade::Failed;
							self.output_validity = Some(TestOutputValidity::UnexpectedValues {
								expected: Some(expected.clone()),
							});
						}
					},
				}
			}
		}
	}

	pub fn grade(&self) -> TestGrade {
		self.grade
	}

	pub fn succeeded(&self) -> bool {
		matches!(self.grade, TestGrade::Success)
	}

	pub fn has_warning(&self) -> bool {
		matches!(self.grade, TestGrade::Warning)
	}

	pub fn has_failed(&self) -> bool {
		matches!(self.grade, TestGrade::Failed)
	}

	pub fn has_missing_results(&self) -> bool {
		matches!(self.output_validity, Some(TestOutputValidity::Unspecified))
	}

	pub fn has_error(&self) -> bool {
		self.error.is_some()
	}

	pub fn is_wip(&self) -> bool {
		self.is_wip
	}
}
