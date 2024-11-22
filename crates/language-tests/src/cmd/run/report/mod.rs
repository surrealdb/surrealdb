use std::time::{Duration, Instant};

use super::cmp::RoughlyEq;
use super::TestJobResult;
use crate::tests::{
    schema::{BoolOr, TestDetailsResults, TestResultFlat},
    testset::TestId,
    TestSet,
};
use similar::{Algorithm, TextDiff};
use surrealdb_core::sql::Value as SurValue;
use tracing::{error, info, warn};

mod update;

/// Enum with the outcome of a test
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
    UnexpectedValues { expected: Option<String> },
    /// The parsing error we got does not line up with the expected error.
    MismatchedParsingError { expected: String },
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

                let Some(expected) = set[self.id]
                    .config
                    .test
                    .as_ref()
                    .and_then(|x| x.results.as_ref())
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
                            self.output_validity =
                                Some(TestOutputValidity::UnexpectParsingError { expected: None });
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
                    results
                        .into_iter()
                        .map(|x| x.result.map_err(|e| e.to_string()))
                        .collect(),
                ));

                let Some(expected) = set[self.id]
                    .config
                    .test
                    .as_ref()
                    .and_then(|x| x.results.as_ref())
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
                            self.output_validity =
                                Some(TestOutputValidity::UnexpectedValues { expected: None });
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

    pub fn short_display(&self, tests: &TestSet) {
        let name = &tests[self.id].path;
        match self.grade {
            TestGrade::Success => {
                info!("Test `{name}` finished successfully.");
                if self.is_wip {
                    let mut s = String::new();
                    println!();
                    swriteln!(&mut s,"Test `{name}` succeeded even though it is marked work in progress, the tested issue might be fixed!");
                    if let Some(issue) = tests[self.id].config.issue() {
                        swriteln!(&mut s,"\tIssue https://github.com/surrealdb/surrealdb/issues/{issue} can possibly be closed.");
                    }
                    warn!("{s}")
                }
            }
            TestGrade::Failed => {
                error!("Test `{name}` failed.")
            }
            TestGrade::Warning => {
                warn!("Test `{name}` finished with warnings.")
            }
        }
    }

    pub fn display(&self, tests: &TestSet) {
        let name = &tests[self.id].path;
        if let TestGrade::Success = self.grade {
            return;
        }

        let mut s = String::new();
        if let Some(error) = self.error.as_ref() {
            match error {
                TestError::Timeout => {
                    swriteln!(&mut s, "Test `{name}` timed out.");
                }
                TestError::Running(x) => {
                    swriteln!(&mut s, "Test `{name}` failed to run:\n{x}");
                }
            }
        } else if let Some(valid) = self.output_validity.as_ref() {
            match valid {
                TestOutputValidity::Unspecified => {
                    swriteln!(&mut s, "Test `{name}` does not specify any results");
                    swriteln!(&mut s, "\t- Got:");
                    match self.outputs.as_ref().unwrap() {
                        TestOutputs::Values(res) => {
                            for e in res {
                                match e {
                                    Ok(x) => {
                                        swriteln!(&mut s, "\t\t- Value: {}", x);
                                    }
                                    Err(e) => {
                                        swriteln!(&mut s, "\t\t- Error: {e}");
                                    }
                                }
                            }
                        }
                        TestOutputs::ParsingError(res) => {
                            swriteln!(&mut s, "Parsing error: {res}");
                        }
                    }
                }
                TestOutputValidity::UnexpectParsingError { expected } => {
                    swriteln!(
                        &mut s,
                        "Test `{name}` returned an unexpected parsing error:"
                    );
                    swriteln!(&mut s, "\t- Got:");
                    let res = self
                        .outputs
                        .as_ref()
                        .and_then(|x| x.as_parsing_error())
                        .unwrap();
                    swriteln!(&mut s, "Parsing error: {res}");
                    if let Some(e) = expected {
                        swriteln!(&mut s, "\t- Expected:");
                        for e in e {
                            match e {
                                TestResultFlat::Value(x) => {
                                    swriteln!(&mut s, "\t\t- Value: {}", x.0);
                                }
                                TestResultFlat::Error(BoolOr::Bool(false)) => {
                                    swriteln!(&mut s, "\t\t- Any value");
                                }
                                TestResultFlat::Error(BoolOr::Bool(true)) => {
                                    swriteln!(&mut s, "\t\t- Any error");
                                }
                                TestResultFlat::Error(BoolOr::Value(e)) => {
                                    swriteln!(&mut s, "\t\t- Error: {e}");
                                }
                            }
                        }
                    }
                }
                TestOutputValidity::UnexpectedValues { expected } => {
                    swriteln!(
                        &mut s,
                        "Test `{name}` returned results where it expected a parsing error:"
                    );
                    swriteln!(&mut s, "\t- Got:");
                    let res = self.outputs.as_ref().and_then(|x| x.as_results()).unwrap();
                    for e in res {
                        match e {
                            Ok(x) => {
                                swriteln!(&mut s, "\t\t- Value: {}", x);
                            }
                            Err(e) => {
                                swriteln!(&mut s, "\t\t- Error: {e}");
                            }
                        }
                    }
                    if let Some(expected) = expected {
                        swriteln!(&mut s, "\t- Expected:");
                        swriteln!(&mut s, "Parsing error: {expected}");
                    }
                }
                TestOutputValidity::MismatchedParsingError { expected } => {
                    swriteln!(&mut s, "Test `{name}` returned mismatched parsing errors:");
                    let res = self
                        .outputs
                        .as_ref()
                        .and_then(|x| x.as_parsing_error())
                        .unwrap();
                    swriteln!(&mut s, "\t- Got:");
                    swriteln!(&mut s, "Parsing error: {res}");
                    swriteln!(&mut s, "\t- Expected:");
                    swriteln!(&mut s, "Parsing error: {expected}");
                }
                TestOutputValidity::MismatchedValues { expected, kind } => {
                    swriteln!(&mut s, "Test `{name}` returned mismatched results:");
                    let res = self.outputs.as_ref().and_then(|x| x.as_results()).unwrap();

                    match kind {
                        MismatchedValuesKind::ResultCount => {
                            swriteln!(
                                &mut s,
                                "Got {} result but expected {} results",
                                res.len(),
                                expected.len()
                            );
                            swriteln!(&mut s, "\t- Got:");
                            for e in res {
                                match e {
                                    Ok(x) => {
                                        swriteln!(&mut s, "\t\t- Value: {}", x);
                                    }
                                    Err(e) => {
                                        swriteln!(&mut s, "\t\t- Error: {e}");
                                    }
                                }
                            }
                            swriteln!(&mut s, "\t- Expected:");
                            for e in expected {
                                match e {
                                    TestResultFlat::Value(x) => {
                                        swriteln!(&mut s, "\t\t- Value: {}", x.0);
                                    }
                                    TestResultFlat::Error(BoolOr::Bool(false)) => {
                                        swriteln!(&mut s, "\t\t- Any value");
                                    }
                                    TestResultFlat::Error(BoolOr::Bool(true)) => {
                                        swriteln!(&mut s, "\t\t- Any error");
                                    }
                                    TestResultFlat::Error(BoolOr::Value(e)) => {
                                        swriteln!(&mut s, "\t\t- Error: {e}");
                                    }
                                }
                            }
                        }
                        MismatchedValuesKind::ValueMismatch(idx) => {
                            swriteln!(&mut s, "Value {idx} was of the proper type but didn't match expected value.",);
                            let got = match res[*idx] {
                                Ok(ref x) => x.to_string(),
                                Err(ref e) => e.to_string(),
                            };
                            let expected = match expected[*idx] {
                                TestResultFlat::Value(ref x) => x.0.to_string(),
                                TestResultFlat::Error(BoolOr::Value(ref e)) => e.clone(),
                                TestResultFlat::Error(BoolOr::Bool(_)) => {
                                    unreachable!()
                                }
                            };
                            swriteln!(&mut s, "\t- Got:");
                            swriteln!(&mut s, "\t\t {got}");
                            swriteln!(&mut s, "\t- Expected:");
                            swriteln!(&mut s, "\t\t {expected}");

                            let diff = TextDiff::configure()
                                .algorithm(Algorithm::Myers)
                                .deadline(Instant::now() + Duration::from_millis(500))
                                .diff_words(got.as_str(), expected.as_str());

                            swriteln!(&mut s, "\t- Diff:");
                            for op in diff.ops() {
                                for change in diff.iter_changes(op) {
                                    match change.tag() {
                                        similar::ChangeTag::Equal => {}
                                        similar::ChangeTag::Delete => {
                                            swrite!(&mut s, "\x1b[0;31m");
                                        }
                                        similar::ChangeTag::Insert => {
                                            swrite!(&mut s, "\x1b[0;32m");
                                        }
                                    }
                                    swrite!(&mut s, "{}\x1b[0m", change.to_string_lossy());
                                }
                            }
                            swriteln!(&mut s, "");
                        }
                        MismatchedValuesKind::InvalidError(idx) => {
                            swriteln!(&mut s, "Value {idx} is an error when an value was expected",);
                            swriteln!(&mut s, "\t- Got:");
                            match res[*idx] {
                                Ok(ref x) => {
                                    swriteln!(&mut s, "\t\t- Value: {}", x);
                                }
                                Err(ref e) => {
                                    swriteln!(&mut s, "\t\t- Error: {e}");
                                }
                            }
                            swriteln!(&mut s, "\t- Expected:");
                            match expected[*idx] {
                                TestResultFlat::Value(ref x) => {
                                    swriteln!(&mut s, "\t\t- Value: {}", x.0);
                                }
                                TestResultFlat::Error(BoolOr::Bool(false)) => {
                                    swriteln!(&mut s, "\t\t- Any value");
                                }
                                TestResultFlat::Error(BoolOr::Bool(true)) => {
                                    swriteln!(&mut s, "\t\t- Any error");
                                }
                                TestResultFlat::Error(BoolOr::Value(ref e)) => {
                                    swriteln!(&mut s, "\t\t- Error: {e}");
                                }
                            }
                        }
                        MismatchedValuesKind::InvalidValue(idx) => {
                            swriteln!(&mut s, "Value {idx} is an value when an error was expected",);
                            swriteln!(&mut s, "\t- Got:");
                            match res[*idx] {
                                Ok(ref x) => {
                                    swriteln!(&mut s, "\t\t- Value: {}", x);
                                }
                                Err(ref e) => {
                                    swriteln!(&mut s, "\t\t- Error: {e}");
                                }
                            }
                            swriteln!(&mut s, "\t- Expected:");
                            match expected[*idx] {
                                TestResultFlat::Value(ref x) => {
                                    swriteln!(&mut s, "\t\t- Value: {}", x.0);
                                }
                                TestResultFlat::Error(BoolOr::Bool(false)) => {
                                    swriteln!(&mut s, "\t\t- Any value");
                                }
                                TestResultFlat::Error(BoolOr::Bool(true)) => {
                                    swriteln!(&mut s, "\t\t- Any error");
                                }
                                TestResultFlat::Error(BoolOr::Value(ref e)) => {
                                    swriteln!(&mut s, "\t\t- Error: {e}");
                                }
                            }
                        }
                    }
                }
            }
        }

        match self.grade {
            TestGrade::Success => info!("{s}"),
            TestGrade::Failed => error!("{s}"),
            TestGrade::Warning => {
                if self.is_wip {
                    warn!("{s}\n");
                    warn!("Test `{name}` produces warnings because the test is marked as work in progress.");
                } else {
                    warn!("{s}")
                }
            }
        }
    }
}
