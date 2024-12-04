use std::{
	fmt::{self, Write},
	time::{Duration, Instant},
};

use crate::{
	cli::ColorMode,
	format::{ansi, IndentFormatter},
	tests::TestSet,
};

use super::{
	MatchValueType, MatcherMismatch, Mismatch, MismatchKind, TestError, TestGrade, TestOutputs,
	TestReport, TestValueExpectation, TypeMismatchReport, ValueMismatchKind,
};
use similar::{Algorithm, TextDiff};
use surrealdb_core::sql::Value as SurValue;

type Fmt<'a> = IndentFormatter<&'a mut String>;

impl TestReport {
	pub fn display(&self, tests: &TestSet, color: ColorMode) {
		if self.grade() == TestGrade::Success && !self.is_wip() {
			// nothing to report
			return;
		}
		let use_color = match color {
			ColorMode::Always => true,
			ColorMode::Never => false,
			ColorMode::Auto => atty::is(atty::Stream::Stdout),
		};
		let mut buffer = String::new();
		let mut f = Fmt::new(&mut buffer, 2);
		f.indent(|f| self.display_grade(tests, use_color, f)).unwrap();

		println!("{buffer}");
	}

	fn display_grade(&self, tests: &TestSet, use_color: bool, f: &mut Fmt) -> fmt::Result {
		self.display_grade_header(tests, use_color, f)?;

		f.indent(|f| match self.kind {
			super::TestReportKind::Error(ref e) => self.display_run_error(e, f),
			super::TestReportKind::NoExpectation {
				ref output,
			} => Self::display_unspecified(output, f),
			super::TestReportKind::Valid => Ok(()),
			super::TestReportKind::MismatchedType(ref mismatch) => {
				Self::display_type_mismatch(mismatch, f)
			}
			super::TestReportKind::MismatchedParsing {
				ref got,
				ref expected,
			} => {
				writeln!(f, "> Test returned invalid parsing error")?;
				f.indent(|f| {
					writeln!(f, "= Expected:")?;
					f.indent(|f| writeln!(f, "- Parsing error: {expected}"))?;
					writeln!(f, "= Got:")?;
					f.indent(|f| writeln!(f, "- Parsing error: {got}"))
				})
			}
			super::TestReportKind::MismatchedValues(ref v) => {
				Self::display_mismatched_values(v, use_color, f)
			}
		})
	}

	fn display_grade_header(&self, tests: &TestSet, use_color: bool, f: &mut Fmt) -> fmt::Result {
		let name = &tests[self.id].path;

		match self.grade() {
			TestGrade::Success => {
				if tests[self.id].config.is_wip() {
					if use_color {
						writeln!(
							f,
							ansi!(
								" ==> ",
								green,
								"Success",
								reset_format,
								" for ",
								bold,
								"{}",
								reset_format,
								":"
							),
							name
						)?;
					} else {
						writeln!(f, " ==> Success for {name}:")?;
					}
					f.indent(|f| {
						writeln!(f, "> Tests succeeded even though it is marked Work in Progress")
					})?;

					if let Some(issue) = tests[self.id].config.issue() {
						f.indent(|f| {
							writeln!(f, "> Issue {issue} could maybe be closed.")?;
							f.indent(|f| {
								writeln!(
									f,
									"- https://github.com/surrealdb/surrealdb/issues/{issue}."
								)
							})
						})?;
					}
				} else {
					return Ok(());
				}
			}
			TestGrade::Failed => {
				if use_color {
					writeln!(
						f,
						ansi!(
							" ==> ",
							red,
							"Error",
							reset_format,
							" for ",
							bold,
							"{}",
							reset_format,
							":"
						),
						name
					)?;
				} else {
					writeln!(f, " ==> Error for {name}:")?;
				}
				f.increase_depth();
			}
			TestGrade::Warning => {
				if use_color {
					writeln!(
						f,
						ansi!(
							" ==> ",
							yellow,
							"Warning",
							reset_format,
							" for ",
							bold,
							"{}",
							reset_format,
							":"
						),
						name
					)?;
				} else {
					writeln!(f, " ==> Warning for {name}")?;
				}
				f.increase_depth();
				if self.is_wip {
					writeln!(
						f,
						"! Test produces warnings because the test is marked as work in progress."
					)?;
				}
			}
		}
		Ok(())
	}

	fn display_run_error(&self, err: &TestError, f: &mut Fmt) -> fmt::Result {
		match err {
			TestError::Timeout => {
				writeln!(f, "> Test exceeded the set time limit")
			}
			TestError::Running(e) => {
				writeln!(f, "> Test failed to run, returning an error before the test could run.")?;
				f.indent(|f| writeln!(f, "- Error: {e}"))
			}
		}
	}

	fn display_unspecified(outputs: &TestOutputs, f: &mut Fmt) -> fmt::Result {
		writeln!(f, "> Test does not specify any results")?;
		f.indent(|f| {
			writeln!(f, "= Got:")?;
			f.indent(|f| match outputs {
				TestOutputs::Values(res) => Self::display_value_list(res, f),
				TestOutputs::ParsingError(res) => {
					writeln!(f, "- Parsing error: {res}")
				}
			})
		})
	}

	fn display_type_mismatch(mismatch: &TypeMismatchReport, f: &mut Fmt) -> fmt::Result {
		match mismatch {
			TypeMismatchReport::ExpectedParsingError {
				got,
				expected,
			} => {
				writeln!(f, "> Test returned a value when a parsing error was expected")?;
				f.indent(|f| {
					writeln!(f, "= Expected:")?;
					f.indent(|f| match expected {
						Some(x) => writeln!(f, "- Error: {x}"),
						None => writeln!(f, "- Any parsing error"),
					})?;
					writeln!(f, "= Got:")?;
					f.indent(|f| Self::display_value_list(&got, f))
				})
			}
			TypeMismatchReport::ExpectedValues {
				got,
				expected,
			} => {
				writeln!(f, "> Test returned a parsing error when normal values were expected")?;
				f.indent(|f| {
					writeln!(f, "= Expected:")?;
					match expected {
						None => f.indent(|f| writeln!(f, "- Any non parsing error result"))?,
						Some(expected) => {
							f.indent(|f| Self::display_expectation_list(expected, f))?
						}
					}
					writeln!(f, "= Got:")?;
					f.indent(|f| writeln!(f, "- Parsing error: {got}"))
				})
			}
		}
	}

	fn display_mismatched_values(values: &[Mismatch], use_color: bool, f: &mut Fmt) -> fmt::Result {
		writeln!(f, "> Some returned values did not meet expectation")?;
		for v in values {
			match v.kind {
				MismatchKind::Unexpected {
					ref got,
				} => f.indent(|f| {
					writeln!(f, "> Value with index `{}` was not expected.", v.index)?;
					writeln!(f, "> Test specified less results than were returned")?;
					f.indent(|f| {
						writeln!(f, "= Got:")?;

						f.indent(|f| Self::display_value(got, f))
					})
				})?,
				MismatchKind::Missing {
					ref expected,
				} => f.indent(|f| {
					writeln!(f, "> Missing value with at index `{}`", v.index)?;
					writeln!(f, "> Test only specified more results than were returned")?;
					f.indent(|f| {
						writeln!(f, "= Expected:")?;
						f.indent(|f| Self::display_expectation(expected, f))
					})
				})?,
				MismatchKind::Value(ref x) => f.indent(|f| {
					writeln!(
						f,
						"> Returned value at index `{}` did not match expectation.",
						v.index
					)?;
					match x {
						ValueMismatchKind::InvalidError {
							expected,
							got,
						} => {
							writeln!(f, "> Got a different error then was expected")?;
							f.indent(|f| {
								writeln!(f, "= Expected:")?;
								f.indent(|f| writeln!(f, "- Error: {expected}"))?;
								writeln!(f, "= Got:")?;
								f.indent(|f| writeln!(f, "- Error: {got}"))?;
								writeln!(f, "= Diff:")?;
								f.indent(|f| Self::display_diff(got, expected, use_color, f))
							})
						}
						ValueMismatchKind::InvalidValue {
							expected,
							got,
						} => {
							writeln!(f, "> Got a different value then was expected")?;
							f.indent(|f| {
								writeln!(f, "= Expected:")?;
								f.indent(|f| writeln!(f, "- Value: {expected}"))?;
								writeln!(f, "= Got:")?;
								f.indent(|f| writeln!(f, "- Value: {got}"))?;
								writeln!(f, "= Diff:")?;
								f.indent(|f| Self::display_diff(got, expected, use_color, f))
							})
						}
						ValueMismatchKind::ExpectedError {
							expected,
							got,
						} => {
							writeln!(f, "> Expected an error but got a value")?;
							f.indent(|f| {
								writeln!(f, "= Expected:")?;
								if let Some(expected) = expected {
									f.indent(|f| writeln!(f, "- Error: {expected}"))?;
								} else {
									f.indent(|f| writeln!(f, "- Any error"))?;
								}
								writeln!(f, "= Got:")?;
								f.indent(|f| writeln!(f, "- Value: {got}"))
							})
						}
						ValueMismatchKind::ExpectedValue {
							expected,
							got,
						} => {
							writeln!(f, "> Expected a value but got an error")?;
							f.indent(|f| {
								writeln!(f, "= Expected:")?;
								if let Some(expected) = expected {
									f.indent(|f| writeln!(f, "- Value: {expected}"))?;
								} else {
									f.indent(|f| writeln!(f, "- Any value"))?;
								}
								writeln!(f, "= Got:")?;
								f.indent(|f| writeln!(f, "- Value: {got}"))
							})
						}
					}
				})?,
				MismatchKind::Matcher(ref m) => match m {
					MatcherMismatch::Error {
						error,
						got,
					} => {
						writeln!(f, "> Running the matching expression resulted in an error")?;
						f.indent(|f| writeln!(f, "- Error: {error}"))?;
						writeln!(f, "> Test returned the following data:")?;
						f.indent(|f| Self::display_value(got, f))?;
					}
					MatcherMismatch::Failed {
						matcher,
						ref value,
					} => {
						writeln!(f, "> Value failed to match matching expression")?;
						f.indent(|f| {
							writeln!(f, "= Matching expression:")?;
							f.indent(|f| writeln!(f, "- : {matcher}"))?;
							writeln!(f, "= Got:")?;
							f.indent(|f| Self::display_value(value, f))
						})?
					}
					MatcherMismatch::UnexpectedValue {
						got,
					} => {
						writeln!(f, "> Matcher expected a value but got an error")?;
						f.indent(|f| {
							writeln!(f, "= Got:")?;
							f.indent(|f| writeln!(f, "- Value: {got}"))
						})?
					}
					MatcherMismatch::UnexpectedError {
						got,
					} => {
						writeln!(f, "> Matcher expected a error but got an value ")?;
						f.indent(|f| {
							writeln!(f, "= Got:")?;
							f.indent(|f| writeln!(f, "- Error: {got}"))
						})?
					}
					MatcherMismatch::OutputType {
						got,
					} => {
						writeln!(f, "> Matching expression did not return a boolean.")?;
						f.indent(|f| {
							writeln!(f, "= Matching expression returned value:")?;
							f.indent(|f| writeln!(f, "- Value: {got}"))
						})?
					}
				},
			}
		}
		Ok(())
	}

	fn display_value_list(values: &[Result<SurValue, String>], f: &mut Fmt) -> fmt::Result {
		for v in values {
			Self::display_value(v, f)?;
		}
		Ok(())
	}
	fn display_value(v: &Result<SurValue, String>, f: &mut Fmt) -> fmt::Result {
		match v {
			Ok(x) => {
				writeln!(f, "- Value: {x}")
			}
			Err(e) => {
				writeln!(f, "- Error: {e}")
			}
		}
	}

	fn display_expectation_list(values: &[TestValueExpectation], f: &mut Fmt) -> fmt::Result {
		for v in values {
			Self::display_expectation(v, f)?;
		}
		Ok(())
	}

	fn display_expectation(v: &TestValueExpectation, f: &mut Fmt) -> fmt::Result {
		match v {
			TestValueExpectation::Error(e) => match e {
				Some(e) => writeln!(f, "- Error: {e}"),
				None => writeln!(f, "- Any Error"),
			},
			TestValueExpectation::Value(v) => match v {
				Some(v) => writeln!(f, "- Value: {}", v.expected),
				None => writeln!(f, "- Any value"),
			},
			TestValueExpectation::Matcher(m) => match m.matcher_value_type {
				MatchValueType::Both => {
					writeln!(f, "- A result to match matching expression: {}", m.value)
				}
				MatchValueType::Error => {
					writeln!(f, "- A error to match matching expression: {}", m.value)
				}
				MatchValueType::Value => {
					writeln!(f, "- A value to match matching expression: {}", m.value)
				}
			},
		}
	}

	fn display_diff<F: fmt::Display>(
		got: &F,
		expected: &F,
		use_color: bool,
		f: &mut Fmt,
	) -> fmt::Result {
		let got = got.to_string();
		let expected = expected.to_string();

		let diff = TextDiff::configure()
			.algorithm(Algorithm::Myers)
			.deadline(Instant::now() + Duration::from_millis(500))
			.diff_words(got.as_str(), expected.as_str());

		write!(f, "- ")?;
		for op in diff.ops() {
			for change in diff.iter_changes(op) {
				match change.tag() {
					similar::ChangeTag::Equal => {}
					similar::ChangeTag::Delete => {
						if use_color {
							write!(f, ansi!(red))?;
						} else {
							write!(f, "-[")?;
						}
					}
					similar::ChangeTag::Insert => {
						if use_color {
							write!(f, ansi!(green))?;
						} else {
							write!(f, "+[")?;
						}
					}
				}
				if use_color {
					write!(f, ansi!("{}", reset_format), change.to_string_lossy())?;
				} else {
					write!(f, "{}]", change.to_string_lossy())?;
				}
			}
		}
		writeln!(f)
	}
}
