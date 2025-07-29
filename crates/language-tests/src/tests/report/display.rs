use std::{
	fmt::{self, Write},
	io::{self, IsTerminal as _},
	time::{Duration, Instant},
};

use crate::{
	cli::ColorMode,
	format::{IndentFormatter, ansi},
	tests::TestSet,
};

use super::{
	MatchValueType, MatcherMismatch, Mismatch, MismatchKind, ResultTypeMismatchReport, TestError,
	TestGrade, TestOutputs, TestReport, TestValueExpectation, ValueMismatchKind,
};
use similar::{Algorithm, TextDiff};
use surrealdb_core::val::Value as SurValue;

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
			ColorMode::Auto => io::stdout().is_terminal(),
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
			super::TestReportKind::MismatchedSignin {
				ref got,
				ref expected,
			} => {
				writeln!(f, "> Test returned invalid signin error")?;
				f.indent(|f| {
					writeln!(f, "= Expected:")?;
					f.indent(|f| writeln!(f, "- Signin error: {expected}"))?;
					writeln!(f, "= Got:")?;
					f.indent(|f| writeln!(f, "- Signin error: {got}"))
				})
			}
			super::TestReportKind::MismatchedSignup {
				ref got,
				ref expected,
			} => {
				writeln!(f, "> Test returned invalid signup error")?;
				f.indent(|f| {
					writeln!(f, "= Expected:")?;
					f.indent(|f| writeln!(f, "- Signup error: {expected}"))?;
					writeln!(f, "= Got:")?;
					f.indent(|f| writeln!(f, "- Signup error: {got}"))
				})
			}
			super::TestReportKind::MismatchedValues(ref v) => {
				Self::display_mismatched_values(v, use_color, f)
			}
		})
	}

	fn display_grade_header(&self, tests: &TestSet, use_color: bool, f: &mut Fmt) -> fmt::Result {
		let name = if let Some(x) = self.extra_name.as_ref() {
			format!("{} {}", tests[self.id].path, x)
		} else {
			tests[self.id].path.clone()
		};

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
			TestError::Paniced(e) => {
				writeln!(f, "> Test failed, tests caused a panic to occur")?;
				f.indent(|f| writeln!(f, "- Panic payload: {e}"))
			}
			TestError::Import(import, error) => {
				writeln!(f, "> Test failed, running import `{import}` caused an error:")?;
				f.indent(|f| writeln!(f, "- {error}"))
			}
		}
	}

	fn display_unspecified(outputs: &TestOutputs, f: &mut Fmt) -> fmt::Result {
		writeln!(f, "> Test does not specify any results")?;
		f.indent(|f| {
			writeln!(f, "= Got:")?;
			f.indent(|f| match outputs {
				TestOutputs::Values(res) => Self::display_value_list(&res, f),
				TestOutputs::ParsingError(res) => {
					writeln!(f, "- Parsing error: {res}")
				}
				TestOutputs::SignupError(res) => {
					writeln!(f, "- Signup error: {res}")
				}
				TestOutputs::SigninError(res) => {
					writeln!(f, "- Signin error: {res}")
				}
			})
		})
	}

	fn display_type_mismatch(mismatch: &ResultTypeMismatchReport, f: &mut Fmt) -> fmt::Result {
		writeln!(f, "> Test returned a different result type then was expected")?;
		f.indent(|f| {
			writeln!(f, "= Expected:")?;
			f.indent(|f| match &mismatch.expected {
				super::TestExpectation::Parsing(e) => match e {
					Some(x) => writeln!(f, "- Parsing Error: {x}"),
					None => writeln!(f, "- Any parsing error"),
				},
				super::TestExpectation::Values(e) => match e {
					Some(e) => Self::display_expectation_list(&e, f),
					None => writeln!(f, "- Any list of query result values"),
				},
				super::TestExpectation::Signin(e) => match e {
					Some(x) => writeln!(f, "- Signin Error: {x}"),
					None => writeln!(f, "- Any signin error"),
				},
				super::TestExpectation::Signup(e) => match e {
					Some(x) => writeln!(f, "- Signup Error: {x}"),
					None => writeln!(f, "- Any signup error"),
				},
			})?;
			writeln!(f, "= Got:")?;
			f.indent(|f| match &mismatch.got {
				TestOutputs::Values(values) => Self::display_value_list(values, f),
				TestOutputs::ParsingError(e) => writeln!(f, "- Parsing error: {e}"),
				TestOutputs::SigninError(e) => writeln!(f, "- Signin error: {e}"),
				TestOutputs::SignupError(e) => writeln!(f, "- Signup error: {e}"),
			})
		})
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
								f.indent(|f| writeln!(f, "- Error: {got}"))
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
						value,
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
