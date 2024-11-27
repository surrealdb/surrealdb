use std::{
	fmt::{self, Write as _},
	io::Write as _,
	time::{Duration, Instant},
};

use similar::{Algorithm, DiffableStr, TextDiff};

use crate::{
	format::{self, ansi},
	tests::{
		schema::{BoolOr, TestResultFlat},
		TestSet,
	},
};

use super::{
	MismatchedValuesKind, TestError, TestGrade, TestOutputValidity, TestOutputs, TestReport,
};

impl TestReport {
	pub fn display(&self, tests: &TestSet) {
		self.display_inner(tests).unwrap()
	}

	fn display_inner(&self, tests: &TestSet) -> fmt::Result {
		let use_ansi = atty::is(atty::Stream::Stdout);
		let mut buffer = String::new();
		let mut f = format::IndentFormatter::new(&mut buffer, 2);
		f.increase_depth();

		let name = &tests[self.id].path;

		match self.grade {
			TestGrade::Success => return Ok(()),
			TestGrade::Failed => {
				if use_ansi {
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
				if use_ansi {
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

		if let Some(error) = self.error.as_ref() {
			match error {
				TestError::Timeout => {
					writeln!(f, "> Test `{name}` timed out.")?;
				}
				TestError::Running(x) => {
					writeln!(f, "> Test `{name}` failed to run:\n{x}")?;
				}
			}
			return f.finish();
		}

		if let Some(valid) = self.output_validity.as_ref() {
			match valid {
				TestOutputValidity::Unspecified => {
					writeln!(f, "> Test does not specify any results")?;
					f.increase_depth();
					writeln!(f, "= Got:")?;
					f.increase_depth();
					match self.outputs.as_ref().unwrap() {
						TestOutputs::Values(res) => {
							for e in res {
								match e {
									Ok(x) => {
										writeln!(f, "- Value: {}", x)?;
									}
									Err(e) => {
										writeln!(f, "- Error: {e}")?;
									}
								}
							}
						}
						TestOutputs::ParsingError(res) => {
							writeln!(f, "- Parsing error: {res}")?;
						}
					}
				}
				TestOutputValidity::UnexpectParsingError {
					expected,
				} => {
					writeln!(f, "> Test returned an unexpected parsing error:")?;
					f.increase_depth();
					writeln!(f, "= Got:")?;
					f.indent(|f| {
						let res = self.outputs.as_ref().and_then(|x| x.as_parsing_error()).unwrap();
						writeln!(f, "- Parsing error: {res}")
					})?;
					if let Some(e) = expected {
						writeln!(f, "= Expected:")?;
						f.increase_depth();
						for e in e {
							match e {
								TestResultFlat::Value(x) => {
									writeln!(f, "- Value: {}", x.0)?;
								}
								TestResultFlat::Error(BoolOr::Bool(false)) => {
									writeln!(f, "- Any value")?;
								}
								TestResultFlat::Error(BoolOr::Bool(true)) => {
									writeln!(f, "- Any error")?;
								}
								TestResultFlat::Error(BoolOr::Value(e)) => {
									writeln!(f, "- Error: {e}")?;
								}
							}
						}
					}
				}
				TestOutputValidity::UnexpectedValues {
					expected,
				} => {
					writeln!(f, "> Test returned results where it expected a parsing error:")?;
					f.indent(|f| {
						writeln!(f, "= Got:")?;
						f.indent(|f| {
							let res = self.outputs.as_ref().and_then(|x| x.as_results()).unwrap();
							for e in res {
								match e {
									Ok(x) => {
										writeln!(f, "- Value: {}", x)?;
									}
									Err(e) => {
										writeln!(f, "- Error: {e}")?;
									}
								}
							}
							Ok(())
						})?;
						if let Some(expected) = expected {
							writeln!(f, "= Expected:")?;
							f.indent(|f| writeln!(f, "- Parsing error: {expected}"))?;
						}
						Ok(())
					})?;
				}
				TestOutputValidity::MismatchedParsingError {
					expected,
				} => {
					writeln!(f, "> Test returned mismatched parsing errors:")?;
					f.indent(|f| {
						let res = self.outputs.as_ref().and_then(|x| x.as_parsing_error()).unwrap();
						writeln!(f, "= Got:")?;
						f.indent(|f| writeln!(f, "- Parsing error: {res}"))?;
						writeln!(f, "= Expected:")?;
						f.indent(|f| writeln!(f, "- Parsing error: {expected}"))
					})?;
				}
				TestOutputValidity::MismatchedValues {
					expected,
					kind,
				} => {
					writeln!(f, "> Test `{name}` returned mismatched results:")?;
					f.indent(|f| {
						let res = self.outputs.as_ref().and_then(|x| x.as_results()).unwrap();
						match kind {
							MismatchedValuesKind::ResultCount => {
								writeln!(
									f,
									"> Got {} result but expected {} results",
									res.len(),
									expected.len()
								)?;
								writeln!(f, "= Got:")?;
								f.indent(|f| {
									for e in res {
										match e {
											Ok(x) => {
												writeln!(f, "- Value: {}", x)?;
											}
											Err(e) => {
												writeln!(f, "- Error: {e}")?;
											}
										}
									}
									Ok(())
								})?;
								writeln!(f, "= Expected:")?;
								f.indent(|f| {
									for e in expected {
										match e {
											TestResultFlat::Value(x) => {
												writeln!(f, "\t\t- Value: {}", x.0)?;
											}
											TestResultFlat::Error(BoolOr::Bool(false)) => {
												writeln!(f, "\t\t- Any value")?;
											}
											TestResultFlat::Error(BoolOr::Bool(true)) => {
												writeln!(f, "\t\t- Any error")?;
											}
											TestResultFlat::Error(BoolOr::Value(e)) => {
												writeln!(f, "\t\t- Error: {e}")?;
											}
										}
									}
									Ok(())
								})
							}
							MismatchedValuesKind::ValueMismatch(idx) => {
								writeln!(f, "> Value {idx} was of the proper type but didn't match expected value.")?;
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
								writeln!(f, "= Got:")?;
								f.indent(|f| writeln!(f, "- {got}"))?;
								writeln!(f, "= Expected:")?;
								f.indent(|f| writeln!(f, "- {expected}"))?;

								let diff = TextDiff::configure()
									.algorithm(Algorithm::Myers)
									.deadline(Instant::now() + Duration::from_millis(500))
									.diff_words(got.as_str(), expected.as_str());

								writeln!(f, "= Diff:")?;
								f.indent(|f| {
									write!(f, "- ")?;
									for op in diff.ops() {
										for change in diff.iter_changes(op) {
											match change.tag() {
												similar::ChangeTag::Equal => {}
												similar::ChangeTag::Delete => {
													if use_ansi {
														write!(f, ansi!(red))?;
													} else {
														write!(f, "-[")?;
													}
												}
												similar::ChangeTag::Insert => {
													if use_ansi {
														write!(f, ansi!(green))?;
													} else {
														write!(f, "+[")?;
													}
												}
											}
											if use_ansi {
												write!(
													f,
													ansi!("{}", reset_format),
													change.to_string_lossy()
												)?;
											} else {
												write!(f, "{}]", change.to_string_lossy())?;
											}
										}
									}

									writeln!(f)
								})
							}
							MismatchedValuesKind::InvalidError(idx) => {
								writeln!(
									f,
									"- Value {idx} is an error when an value was expected"
								)?;
								writeln!(f, "= Got:")?;
								f.indent(|f| match res[*idx] {
									Ok(ref x) => {
										writeln!(f, "- Value: {}", x)
									}
									Err(ref e) => {
										writeln!(f, "- Error: {e}")
									}
								})?;
								writeln!(f, "= Expected:")?;
								f.indent(|f| match expected[*idx] {
									TestResultFlat::Value(ref x) => {
										writeln!(f, "- Value: {}", x.0)
									}
									TestResultFlat::Error(BoolOr::Bool(false)) => {
										writeln!(f, "- Any value")
									}
									TestResultFlat::Error(BoolOr::Bool(true)) => {
										writeln!(f, "- Any error")
									}
									TestResultFlat::Error(BoolOr::Value(ref e)) => {
										writeln!(f, "- Error: {e}")
									}
								})
							}
							MismatchedValuesKind::InvalidValue(idx) => {
								writeln!(
									f,
									"- Value {idx} is an value when an error was expected",
								)?;
								writeln!(f, "= Got:")?;
								f.indent(|f| match res[*idx] {
									Ok(ref x) => {
										writeln!(f, "- Value: {}", x)
									}
									Err(ref e) => {
										writeln!(f, "- Error: {e}")
									}
								})?;
								writeln!(f, "= Expected:")?;
								f.indent(|f| match expected[*idx] {
									TestResultFlat::Value(ref x) => {
										writeln!(f, "- Value: {}", x.0)
									}
									TestResultFlat::Error(BoolOr::Bool(false)) => {
										writeln!(f, "- Any value")
									}
									TestResultFlat::Error(BoolOr::Bool(true)) => {
										writeln!(f, "- Any error")
									}
									TestResultFlat::Error(BoolOr::Value(ref e)) => {
										writeln!(f, "- Error: {e}")
									}
								})
							}
						}
					})?;
				}
			}
		}
		f.finish()?;
		println!("{buffer}");
		return Ok(());
	}
}
