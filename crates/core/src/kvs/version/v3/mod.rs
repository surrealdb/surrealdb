// A lot of the code here will be expanded on later.
// So we allow dead code for now.
#![allow(dead_code)]

use crate::{sql::Object, syn::error::Snippet};

mod pass;
pub use pass::{MigratorPass, PassState};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Severity {
	MightBreak,
	WillBreak,
	BreakingResolution,
}

impl Severity {
	pub fn as_str(&self) -> &str {
		match self {
			Severity::MightBreak => "might_break",
			Severity::WillBreak => "will_break",
			Severity::BreakingResolution => "breaking_resolution",
		}
	}
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum IssueKind {
	IncompatibleFuture,
}

impl IssueKind {
	pub fn as_str(&self) -> &str {
		match *self {
			Self::IncompatibleFuture => "incompatible future",
		}
	}
}

pub struct MigrationIssue {
	/// How bad is the issue
	severity: Severity,
	/// The message telling what is wrong.
	error: String,
	/// Specific information about what is wrong.
	details: String,
	/// The type of issue.
	kind: IssueKind,
	/// The location of the error.
	origin: String,
	/// The location of the error as source code snippet.
	error_location: Option<Snippet>,
	/// Possible resolution
	resolution: Option<Snippet>,
}

impl MigrationIssue {
	pub fn to_object(&self) -> Object {
		let mut obj = Object::default();
		obj.insert("severity".to_string(), self.severity.as_str().to_string().into());
		obj.insert("error".to_string(), self.error.clone().into());
		obj.insert("kind".to_string(), self.kind.as_str().into());
		obj.insert("origin".to_string(), self.origin.clone().into());
		obj.insert("details".to_string(), self.details.clone().into());

		if let Some(x) = &self.error_location {
			obj.insert("location".to_string(), x.to_object().into());
		}
		if let Some(x) = &self.resolution {
			obj.insert("resolution".to_string(), x.to_object().into());
		}

		obj
	}
}
