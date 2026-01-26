// A lot of the code here will be expanded on later.
// So we allow dead code for now.
#![allow(dead_code)]

use crate::{
	sql::{Object, Value},
	syn::error::Snippet,
};

mod pass;
pub use pass::{MigratorPass, PassState};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Severity {
	// Breakage only happens in edge cases.
	UnlikelyBreak,
	// Will only break in some situations.
	CanBreak,
	// Pretty much guarenteed to break on any usage.
	WillBreak,
	// Can be automatically fixed.
	Resolution,
}

impl Severity {
	pub fn as_str(&self) -> &str {
		match self {
			Severity::UnlikelyBreak => "unlikely_break",
			Severity::CanBreak => "can_break",
			Severity::WillBreak => "will_break",
			Severity::Resolution => "resolution",
		}
	}
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum IssueKind {
	IncompatibleFuture,
	StoredClosure,
	AllIdiom,
	FieldIdiomFollowed,
	FunctionLogicalAnd,
	FunctionLogicalOr,
	FunctionMathSqrt,
	FunctionMathMin,
	FunctionMathMax,
	MockValue,
	NumberKeyOrdering,
	IdField,
	SearchIndex,
}

impl IssueKind {
	pub fn as_str(&self) -> &str {
		match *self {
			Self::IncompatibleFuture => "incompatible future",
			Self::StoredClosure => "stored closure",
			Self::AllIdiom => "all idiom",
			Self::FieldIdiomFollowed => "field idiom followed",
			Self::FunctionLogicalAnd => "function logical_and",
			Self::FunctionLogicalOr => "function logical_or",
			Self::FunctionMathSqrt => "function math::sqrt",
			Self::FunctionMathMin => "function math::min",
			Self::FunctionMathMax => "function math::max",
			Self::MockValue => "mock value",
			Self::NumberKeyOrdering => "number key ordering",
			Self::IdField => "id field",
			Self::SearchIndex => "search index",
		}
	}
}

pub struct MigrationIssue {
	/// How bad is the issue
	pub severity: Severity,
	/// The message telling what is wrong.
	pub error: String,
	/// Specific information about what is wrong.
	pub details: String,
	/// The type of issue.
	pub kind: IssueKind,
	/// The location of the error.
	pub origin: Vec<Value>,
	/// The location of the error as source code snippet.
	pub error_location: Option<Snippet>,
	/// Possible resolution
	pub resolution: Option<Snippet>,
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
