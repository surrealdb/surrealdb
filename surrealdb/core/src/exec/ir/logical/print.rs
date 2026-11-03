//! The text form of the logical dialect's attributes.
//!
//! Printing then parsing then printing yields the text printed first, for every logical tag,
//! statement families included. Attribute types print structurally: a definition template prints
//! as its value literal and a stored-clause region prints as ops. An op whose attributes the
//! syntax cannot read answers a [`ParseError`] and never a default op.
//!
//! # Grammar
//!
//! An op's attribute text is `key = value, key = value, …` in declaration order, and is empty
//! for an op with no attributes. A value is one of:
//!
//! - an integer or a float, as a bare token;
//! - `true` or `false`;
//! - a double-quoted string that escapes `\`, `"`, newline, carriage return, tab and every other
//!   control character as `\u{hex}`;
//! - `none` or `some(value)` for an option;
//! - `[value, …]` for a sequence and `(value, value)` for a pair;
//! - `Variant` for a variant without a payload, `Variant(value, …)` for a positional payload and
//!   `Variant { key = value, … }` for a named payload;
//! - `{ key = value, … }` for a struct.
//!
//! A language value that has a SurrealQL text embeds it as a quoted string: a `Kind` is its
//! SurrealQL text, read back with [`crate::syn::kind`], and a template's `Value` is its SurrealQL
//! literal, read back with [`crate::syn::value`].
//!
//! The reader is strict. Keys appear in declaration order, and a missing, extra, reordered or
//! repeated key, an unknown variant, a malformed token or text after the last attribute is a
//! parse error. An attribute the text does not carry is unreadable and answers a parse error:
//! credentials print as `redacted`, and a `DdlChecks` other than `DdlChecks::None` or a join alias
//! that is not of the minted form prints as `unsupported`.
//!
//! An attribute typed `&'static str` (a template's `ty`, a template hole's field name, an arm
//! label) is read through [`intern`], which holds a bounded set of names.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::HashSet;
use std::fmt::Write as _;
use std::ops::Bound;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use smallvec::{Array, SmallVec};
use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql;

use super::ops::{
	AccessAttrs, AggregateShape, AlterAttrs, AssignAttrs, ClosureLoopAttrs, ComputedReadsShape,
	DecisiveKind, DefineAttrs, DerivedField, DerivedReads, EventClause, EventDefs, EventShape,
	EventsAttrs, FieldClause, FieldClauses, FieldGate, FieldShape, FieldStep, FieldsShape,
	GateClause, ImageLayers, ImageView, IndexDefs, LetAttrs, LetBinding, LinkedStages, LiveAttrs,
	LogOp, LogTag, LookupAttrs, LookupSubjectShape, LoopKind, LoopSink, MatchInsertAttrs,
	MatchUpdateAttrs, ProjectAttrs, RebuildAttrs, RecurseAttrs, RecurseInstructionShape,
	RecursePathForm, RemoveAttrs, RepairField, RepairKind, RepairTable, ShowAttrs, SortAttrs,
	SortKeyShape, SortShape, SourceAttrs, SourceOrder, SourceRefShape, StepAttrs, Stored,
	SubscriberAdmission, SubscriberAttrs, SubscriberLayer, SubscriberProjection, TableDefs,
	TxnControl, TxnRegionEnd, TxnRegionMode, TxnStart, UseTarget, WindowCallShape, WindowShape,
	WriteAttrs, WriteBody, WriteBranching, WriteDerived, WriteGates, WriteImage, WriteIndexes,
	WriteMark, WriteMaterialized, WriteRepairs, WriteRetry, WriteSchema, WriteStageKind,
	WriteStages, WriteStored, WriteViews,
};
use crate::doc::Action as DocAction;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::ir::core::parse::ParseError;
use crate::exec::ir::core::print::AttrSyntax;
use crate::exec::ir::expr::ops::{KeyBound, KeyRange};
use crate::exec::ir::expr::print::{
	f32_text, f64_text, read_f32, read_f64, read_kind_text, read_value, value_text,
};
use crate::exec::ir::iface::choose::{
	ArmBody, ArmEquivalence, ArmLabel, ArmSpec, ChooseGranularity, ChooseOrigin, ChooseSpec,
	ExhaustiveFact, GenericArm, Guard, IndexRef, ValueKindClass,
};
use crate::exec::ir::iface::estimate::SizeBucket;
use crate::exec::ir::iface::region_kind::StatementCommit;
use crate::exec::ir::iface::statements::{
	AccessDurationShape, AccessOp, AccessSecrets, AccessSubjectShape, AccessTypeShape, AlterAccess,
	AlterApiClauseShape, AlterDefaultShape, AlterEvent, AlterField, AlterFunction, AlterObject,
	AlterTable, AlterTemplate, AlterUser, ApiActionShape, ApiConfigShape, BearerShape,
	BindExpandShape, BindInsertEdge, BindInsertNode, BindJoinShape, BindPathShape, BindStepShape,
	ConfigShape, ContentMode, DdlChecks, DdlGuard, DefaultConfigShape, DefaultShape, DefineAccess,
	DefineApi, DefineField, DefineFunction, DefineObject, DefineTable, DefineUser,
	DefinitionTemplate, EnumEdit, IndexColumn, IndexSpec, InfoSubject, JwtIssueShape, JwtShape,
	JwtVerifyShape, LiveShape, MatchBindings, MatchColumnShape, MatchData, MatchInsertEdge,
	MatchInsertNode, MatchOutputShape, MatchPattern, MiddlewareShape, OnDeleteShape, OutputShape,
	PermissionShape, PermissionsShape, Place, PlacePart, RecordAccessShape, ReferenceShape,
	RemoveObject, ShowSince, TemplateHole, UnaddressedPart, UserDurationShape, UserSecrets,
	ViewShape, WriteFlags, WriteForm,
};
use crate::exec::ir::iface::unit::UnitKind;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::operators::write::stage::{LightweightReason, RecordImage, TableTypeFor};
use crate::exec::operators::{
	EdgeBinding, EndpointField, ExpandDir, JoinType, PathMode as OperatorPathMode,
	ShortestSelector, SplitTarget,
};
use crate::expr::access_type::{BearerAccessSubject, BearerAccessType};
use crate::expr::graphql_config::{
	GraphQLConfig, GraphQLFunctionsConfig, GraphQLIntrospectionConfig, GraphQLTablesConfig,
};
use crate::expr::index::{DiskAnnParams, Distance, FullTextParams, HnswParams, VectorType};
use crate::expr::language::Language;
use crate::expr::logical::{
	Emission, GuardKind, JoinKind, LookupTrace, OutputPath, OutputPathPart, RelationAlias,
	WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunction,
};
use crate::expr::lookup::LookupKind;
use crate::expr::match_plan::{
	BindingDef, BindingKind, DetachMode, EdgeQuantifier, EdgeStep, ExpandDirection, NodeStep,
	PathMode, PathPrefixPlan, PathSearch,
};
use crate::expr::module::{ModuleExecutable, ModuleName, SiloExecutable, SurrealismExecutable};
use crate::expr::part::Recurse;
use crate::expr::scoring::Scoring;
use crate::expr::statements::access::PurgeKind;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::table::{TableMigrateAction, TableMigrateClause};
use crate::expr::statements::define::{DefineKind, MigrateClause};
use crate::expr::statements::remove::SqlTypeObject;
use crate::expr::table_type::{Relation, TableType};
use crate::expr::tokenizer::SegmentLanguage;
use crate::expr::{
	Algorithm, ApiMethod, AssignOperator, Base, ChangeFeed, Dir, EventKind, Explain, ExplainFormat,
	Filter, Idiom, Kind, RecordIdKeyGen, Tokenizer, With,
};
use crate::iam::{Action as IamAction, ConfigKind, ResourceKind, ScramCredential};
use crate::types::PublicNumber;
use crate::val::{Datetime, File, Uuid, Value};

// ---------------------------------------------------------------------------------------------
// The syntax
// ---------------------------------------------------------------------------------------------

/// The logical dialect's attribute syntax.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LogicalSyntax;

impl AttrSyntax for LogicalSyntax {
	/// Appends `key = value, …` for the attributes of `kind` and nothing for an op without any. An
	/// op of another dialect prints `unsupported`, which no logical tag reads.
	fn print_attrs(&self, kind: &OpKind, out: &mut String) {
		match kind {
			OpKind::Logical(op) => put_op(op, &mut Fields::new(out)),
			OpKind::Expr(_) | OpKind::Phys(_) => out.push_str("unsupported"),
		}
	}

	/// Reads the attributes of the logical op `tag` names. Text that the tag's attribute list does
	/// not match exactly answers a [`ParseError`].
	fn parse_attrs(&self, tag: OpTag, text: &str) -> Result<OpKind, ParseError> {
		let OpTag::Logical(tag) = tag else {
			return Err(ParseError {
				line: 0,
				detail: format!("logical::parse_attrs: {} is not a logical op", tag.mnemonic()),
			});
		};
		let mut cursor = Cursor::new(text);
		let op = get_op(tag, &mut cursor)?;
		cursor.finish()?;
		Ok(OpKind::Logical(op))
	}
}

// ---------------------------------------------------------------------------------------------
// The ops
// ---------------------------------------------------------------------------------------------

/// Appends the attributes of `op` in declaration order.
fn put_op(op: &LogOp, w: &mut Fields<'_>) {
	match op {
		LogOp::Request {
			fragment,
			nested,
		} => {
			w.field("fragment", fragment);
			w.field("nested", nested);
		}
		LogOp::Txn {
			mode,
		} => w.field("mode", mode),
		LogOp::Stmt {
			commit,
			control,
		} => {
			w.field("commit", commit);
			w.field("control", control);
		}
		LogOp::Statement {
			changes_catalog,
			served,
		} => {
			w.field("changes_catalog", changes_catalog);
			w.field("served", served);
		}
		LogOp::Source(attrs) => attrs.put_fields(w),
		LogOp::Values {
			for_update,
			written,
		} => {
			w.field("for_update", for_update);
			w.field("written", written);
		}
		LogOp::Match {
			bindings,
		} => w.field("bindings", bindings),
		LogOp::MatchRead {
			optional_group,
			patterns,
		} => {
			w.field("optional_group", optional_group);
			w.field("patterns", patterns);
		}
		LogOp::MatchUpdate(attrs) => attrs.put_fields(w),
		LogOp::MatchDelete {
			target,
			detach,
		} => {
			w.field("target", target);
			w.field("detach", detach);
		}
		LogOp::MatchInsert(attrs) => attrs.put_fields(w),
		LogOp::MatchOutput(shape) => shape.put_fields(w),
		LogOp::BindStep(shape) => w.field("step", shape),
		LogOp::Write(attrs) => attrs.put_fields(w),
		LogOp::Assign(attrs) => attrs.put_fields(w),
		LogOp::Unset {
			place,
		} => w.field("place", place),
		LogOp::Content {
			mode,
		} => w.field("mode", mode),
		LogOp::DdlGuard {
			guard,
		} => w.field("guard", guard),
		LogOp::Define(attrs) => attrs.put_fields(w),
		LogOp::Remove(attrs) => attrs.put_fields(w),
		LogOp::Alter(attrs) => attrs.put_fields(w),
		LogOp::Rebuild(attrs) => attrs.put_fields(w),
		LogOp::Join {
			kind,
			alias,
		} => {
			w.field("kind", kind);
			w.field("alias", alias);
		}
		LogOp::PermissionFilter {
			guard,
			computed,
		} => {
			w.field("guard", guard);
			w.field("computed", computed);
		}
		LogOp::Aggregate(attrs) => attrs.put_fields(w),
		LogOp::Sort(attrs) => attrs.put_fields(w),
		LogOp::Window(attrs) => attrs.put_fields(w),
		LogOp::Limit {
			limit,
			start,
		} => {
			w.field("limit", limit);
			w.field("start", start);
		}
		LogOp::Project(attrs) => attrs.put_fields(w),
		LogOp::Explain {
			analyze,
			format,
		} => {
			w.field("analyze", analyze);
			w.field("format", format);
		}
		LogOp::Let(attrs) => attrs.put_fields(w),
		LogOp::Loop {
			kind,
			sink,
			order,
			step,
		} => {
			w.field("kind", kind);
			w.field("sink", sink);
			w.field("order", order);
			w.field("step", step);
		}
		LogOp::Info {
			subject,
			structure,
		} => {
			w.field("subject", subject);
			w.field("structure", structure);
		}
		LogOp::Use {
			target,
		} => w.field("target", target),
		LogOp::Option {
			name,
			value,
		} => {
			w.field("name", name);
			w.field("value", value);
		}
		LogOp::Access(attrs) => attrs.put_fields(w),
		LogOp::Live(attrs) => attrs.put_fields(w),
		LogOp::Subscriber(attrs) => attrs.put_fields(w),
		LogOp::Show(attrs) => attrs.put_fields(w),
		LogOp::Events(attrs) => attrs.put_fields(w),
		LogOp::Choose(spec) => spec.put_fields(w),
		LogOp::WriteStage {
			stage,
			mark,
		} => {
			w.field("stage", stage);
			w.field("mark", mark);
		}
		LogOp::Split {
			targets,
		} => w.field("targets", targets),
		LogOp::Subquery
		| LogOp::UnionAll
		| LogOp::Filter
		| LogOp::Distinct
		| LogOp::Fetch
		| LogOp::Timeout
		| LogOp::VersionScope
		| LogOp::Only
		| LogOp::Eval
		| LogOp::Block
		| LogOp::Return
		| LogOp::If
		| LogOp::Sleep
		| LogOp::Kill
		| LogOp::Yield
		| LogOp::Next
		| LogOp::Break => {}
	}
}

/// Reads the attributes of the op `tag` names.
fn get_op(tag: LogTag, c: &mut Cursor<'_>) -> Result<LogOp, ParseError> {
	let first = &mut true;
	Ok(match tag {
		LogTag::Request => {
			let fragment = c.field("fragment", first)?;
			let nested = c.field("nested", first)?;
			LogOp::Request {
				fragment,
				nested,
			}
		}
		LogTag::Txn => LogOp::Txn {
			mode: c.field("mode", first)?,
		},
		LogTag::Stmt => LogOp::Stmt {
			commit: c.field("commit", first)?,
			control: c.field("control", first)?,
		},
		LogTag::Statement => LogOp::Statement {
			changes_catalog: c.field("changes_catalog", first)?,
			served: c.field("served", first)?,
		},
		LogTag::Source => LogOp::Source(Box::new(SourceAttrs::get_fields(c, first)?)),
		LogTag::Values => {
			let for_update = c.field("for_update", first)?;
			let written = c.field("written", first)?;
			LogOp::Values {
				for_update,
				written,
			}
		}
		LogTag::Match => LogOp::Match {
			bindings: c.field("bindings", first)?,
		},
		LogTag::MatchRead => {
			let optional_group = c.field("optional_group", first)?;
			let patterns = c.field("patterns", first)?;
			LogOp::MatchRead {
				optional_group,
				patterns,
			}
		}
		LogTag::MatchUpdate => {
			LogOp::MatchUpdate(Box::new(MatchUpdateAttrs::get_fields(c, first)?))
		}
		LogTag::MatchDelete => {
			let target = c.field("target", first)?;
			let detach = c.field("detach", first)?;
			LogOp::MatchDelete {
				target,
				detach,
			}
		}
		LogTag::MatchInsert => {
			LogOp::MatchInsert(Box::new(MatchInsertAttrs::get_fields(c, first)?))
		}
		LogTag::MatchOutput => {
			LogOp::MatchOutput(Box::new(MatchOutputShape::get_fields(c, first)?))
		}
		LogTag::BindStep => LogOp::BindStep(c.field("step", first)?),
		LogTag::Subquery => LogOp::Subquery,
		LogTag::Write => LogOp::Write(Box::new(WriteAttrs::get_fields(c, first)?)),
		LogTag::Assign => LogOp::Assign(Box::new(AssignAttrs::get_fields(c, first)?)),
		LogTag::Unset => LogOp::Unset {
			place: c.field("place", first)?,
		},
		LogTag::Content => LogOp::Content {
			mode: c.field("mode", first)?,
		},
		LogTag::DdlGuard => LogOp::DdlGuard {
			guard: c.field("guard", first)?,
		},
		LogTag::Define => LogOp::Define(Box::new(DefineAttrs::get_fields(c, first)?)),
		LogTag::Remove => LogOp::Remove(Box::new(RemoveAttrs::get_fields(c, first)?)),
		LogTag::Alter => LogOp::Alter(Box::new(AlterAttrs::get_fields(c, first)?)),
		LogTag::Rebuild => LogOp::Rebuild(Box::new(RebuildAttrs::get_fields(c, first)?)),
		LogTag::UnionAll => LogOp::UnionAll,
		LogTag::Join => {
			let kind = c.field("kind", first)?;
			let alias = c.field("alias", first)?;
			LogOp::Join {
				kind,
				alias,
			}
		}
		LogTag::Filter => LogOp::Filter,
		LogTag::PermissionFilter => {
			let guard = c.field("guard", first)?;
			let computed = c.field("computed", first)?;
			LogOp::PermissionFilter {
				guard,
				computed,
			}
		}
		LogTag::Split => LogOp::Split {
			targets: c.field("targets", first)?,
		},
		LogTag::Aggregate => LogOp::Aggregate(Box::new(AggregateShape::get_fields(c, first)?)),
		LogTag::Distinct => LogOp::Distinct,
		LogTag::Sort => LogOp::Sort(Box::new(SortAttrs::get_fields(c, first)?)),
		LogTag::Window => LogOp::Window(Box::new(WindowShape::get_fields(c, first)?)),
		LogTag::Limit => {
			let limit = c.field("limit", first)?;
			let start = c.field("start", first)?;
			LogOp::Limit {
				limit,
				start,
			}
		}
		LogTag::Project => LogOp::Project(Box::new(ProjectAttrs::get_fields(c, first)?)),
		LogTag::Fetch => LogOp::Fetch,
		LogTag::Timeout => LogOp::Timeout,
		LogTag::VersionScope => LogOp::VersionScope,
		LogTag::Only => LogOp::Only,
		LogTag::Explain => {
			let analyze = c.field("analyze", first)?;
			let format = c.field("format", first)?;
			LogOp::Explain {
				analyze,
				format,
			}
		}
		LogTag::Eval => LogOp::Eval,
		LogTag::Block => LogOp::Block,
		LogTag::Let => LogOp::Let(Box::new(LetAttrs::get_fields(c, first)?)),
		LogTag::Return => LogOp::Return,
		LogTag::Loop => {
			let kind = c.field("kind", first)?;
			let sink = c.field("sink", first)?;
			let order = c.field("order", first)?;
			let step = c.field("step", first)?;
			LogOp::Loop {
				kind,
				sink,
				order,
				step,
			}
		}
		LogTag::If => LogOp::If,
		LogTag::Info => {
			let subject = c.field("subject", first)?;
			let structure = c.field("structure", first)?;
			LogOp::Info {
				subject,
				structure,
			}
		}
		LogTag::Sleep => LogOp::Sleep,
		LogTag::Use => LogOp::Use {
			target: c.field("target", first)?,
		},
		LogTag::Option => {
			let name = c.field("name", first)?;
			let value = c.field("value", first)?;
			LogOp::Option {
				name,
				value,
			}
		}
		LogTag::Access => LogOp::Access(Box::new(AccessAttrs::get_fields(c, first)?)),
		LogTag::Kill => LogOp::Kill,
		LogTag::Live => LogOp::Live(Box::new(LiveAttrs::get_fields(c, first)?)),
		LogTag::Subscriber => LogOp::Subscriber(Box::new(SubscriberAttrs::get_fields(c, first)?)),
		LogTag::Show => LogOp::Show(Box::new(ShowAttrs::get_fields(c, first)?)),
		LogTag::Events => LogOp::Events(Box::new(EventsAttrs::get_fields(c, first)?)),
		LogTag::Choose => LogOp::Choose(Box::new(ChooseSpec::get_fields(c, first)?)),
		LogTag::WriteStage => {
			let stage = c.field("stage", first)?;
			let mark = c.field("mark", first)?;
			LogOp::WriteStage {
				stage,
				mark,
			}
		}
		LogTag::Yield => LogOp::Yield,
		LogTag::Next => LogOp::Next,
		LogTag::Break => LogOp::Break,
	})
}

// ---------------------------------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------------------------------

/// A value of the attribute text: how it prints and how it is read back.
trait Text: Sized {
	fn put(&self, out: &mut String);
	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError>;
}

/// A struct whose fields print as `key = value` pairs, inline at the top of an op's text or
/// inside braces.
trait Record: Sized {
	fn put_fields(&self, w: &mut Fields<'_>);
	fn get_fields(c: &mut Cursor<'_>, first: &mut bool) -> Result<Self, ParseError>;
}

/// The writer of a comma-separated list of fields or positional elements.
struct Fields<'o> {
	out: &'o mut String,
	first: bool,
}

impl<'o> Fields<'o> {
	fn new(out: &'o mut String) -> Self {
		Self {
			out,
			first: true,
		}
	}

	fn separate(&mut self) {
		if !self.first {
			self.out.push_str(", ");
		}
		self.first = false;
	}

	fn begin_field(&mut self, name: &str) {
		self.separate();
		self.out.push_str(name);
		self.out.push_str(" = ");
	}

	fn field<T: Text>(&mut self, name: &str, value: &T) {
		self.begin_field(name);
		value.put(self.out);
	}

	fn elem<T: Text>(&mut self, value: &T) {
		self.separate();
		value.put(self.out);
	}
}

/// The reader of the attribute text.
struct Cursor<'a> {
	text: &'a str,
	at: usize,
}

impl<'a> Cursor<'a> {
	fn new(text: &'a str) -> Self {
		Self {
			text,
			at: 0,
		}
	}

	fn err(&self, detail: impl std::fmt::Display) -> ParseError {
		ParseError {
			line: 0,
			detail: format!("logical attributes, offset {}: {detail}", self.at),
		}
	}

	fn fail<T>(&self, detail: impl std::fmt::Display) -> Result<T, ParseError> {
		Err(self.err(detail))
	}

	fn unknown<T>(&self, ty: &str, name: &str) -> Result<T, ParseError> {
		self.fail(format!("`{name}` is not a variant of {ty}"))
	}

	fn skip_space(&mut self) {
		let rest = &self.text[self.at..];
		self.at += rest.len() - rest.trim_start().len();
	}

	fn peek(&mut self) -> Option<char> {
		self.skip_space();
		self.text[self.at..].chars().next()
	}

	fn eat(&mut self, ch: char) -> bool {
		if self.peek() == Some(ch) {
			self.at += ch.len_utf8();
			true
		} else {
			false
		}
	}

	fn expect(&mut self, ch: char) -> Result<(), ParseError> {
		if self.eat(ch) {
			Ok(())
		} else {
			self.fail(format!("expected `{ch}`"))
		}
	}

	/// An identifier: a letter or `_`, then letters, digits and `_`.
	fn ident(&mut self) -> Result<&'a str, ParseError> {
		self.skip_space();
		let rest = &self.text[self.at..];
		let mut end = 0;
		for (at, ch) in rest.char_indices() {
			let ok = ch == '_' || ch.is_ascii_alphabetic() || (at > 0 && ch.is_ascii_digit());
			if !ok {
				break;
			}
			end = at + ch.len_utf8();
		}
		if end == 0 {
			return self.fail("expected a name");
		}
		self.at += end;
		Ok(&rest[..end])
	}

	fn keyword(&mut self, word: &str) -> Result<(), ParseError> {
		let got = self.ident()?;
		if got == word {
			Ok(())
		} else {
			self.fail(format!("expected `{word}`, found `{got}`"))
		}
	}

	/// A bare token: everything up to whitespace or one of `, ) ] }`.
	fn token(&mut self) -> Result<&'a str, ParseError> {
		self.skip_space();
		let rest = &self.text[self.at..];
		let end = rest
			.find(|ch: char| ch.is_whitespace() || matches!(ch, ',' | ')' | ']' | '}'))
			.unwrap_or(rest.len());
		if end == 0 {
			return self.fail("expected a value");
		}
		self.at += end;
		Ok(&rest[..end])
	}

	/// A double-quoted string.
	fn string(&mut self) -> Result<String, ParseError> {
		self.expect('"')?;
		let mut out = String::new();
		loop {
			let Some(ch) = self.text[self.at..].chars().next() else {
				return self.fail("unterminated string");
			};
			self.at += ch.len_utf8();
			match ch {
				'"' => return Ok(out),
				'\\' => {
					let Some(esc) = self.text[self.at..].chars().next() else {
						return self.fail("unterminated escape");
					};
					self.at += esc.len_utf8();
					match esc {
						'\\' => out.push('\\'),
						'"' => out.push('"'),
						'n' => out.push('\n'),
						'r' => out.push('\r'),
						't' => out.push('\t'),
						'u' => out.push(self.unicode_escape()?),
						other => return self.fail(format!("unknown escape `\\{other}`")),
					}
				}
				other => out.push(other),
			}
		}
	}

	/// The `{hex}` of a `\u{hex}` escape.
	fn unicode_escape(&mut self) -> Result<char, ParseError> {
		if !self.text[self.at..].starts_with('{') {
			return self.fail("expected `{` after `\\u`");
		}
		self.at += 1;
		let rest = &self.text[self.at..];
		let Some(end) = rest.find('}') else {
			return self.fail("unterminated `\\u{` escape");
		};
		let hex = &rest[..end];
		self.at += end + 1;
		match u32::from_str_radix(hex, 16).ok().and_then(char::from_u32) {
			Some(ch) => Ok(ch),
			None => self.fail(format!("`{hex}` is not a character")),
		}
	}

	fn begin_elem(&mut self, first: &mut bool) -> Result<(), ParseError> {
		if !std::mem::replace(first, false) {
			self.expect(',')?;
		}
		Ok(())
	}

	fn begin_field(&mut self, name: &str, first: &mut bool) -> Result<(), ParseError> {
		self.begin_elem(first)?;
		self.keyword(name)?;
		self.expect('=')
	}

	/// A field `name = value`, after a comma unless it is the first of its list.
	fn field<T: Text>(&mut self, name: &str, first: &mut bool) -> Result<T, ParseError> {
		self.begin_field(name, first)?;
		T::get(self)
	}

	/// A positional element, after a comma unless it is the first of its list.
	fn elem<T: Text>(&mut self, first: &mut bool) -> Result<T, ParseError> {
		self.begin_elem(first)?;
		T::get(self)
	}

	/// The end of the text: nothing but whitespace remains.
	fn finish(&mut self) -> Result<(), ParseError> {
		match self.peek() {
			None => Ok(()),
			Some(_) => self.fail("unexpected text after the last attribute"),
		}
	}
}

/// Appends `value` as a quoted string.
fn put_str(out: &mut String, value: &str) {
	out.push('"');
	for ch in value.chars() {
		match ch {
			'\\' => out.push_str("\\\\"),
			'"' => out.push_str("\\\""),
			'\n' => out.push_str("\\n"),
			'\r' => out.push_str("\\r"),
			'\t' => out.push_str("\\t"),
			ch if ch.is_control() => {
				let _ = write!(out, "\\u{{{:x}}}", u32::from(ch));
			}
			ch => out.push(ch),
		}
	}
	out.push('"');
}

/// The most names [`intern`] holds, and the longest one it takes.
const MAX_INTERNED: usize = 512;
const MAX_NAME_LEN: usize = 96;

/// The `&'static str` that spells `name`, for an attribute whose type holds a static name.
///
/// The set is process-wide and bounded: a name longer than [`MAX_NAME_LEN`], or one that would
/// grow the set past [`MAX_INTERNED`], is refused. The names a program writes are literals of the
/// program, so the set stays the size of the program's own names.
fn intern(name: &str) -> Option<&'static str> {
	static NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
	let mut names =
		NAMES.get_or_init(Default::default).lock().unwrap_or_else(PoisonError::into_inner);
	if let Some(known) = names.get(name).copied() {
		return Some(known);
	}
	if name.len() > MAX_NAME_LEN || names.len() >= MAX_INTERNED {
		return None;
	}
	let owned: &'static str = Box::leak(name.to_owned().into_boxed_str());
	names.insert(owned);
	Some(owned)
}

/// Reads a quoted name into a `&'static str`.
fn get_static(c: &mut Cursor<'_>) -> Result<&'static str, ParseError> {
	let name = c.string()?;
	match intern(&name) {
		Some(name) => Ok(name),
		None => c.fail(format!("the name `{name}` is too long or the name table is full")),
	}
}

// ---------------------------------------------------------------------------------------------
// Macros
// ---------------------------------------------------------------------------------------------

/// `Record` and `Text` for a struct whose fields are all `Text`. The text is the fields in
/// braces; the fields alone go inline in an op's text.
macro_rules! text_struct {
	($ty:ident { $($f:ident : $t:ty),+ $(,)? }) => {
		impl Record for $ty {
			fn put_fields(&self, w: &mut Fields<'_>) {
				$( w.field(stringify!($f), &self.$f); )+
			}

			fn get_fields(c: &mut Cursor<'_>, first: &mut bool) -> Result<Self, ParseError> {
				$( let $f = c.field::<$t>(stringify!($f), first)?; )+
				Ok($ty { $($f),+ })
			}
		}

		impl Text for $ty {
			fn put(&self, out: &mut String) {
				out.push_str("{ ");
				self.put_fields(&mut Fields::new(&mut *out));
				out.push_str(" }");
			}

			fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
				c.expect('{')?;
				let first = &mut true;
				let value = <Self as Record>::get_fields(c, first)?;
				c.expect('}')?;
				Ok(value)
			}
		}
	};
}

/// `Text` for an enum whose variants carry no payload.
macro_rules! text_unit_enum {
	($ty:ident { $($v:ident),+ $(,)? }) => {
		impl Text for $ty {
			fn put(&self, out: &mut String) {
				out.push_str(match self { $( $ty::$v => stringify!($v), )+ });
			}

			fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
				let name = c.ident()?;
				match name {
					$( stringify!($v) => Ok($ty::$v), )+
					other => c.unknown(stringify!($ty), other),
				}
			}
		}
	};
}

/// `Text` for an enum, by the shape of each variant's payload: none, positional (`V(a, b)`) or
/// named (`V { a = .., b = .. }`).
macro_rules! text_enum {
	($ty:ident {
		unit [$($u:ident),* $(,)?]
		tuple [$($t:ident ( $($tf:ident : $tt:ty),+ $(,)? )),* $(,)?]
		named [$($n:ident { $($nf:ident : $nt:ty),+ $(,)? }),* $(,)?]
	}) => {
		impl Text for $ty {
			fn put(&self, out: &mut String) {
				match self {
					$( $ty::$u => out.push_str(stringify!($u)), )*
					$(
						$ty::$t($($tf),+) => {
							out.push_str(stringify!($t));
							out.push('(');
							let mut w = Fields::new(&mut *out);
							$( w.elem($tf); )+
							out.push(')');
						}
					)*
					$(
						$ty::$n { $($nf),+ } => {
							out.push_str(stringify!($n));
							out.push_str(" { ");
							let mut w = Fields::new(&mut *out);
							$( w.field(stringify!($nf), $nf); )+
							out.push_str(" }");
						}
					)*
				}
			}

			fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
				let name = c.ident()?;
				match name {
					$( stringify!($u) => Ok($ty::$u), )*
					$(
						stringify!($t) => {
							c.expect('(')?;
							let first = &mut true;
							$( let $tf = c.elem::<$tt>(first)?; )+
							c.expect(')')?;
							Ok($ty::$t($($tf),+))
						}
					)*
					$(
						stringify!($n) => {
							c.expect('{')?;
							let first = &mut true;
							$( let $nf = c.field::<$nt>(stringify!($nf), first)?; )+
							c.expect('}')?;
							Ok($ty::$n { $($nf),+ })
						}
					)*
					other => c.unknown(stringify!($ty), other),
				}
			}
		}
	};
}

/// `Text` for an integer type, as a bare token.
macro_rules! text_int {
	($($ty:ty),+) => {
		$(
			impl Text for $ty {
				fn put(&self, out: &mut String) {
					let _ = write!(out, "{self}");
				}

				fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
					let token = c.token()?;
					token
						.parse::<$ty>()
						.map_err(|_| c.err(format!("`{token}` is not a {}", stringify!($ty))))
				}
			}
		)+
	};
}

text_int!(i8, u8, i16, u16, i32, u32, i64, u64, usize);

impl Text for surrealdb_types::f16 {
	fn put(&self, out: &mut String) {
		let _ = write!(out, "{self}");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let token = c.token()?;
		token.parse().map_err(|_| c.err(format!("`{token}` is not an f16")))
	}
}

// ---------------------------------------------------------------------------------------------
// Scalars and containers
// ---------------------------------------------------------------------------------------------

impl Text for bool {
	fn put(&self, out: &mut String) {
		out.push_str(if *self {
			"true"
		} else {
			"false"
		});
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		match c.ident()? {
			"true" => Ok(true),
			"false" => Ok(false),
			other => c.fail(format!("`{other}` is not a bool")),
		}
	}
}

impl Text for f32 {
	fn put(&self, out: &mut String) {
		out.push_str(&f32_text(*self));
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let token = c.token()?;
		read_f32(token).ok_or_else(|| c.err(format!("`{token}` is not an f32")))
	}
}

impl Text for f64 {
	fn put(&self, out: &mut String) {
		out.push_str(&f64_text(*self));
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let token = c.token()?;
		read_f64(token).ok_or_else(|| c.err(format!("`{token}` is not an f64")))
	}
}

impl Text for () {
	fn put(&self, out: &mut String) {
		out.push_str("()");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.expect('(')?;
		c.expect(')')
	}
}

impl Text for String {
	fn put(&self, out: &mut String) {
		put_str(out, self);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.string()
	}
}

impl Text for Box<str> {
	fn put(&self, out: &mut String) {
		put_str(out, self);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		Ok(c.string()?.into_boxed_str())
	}
}

impl Text for Strand {
	fn put(&self, out: &mut String) {
		put_str(out, self.as_str());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		Ok(Strand::new(c.string()?))
	}
}

impl Text for TableName {
	fn put(&self, out: &mut String) {
		put_str(out, self.as_str());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		Ok(TableName::new(Strand::new(c.string()?)))
	}
}

/// A bounded duration prints as the duration it holds.
impl Text for crate::val::BoundedDuration {
	fn put(&self, out: &mut String) {
		self.0.put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		Duration::get(c).map(crate::val::BoundedDuration)
	}
}

/// A duration prints as `(seconds, nanoseconds)`.
impl Text for Duration {
	fn put(&self, out: &mut String) {
		let _ = write!(out, "({}, {})", self.as_secs(), self.subsec_nanos());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.expect('(')?;
		let first = &mut true;
		let secs = c.elem::<u64>(first)?;
		let nanos = c.elem::<u32>(first)?;
		c.expect(')')?;
		if nanos >= 1_000_000_000 {
			return c.fail("nanoseconds out of range");
		}
		Ok(Duration::new(secs, nanos))
	}
}

impl Text for Uuid {
	fn put(&self, out: &mut String) {
		put_str(out, &self.0.to_string());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match uuid::Uuid::parse_str(&text) {
			Ok(id) => Ok(Uuid(id)),
			Err(_) => c.fail(format!("`{text}` is not a uuid")),
		}
	}
}

impl Text for Datetime {
	fn put(&self, out: &mut String) {
		put_str(out, &self.0.to_rfc3339_opts(SecondsFormat::AutoSi, true));
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match DateTime::parse_from_rfc3339(&text) {
			Ok(at) => Ok(Datetime(at.with_timezone(&Utc))),
			Err(_) => c.fail(format!("`{text}` is not an RFC 3339 datetime")),
		}
	}
}

impl Text for Kind {
	fn put(&self, out: &mut String) {
		put_str(out, &self.to_sql());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match read_kind_text(&text) {
			Ok(kind) => Ok(kind),
			Err(e) => c.fail(e.detail),
		}
	}
}

impl Text for Arc<crate::expr::logical::LogicalWrite> {
	fn put(&self, out: &mut String) {
		put_str(out, &crate::exec::ir::logical::stages::statement_text(self));
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match crate::exec::ir::logical::stages::statement_of_text(&text) {
			Ok(write) => Ok(Arc::new(write)),
			Err(e) => c.fail(e),
		}
	}
}

/// An expression a statement stores as written, as its SurrealQL text.
impl Text for crate::expr::statements::define::SqlType {
	fn put(&self, out: &mut String) {
		put_str(out, self.dialect.keyword());
		put_str(out, &self.text);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		use crate::expr::statements::define::SqlTypeDialect;
		let keyword = c.string()?;
		let dialect = match keyword.as_str() {
			"POSTGRESQL" => SqlTypeDialect::PostgreSql,
			"MYSQL" => SqlTypeDialect::MySql,
			"DYNAMODB" => SqlTypeDialect::DynamoDb,
			other => return c.fail(format!("unknown SQL type dialect `{other}`")),
		};
		let text = c.string()?;
		Ok(crate::expr::statements::define::SqlType {
			dialect,
			text,
		})
	}
}

impl Text for crate::expr::Expr {
	fn put(&self, out: &mut String) {
		put_str(out, &self.to_sql());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match crate::syn::expr(&text) {
			Ok(expr) => Ok(expr),
			Err(e) => c.fail(format!("`{text}` is not an expression: {e}")),
		}
	}
}

impl Text for Idiom {
	fn put(&self, out: &mut String) {
		put_str(out, &self.to_sql());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match crate::syn::idiom(&text) {
			Ok(idiom) => Ok(idiom),
			Err(e) => c.fail(format!("`{text}` is not an idiom: {e}")),
		}
	}
}

impl Text for EventDefs {
	fn put(&self, out: &mut String) {
		put_str(out, &self.to_text());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match EventDefs::from_text(&text) {
			Ok(defs) => Ok(defs),
			Err(e) => c.fail(e),
		}
	}
}

impl Text for IndexDefs {
	fn put(&self, out: &mut String) {
		put_str(out, &self.to_text());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match IndexDefs::from_text(&text) {
			Ok(defs) => Ok(defs),
			Err(e) => c.fail(e),
		}
	}
}

impl Text for TableDefs {
	fn put(&self, out: &mut String) {
		put_str(out, &self.to_text());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match TableDefs::from_text(&text) {
			Ok(defs) => Ok(defs),
			Err(e) => c.fail(e),
		}
	}
}

impl Text for Value {
	fn put(&self, out: &mut String) {
		put_str(out, &value_text(self));
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match read_value(&text) {
			Ok(value) => Ok(value),
			Err(e) => c.fail(e.detail),
		}
	}
}

impl Text for PublicNumber {
	fn put(&self, out: &mut String) {
		match self {
			PublicNumber::Int(n) => {
				out.push_str("Int(");
				n.put(out);
				out.push(')');
			}
			PublicNumber::Float(n) => {
				out.push_str("Float(");
				n.put(out);
				out.push(')');
			}
			PublicNumber::Decimal(n) => {
				out.push_str("Decimal(");
				put_str(out, &n.to_string());
				out.push(')');
			}
			PublicNumber::BigDecimal(n) => {
				out.push_str("BigDecimal(");
				put_str(out, &n.to_string());
				out.push(')');
			}
			PublicNumber::I8(n) => {
				out.push_str("I8(");
				n.put(out);
				out.push(')');
			}
			PublicNumber::U8(n) => {
				out.push_str("U8(");
				n.put(out);
				out.push(')');
			}
			PublicNumber::I16(n) => {
				out.push_str("I16(");
				n.put(out);
				out.push(')');
			}
			PublicNumber::U16(n) => {
				out.push_str("U16(");
				n.put(out);
				out.push(')');
			}
			PublicNumber::I32(n) => {
				out.push_str("I32(");
				n.put(out);
				out.push(')');
			}
			PublicNumber::U32(n) => {
				out.push_str("U32(");
				n.put(out);
				out.push(')');
			}
			PublicNumber::U64(n) => {
				out.push_str("U64(");
				n.put(out);
				out.push(')');
			}
			PublicNumber::F16(n) => {
				out.push_str("F16(");
				n.put(out);
				out.push(')');
			}
			PublicNumber::F32(n) => {
				out.push_str("F32(");
				n.put(out);
				out.push(')');
			}
		}
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let name = c.ident()?;
		c.expect('(')?;
		let number = match name {
			"Int" => PublicNumber::Int(<i64 as Text>::get(c)?),
			"Float" => PublicNumber::Float(<f64 as Text>::get(c)?),
			"Decimal" => {
				let text = c.string()?;
				match text.parse() {
					Ok(n) => PublicNumber::Decimal(n),
					Err(_) => return c.fail(format!("`{text}` is not a decimal")),
				}
			}
			"BigDecimal" => {
				let text = c.string()?;
				match text.parse::<surrealdb_types::BigDecimal>() {
					Ok(n) => PublicNumber::BigDecimal(Box::new(n)),
					Err(_) => return c.fail(format!("`{text}` is not a bigdecimal")),
				}
			}
			"I8" => PublicNumber::I8(<i8 as Text>::get(c)?),
			"U8" => PublicNumber::U8(<u8 as Text>::get(c)?),
			"I16" => PublicNumber::I16(<i16 as Text>::get(c)?),
			"U16" => PublicNumber::U16(<u16 as Text>::get(c)?),
			"I32" => PublicNumber::I32(<i32 as Text>::get(c)?),
			"U32" => PublicNumber::U32(<u32 as Text>::get(c)?),
			"U64" => PublicNumber::U64(<u64 as Text>::get(c)?),
			"F16" => PublicNumber::F16(<surrealdb_types::f16 as Text>::get(c)?),
			"F32" => PublicNumber::F32(<f32 as Text>::get(c)?),
			other => return c.unknown("Number", other),
		};
		c.expect(')')?;
		Ok(number)
	}
}

impl<T: Text> Text for Option<T> {
	fn put(&self, out: &mut String) {
		match self {
			None => out.push_str("none"),
			Some(value) => {
				out.push_str("some(");
				value.put(out);
				out.push(')');
			}
		}
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		match c.ident()? {
			"none" => Ok(None),
			"some" => {
				c.expect('(')?;
				let value = T::get(c)?;
				c.expect(')')?;
				Ok(Some(value))
			}
			other => c.fail(format!("`{other}` is neither `none` nor `some(..)`")),
		}
	}
}

impl<T: Text> Text for Box<T> {
	fn put(&self, out: &mut String) {
		(**self).put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		T::get(c).map(Box::new)
	}
}

impl<A: Text, B: Text> Text for (A, B) {
	fn put(&self, out: &mut String) {
		out.push('(');
		self.0.put(out);
		out.push_str(", ");
		self.1.put(out);
		out.push(')');
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.expect('(')?;
		let first = &mut true;
		let a = c.elem::<A>(first)?;
		let b = c.elem::<B>(first)?;
		c.expect(')')?;
		Ok((a, b))
	}
}

/// Appends `[a, b, …]`.
fn put_seq<'a, T: Text + 'a>(out: &mut String, items: impl IntoIterator<Item = &'a T>) {
	out.push('[');
	for (at, item) in items.into_iter().enumerate() {
		if at > 0 {
			out.push_str(", ");
		}
		item.put(out);
	}
	out.push(']');
}

/// Reads `[a, b, …]`.
fn get_seq<T: Text>(c: &mut Cursor<'_>) -> Result<Vec<T>, ParseError> {
	c.expect('[')?;
	let mut items = Vec::new();
	if c.eat(']') {
		return Ok(items);
	}
	loop {
		items.push(T::get(c)?);
		if !c.eat(',') {
			c.expect(']')?;
			return Ok(items);
		}
	}
}

impl<T: Text> Text for Vec<T> {
	fn put(&self, out: &mut String) {
		put_seq(out, self.iter());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		get_seq(c)
	}
}

impl<T: Text> Text for Box<[T]> {
	fn put(&self, out: &mut String) {
		put_seq(out, self.iter());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		get_seq(c).map(Vec::into_boxed_slice)
	}
}

impl<A: Array> Text for SmallVec<A>
where
	A::Item: Text,
{
	fn put(&self, out: &mut String) {
		put_seq(out, self.iter());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		get_seq(c).map(SmallVec::from_vec)
	}
}

impl<T: Text> Text for AlterKind<T> {
	fn put(&self, out: &mut String) {
		match self {
			AlterKind::None => out.push_str("None"),
			AlterKind::Set(value) => {
				out.push_str("Set(");
				value.put(out);
				out.push(')');
			}
			AlterKind::Drop => out.push_str("Drop"),
		}
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		match c.ident()? {
			"None" => Ok(AlterKind::None),
			"Set" => {
				c.expect('(')?;
				let value = T::get(c)?;
				c.expect(')')?;
				Ok(AlterKind::Set(value))
			}
			"Drop" => Ok(AlterKind::Drop),
			other => c.unknown("AlterKind", other),
		}
	}
}

// ---------------------------------------------------------------------------------------------
// Attributes the text cannot carry, and attributes with a bespoke form
// ---------------------------------------------------------------------------------------------

/// A credential never appears in the text: it prints `redacted` and is not read back.
impl Text for ScramCredential {
	fn put(&self, out: &mut String) {
		out.push_str("redacted");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.fail("a credential is not part of the text form")
	}
}

/// The derived credentials of a user definition print as their presence only and are not read
/// back.
impl Text for UserSecrets {
	fn put(&self, out: &mut String) {
		out.push_str("{ hash_is_set = ");
		self.hash_is_set.put(out);
		out.push_str(", hash = redacted, code = redacted, scram = ");
		self.scram.put(out);
		out.push_str(" }");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.fail("a user's credentials are not part of the text form")
	}
}

/// The keys an access method's statement writes print as their presence only: `none` when there
/// are none, `redacted` otherwise. Only `none` is read back.
impl Text for AccessSecrets {
	fn put(&self, out: &mut String) {
		out.push_str(if self.written.is_empty() {
			"none"
		} else {
			"redacted"
		});
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		match c.ident()? {
			"none" => Ok(AccessSecrets::default()),
			other => {
				c.fail(format!("`{other}` is a credential, which is not part of the text form"))
			}
		}
	}
}

/// An optional credential string: `none` when absent, `redacted` when present. Only `none` is
/// read back.
struct SecretPresence(bool);

impl Text for SecretPresence {
	fn put(&self, out: &mut String) {
		out.push_str(if self.0 {
			"redacted"
		} else {
			"none"
		});
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		match c.ident()? {
			"none" => Ok(SecretPresence(false)),
			other => {
				c.fail(format!("`{other}` is a credential, which is not part of the text form"))
			}
		}
	}
}

/// Only `DdlChecks::None` has a text form (`none`). Any other value prints `unsupported` and is
/// not read back.
impl Text for DdlChecks {
	fn put(&self, out: &mut String) {
		out.push_str(if *self == DdlChecks::None {
			"none"
		} else {
			"unsupported"
		});
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		match c.ident()? {
			"none" => Ok(DdlChecks::None),
			other => c.fail(format!("`{other}` is not a readable set of DDL checks")),
		}
	}
}

/// A join alias prints as the index it was minted with. An alias that is not of the minted form
/// prints `unsupported` and is not read back.
impl Text for RelationAlias {
	fn put(&self, out: &mut String) {
		match self.index() {
			Some(index) => {
				let _ = write!(out, "{index}");
			}
			None => out.push_str("unsupported"),
		}
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let token = c.token()?;
		match token.parse::<usize>() {
			Ok(index) => Ok(RelationAlias::new(index)),
			Err(_) => c.fail(format!("`{token}` is not a join alias index")),
		}
	}
}

impl Text for SizeBucket {
	fn put(&self, out: &mut String) {
		self.index().put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let index = <u8 as Text>::get(c)?;
		if index > SizeBucket::MAX {
			return c.fail(format!("`{index}` is past the largest size bucket"));
		}
		Ok(SizeBucket::new(index))
	}
}

impl Text for ArmLabel {
	fn put(&self, out: &mut String) {
		put_str(out, self.0);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		get_static(c).map(ArmLabel)
	}
}

impl Text for Explain {
	fn put(&self, out: &mut String) {
		self.0.put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		<bool as Text>::get(c).map(Explain)
	}
}

impl Text for MatchBindings {
	fn put(&self, out: &mut String) {
		self.0.put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		<Box<[BindingDef]> as Text>::get(c).map(MatchBindings)
	}
}

impl Text for WriteStages {
	fn put(&self, out: &mut String) {
		self.0.put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		<Box<[WriteStageKind]> as Text>::get(c).map(WriteStages)
	}
}

impl Text for SurrealismExecutable {
	fn put(&self, out: &mut String) {
		self.0.put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		<File as Text>::get(c).map(SurrealismExecutable)
	}
}

/// A template prints as its stored type's name, its value literal and its holes.
impl Text for DefinitionTemplate {
	fn put(&self, out: &mut String) {
		out.push_str("{ ty = ");
		put_str(out, self.ty);
		out.push_str(", value = ");
		self.value.put(out);
		out.push_str(", holes = ");
		self.holes.put(out);
		out.push_str(" }");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.expect('{')?;
		c.keyword("ty")?;
		c.expect('=')?;
		let ty = get_static(c)?;
		let first = &mut false;
		let value = c.field("value", first)?;
		let holes = c.field("holes", first)?;
		c.expect('}')?;
		Ok(DefinitionTemplate {
			ty,
			value,
			holes,
		})
	}
}

/// An alteration's template prints as its stored type's name, its value literal and the fields it
/// replaces.
impl Text for AlterTemplate {
	fn put(&self, out: &mut String) {
		out.push_str("{ ty = ");
		put_str(out, self.ty);
		out.push_str(", value = ");
		self.value.put(out);
		out.push_str(", fields = ");
		self.fields.put(out);
		out.push_str(" }");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.expect('{')?;
		c.keyword("ty")?;
		c.expect('=')?;
		let ty = get_static(c)?;
		let first = &mut false;
		let value = c.field("value", first)?;
		let fields = c.field("fields", first)?;
		c.expect('}')?;
		Ok(AlterTemplate {
			ty,
			value,
			fields,
		})
	}
}

impl Text for TemplateHole {
	fn put(&self, out: &mut String) {
		match self {
			TemplateHole::Name => out.push_str("Name"),
			TemplateHole::Comment => out.push_str("Comment"),
			TemplateHole::Value => out.push_str("Value"),
			TemplateHole::AuthLimit => out.push_str("AuthLimit"),
			TemplateHole::Other(name) => {
				out.push_str("Other(");
				put_str(out, name);
				out.push(')');
			}
		}
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		match c.ident()? {
			"Name" => Ok(TemplateHole::Name),
			"Comment" => Ok(TemplateHole::Comment),
			"Value" => Ok(TemplateHole::Value),
			"AuthLimit" => Ok(TemplateHole::AuthLimit),
			"Other" => {
				c.expect('(')?;
				let name = get_static(c)?;
				c.expect(')')?;
				Ok(TemplateHole::Other(name))
			}
			other => c.unknown("TemplateHole", other),
		}
	}
}

/// An `ALTER USER` prints every attribute, with a credential string shown as `none` when absent
/// and `redacted` when present. A statement that sets a credential is not read back.
impl Record for AlterUser {
	fn put_fields(&self, w: &mut Fields<'_>) {
		w.field("name", &self.name);
		w.field("base", &self.base);
		w.field("password", &self.password);
		w.field("hash", &SecretPresence(self.hash.is_some()));
		w.field("scram", &self.scram);
		w.field("roles", &self.roles);
		w.field("token_duration", &self.token_duration);
		w.field("session_duration", &self.session_duration);
		w.field("comment", &self.comment);
	}

	fn get_fields(c: &mut Cursor<'_>, first: &mut bool) -> Result<Self, ParseError> {
		let name = c.field("name", first)?;
		let base = c.field("base", first)?;
		let password = c.field("password", first)?;
		let SecretPresence(_) = c.field("hash", first)?;
		let scram = c.field("scram", first)?;
		let roles = c.field("roles", first)?;
		let token_duration = c.field("token_duration", first)?;
		let session_duration = c.field("session_duration", first)?;
		let comment = c.field("comment", first)?;
		Ok(AlterUser {
			name,
			base,
			password,
			hash: None,
			scram,
			roles,
			token_duration,
			session_duration,
			comment,
		})
	}
}

impl Text for AlterUser {
	fn put(&self, out: &mut String) {
		out.push_str("{ ");
		self.put_fields(&mut Fields::new(&mut *out));
		out.push_str(" }");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.expect('{')?;
		let first = &mut true;
		let value = <Self as Record>::get_fields(c, first)?;
		c.expect('}')?;
		Ok(value)
	}
}

// ---------------------------------------------------------------------------------------------
// Unit enums
// ---------------------------------------------------------------------------------------------

text_unit_enum!(TxnStart {
	Begin,
	Continued
});
text_unit_enum!(TxnControl {
	Begin,
	Commit,
	Cancel
});
text_unit_enum!(TxnRegionEnd {
	Commit,
	Cancel,
	Unterminated,
	Open
});
text_unit_enum!(Emission {
	Stream,
	Hold
});
text_unit_enum!(StatementCommit {
	Auto,
	InTxn
});
text_unit_enum!(DetachMode {
	Detach,
	NoDetach
});
text_unit_enum!(BindingKind {
	Node,
	Edge,
	EdgeGroup,
	Path
});
text_unit_enum!(ExpandDirection {
	Out,
	In
});
text_unit_enum!(PathMode {
	Walk,
	Trail,
	Simple,
	Acyclic
});
text_unit_enum!(ExpandDir {
	Out,
	In
});
text_unit_enum!(EndpointField {
	In,
	Out
});
text_unit_enum!(OperatorPathMode {
	Walk,
	Trail,
	Simple,
	Acyclic
});
text_unit_enum!(JoinType {
	Inner,
	Left,
	Cross
});
text_enum!(SplitTarget {
	unit [Nowhere]
	tuple [At(path: FieldPath), Every(path: FieldPath), Through(path: FieldPath, inner: Box<SplitTarget>)]
	named []
});

text_enum!(FieldPathPart {
	unit [First, Last]
	tuple [Field(name: String), Index(at: usize), Lookup(name: String)]
	named []
});

/// A field path prints as the list of its parts.
impl Text for FieldPath {
	fn put(&self, out: &mut String) {
		self.0.put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		<Vec<FieldPathPart> as Text>::get(c).map(FieldPath)
	}
}

text_enum!(EdgeBinding {
	unit []
	tuple [Full(name: String), IdOnly(name: String)]
	named []
});
text_enum!(ShortestSelector {
	unit [AnyShortest, AllShortest]
	tuple [Counted(count: u32), CountedGroups(count: u32)]
	named [Any { count: u32 }]
});
text_enum!(BindStepShape {
	unit [Distinct, Drain]
	tuple [
		Expand(shape: Box<BindExpandShape>),
		Path(shape: Box<BindPathShape>),
		Join(shape: Box<BindJoinShape>)
	]
	named [
		Bind { name: String },
		Endpoint { edge: String, field: EndpointField, name: String, label: Option<TableName> },
		DistinctEdges { edges: Box<[String]> },
		Update { target: String, data: MatchData },
		Delete { target: String, detach: DetachMode, is_edge: bool },
		Insert { nodes: Box<[BindInsertNode]>, edges: Box<[BindInsertEdge]> }
	]
});
text_unit_enum!(WriteForm {
	Create,
	Update,
	Upsert,
	Delete,
	Insert,
	Relate
});
text_unit_enum!(OutputShape {
	Default,
	None,
	Null,
	Diff,
	After,
	Before,
	Fields
});
text_unit_enum!(ContentMode {
	Content,
	Merge,
	Patch,
	Replace,
	Single
});
text_unit_enum!(UnaddressedPart {
	Lookup,
	Destructure,
	Recurse,
	Start,
	Method,
	Filter
});
text_unit_enum!(DefineKind {
	Default,
	Overwrite,
	IfNotExists
});
text_unit_enum!(AssignOperator {
	Assign,
	Add,
	Subtract,
	Extend
});
text_unit_enum!(ExplainFormat {
	Text,
	Json
});
text_unit_enum!(JoinKind {
	Inner,
	Left,
	Semi,
	Anti
});
text_unit_enum!(GuardKind {
	Read,
	Write
});
text_unit_enum!(LetBinding {
	Local,
	Session
});
text_unit_enum!(UseTarget {
	Ns,
	Db,
	NsDb,
	Default
});
text_unit_enum!(LoopKind {
	Select,
	Write,
	For,
	Closure,
	ElementFilter,
	GraphStep,
	Recurse,
	Broadcast
});
text_unit_enum!(DecisiveKind {
	Any,
	All,
	Find,
	FindIndex
});
text_unit_enum!(SourceOrder {
	Positional,
	Delivery
});
text_unit_enum!(WriteMark {
	Compute,
	Apply
});
text_unit_enum!(LiveShape {
	Diff,
	Select
});
text_unit_enum!(PurgeKind {
	Expired,
	Revoked,
	Both
});
text_unit_enum!(ChooseGranularity {
	Statement,
	Morsel,
	Batch
});
text_unit_enum!(ArmEquivalence {
	SameRowsSameOrder,
	OrderedByTotalKey
});
text_unit_enum!(ChooseOrigin {
	LogicalDynamicTarget,
	LogicalKindDispatch,
	PhysicalGuarded
});
text_unit_enum!(ValueKindClass {
	RecordId,
	Table,
	Array,
	Other
});
text_unit_enum!(UnitKind {
	Function,
	TableRead,
	FieldState,
	SelectPermission,
	Closure,
	Expr,
	Statement,
	SessionStatement,
	FrontEnd,
	StageBundle,
	FunctionCall,
	ParamGuard,
	SpecialisedBody,
	GenericBody,
	DynamicTargetArm
});
text_unit_enum!(RecordImage {
	Initial,
	Current
});
text_unit_enum!(TableTypeFor {
	Create,
	Upsert,
	Relate,
	Insert
});
text_unit_enum!(LightweightReason {
	Updated,
	GivenADataClause
});
text_unit_enum!(DocAction {
	Create,
	Update,
	Delete
});
text_unit_enum!(IamAction {
	View,
	Edit
});
text_unit_enum!(Base {
	Root,
	Ns,
	Db
});
text_unit_enum!(ConfigKind {
	GraphQL,
	Api,
	Default
});
text_unit_enum!(Algorithm {
	EdDSA,
	Es256,
	Es384,
	Es512,
	Hs256,
	Hs384,
	Hs512,
	Ps256,
	Ps384,
	Ps512,
	Rs256,
	Rs384,
	Rs512
});
text_unit_enum!(ApiMethod {
	Delete,
	Get,
	Patch,
	Post,
	Put,
	Trace
});
text_unit_enum!(BearerAccessType {
	Bearer,
	Refresh
});
text_unit_enum!(BearerAccessSubject {
	Record,
	User
});
text_unit_enum!(Language {
	Arabic,
	Danish,
	Dutch,
	English,
	Finnish,
	French,
	German,
	Greek,
	Hungarian,
	Italian,
	Norwegian,
	Portuguese,
	Romanian,
	Russian,
	Spanish,
	Swedish,
	Tamil,
	Turkish
});
text_unit_enum!(SegmentLanguage {
	Chinese,
	Japanese,
	Korean
});
text_unit_enum!(VectorType {
	F64,
	F32,
	I64,
	I32,
	I16,
	F16,
	I8,
	U8
});
text_unit_enum!(GraphQLIntrospectionConfig {
	Auto,
	None
});

// ---------------------------------------------------------------------------------------------
// Enums with payloads
// ---------------------------------------------------------------------------------------------

text_enum!(TxnRegionMode {
	unit []
	tuple []
	named [
		Explicit { start: TxnStart, end: TxnRegionEnd, emission: Emission },
		Session { id: u32 }
	]
});

text_enum!(SourceRefShape {
	unit [RecordId, Mock, Param, Dynamic]
	tuple [Table(table: TableName)]
	named []
});

text_enum!(With {
	unit [NoIndex]
	tuple [Index(names: Vec<String>)]
	named []
});

text_enum!(PathSearch {
	unit [All, AllShortest, AnyShortest]
	tuple []
	named [
		Any { count: u32 },
		ShortestCounted { count: u32 },
		ShortestGroups { count: u32 }
	]
});

text_enum!(MatchData {
	unit [Content]
	tuple [Set(names: Box<[String]>), Unset(names: Box<[String]>)]
	named []
});

text_enum!(DdlGuard {
	unit [RefuseNoWrite]
	tuple []
	named [Allow { action: IamAction, resource: ResourceKind, base: Base }]
});

text_enum!(ResourceKind {
	unit [
		Any, Namespace, Database, Record, Table, Document, Option, Function, Module, Analyzer,
		Parameter, Model, Event, Field, Index, Access, Api, Bucket, Sequence, Actor
	]
	tuple [Config(kind: ConfigKind)]
	named []
});

text_enum!(FieldsShape {
	unit []
	tuple [Select(fields: Box<[FieldShape]>)]
	named [Value { alias: Option<OutputPath> }]
});

text_enum!(FieldShape {
	unit [All]
	tuple []
	named [Single { alias: Option<OutputPath>, kind: Option<Kind> }]
});

text_enum!(OutputPathPart {
	unit []
	tuple [Field(name: String), Lookup(text: String)]
	named []
});

text_enum!(SortShape {
	unit [Random]
	tuple [Keys(keys: Box<[SortKeyShape]>)]
	named []
});

text_enum!(StepAttrs {
	unit [Flatten]
	tuple [
		Closure(attrs: ClosureLoopAttrs),
		Lookup(attrs: LookupAttrs),
		Recurse(attrs: RecurseAttrs),
		Param(name: Strand)
	]
	named []
});

text_struct!(ClosureLoopAttrs {
	function: Strand,
	method: bool
});

text_struct!(LookupSubjectShape {
	table: TableName,
	referencing_field: Option<String>,
	range: Option<KeyRange>
});

text_struct!(LookupAttrs {
	kind: LookupKind,
	only: bool,
	only_path: bool,
	subjects: Box<[LookupSubjectShape]>,
	fields: Option<FieldsShape>,
	order: Option<SortShape>,
	alias: Option<OutputPath>,
	cond: bool,
	split: Option<u16>,
	group: Option<u16>,
	limit: Option<u16>,
	start: Option<u16>,
	trace: LookupTrace
});

text_unit_enum!(LookupTrace {
	None,
	Traced,
	Partial,
	Inner
});

text_struct!(RecurseAttrs {
	depth: Recurse,
	form: RecursePathForm,
	instruction: Option<RecurseInstructionShape>
});

text_unit_enum!(RecursePathForm {
	Absent,
	Nested,
	Trailing
});

text_enum!(RecurseInstructionShape {
	unit []
	tuple []
	named [
		Path { inclusive: bool },
		Collect { inclusive: bool },
		Shortest { inclusive: bool }
	]
});

text_enum!(LookupKind {
	unit [Reference]
	tuple [Graph(dir: Dir)]
	named []
});

text_unit_enum!(Dir {
	In,
	Out,
	Both
});

text_enum!(Recurse {
	unit []
	tuple [Fixed(depth: u32), Range(from: Option<u32>, to: Option<u32>)]
	named []
});

text_struct!(KeyRange {
	start: Bound<KeyBound>,
	end: Bound<KeyBound>
});

text_enum!(KeyBound {
	unit []
	tuple [
		Number(key: i64),
		String(key: Strand),
		Uuid(key: Uuid),
		Generate(generator: RecordIdKeyGen)
	]
	named [Array { len: u32 }, Object { keys: Box<[String]> }]
});

text_unit_enum!(RecordIdKeyGen {
	Rand,
	Ulid,
	Uuid
});

impl<T: Text> Text for Bound<T> {
	fn put(&self, out: &mut String) {
		match self {
			Bound::Unbounded => out.push_str("Unbounded"),
			Bound::Included(key) => {
				out.push_str("Included(");
				key.put(out);
				out.push(')');
			}
			Bound::Excluded(key) => {
				out.push_str("Excluded(");
				key.put(out);
				out.push(')');
			}
		}
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		match c.ident()? {
			"Unbounded" => Ok(Bound::Unbounded),
			name @ ("Included" | "Excluded") => {
				c.expect('(')?;
				let key = T::get(c)?;
				c.expect(')')?;
				Ok(if name == "Included" {
					Bound::Included(key)
				} else {
					Bound::Excluded(key)
				})
			}
			other => c.unknown("Bound", other),
		}
	}
}

text_enum!(LoopSink {
	unit [Output, Filter, Fold, Discard]
	tuple [Decisive(kind: DecisiveKind), Write(stages: Box<WriteStages>)]
	named []
});

text_enum!(WriteStageKind {
	unit [
		CheckRecordUserAccess, CheckRecordExists, CheckTableNotView, CheckPermissionsQuickCreate,
		CheckUpdatePermissions, CheckDeletePermissions, CheckCreatePermissions,
		RecheckUpdatePermissions, CheckWhereCondition, ComputeInputData, CheckDataFields,
		ProcessRecordData, ProcessMergeData, DefaultRecordData, GenerateRecordId,
		ProcessTableFields, CleanupTableFields, CleanupTableReferences, ClearRecordData,
		StoreRecordData, StoreEdgesData, StoreInlineAdjacencyData, StoreIndexData, ReclaimDocId,
		PurgeRecordData, MaterialiseObservedFields, ProcessTableReferences, ProcessChangefeeds,
		OutputWrite
	]
	tuple [
		CheckTableNotLightweight(reason: LightweightReason),
		CheckTableType(form: TableTypeFor),
		ProcessTableViews(action: DocAction),
		ProcessTableEvents(action: DocAction),
		ProcessTableLives(action: DocAction),
		CheckSelectPermissions(image: RecordImage)
	]
	named []
});

text_enum!(InfoSubject {
	unit [Root, Ns, Db, Table, Index]
	tuple []
	named [User { base: Option<Base> }]
});

text_enum!(AccessSubjectShape {
	unit [Record]
	tuple [User(name: Strand)]
	named []
});

text_enum!(AccessOp {
	unit []
	tuple []
	named [
		Grant { subject: AccessSubjectShape },
		Show { gr: Option<Strand> },
		Revoke { gr: Option<Strand> },
		Purge { kind: PurgeKind, grace: Duration }
	]
});

text_enum!(ShowSince {
	unit []
	tuple [Timestamp(at: Datetime), Versionstamp(stamp: u64)]
	named []
});

text_enum!(Guard {
	unit [Otherwise]
	tuple [
		TableIs(table: TableName),
		IndexReady(index: IndexRef),
		ValueKindIs(class: ValueKindClass),
		TypeHolds(kind: Kind)
	]
	named [SizeAtMost { bucket: SizeBucket }]
});

text_enum!(ArmBody {
	unit [PreLowered]
	tuple [UnitLoad(kind: UnitKind)]
	named []
});

text_enum!(ExhaustiveFact {
	unit []
	tuple [TypeFact(kind: Kind)]
	named []
});

text_enum!(GenericArm {
	unit [Present]
	tuple [ExhaustiveBy(fact: ExhaustiveFact)]
	named []
});

text_enum!(PlacePart {
	unit [All, Flatten, Last, First, Optional, Doc, RepeatRecurse]
	tuple [
		Field(name: Strand),
		Filter(region: u16),
		Dynamic(operand: u16),
		Index(operand: u16),
		Unaddressed(part: UnaddressedPart, text: Box<str>)
	]
	named []
});

text_enum!(PermissionShape {
	unit [None, Full]
	tuple [Specific(region: u16)]
	named []
});

text_enum!(OnDeleteShape {
	unit [Reject, Ignore, Cascade, Unset]
	tuple [Custom(region: u16)]
	named []
});

text_enum!(DefaultShape {
	unit [None]
	tuple [Always(region: u16), Set(region: u16)]
	named []
});

text_enum!(AlterDefaultShape {
	unit [None, Drop]
	tuple [Always(region: u16), Set(region: u16)]
	named []
});

text_enum!(IndexColumn {
	unit []
	tuple [Path(place: Place), Stored(region: u16)]
	named []
});

text_enum!(IndexSpec {
	unit [Idx, Uniq, Spatial]
	tuple [
		Hnsw(params: HnswParams),
		FullText(params: FullTextParams),
		Count(condition: Option<u16>),
		DiskAnn(params: DiskAnnParams)
	]
	named []
});

text_enum!(Distance {
	unit [
		Chebyshev, Cosine, Euclidean, Hamming, Jaccard, Manhattan, Pearson, CosineNormalized,
		InnerProduct
	]
	tuple [Minkowski(order: PublicNumber)]
	named []
});

text_enum!(Scoring {
	unit [Vs]
	tuple []
	named [Bm { k1: f32, b: f32 }]
});

text_enum!(JwtVerifyShape {
	unit []
	tuple []
	named [Key { alg: Algorithm, key: u16 }, Jwks { url: u16 }]
});

text_enum!(AccessTypeShape {
	unit []
	tuple [Record(shape: Box<RecordAccessShape>), Jwt(shape: JwtShape), Bearer(shape: BearerShape)]
	named []
});

text_enum!(ConfigShape {
	unit []
	tuple [
		GraphQL(config: GraphQLConfig),
		Api(config: ApiConfigShape),
		Default(config: DefaultConfigShape)
	]
	named []
});

text_enum!(GraphQLTablesConfig {
	unit [None, Auto]
	tuple [Include(tables: Vec<TableName>), Exclude(tables: Vec<TableName>)]
	named []
});

text_enum!(GraphQLFunctionsConfig {
	unit [None, Auto]
	tuple [Include(functions: Vec<Strand>), Exclude(functions: Vec<Strand>)]
	named []
});

text_enum!(EventKind {
	unit [Sync]
	tuple []
	named [Async { retry: u16, max_depth: u16 }]
});

text_enum!(Filter {
	unit [Ascii, Lowercase, Uppercase]
	tuple [
		EdgeNgram(min: u16, max: u16),
		Ngram(min: u16, max: u16),
		Snowball(language: Language),
		Mapper(path: String)
	]
	named []
});

text_enum!(Tokenizer {
	unit [Blank, Camel, Class, Punct]
	tuple [Segment(language: SegmentLanguage)]
	named []
});

text_enum!(TableType {
	unit [Any, Normal]
	tuple [Relation(relation: Relation)]
	named []
});

text_enum!(ModuleExecutable {
	unit []
	tuple [Surrealism(executable: SurrealismExecutable), Silo(executable: SiloExecutable)]
	named []
});

text_enum!(ModuleName {
	unit []
	tuple [
		Module(name: String),
		Silo(org: String, package: String, major: u32, minor: u32, patch: u32)
	]
	named []
});

text_enum!(DefineObject {
	unit []
	tuple [
		Function(function: Box<DefineFunction>),
		Table(table: Box<DefineTable>),
		Field(field: Box<DefineField>),
		User(user: Box<DefineUser>),
		Access(access: Box<DefineAccess>),
		Config(config: Box<ConfigShape>),
		Api(api: Box<DefineApi>)
	]
	named [
		Namespace { id: Option<u32>, name: u16, comment: u16 },
		Database {
			id: Option<u32>,
			name: u16,
			strict: bool,
			changefeed: Option<ChangeFeed>,
			comment: u16,
			compatibility: Option<u8>
		},
		Analyzer {
			name: u16,
			function: Option<Strand>,
			tokenizers: Option<Box<[Tokenizer]>>,
			filters: Option<Box<[Filter]>>,
			comment: u16
		},
		Param { name: Strand, value: u16, comment: u16, permissions: PermissionShape },
		Event {
			name: u16,
			target_table: u16,
			when: u16,
			then: Box<[u16]>,
			comment: u16,
			event_kind: EventKind
		},
		Index {
			name: u16,
			name_text: Box<str>,
			what: u16,
			columns: Box<[IndexColumn]>,
			index: IndexSpec,
			comment: u16,
			concurrently: bool
		},
		Model {
			hash: Strand,
			name: Strand,
			version: Strand,
			comment: u16,
			permissions: PermissionShape
		},
		Module {
			name: Option<String>,
			executable: ModuleExecutable,
			unsigned: bool,
			signed: bool,
			comment: u16,
			permissions: PermissionShape
		},
		Bucket {
			name: u16,
			base: Base,
			backend: Option<u16>,
			permissions: PermissionShape,
			readonly: bool,
			comment: u16
		},
		Sequence { name: u16, batch: u16, start: u16, timeout: u16 },
		Type { name: Strand, comment: u16, permissions: PermissionShape }
	]
});

text_enum!(EnumEdit {
	unit []
	tuple []
	named [
		Add { value: String, anchor: Option<String>, after: bool, if_not_exists: bool },
		Rename { from: String, to: String }
	]
});

text_enum!(RemoveObject {
	unit []
	tuple []
	named [
		Namespace { name: u16 },
		Database { name: u16 },
		Function { name: Strand },
		Analyzer { name: u16 },
		Access { name: u16, base: Base },
		Param { name: Strand },
		Table { name: u16 },
		Event { name: u16, table: u16 },
		Field { name: u16, table: u16 },
		Index { name: u16, table: u16 },
		User { name: u16, base: Base },
		Model { name: Strand, version: Strand },
		Api { name: u16 },
		Bucket { name: u16, base: Base },
		Sequence { name: u16 },
		Module { name: ModuleName },
		Config { kind: ConfigKind },
		Type { name: Strand, object: Option<SqlTypeObject> }
	]
});

text_enum!(SqlTypeObject {
	unit [Type, Domain]
	tuple []
	named []
});

text_enum!(AlterApiClauseShape {
	unit []
	tuple [SetAction(action: ApiActionShape)]
	named [
		ForAny { config: Option<ApiConfigShape>, fallback: AlterKind<u16> },
		DropAction { methods: Box<[ApiMethod]> }
	]
});

text_enum!(AlterObject {
	unit []
	tuple [
		Table(table: Box<AlterTable>),
		Event(event: Box<AlterEvent>),
		Field(field: Box<AlterField>),
		Function(function: Box<AlterFunction>),
		User(user: Box<AlterUser>),
		Access(access: Box<AlterAccess>)
	]
	named [
		System { query_timeout: AlterKind<u16>, compact: bool, compatibility: Option<u8> },
		Namespace { compact: bool },
		Database { name: Option<u16>, compact: bool, compatibility: Option<u8> },
		Api {
			path: u16,
			clauses: Box<[AlterApiClauseShape]>,
			security: bool,
			comment: AlterKind<String>
		},
		Index { name: u16, table: u16, prepare_remove: bool, comment: AlterKind<String> },
		Sequence { name: u16, timeout: Option<u16> },
		Param {
			name: Strand,
			value: Option<u16>,
			comment: AlterKind<String>,
			permissions: Option<PermissionShape>
		},
		Bucket {
			name: u16,
			base: Base,
			backend: AlterKind<String>,
			permissions: Option<PermissionShape>,
			readonly: AlterKind<()>,
			comment: AlterKind<String>,
			revoke_signatures: bool
		},
		Config { inner: Box<ConfigShape>, comment: AlterKind<String> },
		Analyzer {
			name: u16,
			function: AlterKind<String>,
			tokenizers: AlterKind<Box<[Tokenizer]>>,
			filters: AlterKind<Box<[Filter]>>,
			comment: AlterKind<String>
		},
		Module {
			name: ModuleName,
			comment: AlterKind<String>,
			permissions: Option<PermissionShape>
		},
		Type { name: Strand, edits: Box<[EnumEdit]> }
	]
});

// ---------------------------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------------------------

text_struct!(SourceAttrs {
	source_ref: SourceRefShape,
	with: Option<With>,
	for_update: bool,
	may_be_defined: bool,
	may_meet_change: bool,
	resolved: bool,
});
text_struct!(ComputedReadsShape {
	table: TableName,
	fields: Option<Vec<String>>,
	derived: DerivedReads
});
text_enum!(DerivedReads {
	unit [LateBound, Unlowerable]
	tuple [Stated(fields: Box<[DerivedField]>)]
	named []
});
text_struct!(MatchUpdateAttrs {
	target: u32,
	data: MatchData
});
text_struct!(MatchInsertAttrs { nodes: Box<[MatchInsertNode]>, edges: Box<[MatchInsertEdge]> });
text_struct!(MatchInsertNode {
	binding: u32,
	label: TableName
});
text_struct!(MatchInsertEdge {
	binding: u32,
	label: TableName,
	from: u32,
	to: u32
});
text_struct!(MatchColumnShape {
	name: String,
	hidden: bool
});
text_struct!(MatchOutputShape {
	columns: Box<[MatchColumnShape]>,
	distinct: bool,
	order: Box<[bool]>
});
text_struct!(BindExpandShape {
	source: String,
	direction: ExpandDir,
	edge_tables: Box<[TableName]>,
	edge_binding: EdgeBinding,
	target_binding: String,
	target_label: Option<TableName>,
	predicate: bool,
	optional: bool
});
text_struct!(BindPathShape {
	source: String,
	direction: ExpandDir,
	edge_tables: Box<[TableName]>,
	min: u32,
	max: Option<u32>,
	target_binding: String,
	target_label: Option<TableName>,
	group_binding: Option<String>,
	path_binding: Option<String>,
	mode: OperatorPathMode,
	selector: Option<ShortestSelector>,
	reversed: bool
});
text_struct!(BindJoinShape {
	keys: Box<[String]>,
	join_type: JoinType,
	null_template: Box<[String]>,
	residual: bool
});
text_struct!(BindInsertNode {
	name: String,
	table: TableName
});
text_struct!(BindInsertEdge {
	name: String,
	table: TableName,
	from: String,
	to: String
});
text_struct!(BindingDef {
	name: String,
	kind: BindingKind,
	user_named: bool
});
text_struct!(MatchPattern {
	path_var: Option<u32>,
	search: Option<PathPrefixPlan>,
	start: NodeStep,
	steps: Box<[(EdgeStep, NodeStep)]>
});
text_struct!(PathPrefixPlan {
	search: PathSearch,
	mode: PathMode
});
text_struct!(NodeStep { binding: u32, label: Option<TableName> });
text_struct!(EdgeStep {
	binding: u32,
	label: Option<TableName>,
	direction: ExpandDirection,
	quantifier: Option<EdgeQuantifier>
});
text_struct!(EdgeQuantifier { min: u32, max: Option<u32> });
text_struct!(WriteFlags {
	ignore: bool,
	relation: bool,
	or_update: bool,
	where_never: bool
});
text_struct!(WriteAttrs {
	form: WriteForm,
	only: bool,
	output: OutputShape,
	with: Option<With>,
	explain: Option<Explain>,
	flags: WriteFlags,
	body: WriteBody,
	gates: WriteGates,
	image: WriteImage,
	stored: WriteStored,
	derived: WriteDerived,
	into_text: Option<Box<str>>,
	statement: Option<Arc<crate::expr::logical::LogicalWrite>>,
	linked: Option<Box<LinkedStages>>
});
text_struct!(LinkedStages {
	table: TableName,
	run: Box<[WriteStageKind]>
});
text_struct!(WriteBody {
	branching: WriteBranching,
	first: Box<[u16]>,
	second: Box<[u16]>,
	deferable: bool
});
text_struct!(WriteImage {
	view: ImageView,
	keyed: bool,
	id_default: bool
});
text_unit_enum!(ImageView {
	Plain,
	Cut,
	Derived,
	Dynamic,
	LateBound
});
text_struct!(WriteStored {
	table: Option<TableName>,
	schemafull: bool,
	catalog_fixed: bool,
	fields: Stored,
	references: Stored,
	indexes: Stored,
	events: Stored,
	views: Stored,
	referenced: Stored,
	edges: Stored,
	schema: WriteSchema,
	reference_fields: Box<[Idiom]>,
	materialized: Box<[WriteMaterialized]>,
	index_set: WriteIndexes,
	view_set: WriteViews,
	repair_set: WriteRepairs,
	image_layers: ImageLayers
});
text_enum!(ImageLayers {
	unit [Unstated, Enforced]
	tuple [Stack(layers: Vec<SubscriberLayer>)]
	named []
});
text_struct!(WriteMaterialized {
	field: Idiom,
	table: TableName,
	dest: Idiom,
	filtered: bool
});
text_struct!(WriteIndexes {
	defs: IndexDefs,
	unlowerable: bool
});
text_struct!(WriteViews {
	defs: TableDefs,
	unlowerable: bool
});
text_struct!(WriteRepairs {
	tables: Box<[RepairTable]>,
	unlowerable: bool
});
text_struct!(RepairTable {
	table: TableName,
	fields: Box<[RepairField]>
});
text_struct!(RepairField {
	name: Idiom,
	strategy: RepairKind
});
text_unit_enum!(RepairKind {
	Ignore,
	Reject,
	Cascade,
	Unset,
	Custom
});
text_struct!(EventsAttrs {
	action: DocAction,
	defs: EventDefs,
	events: Box<[EventShape]>,
	unlowerable: bool
});
text_struct!(EventShape {
	when: EventClause,
	then: Box<[EventClause]>,
	dormant: bool
});
text_struct!(EventClause {
	deepest: u8,
	write: u8,
	caller_bound: bool
});
text_struct!(WriteSchema {
	fields: Box<[Strand]>,
	id_field: Option<u16>,
	cycle: Option<Box<str>>,
	steps: Box<[FieldStep]>,
	computed: Box<[Strand]>,
	unlowerable: bool,
	id_default_bound: bool
});
text_enum!(FieldStep {
	unit []
	tuple [Derive(field: DerivedField), Field(clauses: FieldClauses)]
	named []
});
text_struct!(FieldClauses {
	index: u16,
	default: Option<FieldClause>,
	value: Option<FieldClause>,
	assert: Option<FieldClause>,
	create: FieldGate,
	update: FieldGate
});
text_struct!(FieldClause {
	caller_bound: bool
});
text_enum!(FieldGate {
	unit [Allow, Deny]
	tuple []
	named [Predicate { caller_bound: bool, derives: Box<[DerivedField]> }]
});
text_struct!(WriteDerived {
	create: Box<[DerivedField]>,
	update: Box<[DerivedField]>,
	delete: Box<[DerivedField]>,
	select: Box<[DerivedField]>,
	observed: Option<Box<[DerivedField]>>
});
text_struct!(DerivedField {
	name: Strand,
	kind: Option<Kind>,
	caller_bound: bool
});
text_unit_enum!(Stored {
	Nothing,
	Declared,
	Dynamic,
	LateBound
});
text_struct!(WriteGates {
	create: GateClause,
	update: GateClause,
	delete: GateClause,
	select: GateClause
});
text_enum!(GateClause {
	unit [Full, None, Absent, Dynamic, LateBound, Unlowerable]
	tuple []
	named [Predicate { derives: bool }]
});
text_struct!(WriteRetry {
	savepoint_after: u16,
	retry_conflict: bool,
	skip_failures: bool,
	carry_schema_failure: bool
});
text_enum!(WriteBranching {
	unit [Sole, UpFront, ByAction]
	tuple [Retry(retry: WriteRetry)]
	named []
});
text_struct!(AssignAttrs {
	place: Place,
	op: AssignOperator
});
text_struct!(Place { parts: Box<[PlacePart]> });
text_struct!(OutputPath { parts: Vec<OutputPathPart>, exact: bool });
text_struct!(DefineAttrs {
	object: DefineObject,
	mode: DefineKind,
	template: DefinitionTemplate,
	checks: DdlChecks
});
text_struct!(RemoveAttrs {
	object: RemoveObject,
	if_exists: bool,
	expunge: bool,
	checks: DdlChecks
});
text_struct!(AlterAttrs {
	object: AlterObject,
	if_exists: bool,
	template: Option<AlterTemplate>,
	checks: DdlChecks
});
text_struct!(RebuildAttrs {
	index: Strand,
	table: TableName,
	if_exists: bool,
	concurrently: bool
});
text_struct!(SortKeyShape {
	collate: bool,
	numeric: bool,
	ascending: bool,
	path: bool
});
text_struct!(SortAttrs {
	ordering: SortShape,
	tempfiles: bool
});
text_struct!(WindowShape {
	partitions: u16,
	order: Box<[SortKeyShape]>,
	calls: Box<[WindowCallShape]>
});
text_struct!(WindowCallShape {
	output: Strand,
	function: WindowFunction,
	args: u16,
	frame: WindowFrame
});
text_struct!(WindowFrame {
	units: WindowFrameUnits,
	start: WindowFrameBound,
	end: WindowFrameBound
});
text_unit_enum!(WindowFrameUnits {
	Rows,
	Range
});
text_enum!(WindowFrameBound {
	unit [UnboundedPreceding, CurrentRow, UnboundedFollowing]
	tuple [Preceding(offset: u64), Following(offset: u64)]
	named []
});
text_unit_enum!(WindowFunction {
	RowNumber,
	Rank,
	DenseRank,
	PercentRank,
	CumeDist,
	Ntile,
	Lag,
	Lead,
	FirstValue,
	LastValue,
	NthValue,
	Sum,
	Avg,
	Count,
	Min,
	Max
});
text_struct!(AggregateShape {
	groups: u16,
	fields: FieldsShape,
	names: Vec<OutputPath>,
	first_value: Vec<usize>
});
text_struct!(ProjectAttrs {
	fields: FieldsShape,
	layer: bool,
	omit: u16,
	names: Vec<OutputPath>,
	computed: Vec<Strand>,
	sort_keys: Vec<(u16, u16)>,
	judged: Option<Box<ComputedReadsShape>>
});
text_struct!(LetAttrs { name: Strand, binding: LetBinding, ty: Option<Kind> });
text_struct!(AccessAttrs { ac: Strand, base: Option<Base>, op: AccessOp });
text_struct!(LiveAttrs {
	id: Uuid,
	node: Uuid,
	fields: LiveShape,
	template: DefinitionTemplate
});
text_enum!(SubscriberAdmission {
	unit [All, Nothing]
	tuple []
	named [Judge { derived: Box<[DerivedField]> }]
});
text_enum!(SubscriberProjection {
	unit [Diff, Subject]
	tuple []
	named [Fields { fields: FieldsShape, names: Vec<OutputPath> }]
});
text_struct!(SubscriberAttrs {
	checked: bool,
	admission: SubscriberAdmission,
	view: ImageView,
	layers: Vec<SubscriberLayer>,
	projection: SubscriberProjection,
	cond: bool,
	fetch: u16
});
text_struct!(SubscriberLayer {
	fields: FieldsShape,
	computed: Vec<Strand>,
	judged: Option<Box<ComputedReadsShape>>
});
text_struct!(ShowAttrs { table: Option<TableName>, since: ShowSince, limit: Option<u32> });
text_struct!(ChooseSpec {
	granularity: ChooseGranularity,
	arms: SmallVec<[ArmSpec; 4]>,
	generic: GenericArm,
	equivalence: ArmEquivalence,
	origin: ChooseOrigin
});
text_struct!(ArmSpec {
	guard: Guard,
	body: ArmBody,
	label: ArmLabel
});
text_struct!(IndexRef {
	table: TableName,
	index: Strand,
	position: u32,
	scalar: Box<[bool]>
});
text_struct!(PermissionsShape {
	select: PermissionShape,
	create: PermissionShape,
	update: PermissionShape,
	delete: PermissionShape
});
text_struct!(ViewShape {
	what: Box<[TableName]>,
	query: u16,
	backfill: Option<u16>,
	group_fields: Box<[u16]>
});
text_struct!(ReferenceShape {
	on_delete: OnDeleteShape
});
text_struct!(UserDurationShape {
	token: u16,
	session: u16
});
text_struct!(AccessDurationShape {
	grant: u16,
	token: u16,
	session: u16
});
text_struct!(JwtIssueShape {
	alg: Algorithm,
	key: u16
});
text_struct!(JwtShape {
	verify: JwtVerifyShape,
	issue: Option<JwtIssueShape>,
	audience: Option<Box<[u16]>>
});
text_struct!(BearerShape {
	kind: BearerAccessType,
	subject: BearerAccessSubject,
	jwt: JwtShape
});
text_struct!(RecordAccessShape {
	signup: Option<u16>,
	signin: Option<u16>,
	jwt: JwtShape,
	bearer: Option<BearerShape>
});
text_struct!(MiddlewareShape { name: Strand, args: Box<[u16]> });
text_struct!(ApiConfigShape { middleware: Box<[MiddlewareShape]>, permissions: PermissionShape });
text_struct!(ApiActionShape { methods: Box<[ApiMethod]>, action: u16, config: ApiConfigShape });
text_struct!(DefaultConfigShape {
	namespace: u16,
	database: u16
});
text_struct!(GraphQLConfig {
	tables: GraphQLTablesConfig,
	functions: GraphQLFunctionsConfig,
	depth_limit: Option<u32>,
	complexity_limit: Option<u32>,
	introspection: GraphQLIntrospectionConfig
});
text_struct!(ChangeFeed {
	expiry: crate::val::BoundedDuration,
	store_diff: bool
});
text_struct!(Relation {
	from: Vec<TableName>,
	to: Vec<TableName>,
	enforced: bool,
	lightweight: bool
});
text_struct!(FullTextParams {
	az: Strand,
	hl: bool,
	sc: Scoring
});
text_struct!(HnswParams {
	dimension: u16,
	distance: Distance,
	vector_type: VectorType,
	m: u8,
	m0: u8,
	ef_construction: u16,
	extend_candidates: bool,
	keep_pruned_connections: bool,
	ml: PublicNumber,
	use_hashed_vector: bool
});
text_struct!(DiskAnnParams {
	dimension: u16,
	distance: Distance,
	vector_type: VectorType,
	degree: u16,
	l_build: u16,
	alpha: PublicNumber,
	use_hashed_vector: bool
});
text_struct!(File {
	bucket: String,
	key: String
});
text_struct!(SiloExecutable {
	organisation: String,
	package: String,
	major: u32,
	minor: u32,
	patch: u32
});
text_struct!(DefineFunction {
	name: Strand,
	args: Box<[(String, Kind)]>,
	returns: Option<Kind>,
	graphql_alias: Option<String>,
	graphql_deprecated: Option<String>,
	comment: u16,
	body: u16,
	permissions: PermissionShape
});
text_struct!(DefineTable {
	id: Option<u32>,
	name: u16,
	drop: bool,
	full: bool,
	view: Option<ViewShape>,
	permissions: PermissionsShape,
	changefeed: Option<ChangeFeed>,
	comment: u16,
	table_type: TableType,
	graphql_alias: Option<String>,
	graphql_deprecated: Option<String>,
	inline_edges_cap: Option<u32>,
	inline_refs_cap: Option<u32>
});
text_struct!(DefineField {
	name: u16,
	what: u16,
	field_kind: Option<Kind>,
	flexible: bool,
	readonly: bool,
	inline: bool,
	value: Option<u16>,
	assert: Option<u16>,
	computed: Option<u16>,
	default: DefaultShape,
	permissions: PermissionsShape,
	comment: u16,
	reference: Option<ReferenceShape>,
	graphql_alias: Option<String>,
	graphql_deprecated: Option<String>,
	migrate: Option<Box<MigrateClause>>,
	sql_type: Option<crate::expr::statements::define::SqlType>
});
text_struct!(MigrateClause {
	name: String,
	using: crate::expr::Expr,
	fallback: Option<crate::expr::Expr>
});
text_struct!(TableMigrateClause {
	name: String,
	action: TableMigrateAction
});
text_unit_enum!(TableMigrateAction {
	RemoveUndeclaredFields
});
text_struct!(DefineUser {
	name: u16,
	base: Base,
	password: Option<u16>,
	secrets: UserSecrets,
	roles: Box<[String]>,
	duration: UserDurationShape,
	comment: u16
});
text_struct!(DefineAccess {
	name: u16,
	base: Base,
	access_type: AccessTypeShape,
	secrets: AccessSecrets,
	authenticate: Option<u16>,
	context: Option<u16>,
	duration: AccessDurationShape,
	comment: u16
});
text_struct!(DefineApi {
	path: u16,
	actions: Box<[ApiActionShape]>,
	fallback: Option<u16>,
	config: ApiConfigShape,
	comment: u16
});
text_struct!(AlterTable {
	name: u16,
	schemafull: AlterKind<()>,
	permissions: Option<PermissionsShape>,
	changefeed: AlterKind<ChangeFeed>,
	comment: AlterKind<String>,
	compact: bool,
	kind: Option<TableType>,
	inline_edges_cap: AlterKind<u32>,
	inline_refs_cap: AlterKind<u32>,
	migrate: Option<Box<TableMigrateClause>>
});
text_struct!(AlterEvent {
	name: u16,
	what: u16,
	when: AlterKind<u16>,
	then: AlterKind<Box<[u16]>>,
	comment: AlterKind<String>,
	kind: AlterKind<EventKind>
});
text_struct!(AlterField {
	name: u16,
	what: u16,
	kind: AlterKind<Kind>,
	flexible: AlterKind<()>,
	readonly: AlterKind<()>,
	inline: AlterKind<()>,
	value: AlterKind<u16>,
	assert: AlterKind<u16>,
	default: AlterDefaultShape,
	permissions: Option<PermissionsShape>,
	comment: AlterKind<String>,
	reference: AlterKind<ReferenceShape>,
	migrate: Option<Box<MigrateClause>>,
	sql_type: AlterKind<crate::expr::statements::define::SqlType>
});
text_struct!(AlterFunction {
	name: Strand,
	args: AlterKind<Box<[(String, Kind)]>>,
	block: AlterKind<u16>,
	comment: AlterKind<String>,
	permissions: Option<PermissionShape>,
	returns: AlterKind<Kind>
});
text_struct!(AlterAccess {
	name: u16,
	base: Base,
	authenticate: AlterKind<u16>,
	context: AlterKind<u16>,
	grant_duration: AlterKind<Option<Duration>>,
	token_duration: AlterKind<Option<Duration>>,
	session_duration: AlterKind<Option<Duration>>,
	comment: AlterKind<String>
});

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::Number;

	/// `value` printed and read back, the whole text consumed.
	fn reads_back<T: Text>(value: &T) -> T {
		let mut out = String::new();
		value.put(&mut out);
		let mut c = Cursor::new(&out);
		let read = T::get(&mut c).unwrap_or_else(|failure| panic!("{out}: {failure}"));
		c.finish().unwrap_or_else(|failure| panic!("{out}: {failure}"));
		read
	}

	#[test]
	fn floats_values_and_kinds_read_back_exactly() {
		let negative_nan = f64::from_bits(f64::NAN.to_bits() | (1 << 63));
		for f in [f64::NAN, negative_nan, -0.0, 0.0, f64::INFINITY, f64::NEG_INFINITY, 0.1] {
			assert_eq!(reads_back(&f).to_bits(), f.to_bits(), "{f:?}");
		}
		for f in [f32::NAN, -0.0, f32::INFINITY, f32::NEG_INFINITY, 0.1] {
			assert_eq!(reads_back(&f).to_bits(), f.to_bits(), "{f:?}");
		}
		for f in [f64::NAN, -0.0, f64::INFINITY, f64::NEG_INFINITY] {
			let value = Value::Number(Number::Float(f));
			assert_eq!(
				revision::to_vec(&reads_back(&value)).expect("encodes"),
				revision::to_vec(&value).expect("encodes"),
				"{f:?}"
			);
		}
		let kind = crate::syn::kind("int | string").expect("a kind");
		assert_eq!(reads_back(&kind), kind);
		let mut c = Cursor::new("\"int trailing\"");
		assert!(
			<Kind as Text>::get(&mut c).is_err(),
			"a kind is read from its canonical text only"
		);
	}
}
