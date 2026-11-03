//! The text form of the physical dialect's attributes.
//!
//! Printing then parsing then printing yields the text printed first, for every physical tag whose
//! attributes have a text form. A placement decision is held beside the container and is not part
//! of an op's text. An op whose attributes the syntax cannot read answers a [`ParseError`] that
//! names the type with no text form, and never a default op.
//!
//! # Grammar
//!
//! An op prints as its mnemonic, then ` { name: value, … }` holding its attributes in declaration
//! order; an op with no attributes prints as its mnemonic alone. The reader accepts the text with
//! or without the leading mnemonic, which must be the mnemonic of the tag being read. A value is
//! one of:
//!
//! - an integer or a float, as a bare token;
//! - `true` or `false`;
//! - a double-quoted string that escapes `\`, `"`, newline, carriage return, tab and every other
//!   control character as `\u{hex}`;
//! - `none` or `some(value)` for an option;
//! - `[value, …]` for a sequence and `(value, value)` for a pair;
//! - `Variant` for a variant without a payload, `Variant(value, …)` for a positional payload and
//!   `Variant { name: value, … }` for a named payload;
//! - `{ name: value, … }` for a struct.
//!
//! A language value that has a SurrealQL text embeds it as a quoted string: a `Kind` is its
//! SurrealQL text, read back with [`crate::syn::kind`], and a `Value` is its SurrealQL literal,
//! read back with [`crate::syn::value`].
//!
//! The reader is strict. Names appear in declaration order, and a missing, extra, reordered or
//! repeated name, an unknown variant, a malformed token or text after the last attribute is a parse
//! error. An attribute typed `&'static str` (an arm label) is read through [`intern`], which holds
//! a bounded set of names.
//!
//! # Types with no text form
//!
//! A type the syntax cannot carry prints as `unsupported(Type)` and is not read back: reading it
//! answers a [`ParseError`] whose detail is `<mnemonic>: <Type> has no text form`. These are the
//! stored definition and statement attributes of `phys.define`, `phys.alter` and `phys.live`
//! (`DefineAttrs`, `AlterAttrs`, `LiveAttrs`), the decoded-field set of a scan (`DecodeFields`,
//! whose fields are private to its module), a stored value that is not a literal (a closure), an
//! operator of a compound range that is not a comparison, and the key structure of an aggregated
//! view that a key-range scan was planned against (`KeyStructure`, derived from the view's
//! definition; the structure of an `id` field prints as its component names and types).

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::HashSet;
use std::fmt::Write as _;
use std::ops::Bound;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use smallvec::{Array, SmallVec};
use surrealdb_iam::{AuthLimit, Level, Role};
use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql;

use super::ops::{
	AggregateAttrs, Batch, BitmapAttrs, BitmapShape, BranchReadShape, ComputedReadsShape,
	CountAttrs, CountShape, FullTextScanAttrs, IdBoundsShape, IndexBranchShape, IndexLookupAttrs,
	IndexScanAttrs, IndexUnionAttrs, JoinAttrs, KeyBoundShape, KeyEqShape, KeyRangeBoundShape,
	KeyRangeOnShape, KeyRangeScanAttrs, KeyRangeShape, KnnFilterShape, KnnPermissionShape,
	KnnPrefilterShape, KnnScanAttrs, KnnTargetShape, KnnTopKAttrs, LayerAttrs, LookupKeyShape,
	MatchStepAttrs, MaterializeImage, OmissionShape, PermissionFilterAttrs, PhysOp, PhysTag,
	PhysWriteAttrs, PipelineAttrs, PipelineEdge, PipelineId, PrefetchKeys, ProjectedFieldShape,
	ProjectionShape, ReadClauses, RecordScanAttrs, ScanProbes, ScoreSum,
	SortAttrs as PhysSortAttrs, SortKeyReadShape, SortKeySpecShape, TableScanAttrs,
	WalkActionShape, WalkStepShape, WriteSubjectsShape,
};
use crate::catalog::{Distance, IndexId, KeyOrigin, KeyStructure};
use crate::doc::Action as DocAction;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::index::access_path::{
	BTreeAccess, BoundOperand, ClockBound, ClockSign, OperandRange,
};
use crate::exec::index::key_range::DeliveredKeyOrder;
use crate::exec::index::probe::ProbeBound;
use crate::exec::ir::core::parse::ParseError;
use crate::exec::ir::core::print::AttrSyntax;
use crate::exec::ir::core::text::{get_attr_op_id, put_attr_op_id};
use crate::exec::ir::expr::ops::{DestructureEntry, KeyBound, KeyRange};
use crate::exec::ir::expr::print::{
	f32_text, f64_text, read_f32, read_f64, read_kind_text, read_value, value_text,
};
use crate::exec::ir::iface::choose::{
	ArmBody, ArmEquivalence, ArmLabel, ArmSpec, ChooseGranularity, ChooseOrigin, ChooseSpec,
	ExhaustiveFact, GenericArm, Guard, IndexRef, ValueKindClass,
};
use crate::exec::ir::iface::estimate::SizeBucket;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::iface::region_kind::StatementCommit;
use crate::exec::ir::iface::statements::{
	AccessOp, AccessSubjectShape, BindExpandShape, BindInsertEdge, BindInsertNode, BindJoinShape,
	BindPathShape, BindStepShape, ContentMode, DdlChecks, DdlGuard, InfoSubject, MatchBindings,
	MatchColumnShape, MatchData, MatchInsertEdge, MatchInsertNode, MatchOutputShape, MatchPattern,
	OutputShape, Place, PlacePart, RemoveObject, ShowSince, UnaddressedPart, WriteFlags, WriteForm,
};
use crate::exec::ir::iface::unit::{LoadUnitSpec, UnitKind, UnitStaticKey};
use crate::exec::ir::logical::ops::{
	AccessAttrs, AlterAttrs, AssignAttrs, ClosureLoopAttrs, DefineAttrs, DerivedField,
	DerivedReads, EventClause, EventDefs, EventShape, EventsAttrs, FieldClause, FieldClauses,
	FieldGate, FieldShape, FieldStep, FieldsShape, GateClause, ImageLayers, ImageView, IndexDefs,
	LetAttrs, LetBinding, LinkedStages, LiveAttrs, LookupAttrs, LookupSubjectShape, LoopKind,
	MatchInsertAttrs, MatchUpdateAttrs, RebuildAttrs, RecurseAttrs, RecurseInstructionShape,
	RecursePathForm, RemoveAttrs, RepairField, RepairKind, RepairTable, ShowAttrs, SortKeyShape,
	SortShape, SourceRefShape, StepAttrs, Stored, SubscriberAdmission, SubscriberAttrs,
	SubscriberLayer, SubscriberProjection, TableDefs, TxnControl, TxnRegionEnd, TxnRegionMode,
	TxnStart, UseTarget, WindowCallShape, WindowShape, WriteAttrs, WriteBody, WriteBranching,
	WriteDerived, WriteGates, WriteImage, WriteIndexes, WriteMark, WriteMaterialized, WriteRepairs,
	WriteRetry, WriteSchema, WriteStageKind, WriteStored, WriteViews,
};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::object_extract::PathSegment;
use crate::exec::operators::scan::common::DecodeFields;
use crate::exec::operators::scan::{CountOutput, MergeMode};
use crate::exec::operators::write::stage::{LightweightReason, RecordImage, TableTypeFor};
use crate::exec::operators::{
	EdgeBinding, EndpointField, ExpandDir, JoinType, PathMode as OperatorPathMode,
	ShortestSelector, SortDirection, SplitTarget,
};
use crate::exec::planner::CatalogWithin;
use crate::exec::pre_decode_filter::PreDecodeShape;
use crate::exec::vm::payload::{
	CoarseCutoff, CountCap, CountSelect, JoinBuild, JoinMatch, Spill, TopKKey, ValuesRead,
};
use crate::exec::vm::plain::ValueData;
use crate::expr::logical::{
	Emission, GuardKind, LookupTrace, OutputPath, OutputPathPart, Schema, WindowFrame,
	WindowFrameBound, WindowFrameUnits, WindowFunction,
};
use crate::expr::lookup::LookupKind;
use crate::expr::match_plan::{
	BindingDef, BindingKind, DetachMode, EdgeQuantifier, EdgeStep, ExpandDirection, NodeStep,
	PathMode, PathPrefixPlan, PathSearch,
};
use crate::expr::module::ModuleName;
use crate::expr::operator::{BooleanOperator, MatchRef, MatchesOperator};
use crate::expr::part::Recurse;
use crate::expr::statements::access::PurgeKind;
use crate::expr::statements::remove::SqlTypeObject;
use crate::expr::{
	AssignOperator, Base, BinaryOperator, Dir, Explain, ExplainFormat, Idiom, KeyNames, Kind,
	KindLiteral, RecordIdKeyGen, With,
};
use crate::iam::{Action as IamAction, ConfigKind, ResourceKind};
use crate::kvs::Direction;
use crate::val::{Datetime, Number, Range, RecordId, Uuid, Value};

// ---------------------------------------------------------------------------------------------
// The syntax
// ---------------------------------------------------------------------------------------------

/// The physical dialect's attribute syntax.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PhysSyntax;

impl AttrSyntax for PhysSyntax {
	/// Appends the mnemonic of `kind` and, for an op with attributes, ` { name: value, … }`. An op
	/// of another dialect prints `unsupported`, which no physical tag reads.
	fn print_attrs(&self, kind: &OpKind, out: &mut String) {
		match kind {
			OpKind::Phys(op) => {
				out.push_str(op.tag().mnemonic());
				let mut body = String::new();
				put_op(op, &mut Fields::new(&mut body));
				if !body.is_empty() {
					out.push_str(" { ");
					out.push_str(&body);
					out.push_str(" }");
				}
			}
			OpKind::Expr(_) | OpKind::Logical(_) => out.push_str("unsupported"),
		}
	}

	/// Reads the attributes of the physical op `tag` names, after an optional leading mnemonic.
	/// Text that the tag's attribute list does not match exactly answers a [`ParseError`].
	fn parse_attrs(&self, tag: OpTag, text: &str) -> Result<OpKind, ParseError> {
		let OpTag::Phys(tag) = tag else {
			return Err(ParseError {
				line: 0,
				detail: format!("phys::parse_attrs: {} is not a physical op", tag.mnemonic()),
			});
		};
		let mnemonic = tag.mnemonic();
		let body = text.trim_start();
		let body = match body.strip_prefix(mnemonic) {
			Some(rest) if !rest.starts_with(|ch: char| ch == '_' || ch.is_ascii_alphanumeric()) => {
				rest
			}
			_ => body,
		};
		let mut cursor = Cursor::new(mnemonic, body);
		let op = get_op(tag, &mut cursor)?;
		cursor.finish()?;
		Ok(OpKind::Phys(op))
	}
}

// ---------------------------------------------------------------------------------------------
// The ops
// ---------------------------------------------------------------------------------------------

/// Appends the attributes of `op` in declaration order, and nothing for an op without any.
fn put_op(op: &PhysOp, w: &mut Fields<'_>) {
	match op {
		PhysOp::TableScan(attrs) => attrs.put_fields(w),
		PhysOp::IndexScan(attrs) => attrs.put_fields(w),
		PhysOp::IndexUnion(attrs) => attrs.put_fields(w),
		PhysOp::Bitmap(attrs) => attrs.put_fields(w),
		PhysOp::IndexLookup(attrs) => attrs.put_fields(w),
		PhysOp::FullTextScan(attrs) => attrs.put_fields(w),
		PhysOp::KeyRangeScan(attrs) => attrs.put_fields(w),
		PhysOp::KnnScan(attrs) => attrs.put_fields(w),
		PhysOp::KnnTopK(attrs) => attrs.put_fields(w),
		PhysOp::Count(attrs) => attrs.put_fields(w),
		PhysOp::RecordScan(attrs) => attrs.put_fields(w),
		PhysOp::Vacant {
			table,
		} => w.field("table", table),
		PhysOp::Enforced {
			table,
		} => w.field("table", table),
		PhysOp::Values {
			read,
		} => w.field("read", read),
		PhysOp::Subquery => {}
		PhysOp::Probe {
			field,
		} => w.field("field", field),
		PhysOp::Union => {}
		PhysOp::Join(attrs) => attrs.put_fields(w),
		PhysOp::Distinct {
			spill,
		} => w.field("spill", spill),
		PhysOp::PermissionFilter(attrs) => attrs.put_fields(w),
		PhysOp::Layer(attrs) => attrs.put_fields(w),
		PhysOp::Split {
			targets,
		} => w.field("targets", targets),
		PhysOp::Filter | PhysOp::Fetch => {}
		PhysOp::Sort(attrs) => attrs.put_fields(w),
		PhysOp::Window(attrs) => attrs.put_fields(w),
		PhysOp::Aggregate(attrs) => attrs.put_fields(w),
		PhysOp::Limit {
			limit,
			start,
		} => {
			w.field("limit", limit);
			w.field("start", start);
		}
		PhysOp::Project(shape) => w.field("shape", &**shape),
		PhysOp::Timeout | PhysOp::VersionScope | PhysOp::Only => {}
		PhysOp::Explain {
			analyze,
			format,
		} => {
			w.field("analyze", analyze);
			w.field("format", format);
		}
		PhysOp::Eval => {}
		PhysOp::Request {
			fragment,
			nested,
		} => {
			w.field("fragment", fragment);
			w.field("nested", nested);
		}
		PhysOp::Txn {
			mode,
		} => w.field("mode", mode),
		PhysOp::Stmt {
			commit,
			control,
		} => {
			w.field("commit", commit);
			w.field("control", control);
		}
		PhysOp::Statement {
			changes_catalog,
			served,
			within,
			raises_when_reached,
		} => {
			w.field("changes_catalog", changes_catalog);
			w.field("served", served);
			w.field("within", within);
			w.field("raises_when_reached", raises_when_reached);
		}
		PhysOp::Match {
			bindings,
			stages,
		} => {
			w.field("bindings", bindings);
			w.field("stages", stages);
		}
		PhysOp::MatchStep(step) => w.field("step", &**step),
		PhysOp::DdlGuard {
			guard,
		} => w.field("guard", guard),
		PhysOp::Define(attrs) => w.field("attrs", &**attrs),
		PhysOp::Remove(attrs) => attrs.put_fields(w),
		PhysOp::Alter(attrs) => w.field("attrs", &**attrs),
		PhysOp::Rebuild(attrs) => attrs.put_fields(w),
		PhysOp::Info {
			subject,
			structure,
		} => {
			w.field("subject", subject);
			w.field("structure", structure);
		}
		PhysOp::Use {
			target,
		} => w.field("target", target),
		PhysOp::Option {
			name,
			value,
		} => {
			w.field("name", name);
			w.field("value", value);
		}
		PhysOp::Access(attrs) => attrs.put_fields(w),
		PhysOp::Kill => {}
		PhysOp::Live(attrs) => w.field("attrs", &**attrs),
		PhysOp::Subscriber(attrs) => attrs.put_fields(w),
		PhysOp::Show(attrs) => attrs.put_fields(w),
		PhysOp::Events(attrs) => attrs.put_fields(w),
		PhysOp::Write(attrs) => attrs.put_fields(w),
		PhysOp::WriteStage {
			stage,
			mark,
		} => {
			w.field("stage", stage);
			w.field("mark", mark);
		}
		PhysOp::Assign(attrs) => attrs.put_fields(w),
		PhysOp::Unset {
			place,
		} => w.field("place", place),
		PhysOp::Content {
			mode,
		} => w.field("mode", mode),
		PhysOp::Block => {}
		PhysOp::Let(attrs) => attrs.put_fields(w),
		PhysOp::Sleep => {}
		PhysOp::If {
			value,
		} => w.field("value", value),
		PhysOp::Loop {
			kind,
			first,
			batch,
			unroll,
			value,
			step,
		} => {
			w.field("kind", kind);
			w.field("first", first);
			w.field("batch", batch);
			w.field("unroll", unroll);
			w.field("value", value);
			w.field("step", step);
		}
		PhysOp::Pipeline(attrs) => attrs.put_fields(w),
		PhysOp::Choose(spec) => spec.put_fields(w),
		PhysOp::Prefetch(keys) => keys.put_fields(w),
		PhysOp::Materialize {
			image,
		} => w.field("image", image),
		PhysOp::LoadUnit(spec) => spec.put_fields(w),
		PhysOp::Yield | PhysOp::Return | PhysOp::Break | PhysOp::Next => {}
	}
}

/// Reads the attributes of the op `tag` names. An op with attributes holds them in braces.
fn get_op(tag: PhysTag, c: &mut Cursor<'_>) -> Result<PhysOp, ParseError> {
	/// Reads `{ fields }` with `read`, which receives the cursor and the first-field flag.
	fn braced<T>(
		c: &mut Cursor<'_>,
		read: impl FnOnce(&mut Cursor<'_>, &mut bool) -> Result<T, ParseError>,
	) -> Result<T, ParseError> {
		c.expect('{')?;
		let first = &mut true;
		let value = read(c, first)?;
		c.expect('}')?;
		Ok(value)
	}
	/// A boxed struct read inline in the braces.
	macro_rules! record {
		($ty:ty, $variant:ident) => {
			braced(c, |c, first| {
				Ok(PhysOp::$variant(Box::new(<$ty as Record>::get_fields(c, first)?)))
			})
		};
	}
	/// One named field read in the braces, as the op's only attribute.
	macro_rules! one {
		($name:literal, $ty:ty, |$v:ident| $build:expr) => {
			braced(c, |c, first| {
				let $v = c.field::<$ty>($name, first)?;
				Ok($build)
			})
		};
	}
	match tag {
		PhysTag::TableScan => record!(TableScanAttrs, TableScan),
		PhysTag::IndexScan => record!(IndexScanAttrs, IndexScan),
		PhysTag::IndexUnion => record!(IndexUnionAttrs, IndexUnion),
		PhysTag::Bitmap => record!(BitmapAttrs, Bitmap),
		PhysTag::IndexLookup => record!(IndexLookupAttrs, IndexLookup),
		PhysTag::FullTextScan => record!(FullTextScanAttrs, FullTextScan),
		PhysTag::KeyRangeScan => record!(KeyRangeScanAttrs, KeyRangeScan),
		PhysTag::KnnScan => record!(KnnScanAttrs, KnnScan),
		PhysTag::KnnTopK => record!(KnnTopKAttrs, KnnTopK),
		PhysTag::Count => record!(CountAttrs, Count),
		PhysTag::RecordScan => record!(RecordScanAttrs, RecordScan),
		PhysTag::Vacant => one!("table", TableName, |table| PhysOp::Vacant {
			table
		}),
		PhysTag::Enforced => one!("table", TableName, |table| PhysOp::Enforced {
			table
		}),
		PhysTag::Values => one!("read", ValuesRead, |read| PhysOp::Values {
			read
		}),
		PhysTag::Subquery => Ok(PhysOp::Subquery),
		PhysTag::Probe => one!("field", Strand, |field| PhysOp::Probe {
			field
		}),
		PhysTag::Union => Ok(PhysOp::Union),
		PhysTag::Join => record!(JoinAttrs, Join),
		PhysTag::Distinct => one!("spill", Spill, |spill| PhysOp::Distinct {
			spill
		}),
		PhysTag::PermissionFilter => record!(PermissionFilterAttrs, PermissionFilter),
		PhysTag::Layer => record!(LayerAttrs, Layer),
		PhysTag::Filter => Ok(PhysOp::Filter),
		PhysTag::Fetch => Ok(PhysOp::Fetch),
		PhysTag::Split => one!("targets", Box<[SplitTarget]>, |targets| PhysOp::Split {
			targets
		}),
		PhysTag::Sort => record!(PhysSortAttrs, Sort),
		PhysTag::Window => record!(WindowShape, Window),
		PhysTag::Aggregate => record!(AggregateAttrs, Aggregate),
		PhysTag::Limit => braced(c, |c, first| {
			let limit = c.field("limit", first)?;
			let start = c.field("start", first)?;
			Ok(PhysOp::Limit {
				limit,
				start,
			})
		}),
		PhysTag::Project => {
			one!("shape", ProjectionShape, |shape| PhysOp::Project(Box::new(shape)))
		}
		PhysTag::Timeout => Ok(PhysOp::Timeout),
		PhysTag::VersionScope => Ok(PhysOp::VersionScope),
		PhysTag::Only => Ok(PhysOp::Only),
		PhysTag::Explain => braced(c, |c, first| {
			let analyze = c.field("analyze", first)?;
			let format = c.field("format", first)?;
			Ok(PhysOp::Explain {
				analyze,
				format,
			})
		}),
		PhysTag::Eval => Ok(PhysOp::Eval),
		PhysTag::Request => braced(c, |c, first| {
			let fragment = c.field("fragment", first)?;
			let nested = c.field("nested", first)?;
			Ok(PhysOp::Request {
				fragment,
				nested,
			})
		}),
		PhysTag::Txn => one!("mode", TxnRegionMode, |mode| PhysOp::Txn {
			mode
		}),
		PhysTag::Stmt => braced(c, |c, first| {
			let commit = c.field("commit", first)?;
			let control = c.field("control", first)?;
			Ok(PhysOp::Stmt {
				commit,
				control,
			})
		}),
		PhysTag::Statement => braced(c, |c, first| {
			let changes_catalog = c.field("changes_catalog", first)?;
			let served = c.field("served", first)?;
			let within = c.field("within", first)?;
			let raises_when_reached = c.field("raises_when_reached", first)?;
			Ok(PhysOp::Statement {
				changes_catalog,
				served,
				within,
				raises_when_reached,
			})
		}),
		PhysTag::Match => braced(c, |c, first| {
			let bindings = c.field("bindings", first)?;
			let stages = c.field("stages", first)?;
			Ok(PhysOp::Match {
				bindings,
				stages,
			})
		}),
		PhysTag::MatchStep => {
			one!("step", MatchStepAttrs, |step| PhysOp::MatchStep(Box::new(step)))
		}
		PhysTag::DdlGuard => one!("guard", DdlGuard, |guard| PhysOp::DdlGuard {
			guard
		}),
		PhysTag::Define => one!("attrs", DefineAttrs, |attrs| PhysOp::Define(Box::new(attrs))),
		PhysTag::Remove => record!(RemoveAttrs, Remove),
		PhysTag::Alter => one!("attrs", AlterAttrs, |attrs| PhysOp::Alter(Box::new(attrs))),
		PhysTag::Rebuild => record!(RebuildAttrs, Rebuild),
		PhysTag::Info => braced(c, |c, first| {
			let subject = c.field("subject", first)?;
			let structure = c.field("structure", first)?;
			Ok(PhysOp::Info {
				subject,
				structure,
			})
		}),
		PhysTag::Use => one!("target", UseTarget, |target| PhysOp::Use {
			target
		}),
		PhysTag::Option => braced(c, |c, first| {
			let name = c.field("name", first)?;
			let value = c.field("value", first)?;
			Ok(PhysOp::Option {
				name,
				value,
			})
		}),
		PhysTag::Access => record!(AccessAttrs, Access),
		PhysTag::Kill => Ok(PhysOp::Kill),
		PhysTag::Live => one!("attrs", LiveAttrs, |attrs| PhysOp::Live(Box::new(attrs))),
		PhysTag::Subscriber => record!(SubscriberAttrs, Subscriber),
		PhysTag::Show => record!(ShowAttrs, Show),
		PhysTag::Events => record!(EventsAttrs, Events),
		PhysTag::Write => record!(PhysWriteAttrs, Write),
		PhysTag::WriteStage => braced(c, |c, first| {
			let stage = c.field("stage", first)?;
			let mark = c.field("mark", first)?;
			Ok(PhysOp::WriteStage {
				stage,
				mark,
			})
		}),
		PhysTag::Assign => record!(AssignAttrs, Assign),
		PhysTag::Unset => one!("place", Place, |place| PhysOp::Unset {
			place
		}),
		PhysTag::Content => one!("mode", ContentMode, |mode| PhysOp::Content {
			mode
		}),
		PhysTag::Block => Ok(PhysOp::Block),
		PhysTag::Let => record!(LetAttrs, Let),
		PhysTag::Sleep => Ok(PhysOp::Sleep),
		PhysTag::If => one!("value", bool, |value| PhysOp::If {
			value
		}),
		PhysTag::Loop => braced(c, |c, first| {
			let kind = c.field("kind", first)?;
			let first_region = c.field("first", first)?;
			let batch = c.field("batch", first)?;
			let unroll = c.field("unroll", first)?;
			let value = c.field("value", first)?;
			let step = c.field("step", first)?;
			Ok(PhysOp::Loop {
				kind,
				first: first_region,
				batch,
				unroll,
				value,
				step,
			})
		}),
		PhysTag::Pipeline => record!(PipelineAttrs, Pipeline),
		PhysTag::Choose => record!(ChooseSpec, Choose),
		PhysTag::Prefetch => record!(PrefetchKeys, Prefetch),
		PhysTag::Materialize => one!("image", MaterializeImage, |image| PhysOp::Materialize {
			image
		}),
		PhysTag::LoadUnit => record!(LoadUnitSpec, LoadUnit),
		PhysTag::Yield => Ok(PhysOp::Yield),
		PhysTag::Return => Ok(PhysOp::Return),
		PhysTag::Break => Ok(PhysOp::Break),
		PhysTag::Next => Ok(PhysOp::Next),
	}
}

// ---------------------------------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------------------------------

/// A value of the attribute text: how it prints and how it is read back.
trait Text: Sized {
	fn put(&self, out: &mut String);
	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError>;
}

/// A struct whose fields print as `name: value` pairs, inline at the top of an op's text or
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
		self.out.push_str(": ");
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
	/// The mnemonic of the op being read, which names it in an error.
	tag: &'static str,
	text: &'a str,
	at: usize,
}

impl<'a> Cursor<'a> {
	fn new(tag: &'static str, text: &'a str) -> Self {
		Self {
			tag,
			text,
			at: 0,
		}
	}

	fn err(&self, detail: impl std::fmt::Display) -> ParseError {
		ParseError {
			line: 0,
			detail: format!("{}: offset {}: {detail}", self.tag, self.at),
		}
	}

	fn fail<T>(&self, detail: impl std::fmt::Display) -> Result<T, ParseError> {
		Err(self.err(detail))
	}

	/// The failure of reading a type the syntax cannot carry.
	fn no_text<T>(&self, ty: &str) -> Result<T, ParseError> {
		Err(ParseError {
			line: 0,
			detail: format!("{}: {ty} has no text form", self.tag),
		})
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
		self.expect(':')
	}

	/// A field `name: value`, after a comma unless it is the first of its list.
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
/// named (`V { a: .., b: .. }`).
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

impl<T: Text> Text for Arc<[T]> {
	fn put(&self, out: &mut String) {
		put_seq(out, self.iter());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		get_seq(c).map(Vec::into)
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

// ---------------------------------------------------------------------------------------------
// Attribute types shared with the logical dialect
// ---------------------------------------------------------------------------------------------

// unit enums
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
text_unit_enum!(WriteMark {
	Compute,
	Apply
});
text_unit_enum!(GuardKind {
	Read,
	Write
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
text_unit_enum!(PurgeKind {
	Expired,
	Revoked,
	Both
});

// enums
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

text_enum!(ModuleName {
	unit []
	tuple [
		Module(name: String),
		Silo(org: String, package: String, major: u32, minor: u32, patch: u32)
	]
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

// structs
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
text_struct!(OutputPath { parts: Vec<OutputPathPart>, exact: bool });
text_struct!(RemoveAttrs {
	object: RemoveObject,
	if_exists: bool,
	expunge: bool,
	checks: DdlChecks
});
text_struct!(RebuildAttrs {
	index: Strand,
	table: TableName,
	if_exists: bool,
	concurrently: bool
});
text_struct!(AssignAttrs {
	place: Place,
	op: AssignOperator
});
text_struct!(Place { parts: Box<[PlacePart]> });
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
text_struct!(LetAttrs { name: Strand, binding: LetBinding, ty: Option<Kind> });
text_struct!(AccessAttrs { ac: Strand, base: Option<Base>, op: AccessOp });
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

// impls
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

// ---------------------------------------------------------------------------------------------
// Attributes of the physical dialect
// ---------------------------------------------------------------------------------------------

text_unit_enum!(Direction {
	Forward,
	Backward
});
text_unit_enum!(Spill {
	Bounded,
	Spillable
});
text_unit_enum!(JoinBuild {
	Right,
	Left
});
text_unit_enum!(JoinMatch {
	Inner,
	Left,
	Semi
});
text_unit_enum!(CountSelect {
	Allow,
	Deny,
	Conditional
});
text_unit_enum!(LookupKeyShape {
	Parent,
	Probe
});
text_unit_enum!(MaterializeImage {
	KeyAndValue
});
text_unit_enum!(WriteSubjectsShape {
	Targets,
	Rows,
	Guaranteed,
	Inserts,
	Edges
});
text_unit_enum!(PipelineEdge {
	Feeds,
	Builds,
	Probes
});
text_unit_enum!(BooleanOperator {
	And,
	Or
});
text_unit_enum!(Dir {
	In,
	Out,
	Both
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

text_enum!(Recurse {
	unit []
	tuple [Fixed(depth: u32), Range(from: Option<u32>, to: Option<u32>)]
	named []
});

text_enum!(SortShape {
	unit [Random]
	tuple [Keys(keys: Box<[SortKeyShape]>)]
	named []
});

text_struct!(SortKeyShape {
	collate: bool,
	numeric: bool,
	ascending: bool,
	path: bool
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
text_unit_enum!(SortDirection {
	Asc,
	Desc
});
/// A role prints as its name. A role this syntax does not name prints `unsupported(Role)` and is
/// not read back.
impl Text for Role {
	fn put(&self, out: &mut String) {
		out.push_str(match self {
			Role::Viewer => "Viewer",
			Role::Editor => "Editor",
			Role::Owner => "Owner",
			_ => "unsupported(Role)",
		});
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		match c.ident()? {
			"Viewer" => Ok(Role::Viewer),
			"Editor" => Ok(Role::Editor),
			"Owner" => Ok(Role::Owner),
			_ => c.no_text("Role"),
		}
	}
}

text_enum!(Distance {
	unit [
		Chebyshev, Cosine, Euclidean, Hamming, Jaccard, Manhattan, Pearson, CosineNormalized,
		InnerProduct
	]
	tuple [Minkowski(order: Number)]
	named []
});

text_enum!(Level {
	unit [No, Root]
	tuple [
		Namespace(ns: String),
		Database(ns: String, db: String),
		Record(ns: String, db: String, id: String)
	]
	named []
});

text_enum!(ValuesRead {
	unit [Dynamic, Locked]
	tuple []
	named [Expression { written: bool }]
});

text_enum!(CountOutput {
	unit [Value]
	tuple [Fields(names: Vec<String>)]
	named []
});

text_enum!(MergeMode {
	unit []
	tuple [ById(direction: SortDirection)]
	named [
		ByIndexKey { path: FieldPath, direction: SortDirection },
		ByIndexKeyDedup { path: FieldPath, direction: SortDirection }
	]
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

text_enum!(BTreeAccess {
	unit []
	tuple [Equality(value: BoundOperand)]
	named [
		Range { range: OperandRange },
		Compound {
			prefix: Vec<BoundOperand>,
			range: Option<(BinaryOperator, BoundOperand)>,
			upper: Option<(BinaryOperator, BoundOperand)>
		},
		FullText { query: String, operator: MatchesOperator },
		Knn { vector: Arc<[Number]>, k: u32, ef: u32 }
	]
});

text_enum!(BranchReadShape {
	unit []
	tuple [BTree(access: ValueData<BTreeAccess>)]
	named [FullText { query: String, operator: MatchesOperator, column: Option<Idiom>, shared_doc_ids: bool }]
});

text_enum!(BitmapShape {
	unit []
	tuple [And(items: Vec<BitmapShape>), Or(items: Vec<BitmapShape>)]
	named [
		BTree { index: IndexRef, access: BTreeAccess },
		FullText { index: IndexRef, query: String, operator: MatchesOperator },
		Graph { source: RecordId, direction: Dir, edge_tables: Vec<TableName> },
		AndNot { base: Box<BitmapShape>, subtract: Box<BitmapShape> }
	]
});

text_enum!(KnnTargetShape {
	unit []
	tuple [Literal(vector: Vec<Number>), Deferred(text: String, before_rows: bool)]
	named []
});

text_enum!(CountShape {
	unit []
	tuple []
	named [
		All { source: SourceRefShape, output: CountOutput },
		Indexed {
			table: TableName,
			output: CountOutput,
			btree: Option<Box<[(IndexRef, BTreeAccess)]>>,
			bitmap: Option<Box<BitmapShape>>
		}
	]
});

text_enum!(SortKeyReadShape {
	unit [Computed]
	tuple [Path(path: FieldPath)]
	named []
});

text_enum!(DestructureEntry {
	unit [Rest]
	tuple [
		All(name: Strand),
		Field(name: Strand),
		Aliased(name: Strand),
		Destructure(name: Strand, entries: Box<[DestructureEntry]>)
	]
	named []
});

text_enum!(WalkStepShape {
	unit [IndexAt, Matching, Each, First, Last, Nowhere]
	tuple [
		Field(name: String),
		Index(at: usize),
		Destructure(entries: Box<[DestructureEntry]>)
	]
	named []
});

text_enum!(WalkActionShape {
	unit [RemoveIndexAt, RemoveMatching, RemoveFirst, RemoveLast, Clear, Nothing]
	tuple [
		RemoveField(name: String),
		RemoveIndex(at: usize),
		Destructure(entries: Box<[DestructureEntry]>)
	]
	named []
});

text_enum!(OmissionShape {
	unit []
	tuple [Path(path: FieldPath)]
	named [Walk { steps: Vec<WalkStepShape>, action: WalkActionShape }]
});

text_enum!(ProjectionShape {
	unit [All, Value]
	tuple [SortedValue(position: u16)]
	named [
		OmittedValue { omit: Vec<OmissionShape> },
		Fields { fields: Vec<ProjectedFieldShape>, omit: Vec<OmissionShape> },
		Star { fields: Vec<ProjectedFieldShape>, omit: Vec<OmissionShape> }
	]
});

text_enum!(MatchStepAttrs {
	unit []
	tuple [
		Update(attrs: MatchUpdateAttrs),
		Insert(attrs: MatchInsertAttrs),
		Output(shape: MatchOutputShape),
		Bind(shape: BindStepShape)
	]
	named [
		Read { optional_group: Option<u32>, patterns: Box<[MatchPattern]>, predicates: u16 },
		Delete { target: u32, detach: DetachMode }
	]
});

text_enum!(UnitStaticKey {
	unit [None]
	tuple [Named(name: Strand), Table(table: TableName), Shape(digest: u64)]
	named []
});

text_struct!(AuthLimit { level: Level, role: Option<Role> });
text_struct!(MatchesOperator { rf: Option<u8>, operator: BooleanOperator });
text_struct!(CatalogWithin {
	calls_current: bool,
	nested_late: bool
});
text_struct!(Batch {
	max_iterations: u32,
	max_bytes: u32
});
text_struct!(PrefetchKeys { points: SmallVec<[u16; 4]>, ranges: SmallVec<[(u16, u16); 2]> });
text_struct!(PipelineAttrs {
	id: PipelineId,
	edges: SmallVec<[(PipelineEdge, PipelineId); 2]>,
	logical: Option<OpId>
});
text_struct!(LoadUnitSpec {
	kind: UnitKind,
	static_key: UnitStaticKey
});
text_struct!(TableScanAttrs {
	table: TableName,
	read: ReadClauses,
	direction: Direction,
	topk: Option<TopKKey>,
	lightweight: bool,
	id_order: bool,
	prefilter: Option<PreDecodeShape>,
	bounds: Option<IdBoundsShape>,
	probes: Option<Box<ScanProbes>>
});
text_struct!(ScanProbes {
	probes: Vec<(IndexRef, BTreeAccess)>,
	bound: ProbeBound
});
text_enum!(ProbeBound {
	unit []
	tuple [Probe(at: u16), All(parts: Vec<ProbeBound>), Any(parts: Vec<ProbeBound>)]
	named []
});
text_struct!(IdBoundsShape {
	low: Option<KeyRangeBoundShape>,
	high: Option<KeyRangeBoundShape>
});
text_struct!(RecordScanAttrs {
	table: TableName,
	for_update: bool,
	lightweight: bool,
	read: ReadClauses
});
text_struct!(KeyRangeScanAttrs {
	table: TableName,
	read: ReadClauses,
	access: KeyRangeShape,
	topk: Option<TopKKey>,
	prefilter: Option<PreDecodeShape>
});
text_struct!(KeyRangeShape {
	key: Arc<KeyStructure>,
	eq: Vec<KeyEqShape>,
	range: Option<KeyRangeOnShape>,
	direction: Direction,
	delivers: DeliveredKeyOrder,
	ordered_by: Vec<u32>
});
text_struct!(KeyRangeOnShape {
	position: u32,
	low: Option<KeyRangeBoundShape>,
	high: Option<KeyRangeBoundShape>
});
text_struct!(KeyRangeBoundShape {
	bound: KeyBoundShape,
	inclusive: bool
});
text_struct!(CoarseCutoff {
	position: u32,
	keep: u64
});
text_enum!(KeyBoundShape {
	unit []
	tuple [Literal(value: Value), Operand(slot: u16)]
	named []
});
text_enum!(KeyEqShape {
	unit []
	tuple [One(bound: KeyBoundShape), In(bounds: Vec<KeyBoundShape>)]
	named []
});
text_enum!(DeliveredKeyOrder {
	unit [None, Exact]
	tuple []
	named [Coarse { position: u32 }]
});
text_struct!(IndexScanAttrs {
	table: TableName,
	read: ReadClauses,
	index: IndexRef,
	access: BTreeAccess,
	direction: Direction,
	ordered_by: Option<Vec<FieldPath>>,
	pinned: Option<Vec<FieldPath>>,
	stated: Option<Vec<Idiom>>,
	rechecked: bool,
	decode: Option<DecodeFields>
});
text_struct!(IndexBranchShape {
	index: IndexRef,
	read: BranchReadShape,
	direction: Direction
});
text_struct!(IndexUnionAttrs {
	table: TableName,
	read: ReadClauses,
	branches: Vec<IndexBranchShape>,
	merge: Option<MergeMode>,
	ordered_by: Option<Vec<FieldPath>>,
	decode: Option<DecodeFields>,
	topk: Option<(TopKKey, ScoreSum)>
});
text_enum!(ScoreSum {
	unit []
	tuple [Clause(clause: MatchRef), Add(left: Box<ScoreSum>, right: Box<ScoreSum>)]
	named []
});
text_struct!(BitmapAttrs {
	table: TableName,
	read: ReadClauses,
	root: BitmapShape,
	decode: Option<DecodeFields>,
	fallback: bool
});
text_struct!(IndexLookupAttrs {
	table: TableName,
	read: ReadClauses,
	index: IndexRef,
	field: Strand,
	key: LookupKeyShape
});
text_struct!(FullTextScanAttrs {
	table: TableName,
	read: ReadClauses,
	index: IndexRef,
	query: String,
	operator: MatchesOperator,
	column: Option<Idiom>,
	decode: Option<DecodeFields>,
	topk: Option<TopKKey>
});
text_struct!(ReadClauses { schema: Option<Arc<Schema>>, with: Option<With> });
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
text_struct!(KnnPermissionShape {
	auth: Option<AuthLimit>,
	guard: GuardKind,
	computed: Option<ComputedReadsShape>
});
text_struct!(KnnPrefilterShape {
	root: BitmapShape,
	residual: bool,
	uncovered_matches: bool
});
text_struct!(KnnFilterShape {
	permissions: Vec<KnnPermissionShape>,
	residual: bool,
	matches_condition: bool,
	prefilter: Option<KnnPrefilterShape>
});
text_struct!(KnnScanAttrs {
	table: TableName,
	read: ReadClauses,
	index: IndexRef,
	vector: Arc<[Number]>,
	vector_text: Option<String>,
	k: u32,
	ef: u32,
	filter: Option<KnnFilterShape>,
	ids_only: bool
});
text_struct!(KnnTopKAttrs {
	field: FieldPath,
	vector: KnnTargetShape,
	k: u32,
	distance: Distance
});
text_struct!(CountAttrs {
	shape: CountShape,
	select: CountSelect,
	deltas: Option<IndexId>,
	cap: Option<CountCap>
});
text_struct!(CountCap {
	limit: u64,
	start: u64
});
text_struct!(JoinAttrs {
	matching: JoinMatch,
	build: JoinBuild,
	spill: Spill,
	alias: String,
	keys: u16,
	residual: bool,
	lookup: Option<u16>
});
text_struct!(PermissionFilterAttrs {
	guard: GuardKind,
	hides_access_paths: bool,
	computed: Option<ComputedReadsShape>
});
text_struct!(SortKeySpecShape {
	read: SortKeyReadShape,
	ascending: bool,
	collate: bool,
	numeric: bool
});
text_struct!(PhysSortAttrs {
	keys: Vec<SortKeySpecShape>,
	keep: Option<usize>,
	pushdown: bool,
	random: bool,
	spill: Spill,
	tempfiles: bool
});
text_struct!(LayerAttrs {
	fields: FieldsShape,
	names: Vec<OutputPath>,
	computed: Vec<Strand>,
	judged: Option<ComputedReadsShape>
});
text_struct!(AggregateAttrs {
	groups: u16,
	fields: FieldsShape,
	names: Vec<OutputPath>,
	first_value: Vec<usize>,
	spill: Spill
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
text_struct!(ProjectedFieldShape { output: FieldPath, aliased: bool, sort_key: Option<u16> });
text_struct!(PhysWriteAttrs {
	write: WriteAttrs,
	subjects: WriteSubjectsShape
});

// ---------------------------------------------------------------------------------------------
// Attributes with a bespoke form
// ---------------------------------------------------------------------------------------------

/// `Text` for a type the syntax cannot carry: it prints `unsupported(Type)` and reading it fails
/// naming the type.
macro_rules! text_unsupported {
	($($ty:ty => $name:literal),+ $(,)?) => {
		$(
			impl Text for $ty {
				fn put(&self, out: &mut String) {
					out.push_str(concat!("unsupported(", $name, ")"));
				}

				fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
					c.no_text($name)
				}
			}
		)+
	};
}

text_unsupported! {
	Arc<Schema> => "Schema",
	DefineAttrs => "DefineAttrs",
	AlterAttrs => "AlterAttrs",
	LiveAttrs => "LiveAttrs",
	DecodeFields => "DecodeFields",
}

impl Text for PipelineId {
	fn put(&self, out: &mut String) {
		self.0.put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		<u32 as Text>::get(c).map(PipelineId)
	}
}

/// An index id prints as its number.
impl Text for IndexId {
	fn put(&self, out: &mut String) {
		self.0.put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		<u32 as Text>::get(c).map(IndexId)
	}
}

/// An op id prints through the core text: inside the container printer as the `@<n>` or `@?<k>`
/// name provenance gives the same id, and anywhere else as the bits of its key.
impl Text for OpId {
	fn put(&self, out: &mut String) {
		put_attr_op_id(*self, out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let token = c.token()?;
		get_attr_op_id(token).ok_or_else(|| c.err(format!("`{token}` is not an op id")))
	}
}

/// A field path prints as its parts.
impl Text for FieldPath {
	fn put(&self, out: &mut String) {
		self.0.put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		<Vec<FieldPathPart> as Text>::get(c).map(FieldPath)
	}
}

/// The structure of the keys an access was planned against. The structure of an `id` field prints
/// as the names and declared types of its components, and is rebuilt from them as the catalog
/// builds it. The structure of an aggregated view is derived from the view's definition, which the
/// syntax does not carry: it prints `unsupported(KeyStructure)` and is not read back.
impl Text for Arc<KeyStructure> {
	fn put(&self, out: &mut String) {
		let declared: Option<Vec<Kind>> =
			self.components.iter().map(|component| component.declared.clone()).collect();
		let (Some(names), Some(kinds)) = (&self.names, declared) else {
			out.push_str("unsupported(KeyStructure)");
			return;
		};
		if self.origin != KeyOrigin::IdField {
			out.push_str("unsupported(KeyStructure)");
			return;
		}
		out.push_str("{ ");
		let mut w = Fields::new(&mut *out);
		let names: Vec<String> = names.as_slice().iter().map(|n| n.as_str().to_owned()).collect();
		w.field("names", &names);
		w.field("kinds", &kinds);
		out.push_str(" }");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		if c.peek() != Some('{') {
			return c.no_text("KeyStructure");
		}
		c.expect('{')?;
		let first = &mut true;
		let names = c.field::<Vec<String>>("names", first)?;
		let kinds = c.field::<Vec<Kind>>("kinds", first)?;
		c.expect('}')?;
		let names = match KeyNames::new(names.into_iter().map(Strand::from).collect(), kinds.len())
		{
			Ok(names) => names,
			Err(e) => return c.fail(format!("the key names are not valid: {e}")),
		};
		match KeyStructure::from_stored_id_field(&Kind::Literal(KindLiteral::Array(kinds)), &names)
		{
			Some(key) => Ok(Arc::new(key)),
			None => c.fail("the kinds are not a tuple with one element per name"),
		}
	}
}

/// A top-k key prints its path as field names. Its coarse cutoff is the cut of a key-range scan.
impl Text for TopKKey {
	fn put(&self, out: &mut String) {
		out.push_str("{ ");
		let mut w = Fields::new(&mut *out);
		let names: Vec<String> = self.path.iter().map(|part| part.as_str().to_owned()).collect();
		w.field("path", &names);
		w.field("direction", &self.direction);
		w.field("single_key", &self.single_key);
		w.field("depth_limit", &self.depth_limit);
		w.field("packed_rows", &self.packed_rows);
		w.field("coarse_cutoff", &self.coarse_cutoff);
		w.field("rejects_entries", &self.rejects_entries);
		out.push_str(" }");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.expect('{')?;
		let first = &mut true;
		let names = c.field::<Vec<String>>("path", first)?;
		let direction = c.field("direction", first)?;
		let single_key = c.field("single_key", first)?;
		let depth_limit = c.field("depth_limit", first)?;
		let packed_rows = c.field("packed_rows", first)?;
		let coarse_cutoff = c.field("coarse_cutoff", first)?;
		let rejects_entries = c.field("rejects_entries", first)?;
		c.expect('}')?;
		let path: Arc<[PathSegment]> = names.into_iter().map(PathSegment::from).collect();
		Ok(TopKKey {
			path,
			direction,
			single_key,
			depth_limit,
			packed_rows,
			coarse_cutoff,
			rejects_entries,
		})
	}
}

/// A stored value is read through [`BTreeAccess`], whose values are plain.
impl Text for ValueData<BTreeAccess> {
	fn put(&self, out: &mut String) {
		(**self).put(out);
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let access = <BTreeAccess as Text>::get(c)?;
		match ValueData::new(access, "a text-form access") {
			Ok(data) => Ok(data),
			Err(e) => c.fail(format!("the access holds a value no program holds: {}", e.reason)),
		}
	}
}

impl Text for Number {
	fn put(&self, out: &mut String) {
		match self {
			Number::Int(n) => {
				out.push_str("Int(");
				n.put(out);
				out.push(')');
			}
			Number::Float(n) => {
				out.push_str("Float(");
				n.put(out);
				out.push(')');
			}
			Number::Decimal(n) => {
				out.push_str("Decimal(");
				put_str(out, &n.to_string());
				out.push(')');
			}
			Number::BigDecimal(n) => {
				out.push_str("BigDecimal(");
				put_str(out, &n.to_string());
				out.push(')');
			}
			Number::I8(n) => {
				out.push_str("I8(");
				n.put(out);
				out.push(')');
			}
			Number::U8(n) => {
				out.push_str("U8(");
				n.put(out);
				out.push(')');
			}
			Number::I16(n) => {
				out.push_str("I16(");
				n.put(out);
				out.push(')');
			}
			Number::U16(n) => {
				out.push_str("U16(");
				n.put(out);
				out.push(')');
			}
			Number::I32(n) => {
				out.push_str("I32(");
				n.put(out);
				out.push(')');
			}
			Number::U32(n) => {
				out.push_str("U32(");
				n.put(out);
				out.push(')');
			}
			Number::U64(n) => {
				out.push_str("U64(");
				n.put(out);
				out.push(')');
			}
			Number::F16(n) => {
				out.push_str("F16(");
				n.put(out);
				out.push(')');
			}
			Number::F32(n) => {
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
			"Int" => Number::Int(<i64 as Text>::get(c)?),
			"Float" => Number::Float(<f64 as Text>::get(c)?),
			"Decimal" => {
				let text = c.string()?;
				match text.parse() {
					Ok(n) => Number::Decimal(n),
					Err(_) => return c.fail(format!("`{text}` is not a decimal")),
				}
			}
			"BigDecimal" => {
				let text = c.string()?;
				match text.parse::<surrealdb_types::BigDecimal>() {
					Ok(n) => Number::BigDecimal(Box::new(n)),
					Err(_) => return c.fail(format!("`{text}` is not a bigdecimal")),
				}
			}
			"I8" => Number::I8(<i8 as Text>::get(c)?),
			"U8" => Number::U8(<u8 as Text>::get(c)?),
			"I16" => Number::I16(<i16 as Text>::get(c)?),
			"U16" => Number::U16(<u16 as Text>::get(c)?),
			"I32" => Number::I32(<i32 as Text>::get(c)?),
			"U32" => Number::U32(<u32 as Text>::get(c)?),
			"U64" => Number::U64(<u64 as Text>::get(c)?),
			"F16" => Number::F16(<surrealdb_types::f16 as Text>::get(c)?),
			"F32" => Number::F32(<f32 as Text>::get(c)?),
			other => return c.unknown("Number", other),
		};
		c.expect(')')?;
		Ok(number)
	}
}

/// The operators the text of a plan names: the four comparisons a compound range ends in, and the
/// others a pre-decode leaf holds. Any other prints `unsupported(BinaryOperator)` and is not read
/// back.
const NAMED_BINARY: [(BinaryOperator, &str); 17] = [
	(BinaryOperator::Equal, "Equal"),
	(BinaryOperator::ExactEqual, "ExactEqual"),
	(BinaryOperator::NotEqual, "NotEqual"),
	(BinaryOperator::LessThan, "LessThan"),
	(BinaryOperator::LessThanEqual, "LessThanEqual"),
	(BinaryOperator::MoreThan, "MoreThan"),
	(BinaryOperator::MoreThanEqual, "MoreThanEqual"),
	(BinaryOperator::Contain, "Contain"),
	(BinaryOperator::NotContain, "NotContain"),
	(BinaryOperator::ContainAll, "ContainAll"),
	(BinaryOperator::ContainAny, "ContainAny"),
	(BinaryOperator::ContainNone, "ContainNone"),
	(BinaryOperator::Inside, "Inside"),
	(BinaryOperator::NotInside, "NotInside"),
	(BinaryOperator::AllInside, "AllInside"),
	(BinaryOperator::AnyInside, "AnyInside"),
	(BinaryOperator::NoneInside, "NoneInside"),
];

impl Text for BinaryOperator {
	fn put(&self, out: &mut String) {
		let name = NAMED_BINARY.iter().find_map(|(op, name)| (op == self).then_some(*name));
		out.push_str(name.unwrap_or("unsupported(BinaryOperator)"));
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let name = c.ident()?;
		match NAMED_BINARY.iter().find(|(_, named)| *named == name) {
			Some((op, _)) => Ok(op.clone()),
			None => c.no_text("BinaryOperator"),
		}
	}
}

/// A path segment prints as its name, a quoted string.
impl Text for PathSegment {
	fn put(&self, out: &mut String) {
		put_str(out, self.as_str());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		Ok(PathSegment::from(c.string()?.as_str()))
	}
}

text_enum!(PreDecodeShape {
	unit []
	tuple [
		And(parts: Vec<PreDecodeShape>),
		Or(parts: Vec<PreDecodeShape>),
		Not(inner: Box<PreDecodeShape>),
		Selective(inner: Box<PreDecodeShape>),
	]
	named [Leaf { path: Arc<[PathSegment]>, op: BinaryOperator, literal: Value, reversed: bool }]
});

text_enum!(BoundValue {
	unit [Unbounded]
	tuple [Included(value: Value), Excluded(value: Value)]
	named []
});

/// A bound operand prints as its value, a quoted SurrealQL text, or as the bare call that reads
/// the statement clock: `time::now()`, `time::now() + <duration>` or `time::now() - <duration>`.
/// The two never begin alike, so the text tells them apart by its first character.
impl Text for BoundOperand {
	fn put(&self, out: &mut String) {
		match self {
			BoundOperand::Literal(value) => value.put(out),
			clock @ BoundOperand::StatementClock(_) => out.push_str(&clock.describe()),
		}
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		if c.peek() == Some('"') {
			return Ok(BoundOperand::Literal(<Value as Text>::get(c)?));
		}
		const CALL: &str = "time::now()";
		if !c.text[c.at..].starts_with(CALL) {
			return c.fail("expected a quoted value or `time::now()`");
		}
		c.at += CALL.len();
		let sign = match c.peek() {
			Some('+') => ClockSign::Plus,
			Some('-') => ClockSign::Minus,
			_ => {
				return Ok(BoundOperand::StatementClock(ClockBound {
					offset: None,
				}));
			}
		};
		c.at += 1;
		let token = c.token()?;
		match crate::syn::duration(token) {
			Ok(by) => Ok(BoundOperand::StatementClock(ClockBound {
				offset: Some((sign, crate::val::Duration::from(by))),
			})),
			Err(e) => c.fail(format!("`{token}` is not a duration: {e}")),
		}
	}
}

text_enum!(OperandBound {
	unit [Unbounded]
	tuple [Included(value: BoundOperand), Excluded(value: BoundOperand)]
	named []
});

/// A bound of an operand range, which prints as a variant of its own.
enum OperandBound {
	Unbounded,
	Included(BoundOperand),
	Excluded(BoundOperand),
}

/// The bounds of an operand range print as `{ start: Bound, end: Bound }`, as a range of values
/// does; a range that carries a `SpatialPointFilter` prints `unsupported(SpatialPointFilter)` and
/// is not read back.
impl Text for OperandRange {
	fn put(&self, out: &mut String) {
		if self.point_filter.is_some() {
			out.push_str("unsupported(SpatialPointFilter)");
			return;
		}
		let of = |bound: &Bound<BoundOperand>| match bound {
			Bound::Unbounded => OperandBound::Unbounded,
			Bound::Included(operand) => OperandBound::Included(operand.clone()),
			Bound::Excluded(operand) => OperandBound::Excluded(operand.clone()),
		};
		out.push_str("{ ");
		let mut w = Fields::new(&mut *out);
		w.field("start", &of(&self.start));
		w.field("end", &of(&self.end));
		out.push_str(" }");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		if c.peek() != Some('{') {
			return c.no_text("SpatialPointFilter");
		}
		let into = |bound: OperandBound| match bound {
			OperandBound::Unbounded => Bound::Unbounded,
			OperandBound::Included(operand) => Bound::Included(operand),
			OperandBound::Excluded(operand) => Bound::Excluded(operand),
		};
		c.expect('{')?;
		let first = &mut true;
		let start = c.field::<OperandBound>("start", first)?;
		let end = c.field::<OperandBound>("end", first)?;
		c.expect('}')?;
		Ok(OperandRange {
			start: into(start),
			end: into(end),
			point_filter: None,
		})
	}
}

/// A range prints as `{ start: Bound, end: Bound }`.
impl Text for Range {
	fn put(&self, out: &mut String) {
		out.push_str("{ ");
		let mut w = Fields::new(&mut *out);
		w.field("start", &BoundValue::of(&self.start));
		w.field("end", &BoundValue::of(&self.end));
		out.push_str(" }");
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		c.expect('{')?;
		let first = &mut true;
		let start = c.field::<BoundValue>("start", first)?;
		let end = c.field::<BoundValue>("end", first)?;
		c.expect('}')?;
		Ok(Range {
			start: start.into_bound(),
			end: end.into_bound(),
		})
	}
}

/// A bound of a range, which prints as a variant of its own.
enum BoundValue {
	Unbounded,
	Included(Value),
	Excluded(Value),
}

impl BoundValue {
	fn of(bound: &Bound<Value>) -> Self {
		match bound {
			Bound::Unbounded => BoundValue::Unbounded,
			Bound::Included(value) => BoundValue::Included(value.clone()),
			Bound::Excluded(value) => BoundValue::Excluded(value.clone()),
		}
	}

	fn into_bound(self) -> Bound<Value> {
		match self {
			BoundValue::Unbounded => Bound::Unbounded,
			BoundValue::Included(value) => Bound::Included(value),
			BoundValue::Excluded(value) => Bound::Excluded(value),
		}
	}
}

/// A record id prints as the SurrealQL text of the record id value.
impl Text for RecordId {
	fn put(&self, out: &mut String) {
		put_str(out, &Value::RecordId(self.clone()).to_sql());
	}

	fn get(c: &mut Cursor<'_>) -> Result<Self, ParseError> {
		let text = c.string()?;
		match crate::syn::value(&text).map(Value::from) {
			Ok(Value::RecordId(id)) => Ok(id),
			Ok(_) => c.fail(format!("`{text}` is not a record id")),
			Err(e) => c.fail(format!("`{text}` is not a record id: {e}")),
		}
	}
}

#[cfg(test)]
mod tests {
	use slotmap::SlotMap;
	use smallvec::smallvec;

	use super::*;
	use crate::exec::ir::iface::statements::{
		AlterObject, DefineObject, DefinitionTemplate, LiveShape, PermissionShape, RemoveObject,
	};
	use crate::exec::ir::logical::ops::WriteStageKind;
	use crate::exec::ir::phys::ops::key_range_fixtures;
	use crate::expr::statements::define::DefineKind;
	use crate::val::Uuid;

	fn strand(text: &str) -> Strand {
		Strand::new(text)
	}

	fn table(text: &str) -> TableName {
		TableName::new(strand(text))
	}

	/// An access over `[city: string, time: datetime]` that pins `city`, bounds `time` below by an
	/// operand and above by a literal, and orders by both components.
	fn key_range_shape() -> KeyRangeShape {
		KeyRangeShape {
			key: key_range_fixtures::reading(),
			eq: vec![KeyEqShape::One(KeyBoundShape::Literal(Value::from("a \"quoted\" city")))],
			range: Some(KeyRangeOnShape {
				position: 1,
				low: Some(KeyRangeBoundShape {
					bound: KeyBoundShape::Operand(0),
					inclusive: true,
				}),
				high: Some(KeyRangeBoundShape {
					bound: KeyBoundShape::Literal(Value::from(9i64)),
					inclusive: false,
				}),
			}),
			direction: Direction::Backward,
			delivers: DeliveredKeyOrder::Coarse {
				position: 1,
			},
			ordered_by: vec![0, 1],
		}
	}

	fn index_ref() -> IndexRef {
		IndexRef {
			table: table("t"),
			index: strand("ix"),
			position: 0,
			scalar: Box::default(),
		}
	}

	fn field(name: &str) -> FieldPath {
		FieldPath::field(name)
	}

	fn template() -> DefinitionTemplate {
		DefinitionTemplate {
			ty: "StoredParamDefinition",
			value: Value::None,
			holes: SmallVec::new(),
		}
	}

	fn write_attrs() -> WriteAttrs {
		WriteAttrs {
			form: WriteForm::Create,
			only: false,
			output: OutputShape::After,
			with: None,
			explain: None,
			flags: WriteFlags::default(),
			body: crate::exec::ir::logical::ops::WriteBody::default(),
			gates: crate::exec::ir::logical::ops::WriteGates::default(),
			image: crate::exec::ir::logical::ops::WriteImage::default(),
			stored: Default::default(),
			derived: Default::default(),
			into_text: None,
			statement: None,
			linked: None,
		}
	}

	fn matches_operator() -> MatchesOperator {
		MatchesOperator {
			rf: Some(1),
			operator: BooleanOperator::Or,
		}
	}

	fn choose_spec() -> ChooseSpec {
		ChooseSpec {
			granularity: ChooseGranularity::Batch,
			arms: smallvec![
				ArmSpec {
					guard: Guard::IndexReady(index_ref()),
					body: ArmBody::PreLowered,
					label: ArmLabel("index"),
				},
				ArmSpec {
					guard: Guard::TypeHolds(Kind::Int),
					body: ArmBody::UnitLoad(UnitKind::Function),
					label: ArmLabel("typed"),
				},
				ArmSpec {
					guard: Guard::Otherwise,
					body: ArmBody::PreLowered,
					label: ArmLabel("table scan"),
				},
			],
			generic: GenericArm::Present,
			equivalence: ArmEquivalence::OrderedByTotalKey,
			origin: ChooseOrigin::PhysicalGuarded,
		}
	}

	/// An op of `tag` with attributes that exercise the tag's text form: optional parts present,
	/// sequences non-empty, strings that need escapes.
	/// A shape that holds every form: a selective conjunction of a comparison, a reversed
	/// comparison with a string literal, and the negation of a disjunction.
	fn prefilter_shape() -> PreDecodeShape {
		let leaf = |path: &[&str], op, literal: Value, reversed| PreDecodeShape::Leaf {
			path: path.iter().map(|name| PathSegment::from(*name)).collect(),
			op,
			literal,
			reversed,
		};
		PreDecodeShape::Selective(Box::new(PreDecodeShape::And(vec![
			leaf(&["age"], BinaryOperator::MoreThanEqual, Value::from(18i64), false),
			leaf(&["name", "first"], BinaryOperator::Equal, Value::from("a \"b\""), true),
			PreDecodeShape::Not(Box::new(PreDecodeShape::Or(vec![
				leaf(&["tags"], BinaryOperator::Contain, Value::from("x"), false),
				leaf(&["n"], BinaryOperator::NotEqual, Value::from(0i64), false),
			]))),
		])))
	}

	fn fixture(tag: PhysTag) -> PhysOp {
		match tag {
			PhysTag::TableScan => PhysOp::TableScan(Box::new(TableScanAttrs {
				probes: Some(Box::new(ScanProbes {
					probes: vec![
						(
							index_ref(),
							BTreeAccess::Equality(BoundOperand::Literal(Value::from(1i64))),
						),
						(
							index_ref(),
							BTreeAccess::Range {
								range: OperandRange {
									start: Bound::Excluded(BoundOperand::Literal(Value::from(
										2i64,
									))),
									end: Bound::Unbounded,
									point_filter: None,
								},
							},
						),
					],
					bound: ProbeBound::All(vec![
						ProbeBound::Probe(0),
						ProbeBound::Any(vec![ProbeBound::Probe(1), ProbeBound::Probe(0)]),
					]),
				})),
				bounds: Some(IdBoundsShape {
					low: Some(KeyRangeBoundShape {
						bound: KeyBoundShape::Operand(0),
						inclusive: false,
					}),
					high: Some(KeyRangeBoundShape {
						bound: KeyBoundShape::Literal(Value::from("a \"quoted\" key")),
						inclusive: true,
					}),
				}),
				table: table("a \"quoted\" table"),
				direction: Direction::Backward,
				topk: Some(TopKKey {
					path: Arc::from([PathSegment::from("name"), PathSegment::from("first")]),
					direction: SortDirection::Desc,
					single_key: true,
					depth_limit: 4,
					packed_rows: true,
					coarse_cutoff: None,
					rejects_entries: true,
				}),
				lightweight: true,
				id_order: true,
				read: ReadClauses::default(),
				prefilter: Some(prefilter_shape()),
			})),
			PhysTag::IndexScan => PhysOp::IndexScan(Box::new(IndexScanAttrs {
				table: table("t"),
				index: index_ref(),
				access: BTreeAccess::Compound {
					prefix: vec![Value::from(1i64).into(), Value::from("x").into()],
					range: Some((BinaryOperator::MoreThanEqual, Value::from(3i64).into())),
					upper: None,
				},
				direction: Direction::Forward,
				ordered_by: Some(vec![field("a"), FieldPath(vec![FieldPathPart::Index(2)])]),
				pinned: Some(vec![field("a"), field("b"), field("c")]),
				stated: None,
				rechecked: false,
				decode: None,
				read: ReadClauses::default(),
			})),
			PhysTag::IndexUnion => PhysOp::IndexUnion(Box::new(IndexUnionAttrs {
				topk: None,
				table: table("t"),
				read: ReadClauses::default(),
				branches: vec![
					IndexBranchShape {
						index: index_ref(),
						read: BranchReadShape::BTree(
							ValueData::new(
								BTreeAccess::literal_equality(Value::from(1i64)),
								"a branch",
							)
							.expect("the access holds no closure"),
						),
						direction: Direction::Forward,
					},
					IndexBranchShape {
						index: index_ref(),
						read: BranchReadShape::FullText {
							query: "needle".to_owned(),
							operator: matches_operator(),
							column: None,
							shared_doc_ids: true,
						},
						direction: Direction::Backward,
					},
				],
				merge: Some(MergeMode::ByIndexKey {
					path: field("a"),
					direction: SortDirection::Asc,
				}),
				ordered_by: None,
				decode: None,
			})),
			PhysTag::Bitmap => PhysOp::Bitmap(Box::new(BitmapAttrs {
				table: table("t"),
				root: BitmapShape::AndNot {
					base: Box::new(BitmapShape::Or(vec![
						BitmapShape::BTree {
							index: index_ref(),
							access: BTreeAccess::literal_equality(Value::from(1i64)),
						},
						BitmapShape::FullText {
							index: index_ref(),
							query: "q".to_owned(),
							operator: matches_operator(),
						},
					])),
					subtract: Box::new(BitmapShape::And(Vec::new())),
				},
				decode: None,
				fallback: true,
				read: ReadClauses::default(),
			})),
			PhysTag::IndexLookup => PhysOp::IndexLookup(Box::new(IndexLookupAttrs {
				table: table("t"),
				index: index_ref(),
				field: strand("a"),
				key: LookupKeyShape::Probe,
				read: ReadClauses::default(),
			})),
			PhysTag::FullTextScan => PhysOp::FullTextScan(Box::new(FullTextScanAttrs {
				table: table("t"),
				index: index_ref(),
				query: "line one\nline two".to_owned(),
				operator: matches_operator(),
				column: None,
				decode: None,
				read: ReadClauses::default(),
				topk: Some(TopKKey {
					path: Arc::from([]),
					direction: SortDirection::Desc,
					single_key: true,
					depth_limit: 0,
					packed_rows: false,
					coarse_cutoff: None,
					rejects_entries: true,
				}),
			})),
			PhysTag::KeyRangeScan => PhysOp::KeyRangeScan(Box::new(KeyRangeScanAttrs {
				table: table("t"),
				read: ReadClauses::default(),
				access: key_range_shape(),
				topk: Some(TopKKey {
					path: Arc::from([PathSegment::from("time")]),
					direction: SortDirection::Asc,
					single_key: true,
					depth_limit: 4,
					packed_rows: false,
					coarse_cutoff: Some(CoarseCutoff {
						position: 1,
						keep: 3,
					}),
					rejects_entries: true,
				}),
				prefilter: None,
			})),
			PhysTag::KnnScan => PhysOp::KnnScan(Box::new(KnnScanAttrs {
				table: table("t"),
				index: index_ref(),
				vector: vec![Number::Int(1), Number::Float(-0.5), Number::Float(f64::INFINITY)]
					.into(),
				vector_text: None,
				k: 10,
				ef: 40,
				filter: Some(KnnFilterShape {
					permissions: vec![KnnPermissionShape {
						auth: Some(AuthLimit::new(
							Level::Database("ns".to_owned(), "db".to_owned()),
							Some(Role::Editor),
						)),
						guard: GuardKind::Read,
						computed: Some(ComputedReadsShape {
							table: table("t"),
							fields: Some(vec!["a".to_owned()]),
							derived: DerivedReads::LateBound,
						}),
					}],
					residual: true,
					matches_condition: false,
					prefilter: Some(KnnPrefilterShape {
						root: BitmapShape::And(Vec::new()),
						residual: false,
						uncovered_matches: true,
					}),
				}),
				ids_only: false,
				read: ReadClauses::default(),
			})),
			PhysTag::KnnTopK => PhysOp::KnnTopK(Box::new(KnnTopKAttrs {
				field: FieldPath(vec![
					FieldPathPart::Field("v".to_owned()),
					FieldPathPart::Index(1),
				]),
				vector: KnnTargetShape::Literal(vec![Number::Int(2)]),
				k: 3,
				distance: Distance::Minkowski(Number::Int(3)),
			})),
			PhysTag::Count => PhysOp::Count(Box::new(CountAttrs {
				shape: CountShape::Indexed {
					table: table("t"),
					output: CountOutput::Fields(vec!["count".to_owned(), "c".to_owned()]),
					btree: Some(Box::new([
						(
							index_ref(),
							BTreeAccess::literal_range(Range {
								start: Bound::Included(Value::from(1i64)),
								end: Bound::Unbounded,
							}),
						),
						(index_ref(), BTreeAccess::literal_equality(Value::from(0i64))),
					])),
					bitmap: None,
				},
				select: CountSelect::Conditional,
				deltas: Some(IndexId(7)),
				cap: Some(CountCap {
					limit: 1000,
					start: 2,
				}),
			})),
			PhysTag::RecordScan => PhysOp::RecordScan(Box::new(RecordScanAttrs {
				table: TableName::new(Strand::new("t")),
				for_update: true,
				lightweight: false,
				read: ReadClauses {
					schema: None,
					with: Some(With::Index(vec!["ix".to_owned()])),
				},
			})),
			PhysTag::Vacant => PhysOp::Vacant {
				table: table("t"),
			},
			PhysTag::Enforced => PhysOp::Enforced {
				table: table("t"),
			},
			PhysTag::Values => PhysOp::Values {
				read: ValuesRead::Expression {
					written: true,
				},
			},
			PhysTag::Subquery => PhysOp::Subquery,
			PhysTag::Probe => PhysOp::Probe {
				field: strand("a"),
			},
			PhysTag::Union => PhysOp::Union,
			PhysTag::Join => PhysOp::Join(Box::new(JoinAttrs {
				matching: JoinMatch::Semi,
				build: JoinBuild::Left,
				spill: Spill::Spillable,
				alias: "_join0".to_owned(),
				keys: 2,
				residual: true,
				lookup: Some(1),
			})),
			PhysTag::Distinct => PhysOp::Distinct {
				spill: Spill::Spillable,
			},
			PhysTag::PermissionFilter => {
				PhysOp::PermissionFilter(Box::new(PermissionFilterAttrs {
					guard: GuardKind::Write,
					hides_access_paths: true,
					computed: None,
				}))
			}
			PhysTag::Layer => PhysOp::Layer(Box::new(LayerAttrs {
				fields: FieldsShape::Select(Box::new([
					FieldShape::All,
					FieldShape::Single {
						alias: Some(OutputPath {
							parts: vec![OutputPathPart::Field("a".to_owned())],
							exact: true,
						}),
						kind: Some(Kind::Int),
					},
					FieldShape::Single {
						alias: None,
						kind: None,
					},
				])),
				names: vec![OutputPath {
					parts: vec![OutputPathPart::Field("b".to_owned())],
					exact: true,
				}],
				computed: Vec::new(),
				judged: None,
			})),
			PhysTag::Filter => PhysOp::Filter,
			PhysTag::Fetch => PhysOp::Fetch,
			PhysTag::Split => PhysOp::Split {
				targets: Box::new([SplitTarget::At(FieldPath(vec![FieldPathPart::Field(
					"a".into(),
				)]))]),
			},
			PhysTag::Sort => PhysOp::Sort(Box::new(PhysSortAttrs {
				keys: vec![
					SortKeySpecShape {
						read: SortKeyReadShape::Path(field("a")),
						ascending: false,
						collate: true,
						numeric: false,
					},
					SortKeySpecShape {
						read: SortKeyReadShape::Computed,
						ascending: true,
						collate: false,
						numeric: true,
					},
				],
				keep: Some(10),
				pushdown: true,
				random: false,
				spill: Spill::Bounded,
				tempfiles: true,
			})),
			PhysTag::Aggregate => PhysOp::Aggregate(Box::new(AggregateAttrs {
				groups: 2,
				names: Vec::new(),
				fields: FieldsShape::Value {
					alias: None,
				},
				first_value: vec![0, 2],
				spill: Spill::Spillable,
			})),
			PhysTag::Window => PhysOp::Window(Box::new(WindowShape {
				partitions: 1,
				order: Box::new([SortKeyShape {
					collate: false,
					numeric: false,
					ascending: true,
					path: false,
				}]),
				calls: Box::new([WindowCallShape {
					output: strand("w"),
					function: WindowFunction::Lag,
					args: 1,
					frame: WindowFrame {
						units: WindowFrameUnits::Rows,
						start: WindowFrameBound::Preceding(2),
						end: WindowFrameBound::Following(1),
					},
				}]),
			})),
			PhysTag::Limit => PhysOp::Limit {
				limit: Some(0),
				start: Some(1),
			},
			PhysTag::Project => PhysOp::Project(Box::new(ProjectionShape::Fields {
				fields: vec![ProjectedFieldShape {
					output: field("a"),
					aliased: true,
					sort_key: Some(0),
				}],
				omit: vec![
					OmissionShape::Path(field("secret")),
					OmissionShape::Walk {
						steps: vec![
							WalkStepShape::Field("a".to_owned()),
							WalkStepShape::Destructure(Box::new([
								DestructureEntry::Field(strand("b")),
								DestructureEntry::Rest,
							])),
						],
						action: WalkActionShape::RemoveIndex(1),
					},
				],
			})),
			PhysTag::Timeout => PhysOp::Timeout,
			PhysTag::VersionScope => PhysOp::VersionScope,
			PhysTag::Only => PhysOp::Only,
			PhysTag::Explain => PhysOp::Explain {
				analyze: true,
				format: ExplainFormat::Json,
			},
			PhysTag::Eval => PhysOp::Eval,
			PhysTag::Request => PhysOp::Request {
				fragment: Some(3),
				nested: true,
			},
			PhysTag::Txn => PhysOp::Txn {
				mode: TxnRegionMode::Explicit {
					start: TxnStart::Begin,
					end: TxnRegionEnd::Commit,
					emission: Emission::Hold,
				},
			},
			PhysTag::Stmt => PhysOp::Stmt {
				commit: StatementCommit::InTxn,
				control: Some(TxnControl::Cancel),
			},
			PhysTag::Statement => PhysOp::Statement {
				changes_catalog: true,
				served: Some(3),
				within: CatalogWithin {
					calls_current: true,
					nested_late: false,
				},
				raises_when_reached: true,
			},
			PhysTag::Match => PhysOp::Match {
				bindings: MatchBindings(Box::new([BindingDef {
					name: "n".to_owned(),
					kind: BindingKind::Node,
					user_named: true,
				}])),
				stages: 2,
			},
			PhysTag::MatchStep => PhysOp::MatchStep(Box::new(MatchStepAttrs::Read {
				optional_group: Some(1),
				patterns: Box::new([MatchPattern {
					path_var: None,
					search: None,
					start: NodeStep {
						binding: 0,
						label: Some(table("person")),
					},
					steps: Box::new([]),
				}]),
				predicates: 2,
			})),
			PhysTag::DdlGuard => PhysOp::DdlGuard {
				guard: DdlGuard::Allow {
					action: IamAction::Edit,
					resource: ResourceKind::Table,
					base: Base::Db,
				},
			},
			PhysTag::Define => PhysOp::Define(Box::new(DefineAttrs {
				object: DefineObject::Param {
					name: strand("p"),
					value: 0,
					comment: 1,
					permissions: PermissionShape::Full,
				},
				mode: DefineKind::Default,
				template: template(),
				checks: DdlChecks::None,
			})),
			PhysTag::Remove => PhysOp::Remove(Box::new(RemoveAttrs {
				object: RemoveObject::Param {
					name: strand("p"),
				},
				if_exists: true,
				checks: DdlChecks::None,
				expunge: false,
			})),
			PhysTag::Alter => PhysOp::Alter(Box::new(AlterAttrs {
				object: AlterObject::Namespace {
					compact: false,
				},
				if_exists: false,
				checks: DdlChecks::None,
				template: None,
			})),
			PhysTag::Rebuild => PhysOp::Rebuild(Box::new(RebuildAttrs {
				index: strand("ix"),
				table: table("t"),
				if_exists: false,
				concurrently: true,
			})),
			PhysTag::Info => PhysOp::Info {
				subject: InfoSubject::User {
					base: Some(Base::Root),
				},
				structure: true,
			},
			PhysTag::Use => PhysOp::Use {
				target: UseTarget::NsDb,
			},
			PhysTag::Option => PhysOp::Option {
				name: strand("IMPORT"),
				value: true,
			},
			PhysTag::Access => PhysOp::Access(Box::new(AccessAttrs {
				ac: strand("a"),
				base: Some(Base::Db),
				op: AccessOp::Show {
					gr: Some(strand("grant")),
				},
			})),
			PhysTag::Kill => PhysOp::Kill,
			PhysTag::Live => PhysOp::Live(Box::new(LiveAttrs {
				id: Uuid::nil(),
				node: Uuid::nil(),
				fields: LiveShape::Select,
				template: template(),
			})),
			PhysTag::Subscriber => PhysOp::Subscriber(Box::new(SubscriberAttrs {
				checked: true,
				admission: SubscriberAdmission::All,
				view: ImageView::Derived,
				layers: vec![
					SubscriberLayer {
						fields: FieldsShape::Select(Box::new([
							FieldShape::All,
							FieldShape::Single {
								alias: None,
								kind: None,
							},
						])),
						computed: vec![strand("c"), strand("d")],
						judged: None,
					},
					SubscriberLayer {
						fields: FieldsShape::Select(Box::new([FieldShape::All])),
						computed: Vec::new(),
						judged: None,
					},
				],
				projection: SubscriberProjection::Subject,
				cond: false,
				fetch: 0,
			})),
			PhysTag::Show => PhysOp::Show(Box::new(ShowAttrs {
				table: Some(table("t")),
				since: ShowSince::Versionstamp(9),
				limit: Some(5),
			})),
			PhysTag::Write => PhysOp::Write(Box::new(PhysWriteAttrs {
				write: write_attrs(),
				subjects: WriteSubjectsShape::Guaranteed,
			})),
			PhysTag::WriteStage => PhysOp::WriteStage {
				stage: WriteStageKind::OutputWrite,
				mark: WriteMark::Apply,
			},
			PhysTag::Events => PhysOp::Events(Box::new(EventsAttrs {
				action: DocAction::Update,
				defs: EventDefs::default(),
				events: Box::new([]),
				unlowerable: false,
			})),
			PhysTag::Assign => PhysOp::Assign(Box::new(AssignAttrs {
				place: Place {
					parts: Box::new([
						PlacePart::Field(strand("a")),
						PlacePart::Dynamic(1),
						PlacePart::Filter(0),
						PlacePart::Index(2),
						PlacePart::Unaddressed(UnaddressedPart::Recurse, "(1..2)".into()),
						PlacePart::Last,
					]),
				},
				op: AssignOperator::Extend,
			})),
			PhysTag::Unset => PhysOp::Unset {
				place: Place {
					parts: Box::new([PlacePart::Field(strand("a")), PlacePart::All]),
				},
			},
			PhysTag::Content => PhysOp::Content {
				mode: ContentMode::Merge,
			},
			PhysTag::Block => PhysOp::Block,
			PhysTag::Let => PhysOp::Let(Box::new(LetAttrs {
				name: strand("x"),
				binding: LetBinding::Session,
				ty: Some(Kind::String),
			})),
			PhysTag::Sleep => PhysOp::Sleep,
			PhysTag::If => PhysOp::If {
				value: true,
			},
			PhysTag::Loop => PhysOp::Loop {
				kind: LoopKind::For,
				first: true,
				batch: Batch {
					max_iterations: 64,
					max_bytes: 4096,
				},
				unroll: 2,
				value: true,
				step: None,
			},
			PhysTag::Pipeline => PhysOp::Pipeline(Box::new(PipelineAttrs {
				id: PipelineId(0),
				edges: smallvec![
					(PipelineEdge::Feeds, PipelineId(1)),
					(PipelineEdge::Probes, PipelineId(2))
				],
				logical: Some(logical_op()),
			})),
			PhysTag::Choose => PhysOp::Choose(Box::new(choose_spec())),
			PhysTag::Prefetch => PhysOp::Prefetch(Box::new(PrefetchKeys {
				points: smallvec![0, 3],
				ranges: smallvec![(1, 2)],
			})),
			PhysTag::Materialize => PhysOp::Materialize {
				image: MaterializeImage::KeyAndValue,
			},
			PhysTag::LoadUnit => PhysOp::LoadUnit(Box::new(LoadUnitSpec {
				kind: UnitKind::TableRead,
				static_key: UnitStaticKey::Table(table("t")),
			})),
			PhysTag::Yield => PhysOp::Yield,
			PhysTag::Return => PhysOp::Return,
			PhysTag::Break => PhysOp::Break,
			PhysTag::Next => PhysOp::Next,
		}
	}

	/// An op id of a container other than the one the text is read into.
	fn logical_op() -> OpId {
		let mut arena: SlotMap<OpId, ()> = SlotMap::with_key();
		arena.insert(());
		arena.insert(())
	}

	/// The type `tag`'s fixture holds that has no text form, for the tags that hold one.
	fn no_text_form(tag: PhysTag) -> Option<&'static str> {
		match tag {
			PhysTag::Define => Some("DefineAttrs"),
			PhysTag::Alter => Some("AlterAttrs"),
			PhysTag::Live => Some("LiveAttrs"),
			PhysTag::TableScan
			| PhysTag::IndexScan
			| PhysTag::IndexUnion
			| PhysTag::Bitmap
			| PhysTag::IndexLookup
			| PhysTag::FullTextScan
			| PhysTag::KeyRangeScan
			| PhysTag::KnnScan
			| PhysTag::KnnTopK
			| PhysTag::Count
			| PhysTag::RecordScan
			| PhysTag::Vacant
			| PhysTag::Enforced
			| PhysTag::Values
			| PhysTag::Subquery
			| PhysTag::Probe
			| PhysTag::Union
			| PhysTag::Join
			| PhysTag::Distinct
			| PhysTag::PermissionFilter
			| PhysTag::Layer
			| PhysTag::Filter
			| PhysTag::Fetch
			| PhysTag::Split
			| PhysTag::Sort
			| PhysTag::Window
			| PhysTag::Aggregate
			| PhysTag::Limit
			| PhysTag::Project
			| PhysTag::Timeout
			| PhysTag::VersionScope
			| PhysTag::Only
			| PhysTag::Explain
			| PhysTag::Eval
			| PhysTag::Request
			| PhysTag::Txn
			| PhysTag::Stmt
			| PhysTag::Statement
			| PhysTag::Match
			| PhysTag::MatchStep
			| PhysTag::DdlGuard
			| PhysTag::Remove
			| PhysTag::Rebuild
			| PhysTag::Info
			| PhysTag::Use
			| PhysTag::Option
			| PhysTag::Access
			| PhysTag::Kill
			| PhysTag::Subscriber
			| PhysTag::Show
			| PhysTag::Write
			| PhysTag::WriteStage
			| PhysTag::Events
			| PhysTag::Assign
			| PhysTag::Unset
			| PhysTag::Content
			| PhysTag::Block
			| PhysTag::Let
			| PhysTag::Sleep
			| PhysTag::If
			| PhysTag::Loop
			| PhysTag::Pipeline
			| PhysTag::Choose
			| PhysTag::Prefetch
			| PhysTag::Materialize
			| PhysTag::LoadUnit
			| PhysTag::Yield
			| PhysTag::Return
			| PhysTag::Break
			| PhysTag::Next => None,
		}
	}

	fn print_op(op: &PhysOp) -> String {
		let mut out = String::new();
		PhysSyntax.print_attrs(&OpKind::Phys(op.clone()), &mut out);
		out
	}

	fn parse_op(tag: PhysTag, text: &str) -> Result<PhysOp, ParseError> {
		match PhysSyntax.parse_attrs(OpTag::Phys(tag), text)? {
			OpKind::Phys(op) => Ok(op),
			other @ OpKind::Expr(_) | other @ OpKind::Logical(_) => {
				panic!("the text of {} reads as {other:?}", tag.mnemonic())
			}
		}
	}

	#[test]
	fn every_tag_has_a_fixture_of_its_own_tag() {
		for tag in PhysTag::ALL {
			assert_eq!(fixture(*tag).tag(), *tag);
		}
	}

	#[test]
	fn print_parse_round_trip_per_tag() {
		for tag in PhysTag::ALL {
			let text = print_op(&fixture(*tag));
			assert!(text.starts_with(tag.mnemonic()), "{text}");
			match (parse_op(*tag, &text), no_text_form(*tag)) {
				(Ok(parsed), None) => {
					assert_eq!(print_op(&parsed), text, "{}", tag.mnemonic());
					assert_eq!(parsed.tag(), *tag);
				}
				(Err(failure), Some(ty)) => {
					let expected = format!("{}: {ty} has no text form", tag.mnemonic());
					assert_eq!(failure.detail, expected);
				}
				(Ok(_), Some(ty)) => {
					panic!("{} reads though {ty} has no text form", tag.mnemonic())
				}
				(Err(failure), None) => panic!("{}: {failure}\n{text}", tag.mnemonic()),
			}
		}
	}

	#[test]
	fn the_body_of_a_write_reads_back_for_every_branching() {
		let retry = WriteRetry {
			savepoint_after: 2,
			retry_conflict: true,
			skip_failures: false,
			carry_schema_failure: true,
		};
		for branching in
			[WriteBranching::Sole, WriteBranching::UpFront, WriteBranching::Retry(retry)]
		{
			let mut attrs = write_attrs();
			attrs.body = WriteBody {
				branching,
				first: Box::new([0, 1, 4]),
				second: Box::new([0, 2]),
				deferable: true,
			};
			let op = PhysOp::Write(Box::new(PhysWriteAttrs {
				write: attrs,
				subjects: WriteSubjectsShape::Targets,
			}));
			let text = print_op(&op);
			let parsed = parse_op(PhysTag::Write, &text).expect("the text reads");
			assert_eq!(print_op(&parsed), text);
			let PhysOp::Write(parsed) = parsed else {
				panic!("a write reads back as a write");
			};
			assert_eq!(parsed.write.body.branching, branching);
			assert_eq!(*parsed.write.body.first, [0, 1, 4]);
			assert_eq!(*parsed.write.body.second, [0, 2]);
			assert!(parsed.write.body.deferable);
		}
	}

	#[test]
	fn the_image_facts_of_a_write_read_back() {
		for view in [
			ImageView::Plain,
			ImageView::Cut,
			ImageView::Derived,
			ImageView::Dynamic,
			ImageView::LateBound,
		] {
			let mut attrs = write_attrs();
			attrs.image = WriteImage {
				view,
				keyed: view == ImageView::Cut,
				id_default: view == ImageView::Derived,
			};
			let op = PhysOp::Write(Box::new(PhysWriteAttrs {
				write: attrs.clone(),
				subjects: WriteSubjectsShape::Targets,
			}));
			let text = print_op(&op);
			let parsed = parse_op(PhysTag::Write, &text).expect("the text reads");
			assert_eq!(print_op(&parsed), text);
			let PhysOp::Write(parsed) = parsed else {
				panic!("a write reads back as a write");
			};
			assert_eq!(parsed.write.image, attrs.image);
		}
	}

	#[test]
	fn the_derived_fields_of_a_write_read_back() {
		use crate::exec::ir::logical::ops::{DerivedField, WriteDerived};
		let field = |name: &str, kind: Option<crate::expr::Kind>, caller_bound| DerivedField {
			name: surrealdb_strand::Strand::new(name),
			kind,
			caller_bound,
		};
		for observed in [
			None,
			Some(Box::new([]) as Box<[DerivedField]>),
			Some(Box::new([field("a", None, false)]) as Box<[DerivedField]>),
		] {
			let mut attrs = write_attrs();
			attrs.derived = WriteDerived {
				create: Box::new([field("a", None, false)]),
				update: Box::new([
					field("b", Some(crate::expr::Kind::Int), true),
					field("c", None, false),
				]),
				delete: Box::new([]),
				select: Box::new([field("a", None, false)]),
				observed,
			};
			let op = PhysOp::Write(Box::new(PhysWriteAttrs {
				write: attrs.clone(),
				subjects: WriteSubjectsShape::Targets,
			}));
			let text = print_op(&op);
			let parsed = parse_op(PhysTag::Write, &text).expect("the text reads");
			assert_eq!(print_op(&parsed), text);
			let PhysOp::Write(parsed) = parsed else {
				panic!("a write reads back as a write");
			};
			assert_eq!(parsed.write.derived, attrs.derived);
		}
	}

	#[test]
	fn the_field_schema_of_a_write_reads_back() {
		use crate::exec::ir::logical::ops::{
			DerivedField, FieldClause, FieldClauses, FieldGate, FieldStep, WriteSchema,
		};
		let clause = Some(FieldClause {
			caller_bound: true,
		});
		let derived = DerivedField {
			name: surrealdb_strand::Strand::new("net"),
			kind: Some(crate::expr::Kind::Int),
			caller_bound: false,
		};
		let schema = WriteSchema {
			fields: Box::new([
				surrealdb_strand::Strand::new("id"),
				surrealdb_strand::Strand::new("net"),
				surrealdb_strand::Strand::new("a.b"),
			]),
			id_field: Some(0),
			cycle: None,
			steps: Box::new([
				FieldStep::Derive(derived.clone()),
				FieldStep::Field(FieldClauses {
					index: 2,
					default: clause,
					value: None,
					assert: Some(FieldClause {
						caller_bound: false,
					}),
					create: FieldGate::Allow,
					update: FieldGate::Predicate {
						caller_bound: false,
						derives: Box::new([derived]),
					},
				}),
				FieldStep::Field(FieldClauses {
					index: 1,
					default: None,
					value: clause,
					assert: None,
					create: FieldGate::Deny,
					update: FieldGate::Allow,
				}),
			]),
			computed: Box::new([surrealdb_strand::Strand::new("net")]),
			unlowerable: false,
			id_default_bound: true,
		};
		for schema in [
			schema.clone(),
			WriteSchema {
				cycle: Some("a, b".into()),
				steps: Box::new([]),
				..schema
			},
			WriteSchema::default(),
		] {
			let mut attrs = write_attrs();
			attrs.stored.schema = schema.clone();
			let op = PhysOp::Write(Box::new(PhysWriteAttrs {
				write: attrs,
				subjects: WriteSubjectsShape::Targets,
			}));
			let text = print_op(&op);
			let parsed = parse_op(PhysTag::Write, &text).expect("the text reads");
			assert_eq!(print_op(&parsed), text);
			let PhysOp::Write(parsed) = parsed else {
				panic!("a write reads back as a write");
			};
			assert_eq!(parsed.write.stored.schema, schema);
		}
	}

	#[test]
	fn the_stored_facts_of_a_write_read_back() {
		for (table_name, stored) in [
			(None, Stored::Dynamic),
			(Some("t"), Stored::Nothing),
			(Some("t"), Stored::Declared),
			(Some("t"), Stored::LateBound),
		] {
			let mut attrs = write_attrs();
			attrs.stored = WriteStored {
				table: table_name.map(table),
				schemafull: stored == Stored::Declared,
				catalog_fixed: stored == Stored::Nothing,
				fields: stored,
				references: Stored::Nothing,
				indexes: stored,
				events: Stored::Declared,
				views: Stored::LateBound,
				referenced: Stored::Declared,
				edges: Stored::Declared,
				schema: Default::default(),
				reference_fields: Box::default(),
				materialized: Box::default(),
				index_set: Default::default(),
				view_set: crate::exec::ir::logical::ops::WriteViews::default(),
				repair_set: crate::exec::ir::logical::ops::WriteRepairs {
					tables: vec![crate::exec::ir::logical::ops::RepairTable {
						table: table("author"),
						fields: vec![
							crate::exec::ir::logical::ops::RepairField {
								name: crate::expr::Idiom::field(Strand::new("posts")),
								strategy: crate::exec::ir::logical::ops::RepairKind::Unset,
							},
							crate::exec::ir::logical::ops::RepairField {
								name: crate::expr::Idiom::field(Strand::new("owner")),
								strategy: crate::exec::ir::logical::ops::RepairKind::Custom,
							},
						]
						.into_boxed_slice(),
					}]
					.into_boxed_slice(),
					unlowerable: stored == Stored::LateBound,
				},
				image_layers: crate::exec::ir::logical::ops::ImageLayers::Enforced,
			};
			let op = PhysOp::Write(Box::new(PhysWriteAttrs {
				write: attrs.clone(),
				subjects: WriteSubjectsShape::Targets,
			}));
			let text = print_op(&op);
			let parsed = parse_op(PhysTag::Write, &text).expect("the text reads");
			assert_eq!(print_op(&parsed), text);
			let PhysOp::Write(parsed) = parsed else {
				panic!("a write reads back as a write");
			};
			assert_eq!(parsed.write.stored, attrs.stored);
		}
	}

	#[test]
	fn the_references_and_the_indexes_of_a_write_read_back() {
		use crate::catalog::{Index, IndexDefinition, IndexId};
		use crate::exec::ir::logical::ops::{IndexDefs, WriteIndexes};

		let def = |id: u32, kind: Index, removed: bool| IndexDefinition {
			index_id: IndexId(id),
			name: surrealdb_strand::Strand::new("ix"),
			table_name: table("t"),
			cols: vec![crate::syn::idiom("a.b").expect("an idiom")],
			index: kind,
			count_cond: None,
			prepare_remove: removed,
			format_version: 3,
			comment: Some("a `quoted` \"comment\"".to_owned()),
			nulls_not_distinct: false,
		};
		let set = WriteIndexes {
			defs: IndexDefs::new(std::sync::Arc::from(vec![
				def(1, Index::Idx, false),
				def(2, Index::Uniq, true),
			])),
			unlowerable: false,
		};
		for (references, index_set) in [
			(Box::<[crate::expr::Idiom]>::default(), WriteIndexes::default()),
			(
				Box::from([
					crate::syn::idiom("owner").expect("an idiom"),
					crate::syn::idiom("team.lead").expect("an idiom"),
					crate::syn::idiom("tags[*]").expect("an idiom"),
				]),
				set.clone(),
			),
			(
				Box::default(),
				WriteIndexes {
					unlowerable: true,
					..set
				},
			),
		] {
			let mut attrs = write_attrs();
			attrs.stored.references = Stored::Declared;
			attrs.stored.indexes = Stored::Declared;
			attrs.stored.reference_fields = references;
			attrs.stored.index_set = index_set;
			let op = PhysOp::Write(Box::new(PhysWriteAttrs {
				write: attrs.clone(),
				subjects: WriteSubjectsShape::Targets,
			}));
			let text = print_op(&op);
			let parsed = parse_op(PhysTag::Write, &text).expect("the text reads");
			assert_eq!(print_op(&parsed), text);
			let PhysOp::Write(parsed) = parsed else {
				panic!("a write reads back as a write");
			};
			assert_eq!(parsed.write.stored, attrs.stored);
		}
	}

	#[test]
	fn the_views_a_write_maintains_read_back() {
		use crate::exec::ir::logical::ops::{TableDefs, WriteViews};
		use crate::exec::ir::logical::stages::view_table_for_test;

		let set = WriteViews {
			defs: TableDefs::new(std::sync::Arc::from(vec![
				view_table_for_test("m", "a, b AS c", Some("a > 1"), None),
				view_table_for_test("g", "a, count() AS n", None, Some(&["a"])),
			])),
			unlowerable: false,
		};
		for view_set in [
			WriteViews::default(),
			set.clone(),
			WriteViews {
				unlowerable: true,
				..set
			},
		] {
			let mut attrs = write_attrs();
			attrs.stored.views = Stored::Declared;
			attrs.stored.view_set = view_set;
			let op = PhysOp::Write(Box::new(PhysWriteAttrs {
				write: attrs.clone(),
				subjects: WriteSubjectsShape::Targets,
			}));
			let text = print_op(&op);
			let parsed = parse_op(PhysTag::Write, &text).expect("the text reads");
			assert_eq!(print_op(&parsed), text);
			let PhysOp::Write(parsed) = parsed else {
				panic!("a write reads back as a write");
			};
			assert_eq!(parsed.write.stored, attrs.stored);
		}
	}

	#[test]
	fn the_gate_clauses_of_a_write_read_back() {
		let mut attrs = write_attrs();
		attrs.gates = WriteGates {
			create: GateClause::Full,
			update: GateClause::Predicate {
				derives: true,
			},
			delete: GateClause::None,
			select: GateClause::Absent,
		};
		for later in [GateClause::Dynamic, GateClause::LateBound, GateClause::Unlowerable] {
			attrs.gates.select = later;
			let op = PhysOp::Write(Box::new(PhysWriteAttrs {
				write: attrs.clone(),
				subjects: WriteSubjectsShape::Targets,
			}));
			let text = print_op(&op);
			let parsed = parse_op(PhysTag::Write, &text).expect("the text reads");
			assert_eq!(print_op(&parsed), text);
			let PhysOp::Write(parsed) = parsed else {
				panic!("a write reads back as a write");
			};
			assert_eq!(parsed.write.gates, attrs.gates);
		}
	}

	#[test]
	fn the_into_text_of_an_insert_reads_back() {
		for into_text in [None, Some("t"), Some("$table"), Some("a \"quoted\" name")] {
			let mut attrs = write_attrs();
			attrs.into_text = into_text.map(Into::into);
			let op = PhysOp::Write(Box::new(PhysWriteAttrs {
				write: attrs.clone(),
				subjects: WriteSubjectsShape::Inserts,
			}));
			let text = print_op(&op);
			let parsed = parse_op(PhysTag::Write, &text).expect("the text reads");
			assert_eq!(print_op(&parsed), text);
			let PhysOp::Write(parsed) = parsed else {
				panic!("a write reads back as a write");
			};
			assert_eq!(parsed.write.into_text, attrs.into_text);
		}
	}

	#[test]
	fn the_text_of_a_round_trip_reads_back_the_attributes() {
		let op = fixture(PhysTag::IndexScan);
		let parsed = parse_op(PhysTag::IndexScan, &print_op(&op)).expect("the text reads");
		match (op, parsed) {
			(PhysOp::IndexScan(a), PhysOp::IndexScan(b)) => {
				assert_eq!(a.table, b.table);
				assert_eq!(a.index, b.index);
				assert_eq!(a.access, b.access);
				assert_eq!(a.ordered_by, b.ordered_by);
				assert!(b.exact());
			}
			other => panic!("{other:?}"),
		}
	}

	#[test]
	fn an_op_without_attributes_prints_its_mnemonic_alone() {
		assert_eq!(print_op(&PhysOp::Union), "phys.union");
		assert_eq!(print_op(&PhysOp::Yield), "phys.yield");
	}

	#[test]
	fn the_leading_mnemonic_is_optional_and_must_be_the_tags() {
		let text = print_op(&PhysOp::Vacant {
			table: table("t"),
		});
		let bare = text.trim_start_matches("phys.vacant").trim();
		assert!(parse_op(PhysTag::Vacant, bare).is_ok());
		assert!(parse_op(PhysTag::Enforced, &text).is_err());
	}

	#[test]
	fn an_op_without_attributes_reads_no_text_after_its_mnemonic() {
		assert!(parse_op(PhysTag::Union, "phys.union").is_ok());
		assert!(parse_op(PhysTag::Union, "").is_ok());
		assert!(parse_op(PhysTag::Union, "phys.union { x: 1 }").is_err());
	}

	#[test]
	fn the_reader_is_strict() {
		let read = |text: &str| parse_op(PhysTag::RecordScan, text);
		assert!(
			read(
				"{ table: \"t\", for_update: true, lightweight: false, read: { schema: none, with: \
				 none } }"
			)
			.is_ok()
		);
		// A reordered, missing, extra, repeated or unknown attribute, and trailing text.
		for text in [
			"{ for_update: true, table: \"t\", lightweight: false, read: { schema: none, with: none } }",
			"{ table: \"t\", for_update: true, lightweight: false }",
			"{ for_update: true, lightweight: false, read: { schema: none, with: none } }",
			"{ table: \"t\", for_update: true, lightweight: false, read: { schema: none, with: none }, extra: 1 }",
			"{ table: \"t\", for_update: true, for_update: true, read: { schema: none, with: none } }",
			"{ table: \"t\", for_update: true, lightweight: maybe, read: { schema: none, with: none } }",
			"{ table: \"t\", for_update: true, lightweight: false, read: { schema: none, with: none } } trailing",
			"",
		] {
			assert!(read(text).is_err(), "{text:?} reads");
		}
	}

	/// A key-range scan reads back as it was printed: the structure of its key, an operand slot
	/// beside a literal, the order it delivers and the cut of its top-k key.
	#[test]
	fn a_key_range_scan_reads_back_with_its_operand_slots() {
		let op = fixture(PhysTag::KeyRangeScan);
		let text = print_op(&op);
		assert!(text.contains("Operand(0)"), "{text}");
		assert!(text.contains("Coarse { position: 1 }"), "{text}");
		assert!(text.contains("coarse_cutoff: some({ position: 1, keep: 3 })"), "{text}");
		let (PhysOp::KeyRangeScan(read), PhysOp::KeyRangeScan(printed)) =
			(parse_op(PhysTag::KeyRangeScan, &text).expect("the text reads"), op)
		else {
			panic!("{text}");
		};
		assert_eq!(*read, *printed);
	}

	/// The key structure of an aggregated view is derived from the view's definition, which the
	/// syntax does not carry.
	#[test]
	fn a_key_range_scan_over_a_view_names_its_key_as_unsupported() {
		let text = print_op(&fixture(PhysTag::KeyRangeScan));
		let start = text.find("key: { ").expect("the key prints");
		let end = start + text[start..].find(" }, eq:").expect("the key ends") + 2;
		let unsupported =
			format!("{}key: unsupported(KeyStructure){}", &text[..start], &text[end..]);
		let failure = parse_op(PhysTag::KeyRangeScan, &unsupported)
			.expect_err("the structure has no text form");
		assert_eq!(failure.detail, "phys.key_range_scan: KeyStructure has no text form");
	}

	#[test]
	fn an_unsupported_attribute_names_its_type() {
		let mut op = fixture(PhysTag::IndexScan);
		if let PhysOp::IndexScan(attrs) = &mut op {
			attrs.decode = Some(DecodeFields::new(["a".to_owned()]));
		}
		let text = print_op(&op);
		assert!(text.contains("unsupported(DecodeFields)"), "{text}");
		let failure = parse_op(PhysTag::IndexScan, &text).expect_err("the set has no text form");
		assert_eq!(failure.detail, "phys.index_scan: DecodeFields has no text form");
	}

	/// The statement clock prints as the call that reads it, bare beside the quoted text of a
	/// literal, and every access shape that can hold it reads back as it was printed.
	#[test]
	fn a_clock_bound_prints_as_the_call_and_reads_back() {
		let clock = |offset: Option<(ClockSign, u64)>| {
			BoundOperand::StatementClock(ClockBound {
				offset: offset.map(|(sign, secs)| {
					(sign, crate::val::Duration::from(std::time::Duration::from_secs(secs)))
				}),
			})
		};
		let accesses = [
			BTreeAccess::Equality(clock(None)),
			BTreeAccess::Range {
				range: OperandRange {
					start: Bound::Excluded(clock(Some((ClockSign::Minus, 86_400)))),
					end: Bound::Included(clock(Some((ClockSign::Plus, 3_600)))),
					point_filter: None,
				},
			},
			BTreeAccess::Range {
				range: OperandRange {
					start: Bound::Unbounded,
					end: Bound::Excluded(clock(None)),
					point_filter: None,
				},
			},
			BTreeAccess::Compound {
				prefix: vec![Value::from("x").into(), clock(None)],
				range: Some((BinaryOperator::MoreThan, clock(Some((ClockSign::Minus, 90_061))))),
				upper: None,
			},
		];
		for access in accesses {
			let mut op = fixture(PhysTag::IndexScan);
			if let PhysOp::IndexScan(attrs) = &mut op {
				attrs.access = access.clone();
			}
			let text = print_op(&op);
			assert!(text.contains("time::now()"), "{text}");
			let PhysOp::IndexScan(read) =
				parse_op(PhysTag::IndexScan, &text).expect("the text reads")
			else {
				panic!("{text}");
			};
			assert_eq!(read.access, access, "{text}");
		}
		let mut op = fixture(PhysTag::IndexScan);
		if let PhysOp::IndexScan(attrs) = &mut op {
			attrs.access = BTreeAccess::Range {
				range: OperandRange {
					start: Bound::Excluded(clock(Some((ClockSign::Minus, 86_400)))),
					end: Bound::Unbounded,
					point_filter: None,
				},
			};
		}
		let text = print_op(&op);
		assert!(text.contains("Excluded(time::now() - 1d)"), "{text}");
	}

	#[test]
	fn a_compound_range_with_another_operator_has_no_text_form() {
		let mut op = fixture(PhysTag::IndexScan);
		if let PhysOp::IndexScan(attrs) = &mut op {
			attrs.access = BTreeAccess::Compound {
				prefix: Vec::new(),
				range: Some((BinaryOperator::Add, Value::from(1i64).into())),
				upper: None,
			};
		}
		let text = print_op(&op);
		let failure = parse_op(PhysTag::IndexScan, &text).expect_err("`+` is not a range operator");
		assert!(failure.detail.contains("BinaryOperator has no text form"), "{failure}");
	}

	#[test]
	fn an_op_of_another_dialect_is_not_read() {
		let mut out = String::new();
		PhysSyntax.print_attrs(&OpKind::Expr(crate::exec::ir::expr::ops::ExprOp::Yield), &mut out);
		assert_eq!(out, "unsupported");
		let failure = PhysSyntax
			.parse_attrs(
				OpTag::Logical(crate::exec::ir::logical::ops::LogTag::Yield),
				"logical.yield",
			)
			.expect_err("a logical tag is not physical");
		assert!(failure.detail.contains("is not a physical op"), "{failure}");
	}

	#[test]
	fn strings_keep_their_escapes_and_floats_their_bits() {
		let op = PhysOp::FullTextScan(Box::new(FullTextScanAttrs {
			table: table("t\\\"\n\t\u{1}"),
			index: index_ref(),
			query: "\u{e9}\u{1f600}".to_owned(),
			operator: matches_operator(),
			column: None,
			decode: None,
			read: ReadClauses::default(),
			topk: None,
		}));
		let text = print_op(&op);
		assert!(!text.contains('\n'), "{text}");
		let PhysOp::FullTextScan(read) =
			parse_op(PhysTag::FullTextScan, &text).expect("the text reads")
		else {
			panic!("{text}");
		};
		assert_eq!(read.table, table("t\\\"\n\t\u{1}"));
		assert_eq!(read.query, "\u{e9}\u{1f600}");
		for float in [0.1_f64, -0.0, 1e300, f64::MIN_POSITIVE, f64::INFINITY, f64::NEG_INFINITY] {
			let mut out = String::new();
			Number::Float(float).put(&mut out);
			let mut cursor = Cursor::new("phys.test", &out);
			match <Number as Text>::get(&mut cursor).expect("the number reads") {
				Number::Float(back) => assert_eq!(back.to_bits(), float.to_bits(), "{out}"),
				other @ (Number::Int(_)
				| Number::Decimal(_)
				| Number::BigDecimal(_)
				| Number::I8(_)
				| Number::U8(_)
				| Number::I16(_)
				| Number::U16(_)
				| Number::I32(_)
				| Number::U32(_)
				| Number::U64(_)
				| Number::F16(_)
				| Number::F32(_)) => panic!("{other:?}"),
			}
		}
	}

	/// A loop states its step as text, and the text reads back as the step that was printed.
	#[test]
	fn a_loop_step_round_trips() {
		let lookup = LookupAttrs {
			kind: LookupKind::Graph(Dir::Both),
			only: true,
			only_path: false,
			subjects: Box::new([
				LookupSubjectShape {
					table: table("we\"ird"),
					referencing_field: None,
					range: None,
				},
				LookupSubjectShape {
					table: table("knows"),
					referencing_field: Some("a\nb".to_owned()),
					range: Some(KeyRange {
						start: Bound::Included(KeyBound::Number(1)),
						end: Bound::Excluded(KeyBound::Array {
							len: 2,
						}),
					}),
				},
			]),
			fields: Some(FieldsShape::Select(Box::new([FieldShape::All]))),
			order: Some(SortShape::Keys(Box::new([SortKeyShape {
				collate: true,
				numeric: false,
				ascending: false,
				path: false,
			}]))),
			alias: Some(OutputPath::field("friends")),
			cond: true,
			split: Some(1),
			group: Some(0),
			limit: Some(3),
			start: None,
			trace: crate::expr::logical::LookupTrace::None,
		};
		let steps = [
			StepAttrs::Flatten,
			StepAttrs::Closure(ClosureLoopAttrs {
				function: Strand::new("array::some"),
				method: false,
			}),
			StepAttrs::Closure(ClosureLoopAttrs {
				function: Strand::new("map"),
				method: true,
			}),
			StepAttrs::Lookup(lookup),
			StepAttrs::Recurse(RecurseAttrs {
				depth: Recurse::Range(Some(1), None),
				form: RecursePathForm::Trailing,
				instruction: Some(RecurseInstructionShape::Shortest {
					inclusive: true,
				}),
			}),
			StepAttrs::Param(Strand::new("item")),
		];
		for step in steps {
			let op = PhysOp::Loop {
				kind: LoopKind::GraphStep,
				first: false,
				batch: Batch {
					max_iterations: 64,
					max_bytes: 4096,
				},
				unroll: 1,
				value: false,
				step: Some(Box::new(step.clone())),
			};
			let text = print_op(&op);
			let PhysOp::Loop {
				step: read,
				..
			} = parse_op(PhysTag::Loop, &text).expect("the text reads")
			else {
				panic!("{text}");
			};
			assert_eq!(read.as_deref(), Some(&step), "{text}");
		}
	}

	#[test]
	fn an_arm_label_is_read_back_by_its_text() {
		let text = print_op(&fixture(PhysTag::Choose));
		let PhysOp::Choose(spec) = parse_op(PhysTag::Choose, &text).expect("the text reads") else {
			panic!("{text}");
		};
		let labels: Vec<&str> = spec.arms.iter().map(|arm| arm.label.0).collect();
		assert_eq!(labels, vec!["index", "typed", "table scan"]);
	}
}
