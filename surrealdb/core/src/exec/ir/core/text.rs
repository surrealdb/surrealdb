//! The grammar helpers the printer and the parser share: the name tables, the quoting rules, the
//! text of types and locations, and the cursor the parser reads with.
//!
//! The tables are total over their enums: a variant added to a region kind or a type source is a
//! compile error here, because the matches below name every variant. Every name is written and read
//! through the same table, so a name that prints parses.

#![deny(clippy::wildcard_enum_match_arm)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::OnceLock;

use slotmap::{Key, KeyData};
use surrealdb_strand::TableName;
use surrealdb_types::{SqlFormat, ToSql};

use super::location::{LocOrigin, Location, Span};
use super::parse::ParseError;
use crate::exec::ir::iface::conversion::Phase;
use crate::exec::ir::iface::frame::AuthorityFrame;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::iface::region_kind::{
	ClauseRole, ErrorScope, FieldRole, RegionKind, StatementCommit, StoredRole, TxnKind,
};
use crate::exec::ir::iface::types::{FactSource, HintSource, TokenClass, TypeKnowledge};
use crate::exec::ir::op_kind::OpTag;
use crate::exec::vm::program::UnitFrame;
use crate::expr::logical::{EvalFrame, EvalMode};
use crate::iam::{AuthLimit, Level, Role};

/// The dialect names, indexed like [`AttrSyntaxes`](super::print::AttrSyntaxes).
pub(super) const DIALECT_NAMES: [&str; 3] = ["expr", "logical", "phys"];

/// The position of `tag`'s dialect in [`AttrSyntaxes`](super::print::AttrSyntaxes) and
/// [`DIALECT_NAMES`].
pub(super) fn dialect_index(tag: OpTag) -> usize {
	match tag {
		OpTag::Expr(_) => 0,
		OpTag::Logical(_) => 1,
		OpTag::Phys(_) => 2,
	}
}

/// The tag a mnemonic names, from a table built once from [`OpTag::ALL`].
pub(super) fn tag_from_mnemonic(mnemonic: &str) -> Option<OpTag> {
	static TABLE: OnceLock<HashMap<&'static str, OpTag>> = OnceLock::new();
	TABLE
		.get_or_init(|| OpTag::ALL.iter().map(|tag| (tag.mnemonic(), *tag)).collect())
		.get(mnemonic)
		.copied()
}

/// An op id named in text: the op printed at a position, or an id that names no op the text
/// holds, by order of first appearance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OpRef {
	Op(u32),
	Dangling(u32),
}

/// The op ids of the attribute text being written or read by the core printer or parser.
enum AttrIds {
	/// The ids written so far; each was written as a marker holding its index here.
	Writing(Vec<OpId>),
	/// The names read so far; each was read as a placeholder id holding its index here.
	Reading(Vec<OpRef>),
}

thread_local! {
	static ATTR_IDS: RefCell<Option<AttrIds>> = const { RefCell::new(None) };
}

/// The character that brackets the index of an op id written inside [`writing_attr_ids`].
pub(super) const OP_ID_MARK: char = '\u{1}';

/// Writes the op id `id` held in an op's attributes.
///
/// Inside [`writing_attr_ids`] it writes a marker the core printer replaces by the id's `@<n>` or
/// `@?<k>` name, the names provenance uses. Anywhere else it writes the bits of the key, which
/// name an op of the container being printed only.
pub(crate) fn put_attr_op_id(id: OpId, out: &mut String) {
	let marked = ATTR_IDS.with(|cell| match &mut *cell.borrow_mut() {
		Some(AttrIds::Writing(ids)) => {
			ids.push(id);
			Some(ids.len() - 1)
		}
		Some(AttrIds::Reading(_)) | None => None,
	});
	match marked {
		Some(at) => {
			let _ = write!(out, "{OP_ID_MARK}{at}{OP_ID_MARK}");
		}
		None => {
			let _ = write!(out, "{}", id.data().as_ffi());
		}
	}
}

/// Reads an op id held in an op's attributes from `token`, as [`put_attr_op_id`] wrote it.
///
/// Inside [`reading_attr_ids`] an `@<n>` or `@?<k>` name reads as a placeholder the core parser
/// replaces by the op it names once the container is built. Bits read as a key.
pub(crate) fn get_attr_op_id(token: &str) -> Option<OpId> {
	if let Some(name) = token.strip_prefix('@') {
		let named = match name.strip_prefix('?') {
			Some(k) => OpRef::Dangling(k.parse().ok()?),
			None => OpRef::Op(name.parse().ok()?),
		};
		return ATTR_IDS.with(|cell| match &mut *cell.borrow_mut() {
			Some(AttrIds::Reading(refs)) => {
				refs.push(named);
				Some(attr_placeholder(refs.len() - 1))
			}
			Some(AttrIds::Writing(_)) | None => None,
		});
	}
	token.parse::<u64>().ok().map(|bits| OpId::from(KeyData::from_ffi(bits)))
}

/// The placeholder id the `at`th name read inside [`reading_attr_ids`] stands as.
fn attr_placeholder(at: usize) -> OpId {
	OpId::from(KeyData::from_ffi((1 << 32) | u64::try_from(at).unwrap_or(u64::from(u32::MAX))))
}

/// The index of the name the placeholder `id` stands for.
pub(super) fn attr_placeholder_index(id: OpId) -> usize {
	usize::try_from(id.data().as_ffi() & u64::from(u32::MAX)).unwrap_or(usize::MAX)
}

/// Runs `f`, which writes one op's attributes, and answers the op ids it wrote, in the order of
/// their markers.
pub(super) fn writing_attr_ids(f: impl FnOnce()) -> Vec<OpId> {
	let outer = ATTR_IDS.with(|cell| cell.replace(Some(AttrIds::Writing(Vec::new()))));
	f();
	match ATTR_IDS.with(|cell| cell.replace(outer)) {
		Some(AttrIds::Writing(ids)) => ids,
		Some(AttrIds::Reading(_)) | None => Vec::new(),
	}
}

/// Runs `f`, which reads one op's attributes, and answers what it read with the names it read,
/// in the order of their placeholders.
pub(super) fn reading_attr_ids<R>(f: impl FnOnce() -> R) -> (R, Vec<OpRef>) {
	let outer = ATTR_IDS.with(|cell| cell.replace(Some(AttrIds::Reading(Vec::new()))));
	let read = f();
	let refs = match ATTR_IDS.with(|cell| cell.replace(outer)) {
		Some(AttrIds::Reading(refs)) => refs,
		Some(AttrIds::Writing(_)) | None => Vec::new(),
	};
	(read, refs)
}

/// Appends the canonical text of `frame`:
/// `eval(<mode>, <auth>, <key>) unit(<class>, <ns>, <db>, <permissions>, <contract>)`, every field
/// written by name and every name quoted, so equal frames print alike and the text reads back to
/// an equal frame ([`parse_frame`]).
pub(super) fn push_frame(out: &mut String, frame: &AuthorityFrame) {
	let mode = match frame.eval.mode {
		EvalMode::Caller => "caller",
		EvalMode::Permission => "permission",
		EvalMode::MutablePermission => "mutable_permission",
		EvalMode::Computed => "computed",
	};
	let _ = write!(out, "eval({mode}, ");
	match &frame.eval.auth {
		None => out.push_str("none"),
		Some(limit) => {
			out.push_str("limit(");
			match &limit.level {
				Level::No => out.push_str("no"),
				Level::Root => out.push_str("root"),
				Level::Namespace(ns) => {
					out.push_str("ns(");
					push_quoted(out, ns);
					out.push(')');
				}
				Level::Database(ns, db) => {
					out.push_str("db(");
					push_quoted(out, ns);
					out.push_str(", ");
					push_quoted(out, db);
					out.push(')');
				}
				Level::Record(ns, db, id) => {
					out.push_str("record(");
					push_quoted(out, ns);
					out.push_str(", ");
					push_quoted(out, db);
					out.push_str(", ");
					push_quoted(out, id);
					out.push(')');
				}
			}
			match limit.role {
				None => out.push_str(", none)"),
				Some(role) => {
					let _ = write!(out, ", {})", role.to_string().to_ascii_lowercase());
				}
			}
		}
	}
	out.push_str(if frame.eval.key_part {
		", key_part)"
	} else {
		", whole)"
	});
	let (class, ns, db, suppressed, checked) = frame.unit.parts();
	let _ = write!(out, " unit({class:#018x}, ");
	for name in [ns, db] {
		match name {
			Some(name) => push_quoted(out, name),
			None => out.push_str("none"),
		}
		out.push_str(", ");
	}
	out.push_str(if suppressed {
		"suppressed, "
	} else {
		"enforced, "
	});
	out.push_str(if checked {
		"checked)"
	} else {
		"unchecked)"
	});
}

/// Reads the text [`push_frame`] appends.
pub(super) fn parse_frame(c: &mut Cursor<'_>) -> Result<AuthorityFrame, ParseError> {
	fn either(c: &mut Cursor<'_>, yes: &str, no: &str) -> Result<bool, ParseError> {
		if c.eat(yes) {
			Ok(true)
		} else if c.eat(no) {
			Ok(false)
		} else {
			Err(c.error(format!("expected `{yes}` or `{no}`")))
		}
	}
	fn name(c: &mut Cursor<'_>) -> Result<Option<String>, ParseError> {
		if c.eat("none") {
			Ok(None)
		} else {
			c.quoted().map(Some)
		}
	}
	c.expect("eval(")?;
	let mode = if c.eat("caller") {
		EvalMode::Caller
	} else if c.eat("permission") {
		EvalMode::Permission
	} else if c.eat("mutable_permission") {
		EvalMode::MutablePermission
	} else if c.eat("computed") {
		EvalMode::Computed
	} else {
		return Err(c.error("expected an evaluation mode"));
	};
	c.expect(", ")?;
	let auth = if c.eat("none") {
		None
	} else {
		c.expect("limit(")?;
		let level = if c.eat("no") {
			Level::No
		} else if c.eat("root") {
			Level::Root
		} else if c.eat("ns(") {
			let ns = c.quoted()?;
			c.expect(")")?;
			Level::Namespace(ns)
		} else if c.eat("db(") {
			let ns = c.quoted()?;
			c.expect(", ")?;
			let db = c.quoted()?;
			c.expect(")")?;
			Level::Database(ns, db)
		} else if c.eat("record(") {
			let ns = c.quoted()?;
			c.expect(", ")?;
			let db = c.quoted()?;
			c.expect(", ")?;
			let id = c.quoted()?;
			c.expect(")")?;
			Level::Record(ns, db, id)
		} else {
			return Err(c.error("expected a level"));
		};
		c.expect(", ")?;
		let role = if c.eat("none") {
			None
		} else {
			let name = c.take_while(|ch| ch.is_ascii_lowercase());
			Some(name.parse::<Role>().map_err(|_| c.error(format!("`{name}` is not a role")))?)
		};
		c.expect(")")?;
		Some(AuthLimit::new(level, role))
	};
	c.expect(", ")?;
	let key_part = either(c, "key_part", "whole")?;
	c.expect(") unit(0x")?;
	let digits = c.take_while(|ch| ch.is_ascii_hexdigit());
	let class = u64::from_str_radix(digits, 16).map_err(|_| c.error("expected a class digest"))?;
	c.expect(", ")?;
	let ns = name(c)?;
	c.expect(", ")?;
	let db = name(c)?;
	c.expect(", ")?;
	let suppressed = either(c, "suppressed", "enforced")?;
	c.expect(", ")?;
	let checked = either(c, "checked", "unchecked")?;
	c.expect(")")?;
	Ok(AuthorityFrame {
		eval: EvalFrame {
			mode,
			auth,
			key_part,
		},
		unit: UnitFrame::from_parts(class, ns, db, suppressed, checked),
	})
}

/// The text name of a phase.
pub(super) fn phase_name(phase: Phase) -> &'static str {
	match phase {
		Phase::Bound => "bound",
		Phase::Logical => "logical",
		Phase::Physical => "physical",
		Phase::Lowered => "lowered",
	}
}

/// The phase `name` names.
pub(super) fn phase_from_name(name: &str) -> Option<Phase> {
	[Phase::Bound, Phase::Logical, Phase::Physical, Phase::Lowered]
		.into_iter()
		.find(|phase| phase_name(*phase) == name)
}

/// The text name of a region kind, payload included.
pub(super) fn region_kind_name(kind: RegionKind) -> &'static str {
	match kind {
		RegionKind::Request => "request",
		RegionKind::Transaction(TxnKind::Explicit) => "transaction.explicit",
		RegionKind::Transaction(TxnKind::Session) => "transaction.session",
		RegionKind::Statement(StatementCommit::Auto) => "statement.auto",
		RegionKind::Statement(StatementCommit::InTxn) => "statement.in_txn",
		RegionKind::Block => "block",
		RegionKind::ValueBlock => "value_block",
		RegionKind::StatementIf => "statement_if",
		RegionKind::ValueIf => "value_if",
		RegionKind::StatementFor => "statement_for",
		RegionKind::ValueFor => "value_for",
		RegionKind::Loop => "loop",
		RegionKind::Subquery => "subquery",
		RegionKind::FunctionBody => "function_body",
		RegionKind::ClosureBody => "closure_body",
		RegionKind::PermissionFrame => "permission_frame",
		RegionKind::FieldFrame(FieldRole::Value) => "field.value",
		RegionKind::FieldFrame(FieldRole::Default) => "field.default",
		RegionKind::FieldFrame(FieldRole::Assert) => "field.assert",
		RegionKind::FieldFrame(FieldRole::Computed) => "field.computed",
		RegionKind::EventFrame => "event_frame",
		RegionKind::SubscriberFrame => "subscriber_frame",
		RegionKind::Clause(ClauseRole::Where) => "clause.where",
		RegionKind::Clause(ClauseRole::WriteCond) => "clause.write_cond",
		RegionKind::Clause(ClauseRole::OrderKey) => "clause.order_key",
		RegionKind::Clause(ClauseRole::GroupKey) => "clause.group_key",
		RegionKind::Clause(ClauseRole::Split) => "clause.split",
		RegionKind::Clause(ClauseRole::Projection) => "clause.projection",
		RegionKind::Clause(ClauseRole::SetValue) => "clause.set_value",
		RegionKind::Clause(ClauseRole::Content) => "clause.content",
		RegionKind::Clause(ClauseRole::InsertValues) => "clause.insert_values",
		RegionKind::Clause(ClauseRole::DuplicateUpdate) => "clause.duplicate_update",
		RegionKind::Clause(ClauseRole::ReturnClause) => "clause.return",
		RegionKind::Clause(ClauseRole::From) => "clause.from",
		RegionKind::Clause(ClauseRole::Limit) => "clause.limit",
		RegionKind::Clause(ClauseRole::Start) => "clause.start",
		RegionKind::Clause(ClauseRole::Timeout) => "clause.timeout",
		RegionKind::Clause(ClauseRole::IndexColumn) => "clause.index_column",
		RegionKind::Clause(ClauseRole::ViewClause) => "clause.view_clause",
		RegionKind::Clause(ClauseRole::ReferenceRepair) => "clause.reference_repair",
		RegionKind::LazyOperand => "lazy_operand",
		RegionKind::ErrorScope(ErrorScope::ShapeToNone) => "error_scope.shape_to_none",
		RegionKind::ErrorScope(ErrorScope::MethodBody) => "error_scope.method_body",
		RegionKind::ErrorScope(ErrorScope::PermissionCheck) => "error_scope.permission_check",
		RegionKind::ChooseArm => "choose_arm",
		RegionKind::Pipeline => "pipeline",
		RegionKind::JoinLookupArm => "join_lookup_arm",
		RegionKind::WriteStage => "write_stage",
		RegionKind::CatalogUnit => "catalog_unit",
		RegionKind::Deadline => "deadline",
		RegionKind::StoredClause(StoredRole::Value) => "stored.value",
		RegionKind::StoredClause(StoredRole::Assert) => "stored.assert",
		RegionKind::StoredClause(StoredRole::Default) => "stored.default",
		RegionKind::StoredClause(StoredRole::Computed) => "stored.computed",
		RegionKind::StoredClause(StoredRole::Permissions) => "stored.permissions",
		RegionKind::StoredClause(StoredRole::When) => "stored.when",
		RegionKind::StoredClause(StoredRole::Then) => "stored.then",
		RegionKind::StoredClause(StoredRole::Body) => "stored.body",
		RegionKind::StoredClause(StoredRole::View) => "stored.view",
		RegionKind::StoredClause(StoredRole::IndexColumn) => "stored.index_column",
		RegionKind::StoredClause(StoredRole::Authenticate) => "stored.authenticate",
		RegionKind::StoredClause(StoredRole::Context) => "stored.context",
		RegionKind::StoredClause(StoredRole::Signup) => "stored.signup",
		RegionKind::StoredClause(StoredRole::Signin) => "stored.signin",
		RegionKind::StoredClause(StoredRole::Action) => "stored.action",
		RegionKind::StoredClause(StoredRole::Fallback) => "stored.fallback",
		RegionKind::StoredClause(StoredRole::LiveFields) => "stored.live_fields",
		RegionKind::StoredClause(StoredRole::LiveCond) => "stored.live_cond",
		RegionKind::StoredClause(StoredRole::LiveFetch) => "stored.live_fetch",
	}
}

/// Every region kind, payloads included. [`region_kind_from_name`] reads names through it, so a
/// kind missing here prints but does not parse; the round-trip test over this list catches a
/// name that cannot be read back.
pub(super) const ALL_REGION_KINDS: &[RegionKind] = &[
	RegionKind::Request,
	RegionKind::Transaction(TxnKind::Explicit),
	RegionKind::Transaction(TxnKind::Session),
	RegionKind::Statement(StatementCommit::Auto),
	RegionKind::Statement(StatementCommit::InTxn),
	RegionKind::Block,
	RegionKind::ValueBlock,
	RegionKind::StatementIf,
	RegionKind::ValueIf,
	RegionKind::StatementFor,
	RegionKind::ValueFor,
	RegionKind::Loop,
	RegionKind::Subquery,
	RegionKind::FunctionBody,
	RegionKind::ClosureBody,
	RegionKind::PermissionFrame,
	RegionKind::FieldFrame(FieldRole::Value),
	RegionKind::FieldFrame(FieldRole::Default),
	RegionKind::FieldFrame(FieldRole::Assert),
	RegionKind::FieldFrame(FieldRole::Computed),
	RegionKind::EventFrame,
	RegionKind::SubscriberFrame,
	RegionKind::Clause(ClauseRole::Where),
	RegionKind::Clause(ClauseRole::WriteCond),
	RegionKind::Clause(ClauseRole::OrderKey),
	RegionKind::Clause(ClauseRole::GroupKey),
	RegionKind::Clause(ClauseRole::Split),
	RegionKind::Clause(ClauseRole::Projection),
	RegionKind::Clause(ClauseRole::SetValue),
	RegionKind::Clause(ClauseRole::Content),
	RegionKind::Clause(ClauseRole::InsertValues),
	RegionKind::Clause(ClauseRole::DuplicateUpdate),
	RegionKind::Clause(ClauseRole::ReturnClause),
	RegionKind::Clause(ClauseRole::From),
	RegionKind::Clause(ClauseRole::Limit),
	RegionKind::Clause(ClauseRole::Start),
	RegionKind::Clause(ClauseRole::Timeout),
	RegionKind::Clause(ClauseRole::IndexColumn),
	RegionKind::Clause(ClauseRole::ViewClause),
	RegionKind::Clause(ClauseRole::ReferenceRepair),
	RegionKind::LazyOperand,
	RegionKind::ErrorScope(ErrorScope::ShapeToNone),
	RegionKind::ErrorScope(ErrorScope::MethodBody),
	RegionKind::ErrorScope(ErrorScope::PermissionCheck),
	RegionKind::ChooseArm,
	RegionKind::Pipeline,
	RegionKind::JoinLookupArm,
	RegionKind::WriteStage,
	RegionKind::CatalogUnit,
	RegionKind::Deadline,
	RegionKind::StoredClause(StoredRole::Value),
	RegionKind::StoredClause(StoredRole::Assert),
	RegionKind::StoredClause(StoredRole::Default),
	RegionKind::StoredClause(StoredRole::Computed),
	RegionKind::StoredClause(StoredRole::Permissions),
	RegionKind::StoredClause(StoredRole::When),
	RegionKind::StoredClause(StoredRole::Then),
	RegionKind::StoredClause(StoredRole::Body),
	RegionKind::StoredClause(StoredRole::View),
	RegionKind::StoredClause(StoredRole::IndexColumn),
	RegionKind::StoredClause(StoredRole::Authenticate),
	RegionKind::StoredClause(StoredRole::Context),
	RegionKind::StoredClause(StoredRole::Signup),
	RegionKind::StoredClause(StoredRole::Signin),
	RegionKind::StoredClause(StoredRole::Action),
	RegionKind::StoredClause(StoredRole::Fallback),
	RegionKind::StoredClause(StoredRole::LiveFields),
	RegionKind::StoredClause(StoredRole::LiveCond),
	RegionKind::StoredClause(StoredRole::LiveFetch),
];

/// The region kind a name stands for, read through [`region_kind_name`].
pub(super) fn region_kind_from_name(name: &str) -> Option<RegionKind> {
	static TABLE: OnceLock<HashMap<&'static str, RegionKind>> = OnceLock::new();
	TABLE
		.get_or_init(|| {
			ALL_REGION_KINDS.iter().map(|kind| (region_kind_name(*kind), *kind)).collect()
		})
		.get(name)
		.copied()
}

/// Appends `text` in double quotes, escaping `\`, `"` and newline, so a quoted string is one line.
pub(super) fn push_quoted(out: &mut String, text: &str) {
	out.push('"');
	for c in text.chars() {
		match c {
			'\\' => out.push_str("\\\\"),
			'"' => out.push_str("\\\""),
			'\n' => out.push_str("\\n"),
			other => out.push(other),
		}
	}
	out.push('"');
}

/// Appends the text of `ty`: `unknown`, `token(<class>)`, `fact(<kind>,<source>)` or
/// `hint(<kind>,<source>)`.
///
/// The kind is the SurrealQL text of the kind. A source that names a table holds it quoted.
pub(super) fn push_type(out: &mut String, ty: &TypeKnowledge) {
	match ty {
		TypeKnowledge::Unknown => out.push_str("unknown"),
		TypeKnowledge::Token(class) => {
			out.push_str("token(");
			out.push_str(class.name());
			out.push(')');
		}
		TypeKnowledge::Fact {
			kind,
			source,
		} => {
			out.push_str("fact(");
			kind.fmt_sql(out, SqlFormat::SingleLine);
			out.push(',');
			push_fact_source(out, source);
			out.push(')');
		}
		TypeKnowledge::Hint {
			kind,
			source,
		} => {
			out.push_str("hint(");
			kind.fmt_sql(out, SqlFormat::SingleLine);
			out.push(',');
			push_hint_source(out, source);
			out.push(')');
		}
	}
}

fn push_fact_source(out: &mut String, source: &FactSource) {
	match source {
		FactSource::Literal => out.push_str("literal"),
		FactSource::FunctionParam => out.push_str("param"),
		FactSource::Definition => out.push_str("definition"),
		FactSource::StrictRowMigrated {
			table,
		} => push_table_source(out, "strict_row_migrated", table),
		FactSource::Derived => out.push_str("derived"),
		FactSource::Provenance {
			table,
		} => push_table_source(out, "provenance", table),
	}
}

fn push_hint_source(out: &mut String, source: &HintSource) {
	match source {
		HintSource::DeclaredField {
			table,
		} => push_table_source(out, "declared_field", table),
		HintSource::MidMigration {
			table,
		} => push_table_source(out, "mid_migration", table),
		HintSource::Annotation => out.push_str("annotation"),
	}
}

fn push_table_source(out: &mut String, name: &str, table: &TableName) {
	out.push_str(name);
	out.push('(');
	push_quoted(out, table.as_str());
	out.push(')');
}

/// The table `name("<table>")` holds, or `None` for any other text.
fn table_source(text: &str, name: &str) -> Option<TableName> {
	let inner = text.strip_prefix(name)?.strip_prefix('(')?.strip_suffix(')')?;
	let mut cursor = Cursor::new(inner);
	let table = cursor.quoted().ok()?;
	cursor.at_end().then(|| TableName::from(table))
}

fn fact_source(text: &str) -> Option<FactSource> {
	match text {
		"literal" => Some(FactSource::Literal),
		"param" => Some(FactSource::FunctionParam),
		"definition" => Some(FactSource::Definition),
		"derived" => Some(FactSource::Derived),
		other => table_source(other, "strict_row_migrated")
			.map(|table| FactSource::StrictRowMigrated {
				table,
			})
			.or_else(|| {
				table_source(other, "provenance").map(|table| FactSource::Provenance {
					table,
				})
			}),
	}
}

fn hint_source(text: &str) -> Option<HintSource> {
	match text {
		"annotation" => Some(HintSource::Annotation),
		other => table_source(other, "declared_field")
			.map(|table| HintSource::DeclaredField {
				table,
			})
			.or_else(|| {
				table_source(other, "mid_migration").map(|table| HintSource::MidMigration {
					table,
				})
			}),
	}
}

/// Reads one type at the cursor.
///
/// The kind is read with `crate::syn::kind`, and the text must be what the kind prints back as: a
/// kind whose text does not round-trip is an error, so printing a parsed container yields the text
/// it was parsed from.
pub(super) fn parse_type(c: &mut Cursor<'_>) -> Result<TypeKnowledge, ParseError> {
	if c.eat("unknown") {
		return Ok(TypeKnowledge::Unknown);
	}
	if c.eat("token(") {
		let inner = c.balanced_parens()?;
		return TokenClass::named(inner)
			.map(TypeKnowledge::Token)
			.ok_or_else(|| c.error(format!("unknown token class `{inner}`")));
	}
	let is_fact = c.eat("fact(");
	if !is_fact && !c.eat("hint(") {
		return Err(c.error("expected `fact(`, `hint(`, `token(` or `unknown`"));
	}
	let inner = c.balanced_parens()?;
	let Some(comma) = find_top(inner, |ch| ch == ',') else {
		return Err(c.error("a type holds a kind and a source"));
	};
	let (kind_text, rest) = inner.split_at(comma);
	let source_text = rest.strip_prefix(',').unwrap_or(rest);
	if find_top(source_text, |ch| ch == ',').is_some() {
		return Err(c.error("a type holds a kind and a source"));
	}
	let kind = crate::syn::kind(kind_text)
		.map_err(|failure| c.error(format!("kind `{kind_text}`: {failure}")))?;
	if kind.to_sql() != kind_text {
		return Err(c.error(format!("kind `{kind_text}` does not print back as itself")));
	}
	if is_fact {
		let source = fact_source(source_text)
			.ok_or_else(|| c.error(format!("unknown fact source `{source_text}`")))?;
		Ok(TypeKnowledge::Fact {
			kind,
			source,
		})
	} else {
		let source = hint_source(source_text)
			.ok_or_else(|| c.error(format!("unknown hint source `{source_text}`")))?;
		Ok(TypeKnowledge::Hint {
			kind,
			source,
		})
	}
}

/// Appends ` loc(s<stmt>[:<start>..<end>] <origin>)`.
///
/// A catalog origin prints its key and version as the quoted `Debug` text, which carries no
/// grammar of its own.
pub(super) fn push_loc(out: &mut String, loc: &Location) {
	use std::fmt::Write as _;

	let _ = write!(out, " loc(s{}", loc.stmt);
	if let Some(span) = loc.span {
		let _ = write!(out, ":{}..{}", span.start, span.end);
	}
	match &loc.origin {
		LocOrigin::Query | LocOrigin::Written(_) => out.push_str(" query)"),
		LocOrigin::Synthesized => out.push_str(" synth)"),
		LocOrigin::Catalog(held) => {
			out.push_str(" catalog(");
			push_quoted(out, &format!("{:?}", **held));
			out.push_str("))");
		}
	}
}

/// Reads a location after its `loc(`.
pub(super) fn parse_loc(c: &mut Cursor<'_>) -> Result<Location, ParseError> {
	c.expect("s")?;
	let stmt = c.number()?;
	let span = if c.eat(":") {
		let start = c.number()?;
		c.expect("..")?;
		let end = c.number()?;
		Some(Span {
			start,
			end,
		})
	} else {
		None
	};
	c.skip_blanks();
	let origin = if c.eat("query") {
		LocOrigin::Query
	} else if c.eat("synth") {
		LocOrigin::Synthesized
	} else if c.eat("catalog(") {
		return Err(c.error("a catalog origin cannot be read back"));
	} else {
		return Err(c.error("expected `query`, `synth` or `catalog(`"));
	};
	c.expect(")")?;
	Ok(Location {
		stmt,
		span,
		origin,
	})
}

/// The byte offset of the first character that satisfies `stop` at bracket depth zero and outside
/// quotes, or `None` when there is none, when a closing bracket has no opener, or at a newline.
///
/// Brackets are `()`, `[]`, `{}` and `<>`. Quotes are `'`, `"` and `` ` `` with backslash escapes,
/// and `⟨` to `⟩`. A closing bracket at depth zero satisfies `stop` when `stop` accepts it, so a
/// caller that looks for the `)` closing a group finds it.
pub(super) fn find_top(text: &str, stop: impl Fn(char) -> bool) -> Option<usize> {
	let mut depth = 0usize;
	let mut quote: Option<char> = None;
	let mut escaped = false;
	for (at, c) in text.char_indices() {
		if c == '\n' {
			return None;
		}
		if let Some(close) = quote {
			if escaped {
				escaped = false;
			} else if c == '\\' {
				escaped = true;
			} else if c == close {
				quote = None;
			}
			continue;
		}
		match c {
			'\'' | '"' | '`' => quote = Some(c),
			'⟨' => quote = Some('⟩'),
			'(' | '[' | '{' | '<' => depth += 1,
			')' | ']' | '}' | '>' => {
				if depth == 0 {
					return stop(c).then_some(at);
				}
				depth -= 1;
			}
			other => {
				if depth == 0 && stop(other) {
					return Some(at);
				}
			}
		}
	}
	None
}

/// A position in the text being parsed, with the 1-based line it stands on.
///
/// Every token is read from the cursor, so an error carries the line it was met on.
pub(super) struct Cursor<'a> {
	text: &'a str,
	pos: usize,
	line: u32,
}

impl<'a> Cursor<'a> {
	pub(super) fn new(text: &'a str) -> Self {
		Self {
			text,
			pos: 0,
			line: 1,
		}
	}

	/// The 1-based line under the cursor.
	pub(super) fn line(&self) -> u32 {
		self.line
	}

	/// The text not read yet.
	pub(super) fn rest(&self) -> &'a str {
		self.text.get(self.pos..).unwrap_or("")
	}

	pub(super) fn at_end(&self) -> bool {
		self.pos >= self.text.len()
	}

	/// An error at the cursor's line.
	pub(super) fn error(&self, detail: impl Into<String>) -> ParseError {
		ParseError {
			line: self.line,
			detail: detail.into(),
		}
	}

	pub(super) fn peek(&self) -> Option<char> {
		self.rest().chars().next()
	}

	/// Reads `token`, which holds no newline, when the text continues with it.
	pub(super) fn eat(&mut self, token: &str) -> bool {
		if self.rest().starts_with(token) {
			self.pos += token.len();
			true
		} else {
			false
		}
	}

	/// Reads `token`, or fails naming it.
	pub(super) fn expect(&mut self, token: &str) -> Result<(), ParseError> {
		if self.eat(token) {
			Ok(())
		} else {
			Err(self.error(format!("expected `{token}`")))
		}
	}

	/// Skips spaces, tabs and carriage returns. Never a newline.
	pub(super) fn skip_blanks(&mut self) {
		self.take_while(|c| matches!(c, ' ' | '\t' | '\r'));
	}

	/// Reads a newline and moves to the next line.
	pub(super) fn eat_newline(&mut self) -> bool {
		if self.rest().starts_with('\n') {
			self.pos += 1;
			self.line += 1;
			true
		} else {
			false
		}
	}

	/// Reads the longest run of characters that satisfy `keep`. `keep` never accepts a newline.
	pub(super) fn take_while(&mut self, keep: impl Fn(char) -> bool) -> &'a str {
		let rest = self.rest();
		let len = rest.find(|c: char| !keep(c)).unwrap_or(rest.len());
		let (taken, _) = rest.split_at(len);
		self.pos += len;
		taken
	}

	/// Reads the rest of the line, leaving the newline.
	pub(super) fn rest_of_line(&mut self) -> &'a str {
		self.take_while(|c| c != '\n')
	}

	/// Reads a non-negative decimal number.
	pub(super) fn number(&mut self) -> Result<u32, ParseError> {
		let digits = self.take_while(|c| c.is_ascii_digit());
		digits.parse().map_err(|_| self.error("expected a number"))
	}

	/// Reads a double-quoted string, decoding `\\`, `\"` and `\n`.
	pub(super) fn quoted(&mut self) -> Result<String, ParseError> {
		if !self.eat("\"") {
			return Err(self.error("expected a quoted string"));
		}
		let mut out = String::new();
		let mut chars = self.rest().char_indices();
		loop {
			match chars.next() {
				None | Some((_, '\n')) => return Err(self.error("unterminated quoted string")),
				Some((at, '"')) => {
					self.pos += at + 1;
					return Ok(out);
				}
				Some((_, '\\')) => match chars.next() {
					Some((_, '\\')) => out.push('\\'),
					Some((_, '"')) => out.push('"'),
					Some((_, 'n')) => out.push('\n'),
					Some(_) | None => {
						return Err(self.error("unknown escape in a quoted string"));
					}
				},
				Some((_, c)) => out.push(c),
			}
		}
	}

	/// Reads up to the `)` that closes a group whose `(` was just read, and past it. Brackets and
	/// quotes inside nest as in [`find_top`].
	pub(super) fn balanced_parens(&mut self) -> Result<&'a str, ParseError> {
		let rest = self.rest();
		let Some(at) = find_top(rest, |c| c == ')') else {
			return Err(self.error("unbalanced parentheses"));
		};
		let (inner, _) = rest.split_at(at);
		self.pos += at + 1;
		Ok(inner)
	}
}
