//! Tests that span the expression dialect's files, and the host they bind through.
//!
//! [`TestHost`] implements [`ExprHost`] over a container and its rewriter. It records every op the
//! binder creates and every callback that reaches the host, so a test states what was built
//! without reading the container back.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::{Strand, TableName};

use super::ops::{
	ClosureAttrs, ComputedReadAttrs, ConvertAttrs, ConvertMode, ConvertSite, DestructureEntry,
	DispatchTarget, ExprOp, ExprTag, KeyBound, KeyShape, NowWhich, OnFail, ShortCircuitOp,
};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::build::expr::{
	Built, Control, ExprHost, FieldRead, LoopSite, NameRef, bind_body, bind_expr,
};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope};
use crate::exec::ir::core::location::{LocOrigin, Location};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::CatalogObjectKey;
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::{ErrorScope, RegionKind};
use crate::exec::ir::iface::types::{FactSource, TypeKnowledge};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::logical::footprint::CatalogKind;
use crate::exec::logical::pipelines::AggregateCombine;
use crate::exec::vm::compile_cache::ObjectVersion;
use crate::expr::logical::{
	LogicalBreak, LogicalCall, LogicalClosure, LogicalDestructureField, LogicalExpr, LogicalIf,
	LogicalLiteral, LogicalLookup, LogicalOp, LogicalPostfixOperator, LogicalRecordIdKey,
	LogicalRecordIdKeyRange, LogicalRecordIdLit, LogicalRecurse,
};
use crate::expr::lookup::LookupKind;
use crate::expr::part::Recurse;
use crate::expr::{BinaryOperator, Constant, Function, Kind, Mock, Param, PrefixOperator};
use crate::val::{Bytes, Datetime, Duration, File, Geometry, Number, Regex, Uuid, Value};

/// One op the binder created through [`ExprHost::create`].
pub(crate) struct Created {
	pub(crate) id: OpId,
	pub(crate) kind: OpKind,
	pub(crate) operands: SmallVec<[ValueId; 4]>,
	pub(crate) results: SmallVec<[TypeKnowledge; 2]>,
	/// The index in [`TestHost::entered`] of the innermost region open when the op was created.
	pub(crate) region: Option<usize>,
}

/// One region the binder entered.
pub(crate) struct Entered {
	pub(crate) owner: OpId,
	pub(crate) kind: RegionKind,
	pub(crate) block: BlockId,
}

impl Created {
	/// The op, which the binder only builds in the expression dialect.
	pub(crate) fn op(&self) -> &ExprOp {
		match &self.kind {
			OpKind::Expr(op) => op,
			OpKind::Logical(_) | OpKind::Phys(_) => panic!("the binder created a non-expr op"),
		}
	}
}

/// What a callback of the host other than `create` was asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HostCall {
	EnterRegion(RegionKind),
	LeaveRegion,
	ValueStatement,
	ElementFilter,
	GraphStep,
	Broadcast {
		flatten: bool,
	},
	Recurse,
	ClosureSite {
		slot: usize,
	},
	MethodClosureSite {
		slot: usize,
	},
	Closure,
	Control(Control),
}

/// A bind context over one container, whose names, row, current value, timestamp and fields a test
/// sets directly.
pub(crate) struct TestHost {
	pub(crate) ir: Ir,
	facts: FactCache,
	registry: FunctionRegistry,
	/// The block the next op goes to: the innermost entered region's, else the top region's.
	blocks: Vec<BlockId>,
	pub(crate) names: HashMap<String, ValueId>,
	pub(crate) row: Option<ValueId>,
	/// What a field read or `@` reads in place of `row`, while a body over a value is bound.
	pub(crate) field_base: Option<ValueId>,
	pub(crate) current: Option<ValueId>,
	pub(crate) timestamp: Option<ValueId>,
	/// The receivers the binder asked a step root for, in order.
	pub(crate) step_roots: Vec<ValueId>,
	/// The builtins the request's capabilities deny.
	pub(crate) denied: HashSet<String>,
	pub(crate) fields: HashMap<String, FieldRead>,
	pub(crate) aggregating: bool,
	pub(crate) row_names: SmallVec<[ValueId; 2]>,
	next_site: u32,
	pub(crate) created: Vec<Created>,
	pub(crate) calls: Vec<HostCall>,
	/// Every region entered, in the order of entry.
	pub(crate) entered: Vec<Entered>,
	/// The indexes in `entered` of the regions open now, innermost last.
	open: Vec<usize>,
	/// When set, a `create` after this many ops were created fails.
	pub(crate) fail_at: Option<usize>,
}

impl TestHost {
	pub(crate) fn new() -> Self {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let top = ir
			.region(ir.top())
			.and_then(|region| region.blocks.first().copied())
			.expect("the top region holds one block");
		Self {
			ir,
			facts: FactCache::new(),
			registry: FunctionRegistry::with_builtins(),
			blocks: vec![top],
			names: HashMap::new(),
			row: None,
			field_base: None,
			current: None,
			timestamp: None,
			step_roots: Vec::new(),
			denied: HashSet::new(),
			fields: HashMap::new(),
			aggregating: false,
			row_names: SmallVec::new(),
			next_site: 0,
			created: Vec::new(),
			calls: Vec::new(),
			entered: Vec::new(),
			open: Vec::new(),
			fail_at: None,
		}
	}

	/// A host with a row in hand, for a test that reads fields.
	pub(crate) fn with_row() -> Self {
		let mut host = Self::new();
		let row = host.fresh();
		host.row = Some(row);
		host
	}

	/// A value no test expression made, for a name, a row or a timestamp to be bound to. The op
	/// that defines it is not recorded.
	pub(crate) fn fresh(&mut self) -> ValueId {
		let id = self
			.create_op(
				OpKind::Expr(ExprOp::Literal(Box::new(Value::Number(Number::Int(0))))),
				SmallVec::new(),
				smallvec![TypeKnowledge::Unknown],
			)
			.expect("a literal is created");
		self.result_of(id)
	}

	fn create_op(
		&mut self,
		kind: OpKind,
		operands: SmallVec<[ValueId; 4]>,
		results: SmallVec<[TypeKnowledge; 2]>,
	) -> Result<OpId, IrError> {
		let block = *self.blocks.last().expect("a block is current");
		Rewriter::new(&mut self.ir, &mut self.facts).create(
			InsertPoint::End(block),
			OpSpec {
				kind,
				operands,
				results,
				frame: FrameId::new(0),
				snapshot: SnapshotId::new(0),
				loc: Location {
					stmt: 0,
					span: None,
					origin: LocOrigin::Query,
				},
			},
		)
	}

	/// The first result of `op`.
	pub(crate) fn result_of(&self, op: OpId) -> ValueId {
		self.ir.op(op).and_then(|data| data.results.first().copied()).expect("the op has a result")
	}

	/// Binds `expr` at the end of the current block.
	pub(crate) fn bind(&mut self, expr: &LogicalExpr) -> Result<Built, IrError> {
		bind_expr(self, expr)
	}

	/// The tags of the created ops, in creation order.
	pub(crate) fn tags(&self) -> Vec<ExprTag> {
		self.created.iter().map(|created| created.op().tag()).collect()
	}

	/// The `index`th created op.
	pub(crate) fn op(&self, index: usize) -> &Created {
		&self.created[index]
	}

	/// The value the `index`th created op defines.
	pub(crate) fn value_of(&self, index: usize) -> ValueId {
		self.result_of(self.created[index].id)
	}

	/// The tags of the ops created directly in region `region` (`None` for the top block), in
	/// creation order.
	pub(crate) fn tags_in(&self, region: Option<usize>) -> Vec<ExprTag> {
		self.created
			.iter()
			.filter(|created| created.region == region)
			.map(|created| created.op().tag())
			.collect()
	}

	/// The ops created directly in region `region`, in creation order.
	pub(crate) fn ops_in(&self, region: usize) -> Vec<&Created> {
		self.created.iter().filter(|created| created.region == Some(region)).collect()
	}

	/// The `index`th argument of the block of region `region`.
	pub(crate) fn region_arg(&self, region: usize, index: usize) -> ValueId {
		self.ir
			.block(self.entered[region].block)
			.and_then(|block| block.args.get(index).copied())
			.expect("the region block holds the argument")
	}
}

impl ExprHost for TestHost {
	fn ir(&self) -> &Ir {
		&self.ir
	}

	fn registry(&self) -> &FunctionRegistry {
		&self.registry
	}

	fn create(
		&mut self,
		kind: OpKind,
		operands: SmallVec<[ValueId; 4]>,
		results: SmallVec<[TypeKnowledge; 2]>,
	) -> Result<OpId, IrError> {
		if self.fail_at.is_some_and(|limit| self.created.len() >= limit) {
			return Err(IrError::Stale("injected failure"));
		}
		let id = self.create_op(kind.clone(), operands.clone(), results.clone())?;
		self.created.push(Created {
			id,
			kind,
			operands,
			results,
			region: self.open.last().copied(),
		});
		Ok(id)
	}

	fn enter_region(
		&mut self,
		owner: OpId,
		kind: RegionKind,
		args: &[TypeKnowledge],
	) -> Result<(RegionId, BlockId), IrError> {
		let (region, block) =
			Rewriter::new(&mut self.ir, &mut self.facts).create_region(owner, kind, args)?;
		self.blocks.push(block);
		self.open.push(self.entered.len());
		self.entered.push(Entered {
			owner,
			kind,
			block,
		});
		self.calls.push(HostCall::EnterRegion(kind));
		Ok((region, block))
	}

	fn leave_region(&mut self) -> Result<(), IrError> {
		if self.blocks.len() < 2 {
			return Err(IrError::Stale("no region entered"));
		}
		self.blocks.pop();
		self.open.pop();
		self.calls.push(HostCall::LeaveRegion);
		Ok(())
	}

	fn lookup(&self, name: &str) -> NameRef {
		self.names.get(name).copied().map_or(NameRef::Free, NameRef::Value)
	}

	fn row(&mut self) -> Result<ValueId, IrError> {
		self.field_base.or(self.row).ok_or(IrError::Stale("no row"))
	}

	fn current(&mut self) -> Result<ValueId, IrError> {
		self.current.ok_or(IrError::Stale("no current value"))
	}

	fn replace_current(&mut self, value: Option<ValueId>) -> Option<ValueId> {
		std::mem::replace(&mut self.current, value)
	}

	fn replace_row(&mut self, value: Option<ValueId>) -> Option<ValueId> {
		std::mem::replace(&mut self.row, value)
	}

	fn replace_field_base(&mut self, value: Option<ValueId>) -> Option<ValueId> {
		std::mem::replace(&mut self.field_base, value)
	}

	fn step_root(&mut self, receiver: ValueId) -> Result<ValueId, IrError> {
		self.step_roots.push(receiver);
		Ok(self.row.unwrap_or(receiver))
	}

	fn timestamp(&self) -> Option<ValueId> {
		self.timestamp
	}

	fn allows_function(&self, name: &str) -> bool {
		!self.denied.contains(name)
	}

	fn field(&self, name: &Strand) -> FieldRead {
		self.fields.get(name.as_str()).cloned().unwrap_or(FieldRead::Plain)
	}

	fn aggregating(&self) -> bool {
		self.aggregating
	}

	fn next_convert_site(&mut self) -> ConvertSite {
		let site = ConvertSite(self.next_site);
		self.next_site += 1;
		site
	}

	fn row_name_operands(&self) -> SmallVec<[ValueId; 2]> {
		self.row_names.clone()
	}

	fn bind_value_statement(&mut self, _op: &LogicalOp) -> Result<Built, IrError> {
		self.calls.push(HostCall::ValueStatement);
		Ok(Built::Value(self.fresh()))
	}

	fn bind_loop_site(&mut self, site: LoopSite<'_>) -> Result<Built, IrError> {
		self.calls.push(match site {
			LoopSite::Closure {
				slot,
				..
			} => HostCall::ClosureSite {
				slot,
			},
			LoopSite::MethodClosure {
				slot,
				..
			} => HostCall::MethodClosureSite {
				slot,
			},
			LoopSite::ElementFilter {
				..
			} => HostCall::ElementFilter,
			LoopSite::GraphStep {
				..
			} => HostCall::GraphStep,
			LoopSite::Broadcast {
				flatten,
				..
			} => HostCall::Broadcast {
				flatten,
			},
			LoopSite::Recurse(_) => HostCall::Recurse,
		});
		Ok(Built::Value(self.fresh()))
	}

	fn bind_closure(&mut self, _closure: &LogicalClosure) -> Result<ValueId, IrError> {
		self.calls.push(HostCall::Closure);
		Ok(self.fresh())
	}

	fn bind_control(&mut self, control: Control) -> Result<Built, IrError> {
		self.calls.push(HostCall::Control(control));
		Ok(Built::Diverged)
	}

	fn in_recursion_body(&self) -> bool {
		true
	}
}

fn strand(text: &str) -> Strand {
	Strand::new(text)
}

fn table(text: &str) -> TableName {
	TableName::new(strand(text))
}

fn int(n: i64) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::Integer(n))
}

fn text(s: &str) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::String(strand(s)))
}

fn field(name: &str) -> LogicalExpr {
	LogicalExpr::Field(strand(name))
}

fn param(name: &str) -> LogicalExpr {
	LogicalExpr::Param(Param::from(strand(name)))
}

fn boxed(expr: LogicalExpr) -> Box<LogicalExpr> {
	Box::new(expr)
}

fn int_value(n: i64) -> Value {
	Value::Number(Number::Int(n))
}

fn built_value(built: Built) -> ValueId {
	match built {
		Built::Value(v) => v,
		Built::Diverged => panic!("the expression diverged"),
	}
}

/// The value of the single `literal` op the binder created for `expr`.
fn literal_of(expr: &LogicalExpr) -> Result<Value, IrError> {
	let mut host = TestHost::new();
	host.bind(expr)?;
	assert_eq!(host.tags(), [ExprTag::Literal], "{expr:?}");
	let ExprOp::Literal(value) = host.op(0).op() else {
		panic!("the first op is not a literal");
	};
	Ok((**value).clone())
}

#[test]
fn literal_value_mirrors_the_planner() -> Result<(), IrError> {
	let datetime = Datetime::from(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
	let regex: Regex = "a+".parse().expect("a valid pattern");
	let rows = [
		(LogicalLiteral::None, Value::None),
		(LogicalLiteral::Null, Value::Null),
		(LogicalLiteral::UnboundedRange, Value::Range(Box::new(crate::val::Range::unbounded()))),
		(LogicalLiteral::Bool(true), Value::Bool(true)),
		(LogicalLiteral::Float(1.5), Value::Number(Number::Float(1.5))),
		(LogicalLiteral::Integer(7), int_value(7)),
		(
			LogicalLiteral::Decimal(rust_decimal::Decimal::ONE),
			Value::Number(Number::Decimal(rust_decimal::Decimal::ONE)),
		),
		(LogicalLiteral::String(strand("s")), Value::String(strand("s"))),
		(LogicalLiteral::Bytes(Bytes::from(vec![1_u8])), Value::Bytes(Bytes::from(vec![1_u8]))),
		(LogicalLiteral::Regex(regex.clone()), Value::Regex(regex)),
		(LogicalLiteral::Duration(Duration::new(1, 0)), Value::Duration(Duration::new(1, 0))),
		(LogicalLiteral::Datetime(datetime), Value::Datetime(datetime)),
		(LogicalLiteral::Uuid(Uuid::nil()), Value::Uuid(Uuid::nil())),
		(
			LogicalLiteral::Geometry(Geometry::Collection(Vec::new())),
			Value::Geometry(Geometry::Collection(Vec::new())),
		),
		(
			LogicalLiteral::File(File::new("b".into(), "k".into())),
			Value::File(File::new("b".into(), "k".into())),
		),
	];
	for (lit, expected) in rows {
		let label = format!("{lit:?}");
		assert_eq!(literal_of(&LogicalExpr::Literal(lit))?, expected, "{label}");
	}
	Ok(())
}

#[test]
fn a_literal_is_typed_by_its_value() -> Result<(), IrError> {
	let mut host = TestHost::new();
	host.bind(&int(1))?;
	assert_eq!(
		host.op(0).results.as_slice(),
		[TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::Literal,
		}]
	);
	Ok(())
}

#[test]
fn literal_aggregates_collapse() -> Result<(), IrError> {
	// An array of literals is one literal, however it nests.
	let array = LogicalExpr::Array(vec![int(1), LogicalExpr::Array(vec![int(2), text("a")])]);
	assert_eq!(
		literal_of(&array)?,
		Value::Array(crate::val::Array(vec![
			int_value(1),
			Value::Array(crate::val::Array(vec![int_value(2), Value::String(strand("a"))])),
		]))
	);
	assert_eq!(literal_of(&LogicalExpr::Array(Vec::new()))?, Value::Array(Default::default()));

	// A set removes duplicates, as evaluation does.
	let mut expected = crate::val::Set::new();
	expected.insert(int_value(1));
	assert_eq!(literal_of(&LogicalExpr::Set(vec![int(1), int(1)]))?, Value::Set(expected));

	// An object is built through a sorted map: keys sort, and a repeated key keeps its last value.
	let object = LogicalExpr::Object(vec![
		("b".to_owned(), int(1)),
		("a".to_owned(), int(2)),
		("b".to_owned(), int(3)),
	]);
	let mut map = std::collections::BTreeMap::new();
	map.insert("a".to_owned(), int_value(2));
	map.insert("b".to_owned(), int_value(3));
	assert_eq!(literal_of(&object)?, Value::Object(crate::val::Object::from(map)));
	Ok(())
}

#[test]
fn an_array_with_a_field_does_not() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let built = host.bind(&LogicalExpr::Array(vec![int(1), field("a"), int(2)]))?;
	assert_eq!(host.tags(), [ExprTag::Literal, ExprTag::Field, ExprTag::Literal, ExprTag::Array]);
	let operands: Vec<ValueId> = (0..3).map(|i| host.value_of(i)).collect();
	assert_eq!(host.op(3).operands.as_slice(), operands.as_slice());
	assert_eq!(built, Built::Value(host.value_of(3)));

	// A collapsing array inside a non-collapsing one is still one literal.
	let mut host = TestHost::with_row();
	host.bind(&LogicalExpr::Array(vec![LogicalExpr::Array(vec![int(1)]), field("a")]))?;
	assert_eq!(host.tags(), [ExprTag::Literal, ExprTag::Field, ExprTag::Array]);

	// An object keeps its keys, in source order, and a set binds its elements.
	let mut host = TestHost::with_row();
	host.bind(&LogicalExpr::Object(vec![("b".to_owned(), field("x")), ("a".to_owned(), int(1))]))?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Literal, ExprTag::Object]);
	let ExprOp::Object {
		keys,
	} = host.op(2).op()
	else {
		panic!("not an object");
	};
	assert_eq!(&**keys, ["b".to_owned(), "a".to_owned()]);
	let mut host = TestHost::with_row();
	host.bind(&LogicalExpr::Set(vec![field("x")]))?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Set]);
	Ok(())
}

#[test]
fn bound_names_build_no_op() -> Result<(), IrError> {
	let mut host = TestHost::new();
	let bound = host.fresh();
	host.names.insert("x".to_owned(), bound);
	assert_eq!(host.bind(&param("x"))?, Built::Value(bound));
	assert!(host.created.is_empty());
	Ok(())
}

#[test]
fn session_names_build_session_param() -> Result<(), IrError> {
	for name in ["auth", "session", "token", "access"] {
		let mut host = TestHost::new();
		host.bind(&param(name))?;
		assert_eq!(host.tags(), [ExprTag::SessionParam], "{name}");
		let ExprOp::SessionParam {
			name: built,
		} = host.op(0).op()
		else {
			panic!("not a session param");
		};
		assert_eq!(built.as_str(), name);
		assert!(host.op(0).operands.is_empty());
	}
	Ok(())
}

#[test]
fn subscriber_session_names_are_arguments() -> Result<(), IrError> {
	let mut host = TestHost::new();
	let mut arguments = HashMap::new();
	for name in ["auth", "session", "token", "access"] {
		let argument = host.fresh();
		host.names.insert(name.to_owned(), argument);
		arguments.insert(name, argument);
	}
	for (name, argument) in arguments {
		assert_eq!(host.bind(&param(name))?, Built::Value(argument), "{name}");
	}
	assert!(host.created.is_empty());
	Ok(())
}

#[test]
fn free_names_build_free_param() -> Result<(), IrError> {
	// A free name is never specialised to a stored parameter, and the session names are the only
	// ones that read the session.
	for name in ["x", "Auth", "authority", "this", "before"] {
		let mut host = TestHost::new();
		host.bind(&param(name))?;
		assert_eq!(host.tags(), [ExprTag::FreeParam], "{name}");
		let ExprOp::FreeParam {
			name: built,
		} = host.op(0).op()
		else {
			panic!("not a free param");
		};
		assert_eq!(built.as_str(), name);
	}
	Ok(())
}

#[test]
fn field_follows_the_host() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let row = host.row.expect("a row");
	host.bind(&field("a"))?;
	assert_eq!(host.tags(), [ExprTag::Field]);
	assert_eq!(host.op(0).operands.as_slice(), [row]);
	let ExprOp::Field {
		name,
	} = host.op(0).op()
	else {
		panic!("not a field");
	};
	assert_eq!(name.as_str(), "a");

	let attrs = ComputedReadAttrs {
		field: strand("c"),
		def: (
			CatalogObjectKey {
				kind: CatalogKind::Field,
				ns: Some(NamespaceId(0)),
				db: Some(DatabaseId(0)),
				table: Some(table("t")),
				name: None,
			},
			ObjectVersion::Stamped(uuid::Uuid::nil()),
		),
	};
	let mut host = TestHost::with_row();
	host.fields.insert("c".to_owned(), FieldRead::Computed(Box::new(attrs.clone())));
	host.bind(&field("c"))?;
	assert_eq!(host.tags(), [ExprTag::ComputedRead]);
	let ExprOp::ComputedRead(built) = host.op(0).op() else {
		panic!("not a computed read");
	};
	assert_eq!(**built, attrs);
	// Another field of the same row stays plain.
	host.bind(&field("d"))?;
	assert_eq!(host.tags(), [ExprTag::ComputedRead, ExprTag::Field]);
	Ok(())
}

#[test]
fn a_field_read_needs_a_row() {
	let mut host = TestHost::new();
	assert!(host.bind(&field("a")).is_err());
	assert!(host.bind(&LogicalExpr::Doc).is_err());
	assert!(host.created.is_empty());
}

#[test]
fn doc_reads_the_row_and_current_builds_no_op() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let row = host.row.expect("a row");
	host.bind(&LogicalExpr::Doc)?;
	assert_eq!(host.tags(), [ExprTag::Doc]);
	assert_eq!(host.op(0).operands.as_slice(), [row]);

	let element = host.fresh();
	let previous = host.replace_current(Some(element));
	assert_eq!(previous, None);
	assert_eq!(host.bind(&LogicalExpr::Current)?, Built::Value(element));
	assert_eq!(host.tags(), [ExprTag::Doc]);

	host.replace_current(None);
	assert!(host.bind(&LogicalExpr::Current).is_err());
	Ok(())
}

#[test]
fn table_mock_and_constant_build_their_op() -> Result<(), IrError> {
	let mut host = TestHost::new();
	host.bind(&LogicalExpr::Table(table("t")))?;
	host.bind(&LogicalExpr::Mock(Mock::Count(table("t"), 3)))?;
	host.bind(&LogicalExpr::Constant(Constant::DurationMax))?;
	assert_eq!(host.tags(), [ExprTag::Table, ExprTag::Mock, ExprTag::Constant]);
	assert!(host.created.iter().all(|created| created.operands.is_empty()));
	let ExprOp::Table {
		name,
	} = host.op(0).op()
	else {
		panic!("not a table");
	};
	assert_eq!(name, &table("t"));
	Ok(())
}

fn record_id(key: LogicalRecordIdKey) -> LogicalExpr {
	LogicalExpr::RecordId(Box::new(LogicalRecordIdLit {
		table: table("t"),
		key,
	}))
}

fn key_shape_of(host: &TestHost, index: usize) -> KeyShape {
	let ExprOp::RecordId(attrs) = host.op(index).op() else {
		panic!("not a record id");
	};
	attrs.key_shape.clone()
}

#[test]
fn a_record_id_stays_an_op_with_its_key_shape() -> Result<(), IrError> {
	// A constant key is not collapsed to a literal.
	let mut host = TestHost::new();
	host.bind(&record_id(LogicalRecordIdKey::Number(1)))?;
	assert_eq!(host.tags(), [ExprTag::RecordId]);
	assert!(host.op(0).operands.is_empty());
	assert_eq!(key_shape_of(&host, 0), KeyShape::Number(1));

	// Elements and values are operands, in key order.
	let mut host = TestHost::with_row();
	host.bind(&record_id(LogicalRecordIdKey::Array(vec![field("a"), int(1)])))?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Literal, ExprTag::RecordId]);
	assert_eq!(
		key_shape_of(&host, 2),
		KeyShape::Array {
			len: 2
		}
	);
	assert_eq!(host.op(2).operands.as_slice(), [host.value_of(0), host.value_of(1)]);

	let mut host = TestHost::with_row();
	host.bind(&record_id(LogicalRecordIdKey::Object(vec![
		("k".to_owned(), field("a")),
		("j".to_owned(), int(1)),
	])))?;
	let KeyShape::Object {
		keys,
	} = key_shape_of(&host, 2)
	else {
		panic!("not an object key");
	};
	assert_eq!(&*keys, ["k".to_owned(), "j".to_owned()]);
	Ok(())
}

#[test]
fn a_record_id_range_binds_start_before_end() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let range = LogicalRecordIdKeyRange {
		start: Bound::Included(LogicalRecordIdKey::Array(vec![field("s")])),
		end: Bound::Excluded(LogicalRecordIdKey::Array(vec![field("e1"), field("e2")])),
	};
	host.bind(&record_id(LogicalRecordIdKey::Range(Box::new(range))))?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Field, ExprTag::Field, ExprTag::RecordId]);
	let names: Vec<&str> = host.created[..3]
		.iter()
		.map(|created| {
			let ExprOp::Field {
				name,
			} = created.op()
			else {
				panic!("not a field");
			};
			name.as_str()
		})
		.collect();
	assert_eq!(names, ["s", "e1", "e2"]);
	let KeyShape::Range(range) = key_shape_of(&host, 3) else {
		panic!("not a range key");
	};
	assert!(matches!(
		range.start,
		Bound::Included(KeyBound::Array {
			len: 1
		})
	));
	assert!(matches!(
		range.end,
		Bound::Excluded(KeyBound::Array {
			len: 2
		})
	));
	assert_eq!(host.op(3).operands.len(), 3);

	// An unbounded end contributes no operand.
	let mut host = TestHost::new();
	let range = LogicalRecordIdKeyRange {
		start: Bound::Included(LogicalRecordIdKey::Number(1)),
		end: Bound::Unbounded,
	};
	host.bind(&record_id(LogicalRecordIdKey::Range(Box::new(range))))?;
	assert_eq!(host.tags(), [ExprTag::RecordId]);
	assert!(host.op(0).operands.is_empty());
	Ok(())
}

#[test]
fn path_steps_bind_base_then_arguments() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let built = host.bind(&LogicalExpr::Access {
		base: boxed(field("a")),
		field: strand("b"),
	})?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Access]);
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
	assert_eq!(built, Built::Value(host.value_of(1)));

	let mut host = TestHost::with_row();
	host.bind(&LogicalExpr::Index {
		base: boxed(field("a")),
		index: boxed(int(0)),
	})?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Literal, ExprTag::Index]);
	assert_eq!(host.op(2).operands.as_slice(), [host.value_of(0), host.value_of(1)]);

	let mut host = TestHost::with_row();
	host.bind(&LogicalExpr::SetField {
		base: boxed(field("a")),
		field: strand("k"),
		value: boxed(int(1)),
	})?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Literal, ExprTag::SetField]);
	assert_eq!(host.op(2).operands.as_slice(), [host.value_of(0), host.value_of(1)]);

	let steps = [
		(LogicalExpr::First(boxed(field("a"))), ExprTag::First),
		(LogicalExpr::Last(boxed(field("a"))), ExprTag::Last),
		(LogicalExpr::Each(boxed(field("a"))), ExprTag::Each),
		(LogicalExpr::RepeatRecurse(boxed(field("a"))), ExprTag::RepeatRecurse),
		(
			LogicalExpr::Flatten {
				base: boxed(field("a")),
				explicit: true,
			},
			ExprTag::Flatten,
		),
	];
	for (expr, tag) in steps {
		let mut host = TestHost::with_row();
		host.bind(&expr)?;
		assert_eq!(host.tags(), [ExprTag::Field, tag]);
		assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
	}

	// `explicit` is part of the op.
	let mut host = TestHost::with_row();
	host.bind(&LogicalExpr::Flatten {
		base: boxed(field("a")),
		explicit: false,
	})?;
	assert!(matches!(
		host.op(1).op(),
		ExprOp::Flatten {
			explicit: false
		}
	));
	Ok(())
}

#[test]
fn a_diverged_operand_stops_binding() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let built = host.bind(&LogicalExpr::Array(vec![field("a"), LogicalExpr::Break, field("b")]))?;
	assert_eq!(built, Built::Diverged);
	assert_eq!(host.tags(), [ExprTag::Field]);
	assert_eq!(host.calls, [HostCall::Control(Control::Break)]);

	let mut host = TestHost::with_row();
	let built = host.bind(&LogicalExpr::Index {
		base: boxed(LogicalExpr::Continue),
		index: boxed(field("a")),
	})?;
	assert_eq!(built, Built::Diverged);
	assert!(host.created.is_empty());
	Ok(())
}

#[test]
fn a_body_yields_its_value_unless_it_diverged() -> Result<(), IrError> {
	let mut host = TestHost::new();
	bind_body(&mut host, &int(1))?;
	assert_eq!(host.tags(), [ExprTag::Literal, ExprTag::Yield]);
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
	assert!(host.op(1).results.is_empty());

	let mut host = TestHost::new();
	bind_body(&mut host, &LogicalExpr::Break)?;
	assert!(host.created.is_empty());
	Ok(())
}

#[test]
fn per_element_forms_and_control_go_to_the_host() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let filter = LogicalExpr::Filter {
		base: boxed(field("a")),
		predicate: boxed(LogicalExpr::Current),
	};
	assert!(matches!(host.bind(&filter)?, Built::Value(_)));
	let broadcast = LogicalExpr::Broadcast {
		base: boxed(field("a")),
		body: boxed(LogicalExpr::Current),
		flatten: true,
	};
	assert!(matches!(host.bind(&broadcast)?, Built::Value(_)));
	assert_eq!(host.bind(&LogicalExpr::Break)?, Built::Diverged);
	assert_eq!(host.bind(&LogicalExpr::Continue)?, Built::Diverged);
	assert_eq!(
		host.calls,
		[
			HostCall::ElementFilter,
			HostCall::Broadcast {
				flatten: true
			},
			HostCall::Control(Control::Break),
			HostCall::Control(Control::Continue),
		]
	);
	// The host built them: the binder created no op of its own.
	assert!(host.created.is_empty());
	let value = built_value(host.bind(&LogicalExpr::Table(table("t")))?);
	assert_eq!(value, host.value_of(0));
	Ok(())
}

fn binary(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: boxed(left),
		op,
		right: boxed(right),
	}
}

fn call(name: &str, args: Vec<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal(name.to_owned()),
		args,
	}))
}

fn if_chain(
	branches: Vec<(LogicalExpr, LogicalExpr)>,
	otherwise: Option<LogicalExpr>,
) -> LogicalExpr {
	LogicalExpr::If(Box::new(LogicalIf {
		branches,
		otherwise,
	}))
}

fn throw(expr: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Throw(boxed(expr))
}

fn closure_literal() -> LogicalExpr {
	LogicalExpr::Closure(Box::new(LogicalClosure {
		args: Vec::new(),
		returns: None,
		body: int(1),
		body_sql: Arc::from("1"),
	}))
}

fn closure_attrs() -> ClosureAttrs {
	ClosureAttrs {
		params: Box::new([]),
		returns: None,
		body_sql: Arc::from("1"),
		captures: Box::new([]),
		reads_row: false,
		reads_root: false,
		literal: Default::default(),
	}
}

/// Asserts that every region the host entered was left, and none before it was entered.
fn assert_balanced(calls: &[HostCall]) {
	let mut depth = 0_i32;
	for call in calls {
		if let HostCall::EnterRegion(_) = call {
			depth += 1;
		} else if let HostCall::LeaveRegion = call {
			depth -= 1;
			assert!(depth >= 0, "a region was left before it was entered: {calls:?}");
		}
	}
	assert_eq!(depth, 0, "a region was left open: {calls:?}");
}

#[test]
fn strict_binary_operators_build_binary() -> Result<(), IrError> {
	for op in
		[BinaryOperator::Add, BinaryOperator::Equal, BinaryOperator::Contain, BinaryOperator::Range]
	{
		let mut host = TestHost::new();
		let built = host.bind(&binary(int(1), op.clone(), int(2)))?;
		assert_eq!(host.tags(), [ExprTag::Literal, ExprTag::Literal, ExprTag::Binary], "{op:?}");
		assert_eq!(host.op(2).operands.as_slice(), [host.value_of(0), host.value_of(1)]);
		assert!(matches!(host.op(2).op(), ExprOp::Binary { op: got } if *got == op));
		assert_eq!(built, Built::Value(host.value_of(2)));
		assert!(host.calls.is_empty());
	}

	// A comparison is known to answer a bool.
	let mut host = TestHost::new();
	host.bind(&binary(int(1), BinaryOperator::Equal, int(2)))?;
	assert_eq!(
		host.op(2).results.as_slice(),
		[TypeKnowledge::Fact {
			kind: Kind::Bool,
			source: FactSource::Derived,
		}]
	);

	// Either side that diverges ends the expression, and the right is not bound after the left.
	let mut host = TestHost::with_row();
	let built = host.bind(&binary(LogicalExpr::Break, BinaryOperator::Add, field("a")))?;
	assert_eq!(built, Built::Diverged);
	assert!(host.created.is_empty());
	Ok(())
}

#[test]
fn short_circuit_rhs_is_a_lazy_region() -> Result<(), IrError> {
	let rows = [
		(BinaryOperator::And, ShortCircuitOp::And),
		(BinaryOperator::Or, ShortCircuitOp::Or),
		(BinaryOperator::NullCoalescing, ShortCircuitOp::NullCoalescing),
		(BinaryOperator::TenaryCondition, ShortCircuitOp::TernaryCondition),
	];
	for (op, lazy) in rows {
		let mut host = TestHost::with_row();
		let built = host.bind(&binary(field("a"), op.clone(), field("b")))?;
		// The left is bound before the owner, and the right only inside the region.
		assert_eq!(
			host.tags(),
			[ExprTag::Field, ExprTag::ShortCircuit, ExprTag::Field, ExprTag::Yield],
			"{op:?}"
		);
		assert_eq!(host.tags_in(None), [ExprTag::Field, ExprTag::ShortCircuit]);
		assert_eq!(host.tags_in(Some(0)), [ExprTag::Field, ExprTag::Yield]);
		assert!(matches!(host.op(1).op(), ExprOp::ShortCircuit { op: got } if *got == lazy));
		assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
		assert_eq!(host.entered[0].kind, RegionKind::LazyOperand);
		assert_eq!(host.entered[0].owner, host.op(1).id);
		assert_eq!(host.op(3).operands.as_slice(), [host.value_of(2)]);
		assert_eq!(built, Built::Value(host.value_of(1)));
		assert_eq!(
			host.calls,
			[HostCall::EnterRegion(RegionKind::LazyOperand), HostCall::LeaveRegion]
		);
	}

	// A left side that diverges enters no region.
	let mut host = TestHost::new();
	let built = host.bind(&binary(LogicalExpr::Break, BinaryOperator::And, int(1)))?;
	assert_eq!(built, Built::Diverged);
	assert!(host.created.is_empty());
	assert!(host.entered.is_empty());

	// A right side that diverges ends its region with its terminator and no `yield`; the
	// expression itself still has a value.
	let mut host = TestHost::with_row();
	let built = host.bind(&binary(field("a"), BinaryOperator::And, throw(text("x"))))?;
	assert_eq!(host.tags_in(Some(0)), [ExprTag::Literal, ExprTag::Throw]);
	assert_eq!(built, Built::Value(host.value_of(1)));
	assert_balanced(&host.calls);
	Ok(())
}

#[test]
fn prefix_and_postfix_build_their_op() -> Result<(), IrError> {
	let prefixes = [
		PrefixOperator::Not,
		PrefixOperator::Positive,
		PrefixOperator::Negate,
		PrefixOperator::Range,
		PrefixOperator::RangeInclusive,
	];
	for op in prefixes {
		let mut host = TestHost::with_row();
		let built = host.bind(&LogicalExpr::Prefix {
			op: op.clone(),
			expr: boxed(field("a")),
		})?;
		assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Prefix], "{op:?}");
		assert!(matches!(host.op(1).op(), ExprOp::Prefix(got) if **got == op));
		assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
		assert_eq!(built, Built::Value(host.value_of(1)));
	}

	let mut host = TestHost::with_row();
	host.bind(&LogicalExpr::Prefix {
		op: PrefixOperator::Not,
		expr: boxed(field("a")),
	})?;
	assert_eq!(
		host.op(1).results.as_slice(),
		[TypeKnowledge::Fact {
			kind: Kind::Bool,
			source: FactSource::Derived,
		}]
	);

	for op in [LogicalPostfixOperator::Range, LogicalPostfixOperator::RangeSkip] {
		let mut host = TestHost::with_row();
		host.bind(&LogicalExpr::Postfix {
			expr: boxed(field("a")),
			op: op.clone(),
		})?;
		assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Postfix], "{op:?}");
		assert!(matches!(host.op(1).op(), ExprOp::Postfix { op: got } if *got == op));
		assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
	}

	let mut host = TestHost::new();
	let built = host.bind(&LogicalExpr::Prefix {
		op: PrefixOperator::Negate,
		expr: boxed(LogicalExpr::Break),
	})?;
	assert_eq!(built, Built::Diverged);
	assert!(host.created.is_empty());
	Ok(())
}

#[test]
fn cast_takes_a_fresh_site_each_time() -> Result<(), IrError> {
	let cast = |kind: Kind, expr: LogicalExpr| LogicalExpr::Prefix {
		op: PrefixOperator::Cast(kind),
		expr: boxed(expr),
	};
	let mut host = TestHost::with_row();
	host.bind(&binary(
		cast(Kind::Int, field("a")),
		BinaryOperator::Add,
		cast(Kind::String, field("b")),
	))?;
	assert_eq!(
		host.tags(),
		[ExprTag::Field, ExprTag::Convert, ExprTag::Field, ExprTag::Convert, ExprTag::Binary]
	);
	let attrs = |host: &TestHost, index: usize| {
		let ExprOp::Convert(attrs) = host.op(index).op() else {
			panic!("not a convert");
		};
		(**attrs).clone()
	};
	assert_eq!(
		attrs(&host, 1),
		ConvertAttrs {
			mode: ConvertMode::Cast,
			kind: Kind::Int,
			site: ConvertSite(0),
			on_fail: OnFail::Error,
		}
	);
	assert_eq!(
		attrs(&host, 3),
		ConvertAttrs {
			mode: ConvertMode::Cast,
			kind: Kind::String,
			site: ConvertSite(1),
			on_fail: OnFail::Error,
		}
	);
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
	assert_eq!(
		host.op(1).results.as_slice(),
		[TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::Derived,
		}]
	);

	// The same cast bound again is a different conversion, and a cast whose operand diverged
	// takes no site.
	assert_eq!(host.bind(&cast(Kind::Int, LogicalExpr::Break))?, Built::Diverged);
	host.bind(&cast(Kind::Int, field("a")))?;
	let last = host.created.len() - 1;
	assert_eq!(attrs(&host, last).site, ConvertSite(2));
	Ok(())
}

#[test]
fn method_dispatch_precedes_arguments() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let built = host.bind(&LogicalExpr::Method {
		base: boxed(field("a")),
		name: strand("len"),
		args: vec![field("b"), int(1)],
	})?;
	assert_eq!(
		host.tags(),
		[ExprTag::Field, ExprTag::Dispatch, ExprTag::Field, ExprTag::Literal, ExprTag::Method]
	);
	let ExprOp::Dispatch(target) = host.op(1).op() else {
		panic!("not a dispatch");
	};
	assert_eq!(**target, DispatchTarget::Method(strand("len")));
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
	assert_eq!(
		host.op(4).operands.as_slice(),
		[host.value_of(1), host.value_of(0), host.value_of(2), host.value_of(3)]
	);
	assert!(matches!(host.op(4).op(), ExprOp::Method { name } if name.as_str() == "len"));
	assert_eq!(built, Built::Value(host.value_of(4)));

	// No arguments: the operands are the dispatch result and the base.
	let mut host = TestHost::with_row();
	host.bind(&LogicalExpr::Method {
		base: boxed(field("a")),
		name: strand("len"),
		args: Vec::new(),
	})?;
	assert_eq!(host.op(2).operands.as_slice(), [host.value_of(1), host.value_of(0)]);

	// The base that diverges binds no dispatch; an argument that diverges stops the rest.
	let mut host = TestHost::with_row();
	let built = host.bind(&LogicalExpr::Method {
		base: boxed(LogicalExpr::Break),
		name: strand("len"),
		args: vec![field("b")],
	})?;
	assert_eq!(built, Built::Diverged);
	assert!(host.created.is_empty());
	let built = host.bind(&LogicalExpr::Method {
		base: boxed(field("a")),
		name: strand("len"),
		args: vec![LogicalExpr::Break, field("b")],
	})?;
	assert_eq!(built, Built::Diverged);
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Dispatch]);
	Ok(())
}

#[test]
fn invoke_dispatch_precedes_arguments() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let built = host.bind(&LogicalExpr::Invoke {
		target: boxed(field("f")),
		args: vec![field("b"), int(1)],
	})?;
	assert_eq!(
		host.tags(),
		[ExprTag::Field, ExprTag::Dispatch, ExprTag::Field, ExprTag::Literal, ExprTag::Invoke]
	);
	let ExprOp::Dispatch(target) = host.op(1).op() else {
		panic!("not a dispatch");
	};
	assert_eq!(**target, DispatchTarget::Invoke);
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
	// The target is an operand of the dispatch only; the invocation reads the dispatch result.
	assert_eq!(
		host.op(4).operands.as_slice(),
		[host.value_of(1), host.value_of(2), host.value_of(3)]
	);
	assert_eq!(built, Built::Value(host.value_of(4)));

	// A closure literal as the target is bound through the host.
	let mut host = TestHost::new();
	host.bind(&LogicalExpr::Invoke {
		target: boxed(closure_literal()),
		args: vec![int(1)],
	})?;
	assert_eq!(host.calls, [HostCall::Closure]);
	assert_eq!(host.tags(), [ExprTag::Dispatch, ExprTag::Literal, ExprTag::Invoke]);
	Ok(())
}

#[test]
fn optional_and_destructure_bind_current() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let outer = host.fresh();
	host.current = Some(outer);
	let built = host.bind(&LogicalExpr::Optional {
		base: boxed(field("a")),
		then: boxed(LogicalExpr::Access {
			base: boxed(LogicalExpr::Current),
			field: strand("b"),
		}),
	})?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Optional, ExprTag::Access, ExprTag::Yield]);
	assert_eq!(host.tags_in(None), [ExprTag::Field, ExprTag::Optional]);
	assert_eq!(host.tags_in(Some(0)), [ExprTag::Access, ExprTag::Yield]);
	assert_eq!(host.entered[0].kind, RegionKind::LazyOperand);
	assert_eq!(host.entered[0].owner, host.op(1).id);
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
	// `Current` inside the region is the region's argument, and the outer value is back after it.
	assert_eq!(host.op(2).operands.as_slice(), [host.region_arg(0, 0)]);
	assert_eq!(host.op(3).operands.as_slice(), [host.value_of(2)]);
	assert_eq!(host.current, Some(outer));
	assert_eq!(built, Built::Value(host.value_of(1)));

	// A trailing `.?` yields the argument itself.
	let mut host = TestHost::with_row();
	host.bind(&LogicalExpr::Optional {
		base: boxed(field("a")),
		then: boxed(LogicalExpr::Current),
	})?;
	assert_eq!(host.tags_in(Some(0)), [ExprTag::Yield]);
	assert_eq!(host.op(2).operands.as_slice(), [host.region_arg(0, 0)]);
	assert_eq!(host.current, None);

	// A base that diverges enters no region.
	let mut host = TestHost::new();
	let built = host.bind(&LogicalExpr::Optional {
		base: boxed(LogicalExpr::Break),
		then: boxed(LogicalExpr::Current),
	})?;
	assert_eq!(built, Built::Diverged);
	assert!(host.entered.is_empty());
	Ok(())
}

/// A field read or `@` in an optional chain's `then` or a destructure alias's body reads the
/// region's argument, the value the body is over. A step's argument reads the document root: the
/// row in an optional chain's `then`, and in an alias's body the value destructured, which is the
/// body's document. The row is back after the body.
#[test]
fn a_body_over_a_value_reads_its_fields_from_that_value() -> Result<(), IrError> {
	let then = LogicalExpr::Index {
		base: boxed(field("c")),
		index: boxed(field("d")),
	};
	let mut host = TestHost::with_row();
	let row = host.row.expect("the host holds a row");
	host.bind(&LogicalExpr::Optional {
		base: boxed(field("a")),
		then: boxed(then.clone()),
	})?;
	let body = host.ops_in(0);
	assert_eq!(
		body.iter().map(|op| op.op().tag()).collect::<Vec<_>>(),
		[ExprTag::Field, ExprTag::Field, ExprTag::Index, ExprTag::Yield]
	);
	assert_eq!(body[0].operands.as_slice(), [host.region_arg(0, 0)], "`c` reads the base");
	assert_eq!(body[1].operands.as_slice(), [row], "the index `d` reads the document root");
	assert_eq!(host.field_base, None);
	assert_eq!(host.row, Some(row));

	let mut host = TestHost::with_row();
	let row = host.row.expect("the host holds a row");
	host.bind(&LogicalExpr::Destructure {
		base: boxed(field("b")),
		fields: vec![
			LogicalDestructureField::Aliased(strand("x"), then),
			LogicalDestructureField::Aliased(strand("y"), LogicalExpr::Doc),
		],
	})?;
	let first = host.ops_in(0);
	assert_eq!(first[0].operands.as_slice(), [host.region_arg(0, 0)], "`c` reads the value");
	assert_eq!(
		first[1].operands.as_slice(),
		[host.region_arg(0, 0)],
		"the index `d` reads the value destructured, the body's document"
	);
	let second = host.ops_in(1);
	assert_eq!(second[0].op().tag(), ExprTag::Doc);
	assert_eq!(second[0].operands.as_slice(), [host.region_arg(1, 0)], "`@` reads the value");
	assert_eq!(host.field_base, None);
	assert_eq!(host.row, Some(row));
	Ok(())
}

#[test]
fn destructure_binds_one_region_per_alias_in_source_order() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let fields = vec![
		LogicalDestructureField::Field(strand("a")),
		LogicalDestructureField::Aliased(
			strand("x"),
			LogicalExpr::Access {
				base: boxed(LogicalExpr::Current),
				field: strand("y"),
			},
		),
		LogicalDestructureField::Destructure(
			strand("n"),
			vec![
				LogicalDestructureField::Aliased(strand("z"), LogicalExpr::Current),
				LogicalDestructureField::Rest,
			],
		),
		LogicalDestructureField::All(strand("m")),
	];
	let built = host.bind(&LogicalExpr::Destructure {
		base: boxed(field("b")),
		fields,
	})?;
	assert_eq!(
		host.tags(),
		[ExprTag::Field, ExprTag::Destructure, ExprTag::Access, ExprTag::Yield, ExprTag::Yield]
	);
	assert_eq!(host.tags_in(None), [ExprTag::Field, ExprTag::Destructure]);
	let ExprOp::Destructure {
		shape,
	} = host.op(1).op()
	else {
		panic!("not a destructure");
	};
	assert_eq!(
		&**shape,
		[
			DestructureEntry::Field(strand("a")),
			DestructureEntry::Aliased(strand("x")),
			DestructureEntry::Destructure(
				strand("n"),
				Box::new([DestructureEntry::Aliased(strand("z")), DestructureEntry::Rest]),
			),
			DestructureEntry::All(strand("m")),
		]
	);
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
	// The nested alias is the second region: a nested pattern's aliases follow the entries before
	// it.
	assert_eq!(host.entered.len(), 2);
	for region in &host.entered {
		assert_eq!(region.kind, RegionKind::LazyOperand);
		assert_eq!(region.owner, host.op(1).id);
	}
	assert_eq!(host.tags_in(Some(0)), [ExprTag::Access, ExprTag::Yield]);
	assert_eq!(host.tags_in(Some(1)), [ExprTag::Yield]);
	assert_eq!(host.op(2).operands.as_slice(), [host.region_arg(0, 0)]);
	assert_eq!(host.op(4).operands.as_slice(), [host.region_arg(1, 0)]);
	assert_eq!(host.current, None);
	assert_eq!(built, Built::Value(host.value_of(1)));

	// A pattern with no alias has no region.
	let mut host = TestHost::with_row();
	host.bind(&LogicalExpr::Destructure {
		base: boxed(field("b")),
		fields: vec![LogicalDestructureField::Field(strand("a"))],
	})?;
	assert!(host.entered.is_empty());
	Ok(())
}

#[test]
fn if_chains_nest_in_the_else_region() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let built =
		host.bind(&if_chain(vec![(field("c1"), int(1)), (field("c2"), int(2))], Some(int(3))))?;
	assert_eq!(host.tags_in(None), [ExprTag::Field, ExprTag::If]);
	assert_eq!(host.tags_in(Some(0)), [ExprTag::Literal, ExprTag::Yield]);
	assert_eq!(host.tags_in(Some(1)), [ExprTag::Field, ExprTag::If, ExprTag::Yield]);
	assert_eq!(host.tags_in(Some(2)), [ExprTag::Literal, ExprTag::Yield]);
	assert_eq!(host.tags_in(Some(3)), [ExprTag::Literal, ExprTag::Yield]);
	assert_eq!(host.entered.len(), 4);
	assert!(host.entered.iter().all(|region| region.kind == RegionKind::ValueIf));

	let outer = host.op(1).id;
	let nested = host.ops_in(1)[1].id;
	assert_eq!(host.entered[0].owner, outer);
	assert_eq!(host.entered[1].owner, outer);
	assert_eq!(host.entered[2].owner, nested);
	assert_eq!(host.entered[3].owner, nested);
	// The first condition is the outer operand; the second is bound in the else region, as the
	// nested operand.
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);
	assert_eq!(host.ops_in(1)[1].operands.as_slice(), [host.result_of(host.ops_in(1)[0].id)]);
	// The else region yields the nested chain's value.
	assert_eq!(host.ops_in(1)[2].operands.as_slice(), [host.result_of(nested)]);
	let literal = |region: usize| {
		let ExprOp::Literal(value) = host.ops_in(region)[0].op() else {
			panic!("not a literal");
		};
		(**value).clone()
	};
	assert_eq!(literal(0), int_value(1));
	assert_eq!(literal(2), int_value(2));
	assert_eq!(literal(3), int_value(3));
	assert_eq!(built, Built::Value(host.value_of(1)));
	assert_balanced(&host.calls);
	Ok(())
}

#[test]
fn if_without_else_yields_none() -> Result<(), IrError> {
	let none_of = |host: &TestHost, region: usize| {
		let ExprOp::Literal(value) = host.ops_in(region)[0].op() else {
			panic!("not a literal");
		};
		(**value).clone()
	};

	let mut host = TestHost::with_row();
	host.bind(&if_chain(vec![(field("c"), int(1))], None))?;
	assert_eq!(host.tags_in(Some(1)), [ExprTag::Literal, ExprTag::Yield]);
	assert_eq!(none_of(&host, 1), Value::None);

	// Only the last arm of a chain without `ELSE` falls to `NONE`.
	let mut host = TestHost::with_row();
	host.bind(&if_chain(vec![(field("c1"), int(1)), (field("c2"), int(2))], None))?;
	assert_eq!(host.tags_in(Some(1)), [ExprTag::Field, ExprTag::If, ExprTag::Yield]);
	assert_eq!(none_of(&host, 3), Value::None);
	assert_eq!(none_of(&host, 2), int_value(2));
	Ok(())
}

#[test]
fn if_arms_that_diverge_end_their_own_region() -> Result<(), IrError> {
	// A condition that diverges binds no `if`.
	let mut host = TestHost::new();
	let built = host.bind(&if_chain(vec![(LogicalExpr::Break, int(1))], None))?;
	assert_eq!(built, Built::Diverged);
	assert!(host.created.is_empty());

	// An arm that diverges ends its region with its terminator and no `yield`; the `if` still has
	// a value.
	let mut host = TestHost::with_row();
	let built = host.bind(&if_chain(vec![(field("c"), throw(text("x")))], Some(int(1))))?;
	assert_eq!(host.tags_in(Some(0)), [ExprTag::Literal, ExprTag::Throw]);
	assert_eq!(host.tags_in(Some(1)), [ExprTag::Literal, ExprTag::Yield]);
	assert_eq!(built, Built::Value(host.value_of(1)));

	// A nested condition that diverges leaves the else region with no `yield`.
	let mut host = TestHost::with_row();
	let built =
		host.bind(&if_chain(vec![(field("c"), int(1)), (LogicalExpr::Break, int(2))], None))?;
	assert_eq!(host.tags_in(Some(1)), Vec::<ExprTag>::new());
	assert!(matches!(built, Built::Value(_)));
	assert_balanced(&host.calls);
	Ok(())
}

#[test]
fn throw_diverges_and_stops_binding() -> Result<(), IrError> {
	let mut host = TestHost::new();
	let built = host.bind(&LogicalExpr::Array(vec![int(1), throw(text("x")), int(2)]))?;
	assert_eq!(built, Built::Diverged);
	// The thrown value and the `throw`; nothing for `2`, and no `yield`.
	assert_eq!(host.tags(), [ExprTag::Literal, ExprTag::Literal, ExprTag::Throw]);
	assert_eq!(host.op(2).operands.as_slice(), [host.value_of(1)]);
	assert!(host.op(2).results.is_empty());

	// As a body, a throw is the region's terminator and nothing follows it.
	let mut host = TestHost::new();
	bind_body(&mut host, &throw(text("x")))?;
	assert_eq!(host.tags(), [ExprTag::Literal, ExprTag::Throw]);

	// A thrown value that diverges leaves no `throw`.
	let mut host = TestHost::new();
	let built = host.bind(&throw(LogicalExpr::Break))?;
	assert_eq!(built, Built::Diverged);
	assert!(host.created.is_empty());
	Ok(())
}

#[test]
fn time_now_is_a_clock_read() -> Result<(), IrError> {
	let mut host = TestHost::new();
	let timestamp = host.fresh();
	host.timestamp = Some(timestamp);
	let built = host.bind(&call("time::now", Vec::new()))?;
	assert_eq!(host.tags(), [ExprTag::Now]);
	assert!(matches!(
		host.op(0).op(),
		ExprOp::Now {
			which: NowWhich::Datetime
		}
	));
	assert_eq!(host.op(0).operands.as_slice(), [timestamp]);
	assert_eq!(built, Built::Value(host.value_of(0)));

	// Another call, or a clock call with an argument, is a call.
	let mut host = TestHost::with_row();
	let timestamp = host.fresh();
	host.timestamp = Some(timestamp);
	host.bind(&call("time::unix", Vec::new()))?;
	host.bind(&call("time::now", vec![field("a")]))?;
	assert_eq!(host.tags(), [ExprTag::Call, ExprTag::Field, ExprTag::Call]);
	Ok(())
}

#[test]
fn a_denied_time_now_stays_a_call() -> Result<(), IrError> {
	// A timestamp is in scope, but the capabilities deny the name: the call form stays, so the
	// run-time capability check refuses it.
	let mut host = TestHost::new();
	let timestamp = host.fresh();
	host.timestamp = Some(timestamp);
	host.denied.insert("time::now".to_owned());
	let built = host.bind(&call("time::now", Vec::new()))?;
	assert_eq!(host.tags(), [ExprTag::Call]);
	assert!(matches!(
		host.op(0).op(),
		ExprOp::Call(function) if function.function == Function::Normal("time::now".to_owned())
	));
	assert!(host.op(0).operands.is_empty());
	assert_eq!(built, Built::Value(host.value_of(0)));

	// Denying another name leaves the clock read.
	let mut host = TestHost::new();
	let timestamp = host.fresh();
	host.timestamp = Some(timestamp);
	host.denied.insert("math::max".to_owned());
	host.bind(&call("time::now", Vec::new()))?;
	assert_eq!(host.tags(), [ExprTag::Now]);
	Ok(())
}

#[test]
fn time_now_without_a_timestamp_stays_a_call() -> Result<(), IrError> {
	let mut host = TestHost::new();
	let built = host.bind(&call("time::now", Vec::new()))?;
	assert_eq!(host.tags(), [ExprTag::Call]);
	assert!(matches!(
		host.op(0).op(),
		ExprOp::Call(function) if function.function == Function::Normal("time::now".to_owned())
	));
	assert!(host.op(0).operands.is_empty());
	assert_eq!(built, Built::Value(host.value_of(0)));
	Ok(())
}

#[test]
fn aggregates_bind_in_an_error_scope() -> Result<(), IrError> {
	let scope = RegionKind::ErrorScope(ErrorScope::ShapeToNone);
	let mut host = TestHost::with_row();
	host.aggregating = true;
	let built = host.bind(&call("math::mean", vec![field("a")]))?;
	assert_eq!(host.tags(), [ExprTag::Agg, ExprTag::Field, ExprTag::Yield]);
	assert_eq!(host.tags_in(None), [ExprTag::Agg]);
	assert_eq!(host.tags_in(Some(0)), [ExprTag::Field, ExprTag::Yield]);
	assert!(matches!(
		host.op(0).op(),
		ExprOp::Agg {
			kind: "math::mean",
			combine: AggregateCombine::InexactFloat
		}
	));
	assert!(host.op(0).operands.is_empty());
	assert_eq!(host.entered[0].kind, scope);
	assert_eq!(host.entered[0].owner, host.op(0).id);
	assert_eq!(host.op(2).operands.as_slice(), [host.value_of(1)]);
	assert_eq!(built, Built::Value(host.value_of(0)));

	// The arguments are the yield, in order; no argument is an empty yield.
	let mut host = TestHost::with_row();
	host.aggregating = true;
	host.bind(&call("array::join", vec![field("a"), field("b")]))?;
	assert_eq!(host.op(3).operands.as_slice(), [host.value_of(1), host.value_of(2)]);
	assert!(matches!(
		host.op(0).op(),
		ExprOp::Agg {
			kind: "array::join",
			combine: AggregateCombine::Exact
		}
	));
	let mut host = TestHost::new();
	host.aggregating = true;
	host.bind(&call("count", Vec::new()))?;
	assert_eq!(host.tags(), [ExprTag::Agg, ExprTag::Yield]);
	assert!(host.op(1).operands.is_empty());

	// An argument that diverges ends the region with its terminator; the aggregate has a value.
	let mut host = TestHost::new();
	host.aggregating = true;
	let built = host.bind(&call("math::mean", vec![throw(text("x"))]))?;
	assert_eq!(host.tags_in(Some(0)), [ExprTag::Literal, ExprTag::Throw]);
	assert_eq!(built, Built::Value(host.value_of(0)));
	assert_balanced(&host.calls);

	// Outside an aggregating statement the same name is a plain call, and so is any other name
	// inside one.
	let mut host = TestHost::with_row();
	host.bind(&call("math::mean", vec![field("a")]))?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Call]);
	assert!(host.entered.is_empty());
	let mut host = TestHost::with_row();
	host.aggregating = true;
	host.bind(&call("math::abs", vec![field("a")]))?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Call]);
	assert!(host.entered.is_empty());
	Ok(())
}

fn graph_lookup() -> Box<LogicalLookup> {
	Box::new(LogicalLookup {
		kind: LookupKind::Reference,
		expr: None,
		only: false,
		what: Vec::new(),
		cond: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		alias: None,
		trace: crate::expr::logical::LookupTrace::None,
	})
}

#[test]
fn loop_forms_go_to_the_host() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let forms = [
		(
			LogicalExpr::Filter {
				base: boxed(field("a")),
				predicate: boxed(LogicalExpr::Current),
			},
			HostCall::ElementFilter,
		),
		(
			LogicalExpr::Lookup {
				base: boxed(field("a")),
				lookup: graph_lookup(),
			},
			HostCall::GraphStep,
		),
		(
			LogicalExpr::Recurse(Box::new(LogicalRecurse {
				base: field("a"),
				recurse: Recurse::Fixed(2),
				path: None,
				instruction: None,
			})),
			HostCall::Recurse,
		),
		(
			LogicalExpr::Broadcast {
				base: boxed(field("a")),
				body: boxed(LogicalExpr::Current),
				flatten: false,
			},
			HostCall::Broadcast {
				flatten: false,
			},
		),
	];
	for (expr, expected) in forms {
		host.calls.clear();
		assert!(matches!(host.bind(&expr)?, Built::Value(_)), "{expected:?}");
		assert_eq!(host.calls, [expected]);
	}
	// The host binds a loop form's own parts; the expression binder created no op for any of them.
	assert!(host.created.is_empty());
	Ok(())
}

#[test]
fn a_builtin_with_a_visible_closure_is_a_loop_site() -> Result<(), IrError> {
	// A closure written in the call.
	let mut host = TestHost::with_row();
	let built = host.bind(&call("array::map", vec![field("a"), closure_literal()]))?;
	assert_eq!(
		host.calls,
		[HostCall::ClosureSite {
			slot: 1
		}]
	);
	assert!(host.created.is_empty());
	assert!(matches!(built, Built::Value(_)));

	// A name bound to the result of a `closure_create`.
	let mut host = TestHost::with_row();
	host.create(
		OpKind::Expr(ExprOp::ClosureCreate(Box::new(closure_attrs()))),
		SmallVec::new(),
		smallvec![TypeKnowledge::Unknown],
	)?;
	let closure = host.value_of(0);
	host.names.insert("f".to_owned(), closure);
	host.bind(&call("array::map", vec![field("a"), param("f")]))?;
	assert_eq!(
		host.calls,
		[HostCall::ClosureSite {
			slot: 1
		}]
	);
	assert_eq!(host.tags(), [ExprTag::ClosureCreate], "the host binds the arguments of the site");

	// A free name is not a visible closure: the call stays a call, with the name as an operand.
	let mut host = TestHost::with_row();
	host.bind(&call("array::map", vec![field("a"), param("f")]))?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::FreeParam, ExprTag::Call]);
	assert!(host.calls.is_empty());

	// So is a bound name whose value no `closure_create` made.
	let mut host = TestHost::with_row();
	let other = host.fresh();
	host.names.insert("f".to_owned(), other);
	host.bind(&call("array::map", vec![field("a"), param("f")]))?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Call]);
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0), other]);
	assert!(host.calls.is_empty());

	// A builtin that takes no closure never makes a loop site of its arguments.
	let mut host = TestHost::with_row();
	host.bind(&call("math::abs", vec![closure_literal()]))?;
	assert_eq!(host.calls, [HostCall::Closure]);
	assert_eq!(host.tags(), [ExprTag::Call]);
	Ok(())
}

#[test]
fn a_method_with_a_visible_closure_is_a_loop_site() -> Result<(), IrError> {
	let method = |name: &str, args: Vec<LogicalExpr>| LogicalExpr::Method {
		base: boxed(field("a")),
		name: strand(name),
		args,
	};

	// A closure written in the call: the host binds the receiver and the arguments itself.
	let mut host = TestHost::with_row();
	let built = host.bind(&method("map", vec![closure_literal()]))?;
	assert_eq!(
		host.calls,
		[HostCall::MethodClosureSite {
			slot: 0
		}]
	);
	assert!(host.created.is_empty());
	assert!(host.step_roots.is_empty());
	assert!(matches!(built, Built::Value(_)));

	// The slot indexes the arguments, the receiver not counted.
	let mut host = TestHost::with_row();
	host.bind(&method("fold", vec![int(0), closure_literal()]))?;
	assert_eq!(
		host.calls,
		[HostCall::MethodClosureSite {
			slot: 1
		}]
	);

	// A generic method that runs a closure argument.
	let mut host = TestHost::with_row();
	host.bind(&method("chain", vec![closure_literal()]))?;
	assert_eq!(
		host.calls,
		[HostCall::MethodClosureSite {
			slot: 0
		}]
	);

	// A name bound to the result of a `closure_create`.
	let mut host = TestHost::with_row();
	host.create(
		OpKind::Expr(ExprOp::ClosureCreate(Box::new(closure_attrs()))),
		SmallVec::new(),
		smallvec![TypeKnowledge::Unknown],
	)?;
	let closure = host.value_of(0);
	host.names.insert("f".to_owned(), closure);
	host.bind(&method("map", vec![param("f")]))?;
	assert_eq!(
		host.calls,
		[HostCall::MethodClosureSite {
			slot: 0
		}]
	);
	assert_eq!(host.tags(), [ExprTag::ClosureCreate]);

	// A free name is not a visible closure: the method stays a dispatch and a method.
	let mut host = TestHost::with_row();
	host.bind(&method("map", vec![param("f")]))?;
	assert_eq!(
		host.tags(),
		[ExprTag::Field, ExprTag::Dispatch, ExprTag::FreeParam, ExprTag::Method]
	);
	assert!(host.calls.is_empty());

	// A method no builtin of which runs a closure never makes a loop site of its arguments.
	let mut host = TestHost::with_row();
	host.bind(&method("len", vec![closure_literal()]))?;
	assert_eq!(host.calls, [HostCall::Closure]);
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Dispatch, ExprTag::Method]);
	Ok(())
}

#[test]
fn step_arguments_read_the_document_root() -> Result<(), IrError> {
	// A method argument in a frame with no row reads the receiver: `RETURN [{ k: 3 }].concat([k])`.
	let mut host = TestHost::new();
	let receiver = host.fresh();
	host.names.insert("o".to_owned(), receiver);
	let built = host.bind(&LogicalExpr::Method {
		base: boxed(param("o")),
		name: strand("concat"),
		args: vec![LogicalExpr::Array(vec![field("k")])],
	})?;
	assert_eq!(host.tags(), [ExprTag::Dispatch, ExprTag::Field, ExprTag::Array, ExprTag::Method]);
	assert_eq!(host.op(1).operands.as_slice(), [receiver]);
	assert_eq!(host.op(3).operands.as_slice(), [host.value_of(0), receiver, host.value_of(2)]);
	assert_eq!(built, Built::Value(host.value_of(3)));
	assert_eq!(host.step_roots, [receiver]);
	assert_eq!(host.row, None);

	// An index in a frame with no row: `$obj[k]`.
	let mut host = TestHost::new();
	let receiver = host.fresh();
	host.names.insert("obj".to_owned(), receiver);
	host.bind(&LogicalExpr::Index {
		base: boxed(param("obj")),
		index: boxed(field("k")),
	})?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Index]);
	assert_eq!(host.op(0).operands.as_slice(), [receiver]);
	assert_eq!(host.op(1).operands.as_slice(), [receiver, host.value_of(0)]);
	assert_eq!(host.step_roots, [receiver]);

	// With a row, the row is the root, and so is what `@` and the current value denote inside the
	// argument.
	let mut host = TestHost::with_row();
	let row = host.row.expect("a row");
	let outer = host.fresh();
	host.current = Some(outer);
	let receiver = host.fresh();
	host.names.insert("o".to_owned(), receiver);
	host.bind(&LogicalExpr::Method {
		base: boxed(param("o")),
		name: strand("concat"),
		args: vec![field("k"), LogicalExpr::Current],
	})?;
	assert_eq!(host.op(1).operands.as_slice(), [row]);
	assert_eq!(host.op(2).operands.as_slice(), [host.value_of(0), receiver, host.value_of(1), row]);
	assert_eq!(host.row, Some(row));
	assert_eq!(host.current, Some(outer));

	// The base of an index is bound as it stands, and an argument that reads no row asks for no
	// root.
	let mut host = TestHost::new();
	let receiver = host.fresh();
	host.names.insert("o".to_owned(), receiver);
	host.bind(&LogicalExpr::Method {
		base: boxed(param("o")),
		name: strand("len"),
		args: vec![int(1)],
	})?;
	host.bind(&LogicalExpr::Index {
		base: boxed(param("o")),
		index: boxed(int(0)),
	})?;
	assert!(host.step_roots.is_empty());
	Ok(())
}

#[test]
fn the_row_and_current_value_are_restored_when_a_step_argument_fails() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let row = host.row;
	let outer = host.fresh();
	host.current = Some(outer);
	let receiver = host.fresh();
	host.names.insert("o".to_owned(), receiver);
	host.fail_at = Some(0);
	let failed = host.bind(&LogicalExpr::Index {
		base: boxed(param("o")),
		index: boxed(field("k")),
	});
	assert!(failed.is_err());
	assert_eq!(host.row, row);
	assert_eq!(host.current, Some(outer));
	Ok(())
}

#[test]
fn closures_subqueries_and_control_go_to_the_host() -> Result<(), IrError> {
	let mut host = TestHost::new();
	let built = host.bind(&closure_literal())?;
	assert!(matches!(built, Built::Value(_)));
	let built = host.bind(&LogicalExpr::Subquery(Box::new(LogicalOp::Break(LogicalBreak))))?;
	assert!(matches!(built, Built::Value(_)));
	assert_eq!(host.bind(&LogicalExpr::Break)?, Built::Diverged);
	assert_eq!(host.bind(&LogicalExpr::Continue)?, Built::Diverged);
	assert_eq!(
		host.calls,
		[
			HostCall::Closure,
			HostCall::ValueStatement,
			HostCall::Control(Control::Break),
			HostCall::Control(Control::Continue),
		]
	);
	assert!(host.created.is_empty());

	// A closure is an operand like any other value.
	let mut host = TestHost::new();
	host.bind(&LogicalExpr::Array(vec![closure_literal(), int(1)]))?;
	assert_eq!(host.tags(), [ExprTag::Literal, ExprTag::Array]);
	Ok(())
}

#[test]
fn fn_calls_take_row_name_operands() -> Result<(), IrError> {
	let mut host = TestHost::with_row();
	let first = host.fresh();
	let second = host.fresh();
	host.row_names = smallvec![first, second];
	let custom = LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Custom("f".to_owned()),
		args: vec![field("a")],
	}));
	let built = host.bind(&custom)?;
	assert_eq!(host.tags(), [ExprTag::Field, ExprTag::Call]);
	// The arguments, then the row names.
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0), first, second]);
	assert!(matches!(
		host.op(1).op(),
		ExprOp::Call(function) if function.function == Function::Custom("f".to_owned())
	));
	assert_eq!(built, Built::Value(host.value_of(1)));

	// A builtin that reads no row passes none.
	let mut host = TestHost::with_row();
	let row_name = host.fresh();
	host.row_names = smallvec![row_name];
	host.bind(&call("math::abs", vec![field("a")]))?;
	assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0)]);

	// A projection function and an index function read the row without taking it.
	for name in ["type::field", "search::score"] {
		let mut host = TestHost::with_row();
		let row_name = host.fresh();
		host.row_names = smallvec![row_name];
		host.bind(&call(name, vec![field("a")]))?;
		assert_eq!(host.op(1).operands.as_slice(), [host.value_of(0), row_name], "{name}");
	}

	// An argument that diverges ends the call before any operand is asked of the host.
	let mut host = TestHost::new();
	let built = host.bind(&LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Custom("f".to_owned()),
		args: vec![LogicalExpr::Break],
	})))?;
	assert_eq!(built, Built::Diverged);
	assert!(host.created.is_empty());
	Ok(())
}

#[test]
fn regions_are_left_on_error() -> Result<(), IrError> {
	// Every form that enters a region, in one expression: a lazy right side, an optional chain, a
	// destructure alias, and an `IF` chain whose else region nests another.
	let composite = if_chain(
		vec![
			(field("a"), binary(field("b"), BinaryOperator::NullCoalescing, field("c"))),
			(
				field("d"),
				LogicalExpr::Optional {
					base: boxed(field("e")),
					then: boxed(LogicalExpr::Access {
						base: boxed(LogicalExpr::Current),
						field: strand("x"),
					}),
				},
			),
		],
		Some(LogicalExpr::Destructure {
			base: boxed(field("f")),
			fields: vec![LogicalDestructureField::Aliased(strand("k"), LogicalExpr::Current)],
		}),
	);
	let mut failed = 0;
	let mut succeeded = false;
	for limit in 0..64 {
		let mut host = TestHost::with_row();
		let outer = host.fresh();
		host.current = Some(outer);
		host.fail_at = Some(limit);
		match host.bind(&composite) {
			Ok(_) => {
				succeeded = true;
				assert_balanced(&host.calls);
				break;
			}
			Err(_) => {
				failed += 1;
				// Wherever the failure struck, no region is left open and `Current` is as it was.
				assert_balanced(&host.calls);
				assert_eq!(host.current, Some(outer), "failure after {limit} ops");
			}
		}
	}
	assert!(succeeded, "the expression binds once no op fails");
	assert!(failed > 8, "the failures reached the regions");

	// A failure inside the region of an aggregate.
	let mut host = TestHost::with_row();
	host.aggregating = true;
	host.fail_at = Some(1);
	assert!(host.bind(&call("math::mean", vec![field("a")])).is_err());
	assert_eq!(
		host.calls,
		[
			HostCall::EnterRegion(RegionKind::ErrorScope(ErrorScope::ShapeToNone)),
			HostCall::LeaveRegion
		]
	);
	Ok(())
}
