//! The leaf profile: what of a program a read-only executor over a snapshot
//! of storage — a leaf — can run, and the native host services it needs.
//!
//! Every instruction, source and host step has one [`Class`], stated once in
//! this module's exhaustive matches: [`step`] for the host steps, [`instr`]
//! for the instructions (an instruction the interpreter answers alone is
//! [`Class::Leaf`]; one that yields to the host takes its step's class), and
//! [`source`] for the sources. A variant added to [`Instr`], [`Step`],
//! [`Source`], [`BlockInstr`], [`StoreInstr`] or [`WriteInstr`] does not
//! compile until it is classified here. A builtin's class is derived from its
//! declared [`EffectSet`], and an external callee's from its kind.
//!
//! [`profile`] walks a program and every unit it holds and reports what
//! blocks it from a leaf and which seams it reaches. Nothing here changes how
//! a program runs: the profile is read by the `SURREAL_VM_LEAF` report and by
//! tests. `doc/executor/VM_LEAF.md` states the classification and its
//! rationale.

// Runtime the IR lowering emits.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::sync::Arc;

use super::interp::Step;
use super::program::{BlockInstr, ExternalCallee, Instr, Program, Source, StoreInstr, WriteInstr};
use crate::exec::function::{EffectSet, FunctionRegistry, ScalarFunction};

/// A narrow interface a leaf's host serves a [`Class::Kernel`] item through,
/// in place of the engine's execution context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Seam {
	/// Reads of records, ranges, index structures and the adjacency, as of
	/// the snapshot.
	Storage,
	/// Reads of definitions: tables, fields, functions, parameters.
	Catalog,
	/// The principal, its table and field permissions, and the capabilities
	/// that admit a function.
	Permissions,
	/// The clock, the statement's deadline and its cancellation.
	Clock,
	/// The session's and the frame's parameters.
	Params,
	/// Links a unit for a closure, or reads one by a table name the data gives.
	/// If no table holds the unit, the request falls back to the compiler.
	Link,
}

/// Why a leaf never runs an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Bar {
	/// Stages a write, or advances a counter the snapshot does not hold.
	Write,
	/// Defines, alters or reports the catalog, or acts on the process.
	Schema,
	/// Notifies or kills a live subscription.
	Live,
	/// Writes the request's session: its options, its namespace and database, its parameters.
	Session,
	/// Runs an embedded script or a module.
	Script,
	/// Runs a machine-learning model.
	Model,
	/// Reads or writes outside the snapshot: a remote endpoint, a bucket,
	/// the API request being served.
	Network,
	/// Waits.
	Sleep,
	/// Runs a body the program does not hold: a statement given as text, a
	/// stored endpoint.
	Opaque,
	/// Takes a value that speculative planning reserved in advance: an id, a uuid.
	Speculation,
}

/// What a leaf makes of an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Class {
	/// The interpreter, or the host from the program and the row alone,
	/// answers it.
	Leaf,
	/// A leaf needs the native host runtime and the listed interfaces. An
	/// empty slice still requires the host runtime, as an async pure call does.
	Kernel(&'static [Seam]),
	/// A leaf never runs it.
	NonLeaf(Bar),
	/// The class is that of the instruction or callee the step services.
	ByOperand,
}

use Seam::{Catalog, Clock, Link, Params, Permissions, Storage};

/// The class of every host step: the one table of what the host does for a
/// running program.
pub(super) fn step(step: &Step) -> Class {
	match step {
		Step::Next
		| Step::Stop
		| Step::LandRow {
			..
		}
		| Step::BindRow {
			..
		}
		| Step::Signal {
			..
		}
		| Step::Yield {
			..
		} => Class::Leaf,
		Step::FetchRow {
			..
		}
		| Step::Lookup {
			..
		}
		| Step::Destructure {
			..
		}
		| Step::JudgeCopy {
			..
		} => Class::Kernel(&[Storage, Permissions]),
		Step::Omit {
			..
		} => Class::Kernel(&[Params]),
		Step::Fetch {
			..
		}
		| Step::Access {
			..
		}
		| Step::Each {
			..
		}
		| Step::Recurse {
			..
		} => Class::Kernel(&[Storage, Permissions]),
		Step::WriteProjection {
			..
		} => Class::Kernel(&[]),
		Step::CallAllowed {
			..
		} => Class::Kernel(&[Permissions]),
		Step::FieldTypes {
			..
		} => Class::Kernel(&[Catalog]),
		Step::Subquery {
			..
		} => Class::Kernel(&[Params]),
		Step::Closure {
			..
		} => Class::Kernel(&[Link]),
		Step::NeedUnit {
			..
		} => Class::Kernel(&[Link]),
		Step::Function {
			..
		} => Class::Kernel(&[Catalog, Permissions]),
		Step::Param {
			..
		} => Class::Kernel(&[Params, Catalog]),
		Step::IndexFn {
			..
		}
		| Step::Matches {
			..
		} => Class::Kernel(&[Storage]),
		Step::Degree {
			..
		} => Class::Kernel(&[Storage]),
		// The query vector is evaluated as an expression the program holds,
		// whose own items are classified where it is lowered.
		Step::KnnQuery {
			..
		} => Class::Kernel(&[Params]),
		// The host's answer for a row checks the capability, which reads the permissions, and
		// reads the query parameter as `Step::Param` reads one: from the frame, or from the
		// catalog where no frame binds it.
		Step::Measure {
			..
		} => Class::Kernel(&[Permissions, Params, Catalog]),
		Step::Write {
			..
		} => Class::NonLeaf(Bar::Write),
		Step::Call {
			..
		}
		| Step::Method {
			..
		}
		| Step::Block {
			..
		}
		| Step::Store {
			..
		}
		| Step::Ir {
			..
		}
		| Step::External {
			..
		} => Class::ByOperand,
	}
}

/// The class of block instruction `instr`.
fn block(instr: &BlockInstr) -> Class {
	match instr {
		BlockInstr::RunEmbedded {
			..
		}
		| BlockInstr::CoerceKind {
			..
		}
		| BlockInstr::Enter
		| BlockInstr::CheckDepth {
			..
		}
		| BlockInstr::Return {
			..
		}
		| BlockInstr::IterStart {
			..
		}
		| BlockInstr::IterNext {
			..
		}
		| BlockInstr::Bind {
			frame: false,
			..
		} => Class::Leaf,
		BlockInstr::Bind {
			frame: true,
			..
		}
		| BlockInstr::Save
		| BlockInstr::Restore
		| BlockInstr::Nested {
			..
		}
		| BlockInstr::Member {
			..
		} => Class::Kernel(&[Params]),
		BlockInstr::Resolve => Class::Kernel(&[Catalog]),
		BlockInstr::Clock => Class::Kernel(&[Clock]),
		// The analysis evaluates the bitmap trees the run decided by again.
		BlockInstr::RunAnalyzed {
			..
		} => Class::Kernel(&[Clock, Storage]),
		// A late-bound statement is served from the compile cache, compiled
		// there on a miss, and run in the frame.
		BlockInstr::RunServed {
			..
		} => Class::Kernel(&[Catalog, Link, Params]),
		BlockInstr::Sleep {
			..
		} => Class::NonLeaf(Bar::Sleep),
	}
}

/// The class of store instruction `instr`: each is part of a schema
/// statement's, an `INFO`'s or a view backfill's work over the catalog and
/// the process.
fn store(instr: &StoreInstr) -> Class {
	match instr {
		StoreInstr::NextId {
			..
		}
		| StoreInstr::NewUuid {
			..
		}
		| StoreInstr::RealRunOnly
		| StoreInstr::JumpIfSpeculating {
			..
		} => Class::NonLeaf(Bar::Speculation),
		StoreInstr::KillOnCommit {
			..
		}
		| StoreInstr::RealtimeChecks
		| StoreInstr::Live {
			..
		}
		| StoreInstr::Kill {
			..
		} => Class::NonLeaf(Bar::Live),
		StoreInstr::SessionOption {
			..
		}
		| StoreInstr::SessionUseName {
			..
		}
		| StoreInstr::SessionUse {
			..
		}
		| StoreInstr::SessionLetChecks {
			..
		}
		| StoreInstr::SessionLet {
			..
		} => Class::NonLeaf(Bar::Session),
		StoreInstr::DeleteModelArtifact {
			..
		} => Class::NonLeaf(Bar::Model),
		StoreInstr::EvictModule {
			..
		}
		| StoreInstr::LoadModule {
			..
		} => Class::NonLeaf(Bar::Script),
		StoreInstr::ConnectBucket {
			..
		}
		| StoreInstr::VerifyModule {
			..
		} => Class::NonLeaf(Bar::Network),
		// An access statement reads or writes the grants of an access method
		// in the catalog, and a filter of its grants runs host code.
		StoreInstr::ShowGrants {
			..
		}
		| StoreInstr::IssueGrant {
			..
		}
		| StoreInstr::RevokeGrants {
			..
		}
		| StoreInstr::PurgeGrants {
			..
		} => Class::NonLeaf(Bar::Schema),
		StoreInstr::EvaluateOver {
			..
		}
		| StoreInstr::BackfillRow {
			..
		}
		| StoreInstr::BackfillGroup {
			..
		}
		| StoreInstr::BackfillGroupRow {
			..
		}
		| StoreInstr::RetireViewRows {
			..
		} => Class::NonLeaf(Bar::Write),
		StoreInstr::RefuseNoWrite
		| StoreInstr::Allow {
			..
		}
		| StoreInstr::ViewSources {
			..
		}
		| StoreInstr::RefuseRemovingViewed {
			..
		}
		| StoreInstr::RefuseRemovingLightweight {
			..
		}
		| StoreInstr::PurgeEdgePointers {
			..
		}
		| StoreInstr::ClearViewTable {
			..
		}
		| StoreInstr::RefuseViewCycle {
			..
		}
		| StoreInstr::RefuseClauseEffects {
			..
		}
		| StoreInstr::RefuseRedefiningTable {
			..
		}
		| StoreInstr::RefuseNamedIdConversion {
			..
		}
		| StoreInstr::CheckTableChange {
			..
		}
		| StoreInstr::ValidateRecordKeys {
			..
		}
		| StoreInstr::Get {
			..
		}
		| StoreInstr::StampCatalog {
			..
		}
		| StoreInstr::Put {
			..
		}
		| StoreInstr::Replace {
			..
		}
		| StoreInstr::GetField {
			..
		}
		| StoreInstr::SessionDb {
			..
		}
		| StoreInstr::SessionAuthLimit {
			..
		}
		| StoreInstr::Lock {
			..
		}
		| StoreInstr::Delete {
			..
		}
		| StoreInstr::DeleteRange {
			..
		}
		| StoreInstr::DiscardTableMigration {
			..
		}
		| StoreInstr::HoldTableForRemoval {
			..
		}
		| StoreInstr::BufferTableChange {
			..
		}
		| StoreInstr::SessionNs {
			..
		}
		| StoreInstr::BumpChangefeedFence {
			..
		}
		| StoreInstr::ClearDatastoreCache
		| StoreInstr::JumpIfImport {
			..
		}
		| StoreInstr::Raise {
			..
		}
		| StoreInstr::Refuse {
			..
		}
		| StoreInstr::ClearCache
		| StoreInstr::Scan {
			..
		}
		| StoreInstr::ScanAt {
			..
		}
		| StoreInstr::VersionStamp {
			..
		}
		| StoreInstr::Clear {
			..
		}
		| StoreInstr::ClearRange {
			..
		}
		| StoreInstr::Compact {
			..
		}
		| StoreInstr::CompactAll
		| StoreInstr::SetQueryTimeout {
			..
		}
		| StoreInstr::DropMethods {
			..
		}
		| StoreInstr::Push {
			..
		}
		| StoreInstr::Render {
			..
		}
		| StoreInstr::RenderOne {
			..
		}
		| StoreInstr::CoerceString {
			..
		}
		| StoreInstr::CoerceName {
			..
		}
		| StoreInstr::Convert {
			..
		}
		| StoreInstr::CheckDepth {
			..
		}
		| StoreInstr::AccessCheck {
			..
		}
		| StoreInstr::ParseApiPath {
			..
		}
		| StoreInstr::CastComment {
			..
		}
		| StoreInstr::ProcessInfo {
			..
		}
		| StoreInstr::InfoIndex {
			..
		}
		| StoreInstr::JumpIfNoService {
			..
		}
		| StoreInstr::RefuseField {
			..
		}
		| StoreInstr::FieldCheck {
			..
		}
		| StoreInstr::RefuseLightweight {
			..
		}
		| StoreInstr::PurgeDroppedReferences {
			..
		}
		| StoreInstr::ClaimDocIdsReclaim {
			..
		}
		| StoreInstr::AdmitIndexFormat {
			..
		}
		| StoreInstr::NextIndexId {
			..
		}
		| StoreInstr::IndexReplayReuses {
			..
		}
		| StoreInstr::FieldAdmitsSubFields {
			..
		}
		| StoreInstr::RefuseBlockingBuild {
			..
		}
		| StoreInstr::BuildIndex {
			..
		}
		| StoreInstr::RetireIndexLocally {
			..
		}
		| StoreInstr::SharesDocIds {
			..
		}
		| StoreInstr::LoadAnalyzer {
			..
		}
		| StoreInstr::CleanupMappers {
			..
		}
		| StoreInstr::SequenceDefinedHere {
			..
		}
		| StoreInstr::EvictSequence {
			..
		}
		| StoreInstr::JumpIfSessionBase {
			..
		}
		| StoreInstr::ResolvePath {
			..
		}
		| StoreInstr::ResolveColumns {
			..
		}
		| StoreInstr::DefineField {
			..
		}
		| StoreInstr::RemoveFieldDefinition {
			..
		}
		| StoreInstr::AlterFieldDefinition {
			..
		}
		| StoreInstr::Migrate {
			..
		}
		| StoreInstr::MigrateTable {
			..
		}
		| StoreInstr::CheckIndexMigrations {
			..
		}
		| StoreInstr::QueueVerification {
			..
		}
		| StoreInstr::CheckFieldChange {
			..
		}
		| StoreInstr::RemoveFieldStep {
			..
		}
		| StoreInstr::InfoMigrations {
			..
		}
		| StoreInstr::AlterFieldClauses {
			..
		}
		| StoreInstr::CheckIndexColumns {
			..
		}
		| StoreInstr::AlterType {
			..
		}
		| StoreInstr::RewriteEnumLabels {
			..
		}
		| StoreInstr::CheckTypeObject {
			..
		}
		| StoreInstr::CheckTypeUnreferenced {
			..
		}
		| StoreInstr::CheckTypesDefined {
			..
		}
		| StoreInstr::CheckTypeDefinition {
			..
		}
		| StoreInstr::CheckTableAlter {
			..
		}
		| StoreInstr::NewDatabaseCompatibility {
			..
		}
		| StoreInstr::SetDefaultCompatibility {
			..
		}
		| StoreInstr::VerifyCompatibility {
			..
		} => Class::NonLeaf(Bar::Schema),
	}
}

/// The class of write instruction `instr`: every one is part of a record's
/// write, and a subscription's notification is the write's too.
fn write(instr: &WriteInstr) -> Class {
	match instr {
		WriteInstr::Observe(_) => Class::NonLeaf(Bar::Live),
		WriteInstr::Record
		| WriteInstr::Gate {
			..
		}
		| WriteInstr::Begin
		| WriteInstr::LoadNamedSubject
		| WriteInstr::JumpIfNew {
			..
		}
		| WriteInstr::JumpIfNotIterationInitial {
			..
		}
		| WriteInstr::SavepointPush
		| WriteInstr::SavepointRelease
		| WriteInstr::SavepointRollback
		| WriteInstr::SavepointEndSkipped
		| WriteInstr::ReachedStore
		| WriteInstr::Rethrow
		| WriteInstr::TryBegin {
			..
		}
		| WriteInstr::TryEnd
		| WriteInstr::ClearMutated
		| WriteInstr::RethrowSignal
		| WriteInstr::ClassifyCreateFailure {
			..
		}
		| WriteInstr::JumpIfDone {
			..
		}
		| WriteInstr::ReloadForUpdate
		| WriteInstr::CarryBegin
		| WriteInstr::CarryEnd
		| WriteInstr::RaiseCarried
		| WriteInstr::End {
			..
		}
		| WriteInstr::JumpIfImport {
			..
		}
		| WriteInstr::JumpIfNoPostImage {
			..
		}
		| WriteInstr::ClearCurrentReduced
		| WriteInstr::ShowImage {
			..
		}
		| WriteInstr::SetOutput {
			..
		}
		| WriteInstr::DispatchTable {
			..
		}
		| WriteInstr::Data(_)
		| WriteInstr::Schema(_)
		| WriteInstr::Permissions(_)
		| WriteInstr::Storage(_) => Class::NonLeaf(Bar::Write),
	}
}

/// The class of the builtin `name`, from its declared effects. A call that runs an
/// unseen body, writes, reaches outside the snapshot or waits is barred. A pure
/// synchronous call runs on its arguments alone. Any other gets the evaluation
/// context, for the reads its effects name.
pub(crate) fn builtin(name: &str, registry: &FunctionRegistry) -> Class {
	if registry.get_projection(name).is_some() {
		return Class::Kernel(&[]);
	}
	let Some(func) = registry.get(name) else {
		return Class::Kernel(&[]);
	};
	function(func)
}

/// The class of builtin `func`, as [`builtin`] states it.
fn function(func: &dyn ScalarFunction) -> Class {
	let effects = func.effects();
	if effects.intersects(EffectSet::OPAQUE_BODY) {
		return Class::NonLeaf(Bar::Opaque);
	}
	if effects.intersects(EffectSet::WRITES_DATA.union(EffectSet::ADVANCES_STATE)) {
		return Class::NonLeaf(Bar::Write);
	}
	if effects.intersects(EffectSet::WRITES_EXTERNAL.union(EffectSet::READS_EXTERNAL)) {
		return Class::NonLeaf(Bar::Network);
	}
	if effects.intersects(EffectSet::BLOCKING) {
		return Class::NonLeaf(Bar::Sleep);
	}
	if func.is_pure() && !func.is_async() {
		return Class::Leaf;
	}
	const SEAMS: [(EffectSet, &[Seam]); 4] = [
		(EffectSet::READS_DATA, &[Storage, Permissions]),
		(EffectSet::READS_CATALOG, &[Catalog]),
		(EffectSet::READS_SESSION, &[Params]),
		(EffectSet::CLOCK.union(EffectSet::ENTROPY), &[Clock]),
	];
	SEAMS
		.iter()
		.find(|(flag, _)| effects.intersects(*flag))
		.map_or(Class::Kernel(&[]), |(_, seams)| Class::Kernel(seams))
}

/// The class of a method named `name`: the class of the most constrained
/// implementation any receiver resolves it to.
fn method(name: &str, registry: &FunctionRegistry) -> Class {
	let Some(descriptor) = registry.get_method(name) else {
		return Class::Kernel(&[]);
	};
	descriptor
		.type_impls
		.iter()
		.map(|(_, func)| *func)
		.chain(descriptor.fallback)
		.map(function)
		.fold(Class::Leaf, worse)
}

/// The more constrained of two classes: a bar over a seam, a seam over a
/// leaf.
fn worse(a: Class, b: Class) -> Class {
	match (a, b) {
		(Class::NonLeaf(bar), _) | (_, Class::NonLeaf(bar)) => Class::NonLeaf(bar),
		(Class::Kernel(seams), _) | (_, Class::Kernel(seams)) => Class::Kernel(seams),
		_ => Class::Leaf,
	}
}

/// The class of instruction `instr` of `program`, with the name its report
/// gives it: the interpreter answers it alone ([`Class::Leaf`]), or it takes
/// the class of the host step it yields.
pub(super) fn instr(
	instr: &Instr,
	program: &Program,
	registry: &FunctionRegistry,
) -> (Class, String) {
	let named = |class: Class| (class, variant(instr));
	match instr {
		Instr::CoerceField {
			kind,
			..
		} if program.kinds[*kind as usize].has_named_type() => named(step(&Step::FieldTypes {
			at: 0,
		})),
		Instr::CoerceField {
			..
		}
		| Instr::BeginComputing {
			..
		}
		| Instr::JumpIfPermissionsBypassed {
			..
		}
		| Instr::CompareFieldInt {
			..
		}
		| Instr::JumpIfFieldInt {
			..
		}
		| Instr::JumpIfCompare {
			..
		}
		| Instr::Compare {
			..
		}
		| Instr::Arith {
			..
		}
		| Instr::Unary {
			..
		}
		| Instr::NewArray {
			..
		}
		| Instr::ArrayPush {
			..
		}
		| Instr::LoadSlot {
			..
		}
		| Instr::SortKey {
			..
		}
		| Instr::Throw {
			..
		}
		| Instr::FilterBegin {
			..
		}
		| Instr::MapBegin {
			..
		}
		| Instr::CollectPush {
			..
		}
		| Instr::Nav {
			..
		}
		| Instr::ArrayArg {
			..
		}
		| Instr::ElemNext {
			..
		}
		| Instr::ClosureBind {
			..
		}
		| Instr::ClosureReturn {
			..
		}
		| Instr::ClosureField {
			..
		}
		| Instr::RequireClosure {
			..
		}
		| Instr::Extract {
			..
		}
		| Instr::Mock {
			..
		}
		| Instr::RecordId {
			..
		}
		| Instr::MethodResolve {
			..
		}
		| Instr::JumpIfNullish {
			..
		}
		| Instr::JumpIfNone {
			..
		}
		| Instr::JumpIf {
			..
		}
		| Instr::Skip {
			..
		}
		| Instr::Take {
			..
		}
		| Instr::JumpIfLimitReached {
			..
		}
		| Instr::NewObject {
			..
		}
		| Instr::SeedObject {
			..
		}
		| Instr::BuildObject {
			..
		}
		| Instr::LoadPath {
			..
		}
		| Instr::LoadRow {
			..
		}
		| Instr::RemoveField {
			..
		}
		| Instr::RemovePath {
			..
		}
		| Instr::SetField {
			..
		}
		| Instr::Load {
			..
		}
		| Instr::Emit {
			..
		}
		| Instr::EmitEnd {
			..
		}
		| Instr::BuildObjectEmit {
			..
		}
		| Instr::FilterFieldInt {
			..
		}
		| Instr::FilterCompare {
			..
		}
		| Instr::Filter {
			..
		}
		| Instr::SplitPush {
			..
		}
		| Instr::SortPush {
			..
		}
		| Instr::Distinct {
			..
		}
		| Instr::JoinProbe {
			..
		}
		| Instr::JoinPair {
			..
		}
		| Instr::VerdictJump {
			..
		}
		| Instr::VerdictSet {
			..
		}
		| Instr::GroupKey {
			..
		}
		| Instr::GroupSelect {
			..
		}
		| Instr::AccUpdate {
			..
		}
		| Instr::Jump {
			..
		}
		| Instr::Next {
			..
		}
		| Instr::EndRow
		| Instr::StopPipeline
		| Instr::TryNone {
			..
		}
		| Instr::MethodRegion {
			..
		}
		| Instr::PermissionRegion {
			..
		}
		| Instr::TryEnd
		| Instr::GroupFirstNeeded {
			..
		}
		| Instr::GroupFirstSet {
			..
		}
		| Instr::NewSet {
			..
		}
		| Instr::JumpIfNotObject {
			..
		}
		| Instr::JumpIfOpaque {
			..
		}
		| Instr::InvalidControlFlow
		| Instr::JumpIfNotRecordId {
			..
		}
		| Instr::DocumentRoot {
			..
		} => named(Class::Leaf),
		Instr::LandRow {
			..
		} => named(step(&Step::LandRow {
			resume: 0,
		})),
		Instr::BindRow {
			..
		} => named(step(&Step::BindRow {
			name: 0,
			resume: 0,
		})),
		Instr::WriteProjection {
			..
		} => named(step(&Step::WriteProjection {
			at: 0,
		})),
		Instr::LoadParam {
			..
		} => named(step(&Step::Param {
			param: 0,
			dst: 0,
			frame: None,
			resume: 0,
		})),
		Instr::Fetch {
			..
		} => named(step(&Step::Fetch {
			at: 0,
		})),
		Instr::FetchRow {
			..
		} => named(step(&Step::FetchRow {
			at: 0,
		})),
		Instr::Access {
			..
		} => named(step(&Step::Access {
			at: 0,
		})),
		Instr::Each {
			..
		} => named(step(&Step::Each {
			at: 0,
		})),
		Instr::CallAllowed {
			..
		} => named(step(&Step::CallAllowed {
			call: 0,
			frame: None,
			resume: 0,
		})),
		Instr::Call {
			call,
			..
		} => {
			let spec = &program.calls[*call as usize];
			let class = builtin(&spec.name, registry);
			let class = if spec.closure_args.is_empty() {
				class
			} else {
				worse(
					class,
					step(&Step::NeedUnit {
						at: 0,
					}),
				)
			};
			(class, format!("call {}", spec.name))
		}
		Instr::Method {
			method: at,
			..
		} => {
			let spec = &program.methods[*at as usize];
			(method(&spec.name, registry), format!("method .{}()", spec.name))
		}
		Instr::MethodClosure {
			..
		}
		| Instr::MakeClosure {
			..
		}
		| Instr::Invoke {
			..
		} => named(step(&Step::Closure {
			at: 0,
		})),
		Instr::FunctionEnter {
			..
		}
		| Instr::FunctionBind {
			..
		}
		| Instr::FunctionReturn {
			..
		}
		| Instr::FunctionCall {
			..
		}
		| Instr::Dive {
			..
		} => named(step(&Step::Function {
			at: 0,
		})),
		Instr::Subquery {
			..
		} => named(step(&Step::Subquery {
			at: 0,
		})),
		Instr::Lookup {
			..
		} => named(step(&Step::Lookup {
			at: 0,
		})),
		Instr::Signal {
			..
		} => named(step(&Step::Signal {
			at: 0,
		})),
		Instr::Recurse {
			..
		}
		| Instr::RepeatRecurse {
			..
		} => named(step(&Step::Recurse {
			at: 0,
		})),
		Instr::Destructure {
			..
		} => named(step(&Step::Destructure {
			at: 0,
		})),
		Instr::JudgeCopy {
			..
		} => named(step(&Step::JudgeCopy {
			at: 0,
		})),
		Instr::Ir(i) => named(i.leaf_class()),
		Instr::Omit {
			..
		} => named(step(&Step::Omit {
			at: 0,
		})),
		Instr::CallIndexFn {
			..
		} => named(step(&Step::IndexFn {
			at: 0,
		})),
		Instr::Matches {
			..
		} => named(step(&Step::Matches {
			at: 0,
		})),
		Instr::Degree {
			..
		} => named(step(&Step::Degree {
			at: 0,
		})),
		// A query vector computed when the statement runs is evaluated by the
		// host before the first row is ranked against it.
		Instr::KnnRank {
			rank,
			..
		} => match rank.vector {
			super::program::KnnVector::Literal(_) => named(Class::Leaf),
			super::program::KnnVector::Query(_) => named(step(&Step::KnnQuery {
				at: 0,
			})),
		},
		// A vector measure is the call of its builtin, settled by the host as the run starts: a
		// literal query is the call's capability check alone, and a parameter query is read as
		// the host's answer for a row reads it ([`Step::Measure`]).
		Instr::Measure {
			measure,
			..
		} => named(worse(
			builtin(&program.calls[measure.call as usize].name, registry),
			step(&match measure.query {
				super::measure::MeasureQuery::Const(_) => Step::CallAllowed {
					call: measure.call,
					frame: None,
					resume: 0,
				},
				super::measure::MeasureQuery::Param(_) => Step::Measure {
					at: 0,
				},
			}),
		)),
		Instr::ExternalEnter {
			external,
			..
		}
		| Instr::ExternalCall {
			external,
			..
		} => {
			let (bar, kind) = match &program.externals[*external as usize].callee {
				ExternalCallee::Script {
					..
				} => (Bar::Script, "script"),
				ExternalCallee::Model {
					..
				} => (Bar::Model, "model"),
				ExternalCallee::Module {
					..
				} => (Bar::Script, "module"),
				ExternalCallee::Function {
					..
				} => (Bar::Script, "function"),
			};
			(Class::NonLeaf(bar), format!("external {kind}"))
		}
		Instr::Block(instr) => (block(instr), format!("Block::{}", variant(instr))),
		Instr::Store(instr) => (store(instr), format!("Store::{}", variant(instr))),
		Instr::Write(instr) => (write(instr), format!("Write::{}", variant(instr))),
	}
}

/// The class of source `source`, with the name its report gives it.
pub(super) fn source(source: &Source) -> (Class, String) {
	let class = match source {
		Source::Sorted {
			..
		}
		| Source::Groups {
			..
		}
		| Source::Split {
			..
		}
		| Source::Join {
			..
		}
		| Source::Once
		| Source::Union(_)
		| Source::Nested {
			..
		}
		| Source::Probe {
			..
		} => Class::Leaf,
		Source::Scan {
			..
		}
		| Source::Record {
			..
		}
		| Source::Index {
			..
		}
		| Source::Lookup {
			..
		}
		| Source::IndexUnion {
			..
		}
		| Source::Bitmap {
			..
		}
		| Source::FullText {
			..
		}
		| Source::Knn {
			..
		}
		| Source::LightweightEdges {
			..
		}
		| Source::Vacant {
			..
		} => Class::Kernel(&[Storage]),
		Source::Count {
			..
		}
		| Source::Values {
			..
		}
		| Source::Changes {
			..
		} => Class::Kernel(&[Storage, Permissions]),
		Source::WriteTargets {
			..
		}
		| Source::WriteInserts {
			..
		}
		| Source::WriteEdges {
			..
		} => Class::NonLeaf(Bar::Write),
	};
	(class, format!("source {}", variant(source)))
}

/// The name of the variant `value` is, as its `Debug` renders it.
fn variant(value: &impl std::fmt::Debug) -> String {
	let rendered = format!("{value:?}");
	let end = rendered.find(|c: char| !(c.is_alphanumeric() || c == '_')).unwrap_or(rendered.len());
	rendered[..end].to_owned()
}

/// What keeps a program from a leaf, and what of the engine a leaf host
/// would have to serve it.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Profile {
	/// The items no leaf runs, each with its bar.
	pub(crate) blockers: BTreeSet<(Bar, String)>,
	/// The items a leaf runs through the engine, by name.
	pub(crate) kernel: BTreeSet<String>,
	/// The seams those items reach.
	pub(crate) seams: BTreeSet<Seam>,
}

impl Profile {
	/// Whether nothing in the program is barred from a leaf.
	pub(crate) fn leaf_clean(&self) -> bool {
		self.blockers.is_empty()
	}

	/// Whether the program needs nothing of the engine either: every item is
	/// [`Class::Leaf`].
	pub(crate) fn host_free(&self) -> bool {
		self.blockers.is_empty() && self.kernel.is_empty()
	}

	fn note(&mut self, class: Class, name: impl FnOnce() -> String) {
		match class {
			Class::Leaf | Class::ByOperand => {}
			Class::Kernel(seams) => {
				self.kernel.insert(name());
				self.seams.extend(seams.iter().copied());
			}
			Class::NonLeaf(bar) => {
				self.blockers.insert((bar, name()));
			}
		}
	}
}

/// The leaf profile of `program` and of every unit it holds, its builtins
/// classified against `registry`.
pub(crate) fn profile(program: &Program, registry: &FunctionRegistry) -> Profile {
	let mut profile = Profile::default();
	let mut seen = Vec::new();
	visit(program, registry, &mut profile, &mut seen);
	profile
}

fn visit(
	program: &Program,
	registry: &FunctionRegistry,
	profile: &mut Profile,
	seen: &mut Vec<*const Program>,
) {
	if seen.contains(&std::ptr::from_ref(program)) {
		return;
	}
	seen.push(std::ptr::from_ref(program));
	if program.write.is_some() || program.groups.iter().any(|group| group.write.is_some()) {
		profile.note(Class::NonLeaf(Bar::Write), || "program write".to_owned());
	}
	if program.events.is_some() {
		profile.note(Class::NonLeaf(Bar::Write), || "program stages".to_owned());
	}
	if !program.reservations.is_empty() || !program.implicit_tables.is_empty() {
		profile.note(Class::NonLeaf(Bar::Speculation), || "program reservations".to_owned());
	}
	if program.reach.is_some() {
		profile.note(Class::Kernel(&[Link, Catalog]), || "program dispatch".to_owned());
	}
	let timeout = program.tail.as_ref().is_some_and(|tail| tail.timeout.is_some())
		|| program.groups.iter().any(|group| group.timeout.is_some());
	if timeout {
		profile.note(Class::Kernel(&[Clock]), || "program timeout".to_owned());
	}
	let version = program.tail.as_ref().is_some_and(|tail| tail.version.is_some())
		|| program.groups.iter().any(|group| group.version.is_some());
	if version {
		profile.note(Class::Kernel(&[Storage]), || "program version".to_owned());
	}
	if !program.permissions.is_empty() {
		profile.note(Class::Kernel(&[Permissions]), || "program permission frames".to_owned());
	}
	for pipeline in program.pipelines.iter().chain(&program.host_code) {
		visit_source(&pipeline.source, profile);
		for code in &pipeline.code {
			let (class, name) = instr(code, program, registry);
			profile.note(class, || name);
		}
	}
	let units: &[Arc<Program>] = &program.units;
	for unit in units {
		visit(unit, registry, profile, seen);
	}
}

fn visit_source(of: &Source, profile: &mut Profile) {
	let (class, name) = source(of);
	profile.note(class, || name);
	if let Source::Union(sources) = of {
		for inner in sources {
			visit_source(inner, profile);
		}
	}
	if let Source::Bitmap {
		fallback: Some(fallback),
		..
	} = of
	{
		visit_source(fallback, profile);
	}
}

/// The class of each method of [`super::services::Services`], by name: what a
/// leaf's host serves of the storage and index interface. The engine's
/// implementation is the only one, so the table is read by the test that
/// holds it to the trait.
#[cfg(test)]
pub(crate) const SERVICES: &[(&str, Class)] = &[
	// The IR host runs instructions the interpreter hands it: it needs the native host runtime,
	// and the interfaces each instruction reaches are classified at that instruction's step.
	("ir_host", Class::Kernel(&[])),
	("read_range", Class::Kernel(&[Storage])),
	("read_range_keys", Class::Kernel(&[Storage])),
	("evaluate_bitmap", Class::Kernel(&[Storage])),
	("resolve_doc_ids", Class::Kernel(&[Storage])),
	("read_values", Class::Kernel(&[Storage])),
	("open_full_text", Class::Kernel(&[Storage])),
	("pending_term_changes", Class::Kernel(&[Storage])),
	("knn_candidates", Class::Kernel(&[Storage])),
	("scan_lightweight_edges", Class::Kernel(&[Storage])),
	("read_records", Class::Kernel(&[Storage])),
	("read_stored", Class::Kernel(&[Storage])),
	("read_record", Class::Kernel(&[Storage])),
	("table_range", Class::Kernel(&[Storage])),
	("record_range", Class::Kernel(&[Storage])),
	("count_keys", Class::Kernel(&[Storage])),
	("record_exists", Class::Kernel(&[Storage])),
	("count_index_deltas", Class::Kernel(&[Storage])),
	("bitmap_cardinality", Class::Kernel(&[Storage])),
	("read_table", Class::Kernel(&[Catalog])),
	("read_function", Class::Kernel(&[Catalog])),
	("read_db_param", Class::Kernel(&[Params, Catalog])),
	("check_script", Class::NonLeaf(Bar::Script)),
	("call_script_dynamic", Class::NonLeaf(Bar::Script)),
	("read_model", Class::NonLeaf(Bar::Model)),
	("compute_model", Class::NonLeaf(Bar::Model)),
	("read_module", Class::NonLeaf(Bar::Script)),
	("module_export", Class::NonLeaf(Bar::Script)),
	("call_module", Class::NonLeaf(Bar::Script)),
];

/// The kind of statement `op` is, for the leaf report: `None` for one that
/// writes, defines, reports the catalog, waits, binds or loops, or explains,
/// which the report does not measure.
fn measured_kind(op: &crate::expr::logical::LogicalOp) -> Option<&'static str> {
	use crate::expr::logical::LogicalOp;
	let excluded = op.any(|node| {
		matches!(
			node,
			LogicalOp::Write(_)
				| LogicalOp::Ddl(_)
				| LogicalOp::Info(_)
				| LogicalOp::Sleep(_)
				| LogicalOp::Let(_)
				| LogicalOp::Foreach(_)
				| LogicalOp::Sequence(_)
				| LogicalOp::Explain(_)
		)
	});
	if excluded {
		return None;
	}
	Some(match op {
		LogicalOp::Return(_) => "return",
		LogicalOp::Eval(_) => "expr",
		LogicalOp::Window(_) => "window",
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => "select",
	})
}

/// Whether `SURREAL_VM_LEAF` is `report`. Read once for the process.
///
/// A measurement tool: off by default, never persisted and not a session
/// setting.
pub(super) fn reporting() -> bool {
	static REPORT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
	*REPORT.get_or_init(|| std::env::var("SURREAL_VM_LEAF").is_ok_and(|v| v.trim() == "report"))
}

/// Write the leaf profile of `program`, lowered from statement `op`, to
/// standard error as one `VM_LEAF` line, for a statement [`measured_kind`]
/// measures. The line names the statement by a hash of its bound form, so
/// the runs of one statement under several strategies count once.
pub(super) fn report(
	op: &crate::expr::logical::LogicalOp,
	program: &Program,
	registry: &FunctionRegistry,
) {
	use std::hash::{Hash, Hasher};
	let Some(kind) = measured_kind(op) else {
		return;
	};
	let profile = profile(program, registry);
	let mut hasher = std::collections::hash_map::DefaultHasher::new();
	op.hash(&mut hasher);
	let blockers = profile
		.blockers
		.iter()
		.map(|(bar, name)| format!("{bar:?}:{name}"))
		.collect::<Vec<_>>()
		.join(";");
	let kernel = profile.kernel.iter().cloned().collect::<Vec<_>>().join(";");
	let seams = profile.seams.iter().map(|seam| format!("{seam:?}")).collect::<Vec<_>>().join(";");
	eprintln!(
		"VM_LEAF kind={kind} stmt={:016x} clean={} host_free={} blockers=[{blockers}] kernel=[{kernel}] seams=[{seams}]",
		hasher.finish(),
		profile.leaf_clean(),
		profile.host_free(),
	);
}
