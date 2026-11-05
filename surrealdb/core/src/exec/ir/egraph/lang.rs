//! The e-graph's node language: the encoding of an op's identity as an e-node.
//!
//! A node's identity is the value identity of the op it encodes (tag, attribute digest, operands,
//! frame, snapshot and epoch), so two nodes are equal exactly when two ops compute one value. An
//! op whose key is opaque is never merged with another.

use egg::{Id, Language};
use smallvec::SmallVec;

use super::decl::{ClassRef, ENodeSnapshot, NodeIdentity, NodeShape};
use crate::exec::ir::iface::region_kind::{
	ClauseRole, ErrorScope, FieldRole, RegionKind, StatementCommit, StoredRole, TxnKind,
};
use crate::exec::ir::op_kind::OpTag;

/// An e-node: the identity of an op, and the classes of its operands.
///
/// The identity does not hold the operands: the e-graph compares `kids`, and canonicalises them
/// as classes merge.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ENode {
	pub(crate) id: NodeIdentity,
	pub(crate) kids: SmallVec<[Id; 4]>,
}

/// The cases of the node language, for the e-graph's per-case class index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Disc {
	/// An op of this tag.
	Op(OpTag),
	/// A region of the kind with this key (see [`region_kind_key`]).
	Region(u16),
	Arg,
	Opaque,
}

impl Language for ENode {
	type Discriminant = Disc;

	fn discriminant(&self) -> Disc {
		match self.id.shape {
			NodeShape::Op {
				tag,
				..
			} => Disc::Op(tag),
			NodeShape::Region {
				kind_key,
				..
			} => Disc::Region(kind_key),
			NodeShape::Arg {
				..
			} => Disc::Arg,
			NodeShape::Opaque {
				..
			} => Disc::Opaque,
		}
	}

	/// Whether two nodes have one identity and one arity; the e-graph compares the kids.
	fn matches(&self, other: &Self) -> bool {
		self.id == other.id && self.kids.len() == other.kids.len()
	}

	fn children(&self) -> &[Id] {
		&self.kids
	}

	fn children_mut(&mut self) -> &mut [Id] {
		&mut self.kids
	}
}

/// The key of a region kind in a node's identity: distinct for every kind, and for every payload
/// of a kind that carries one.
///
/// Every variant is named, so a new kind fails the build here until it is given a key.
pub(crate) fn region_kind_key(kind: RegionKind) -> u16 {
	match kind {
		RegionKind::Request => 0,
		RegionKind::Transaction(txn) => match txn {
			TxnKind::Explicit => 1,
			TxnKind::Session => 2,
		},
		RegionKind::Statement(commit) => match commit {
			StatementCommit::Auto => 3,
			StatementCommit::InTxn => 4,
		},
		RegionKind::Block => 5,
		RegionKind::ValueBlock => 6,
		RegionKind::StatementIf => 7,
		RegionKind::ValueIf => 8,
		RegionKind::StatementFor => 9,
		RegionKind::ValueFor => 10,
		RegionKind::Loop => 11,
		RegionKind::Subquery => 12,
		RegionKind::FunctionBody => 13,
		RegionKind::ClosureBody => 14,
		RegionKind::PermissionFrame => 15,
		RegionKind::FieldFrame(role) => match role {
			FieldRole::Value => 16,
			FieldRole::Default => 17,
			FieldRole::Assert => 18,
			FieldRole::Computed => 19,
		},
		RegionKind::EventFrame => 20,
		RegionKind::SubscriberFrame => 21,
		RegionKind::Clause(role) => match role {
			ClauseRole::Where => 22,
			ClauseRole::WriteCond => 23,
			ClauseRole::OrderKey => 24,
			ClauseRole::GroupKey => 25,
			ClauseRole::Split => 26,
			ClauseRole::Projection => 27,
			ClauseRole::SetValue => 28,
			ClauseRole::Content => 29,
			ClauseRole::InsertValues => 30,
			ClauseRole::DuplicateUpdate => 31,
			ClauseRole::ReturnClause => 32,
			ClauseRole::From => 33,
			ClauseRole::Limit => 34,
			ClauseRole::Start => 35,
			ClauseRole::Timeout => 36,
			ClauseRole::IndexColumn => 65,
			ClauseRole::ViewClause => 66,
			ClauseRole::ReferenceRepair => 67,
		},
		RegionKind::LazyOperand => 37,
		RegionKind::ErrorScope(scope) => match scope {
			ErrorScope::ShapeToNone => 38,
			ErrorScope::MethodBody => 39,
			ErrorScope::PermissionCheck => 40,
		},
		RegionKind::ChooseArm => 41,
		RegionKind::Pipeline => 42,
		RegionKind::JoinLookupArm => 68,
		RegionKind::WriteStage => 43,
		RegionKind::CatalogUnit => 44,
		RegionKind::Deadline => 45,
		RegionKind::StoredClause(role) => match role {
			StoredRole::Value => 46,
			StoredRole::Assert => 47,
			StoredRole::Default => 48,
			StoredRole::Computed => 49,
			StoredRole::Permissions => 50,
			StoredRole::When => 51,
			StoredRole::Then => 52,
			StoredRole::Body => 53,
			StoredRole::View => 54,
			StoredRole::IndexColumn => 55,
			StoredRole::Authenticate => 56,
			StoredRole::Context => 57,
			StoredRole::Signup => 58,
			StoredRole::Signin => 59,
			StoredRole::Action => 60,
			StoredRole::Fallback => 61,
			StoredRole::LiveFields => 62,
			StoredRole::LiveCond => 63,
			StoredRole::LiveFetch => 64,
		},
	}
}

impl From<Id> for ClassRef {
	fn from(id: Id) -> Self {
		ClassRef(usize::from(id) as u32)
	}
}

impl From<ClassRef> for Id {
	fn from(class: ClassRef) -> Self {
		Id::from(class.0 as usize)
	}
}

/// The e-node a snapshot captured: its identity, with its kids as e-graph ids.
pub(crate) fn identity_of(snapshot: &ENodeSnapshot) -> ENode {
	ENode {
		id: snapshot.identity,
		kids: snapshot.kids.iter().copied().map(Id::from).collect(),
	}
}

/// The snapshot of `node`, with its kids as boundary handles.
pub(crate) fn snapshot_of(node: &ENode) -> ENodeSnapshot {
	ENodeSnapshot {
		identity: node.id,
		kids: node.kids.iter().copied().map(ClassRef::from).collect(),
	}
}
