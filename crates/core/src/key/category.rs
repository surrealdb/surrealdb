use std::fmt::{Display, Formatter};

#[expect(unused)]
pub(crate) trait Categorise {
	/// Returns the category of the key for error reporting
	fn categorise(&self) -> Category;
}

#[derive(Debug, Copy, Clone)]
#[expect(unused)]
pub enum Category {
	/// crate::key::storage::version         /sv
	Version,
	/// crate::key::root::all                /
	Root,
	/// crate::key::root::access::ac         /!ac{ac}
	Access,
	/// crate::key::root::access::all        /*{ac}
	AccessRoot,
	/// crate::key::root::access::gr         /*{ac}!gr{gr}
	AccessGrant,
	/// crate::key::root::nd                 /!nd{nd}
	Node,
	/// crate::key::root::ni                 /!ni
	NamespaceIdentifier,
	/// crate::key::root::ns                 /!ns{ns}
	Namespace,
	/// crate::key::root::us                 /!us{us}
	User,
	/// crate::key::root::tl                 /!tl{tl}
	TaskLease,
	/// crate::key::root::ic                 /!ic{ns}{db}{tb}{ix}{nid}{uuid}
	IndexCompaction,
	///
	/// ------------------------------
	///
	/// crate::key::node::all                /${nd}
	NodeRoot,
	/// crate::key::node::lq                 /${nd}!lq{lq}{ns}{db}
	NodeLiveQuery,
	///
	/// ------------------------------
	///
	/// crate::key::namespace::di            /+{ni}!di
	DatabaseIdentifier,
	/// crate::key::database::ti             /+{ni}*{di}!ti
	DatabaseTableIdentifier,
	///
	/// ------------------------------
	///
	/// crate::key::namespace::all           /*{ns}
	NamespaceRoot,
	/// crate::key::namespace::db            /*{ns}!db{db}
	DatabaseAlias,
	/// crate::key::namespace::access::ac    /*{ns}!ac{ac}
	NamespaceAccess,
	/// crate::key::namespace::access::all   /*{ns}*{ac}
	NamespaceAccessRoot,
	/// crate::key::namespace::access::gr    /*{ns}*{ac}!gr{gr}
	NamespaceAccessGrant,
	/// crate::key::namespace::us            /*{ns}!us{us}
	NamespaceUser,
	///
	/// ------------------------------
	///
	/// crate::key::database::all            /*{ns}*{db}
	DatabaseRoot,
	/// crate::key::database::access::ac     /*{ns}*{db}!ac{ac}
	DatabaseAccess,
	/// crate::key::database::access::all    /*{ns}*{db}*{ac}
	DatabaseAccessRoot,
	/// crate::key::database::access::gr     /*{ns}*{db}*ac!gr{gr}
	DatabaseAccessGrant,
	/// crate::key::database::ap             /*{ns}*{db}!ap{ap}
	DatabaseApi,
	/// crate::key::database::az             /*{ns}*{db}!az{az}
	DatabaseAnalyzer,
	/// crate::key::database::bu             /*{ns}*{db}!bu{bu}
	DatabaseBucket,
	/// crate::key::database::fc             /*{ns}*{db}!fn{fc}
	DatabaseFunction,
	/// crate::key::database::ml             /*{ns}*{db}!ml{ml}{vn}
	DatabaseModel,
	/// crate::key::database::pa             /*{ns}*{db}!pa{pa}
	DatabaseParameter,
	/// crate::key::database::tb             /*{ns}*{db}!tb{tb}
	DatabaseTable,
	/// crate::key::database::ts             /*{ns}*{db}!ts{ts}
	DatabaseTimestamp,
	/// crate::key::database::us             /*{ns}*{db}!us{us}
	DatabaseUser,
	/// crate::key::database::vs             /*{ns}*{db}!vs
	DatabaseVersionstamp,
	/// crate::key::database::cg             /*{ns}*{db}!cg{ty}
	DatabaseConfig,
	/// crate::key::database::sq             /*{ns}*{db}*sq{sq}
	DatabaseSequence,
	///
	/// ------------------------------
	///
	/// crate::key::table::all               /*{ns}*{db}*{tb}
	TableRoot,
	/// crate::key::table::ev                /*{ns}*{db}*{tb}!ev{ev}
	TableEvent,
	/// crate::key::table::fd                /*{ns}*{db}*{tb}!fd{fd}
	TableField,
	/// crate::key::table::ft                /*{ns}*{db}*{tb}!ft{ft}
	TableView, // (ft = foreign table = view)
	/// crate::key::table::ix                /*{ns}*{db}*{tb}!ix{ix}
	IndexDefinition,
	/// crate::key::table::lq                /*{ns}*{db}*{tb}!lq{lq}
	TableLiveQuery,
	///
	/// ------------------------------
	///
	/// crate::key::index::all               /*{ns}*{db}*{tb}+{ix}
	IndexRoot,
	/// crate::key::index::bc                /*{ns}*{db}*{tb}+{ix}!bc{id}
	IndexTermDocList,
	/// crate::key::index::bd                /*{ns}*{db}*{tb}+{ix}!bd{id}
	IndexBTreeNode,
	/// crate::key::index::bf                /*{ns}*{db}*{tb}+{ix}!bf{id}
	IndexTermDocFrequency,
	/// crate::key::index::bi                /*{ns}*{db}*{tb}+{ix}!bi{id}
	IndexDocKeys,
	/// crate::key::index::bk                /*{ns}*{db}*{tb}+{ix}!bk{id}
	IndexTermList,
	/// crate::key::index::bl                /*{ns}*{db}*{tb}+{ix}!bl{id}
	IndexBTreeNodeDocLengths,
	/// crate::key::index::bo                /*{ns}*{db}*{tb}+{ix}!bo{id}
	IndexOffset,
	/// crate::key::index::bp                /*{ns}*{db}*{tb}+{ix}!bp{id}
	IndexBTreeNodePostings,
	/// crate::key::index::bs                /*{ns}*{db}*{tb}+{ix}!bs
	IndexFullTextState,
	/// crate::key::index::bt                /*{ns}*{db}*{tb}+{ix}!bt{id}
	IndexBTreeNodeTerms,
	/// crate::key::index::bu                /*{ns}*{db}*{tb}+{ix}!bu{id}
	IndexTerms,
	/// crate::key::index::dc                /*{ns}*{db}*{tb}+{ix}!dc{id}
	IndexFullTextDocCountAndLength,
	/// crate::key::index::dl                /*{ns}*{db}*{tb}+{ix}!dl{id}
	IndexDocLength,
	/// crate::key::index::td                /*{ns}*{db}*{tb}+{ix}!td{term}{id}
	IndexTermDocument,
	/// crate::key::index::tt
	/// /*{ns}*{db}*{tb}+{ix}!td{term}{uuid}{uuid}
	IndexTermDocuments,
	/// crate::key::index::he                /*{ns}*{db}*{tb}+{ix}!he{id}
	IndexHnswElements,
	/// crate::key::index::hd                /*{ns}*{db}*{tb}+{ix}!hd{id}
	IndexHnswDocIds,
	/// crate::key::index::hi               /*{ns}*{db}*{tb}+{ix}!hi{id}
	IndexHnswThings,
	/// crate::key::index::hv                /*{ns}*{db}*{tb}+{ix}!hv{vec}
	IndexHnswVec,
	/// crate::key::index::ia                /*{ns}*{db}*{tb}+{ix}!ia{id}
	IndexAppendings,
	/// crate::key::index::ib                /*{ns}*{db}*{tb}+{ix}!ib{id}
	IndexInvertedDocIds,
	/// crate::key::index::ip                /*{ns}*{db}*{tb}+{ix}!ip{id}
	IndexPrimaryAppending,
	/// crate::key::index::is                /*{ns}*{db}*{tb}+{ix}!is{uuid}
	IndexFullTextDocIdsSequenceState,
	/// crate::key::index                    /*{ns}*{db}*{tb}+{ix}*{fd}{id}
	Index,
	///
	/// ------------------------------
	///
	/// crate::key::change                   /*{ns}*{db}#{ts}
	ChangeFeed,
	///
	/// ------------------------------
	///
	/// crate::key::thing                    /*{ns}*{db}*{tb}*{id}
	Thing,
	///
	/// ------------------------------
	///
	/// crate::key::graph                    /*{ns}*{db}*{tb}~{id}{eg}{ft}{fk}
	Graph,
	///
	/// ------------------------------
	///
	/// crate::key::ref                      /*{ns}*{db}*{tb}&{id}{ft}{ff}{fk}
	Ref,
	///
	/// ------------------------------
	///
	/// crate::seq::state                      /*{ns}*{db}!sq{sq}!st{nid}
	SequenceState,
	/// crate::seq::batch                      /*{ns}*{db}!sq{sq}!ba{start}
	SequenceBatch,
}

impl Display for Category {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let name = match self {
			Self::Version => "StorageVersion",
			Self::Root => "Root",
			Self::Access => "Access",
			Self::AccessRoot => "AccessRoot",
			Self::AccessGrant => "AccessGrant",
			Self::Node => "Node",
			Self::NamespaceIdentifier => "NamespaceIdentifier",
			Self::Namespace => "Namespace",
			Self::User => "User",
			Self::NodeRoot => "NodeRoot",
			Self::NodeLiveQuery => "NodeLiveQuery",
			Self::NamespaceRoot => "NamespaceRoot",
			Self::DatabaseAlias => "DatabaseAlias",
			Self::DatabaseIdentifier => "DatabaseIdentifier",
			Self::NamespaceAccess => "NamespaceAccess",
			Self::NamespaceAccessRoot => "NamespaceAccessRoot",
			Self::NamespaceAccessGrant => "NamespaceAccessGrant",
			Self::NamespaceUser => "NamespaceUser",
			Self::DatabaseRoot => "DatabaseRoot",
			Self::DatabaseAccess => "DatabaseAccess",
			Self::DatabaseAccessRoot => "DatabaseAccessRoot",
			Self::DatabaseAccessGrant => "DatabaseAccessGrant",
			Self::DatabaseApi => "DatabaseApi",
			Self::DatabaseAnalyzer => "DatabaseAnalyzer",
			Self::DatabaseBucket => "DatabaseBucket",
			Self::DatabaseFunction => "DatabaseFunction",
			Self::DatabaseModel => "DatabaseModel",
			Self::DatabaseParameter => "DatabaseParameter",
			Self::DatabaseTable => "DatabaseTable",
			Self::DatabaseTableIdentifier => "DatabaseTableIdentifier",
			Self::DatabaseTimestamp => "DatabaseTimestamp",
			Self::DatabaseUser => "DatabaseUser",
			Self::DatabaseVersionstamp => "DatabaseVersionstamp",
			Self::DatabaseSequence => "DatabaseSequence",
			Self::DatabaseConfig => "DatabaseConfig",
			Self::TableRoot => "TableRoot",
			Self::TableEvent => "TableEvent",
			Self::TableField => "TableField",
			Self::TableView => "TableView",
			Self::IndexDefinition => "IndexDefinition",
			Self::TableLiveQuery => "TableLiveQuery",
			Self::IndexRoot => "IndexRoot",
			Self::IndexTermDocList => "IndexTermDocList",
			Self::IndexBTreeNode => "IndexBTreeNode",
			Self::IndexTermDocFrequency => "IndexTermDocFrequency",
			Self::IndexDocKeys => "IndexDocKeys",
			Self::IndexDocLength => "IndexDocLength",
			Self::IndexTermDocument => "IndexTermDocument",
			Self::IndexTermList => "IndexTermList",
			Self::IndexBTreeNodeDocLengths => "IndexBTreeNodeDocLengths",
			Self::IndexOffset => "IndexOffset",
			Self::IndexBTreeNodePostings => "IndexBTreeNodePostings",
			Self::IndexFullTextState => "IndexFullTextState",
			Self::IndexBTreeNodeTerms => "IndexBTreeNodeTerms",
			Self::IndexTerms => "IndexTerms",
			Self::IndexHnswElements => "IndexHnswElements",
			Self::IndexHnswDocIds => "IndexHnswDocIds",
			Self::IndexHnswThings => "IndexHnswThings",
			Self::IndexHnswVec => "IndexHnswVec",
			Self::IndexAppendings => "IndexAppendings",
			Self::IndexPrimaryAppending => "IndexPrimaryAppending",
			Self::Index => "Index",
			Self::ChangeFeed => "ChangeFeed",
			Self::Thing => "Thing",
			Self::Graph => "Graph",
			Self::Ref => "Ref",
			Self::SequenceState => "SequenceState",
			Self::SequenceBatch => "SequenceBatch",
			Self::TaskLease => "TaskLease",
			Self::IndexInvertedDocIds => "IndexInvertedDocIds",
			Self::IndexFullTextDocIdsSequenceState => "IndexFullTextDocIdsSequenceState",
			Self::IndexFullTextDocCountAndLength => "IndexFullTextDocCountAndLength",
			Self::IndexTermDocuments => "IndexTermDocuments",
			Self::IndexCompaction => "IndexCompaction",
		};
		write!(f, "{}", name)
	}
}
