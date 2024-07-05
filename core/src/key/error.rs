use std::fmt::{Display, Formatter};

#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum KeyCategory {
	/// This category is reserved for cases when we do not know the category
	/// It should be caught and re-populated with the correct category where appropriate
	Unknown,
	/// crate::key::root::all                /
	Root,
	/// crate::key::root::ac                 /!ac{ac}
	Access,
	/// crate::key::root::hb                 /!hb{ts}/{nd}
	Heartbeat,
	/// crate::key::root::nd                 /!nd{nd}
	Node,
	/// crate::key::root::ni                 /!ni
	NamespaceIdentifier,
	/// crate::key::root::ns                 /!ns{ns}
	Namespace,
	/// crate::key::root::us                 /!us{us}
	User,
	///
	/// crate::key::node::all                /${nd}
	NodeRoot,
	/// crate::key::node::lq                 /${nd}!lq{lq}{ns}{db}
	NodeLiveQuery,
	///
	/// crate::key::namespace::all           /*{ns}
	NamespaceRoot,
	/// crate::key::namespace::db            /*{ns}!db{db}
	DatabaseAlias,
	/// crate::key::namespace::di            /+{ns id}!di
	DatabaseIdentifier,
	/// crate::key::namespace::lg            /*{ns}!lg{lg}
	DatabaseLogAlias,
	/// crate::key::namespace::access::all   /*{ns}*{ac}
	NamespaceAccessRoot,
	/// crate::key::namespace::access::ac    /*{ns}!ac{ac}
	NamespaceAccess,
	/// crate::key::namespace::access::gr    /*{ns}*{ac}!gr{gr}
	NamespaceAccessGrant,
	/// crate::key::namespace::us            /*{ns}!us{us}
	NamespaceUser,
	///
	/// crate::key::database::all            /*{ns}*{db}
	DatabaseRoot,
	/// crate::key::database::access::all    /*{ns}*{db}*{ac}
	DatabaseAccessRoot,
	/// crate::key::database::access::ac     /*{ns}*{db}!ac{ac}
	DatabaseAccess,
	/// crate::key::database::access::gr     /*{ns}*{db}*ac!gr{gr}
	DatabaseAccessGrant,
	/// crate::key::database::az             /*{ns}*{db}!az{az}
	DatabaseAnalyzer,
	/// crate::key::database::fc             /*{ns}*{db}!fn{fc}
	DatabaseFunction,
	/// crate::key::database::lg             /*{ns}*{db}!lg{lg}
	DatabaseLog,
	/// crate::key::database::ml             /*{ns}*{db}!ml{ml}{vn}
	DatabaseModel,
	/// crate::key::database::pa             /*{ns}*{db}!pa{pa}
	DatabaseParameter,
	/// crate::key::database::tb             /*{ns}*{db}!tb{tb}
	DatabaseTable,
	/// crate::key::database::ti             /+{ns id}*{db id}!ti
	DatabaseTableIdentifier,
	/// crate::key::database::ts             /*{ns}*{db}!ts{ts}
	DatabaseTimestamp,
	/// crate::key::database::us             /*{ns}*{db}!us{us}
	DatabaseUser,
	/// crate::key::database::vs             /*{ns}*{db}!vs
	DatabaseVersionstamp,
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
	/// crate::key::index                    /*{ns}*{db}*{tb}+{ix}*{fd}{id}
	Index,
	///
	/// crate::key::change                   /*{ns}*{db}#{ts}
	ChangeFeed,
	///
	/// crate::key::thing                    /*{ns}*{db}*{tb}*{id}
	Thing,
	///
	/// crate::key::graph                    /*{ns}*{db}*{tb}~{id}{eg}{fk}
	Graph,
}

impl Display for KeyCategory {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let name = match self {
			KeyCategory::Unknown => "Unknown",
			KeyCategory::Root => "Root",
			KeyCategory::Access => "Access",
			KeyCategory::Heartbeat => "Heartbeat",
			KeyCategory::Node => "Node",
			KeyCategory::NamespaceIdentifier => "NamespaceIdentifier",
			KeyCategory::Namespace => "Namespace",
			KeyCategory::User => "User",
			KeyCategory::NodeRoot => "NodeRoot",
			KeyCategory::NodeLiveQuery => "NodeLiveQuery",
			KeyCategory::NamespaceRoot => "NamespaceRoot",
			KeyCategory::DatabaseAlias => "DatabaseAlias",
			KeyCategory::DatabaseIdentifier => "DatabaseIdentifier",
			KeyCategory::DatabaseLogAlias => "DatabaseLogAlias",
			KeyCategory::NamespaceAccessRoot => "NamespaceAccessRoot",
			KeyCategory::NamespaceAccess => "NamespaceAccess",
			KeyCategory::NamespaceAccessGrant => "NamespaceAccessGrant",
			KeyCategory::NamespaceUser => "NamespaceUser",
			KeyCategory::DatabaseRoot => "DatabaseRoot",
			KeyCategory::DatabaseAccessRoot => "DatabaseAccessRoot",
			KeyCategory::DatabaseAccess => "DatabaseAccess",
			KeyCategory::DatabaseAccessGrant => "DatabaseAccessGrant",
			KeyCategory::DatabaseAnalyzer => "DatabaseAnalyzer",
			KeyCategory::DatabaseFunction => "DatabaseFunction",
			KeyCategory::DatabaseLog => "DatabaseLog",
			KeyCategory::DatabaseModel => "DatabaseModel",
			KeyCategory::DatabaseParameter => "DatabaseParameter",
			KeyCategory::DatabaseTable => "DatabaseTable",
			KeyCategory::DatabaseTableIdentifier => "DatabaseTableIdentifier",
			KeyCategory::DatabaseTimestamp => "DatabaseTimestamp",
			KeyCategory::DatabaseUser => "DatabaseUser",
			KeyCategory::DatabaseVersionstamp => "DatabaseVersionstamp",
			KeyCategory::TableRoot => "TableRoot",
			KeyCategory::TableEvent => "TableEvent",
			KeyCategory::TableField => "TableField",
			KeyCategory::TableView => "TableView",
			KeyCategory::IndexDefinition => "IndexDefinition",
			KeyCategory::TableLiveQuery => "TableLiveQuery",
			KeyCategory::IndexRoot => "IndexRoot",
			KeyCategory::IndexTermDocList => "IndexTermDocList",
			KeyCategory::IndexBTreeNode => "IndexBTreeNode",
			KeyCategory::IndexTermDocFrequency => "IndexTermDocFrequency",
			KeyCategory::IndexDocKeys => "IndexDocKeys",
			KeyCategory::IndexTermList => "IndexTermList",
			KeyCategory::IndexBTreeNodeDocLengths => "IndexBTreeNodeDocLengths",
			KeyCategory::IndexOffset => "IndexOffset",
			KeyCategory::IndexBTreeNodePostings => "IndexBTreeNodePostings",
			KeyCategory::IndexFullTextState => "IndexFullTextState",
			KeyCategory::IndexBTreeNodeTerms => "IndexBTreeNodeTerms",
			KeyCategory::IndexTerms => "IndexTerms",
			KeyCategory::Index => "Index",
			KeyCategory::ChangeFeed => "ChangeFeed",
			KeyCategory::Thing => "Thing",
			KeyCategory::Graph => "Graph",
		};
		write!(f, "{}", name)
	}
}
