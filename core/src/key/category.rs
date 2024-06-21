use std::fmt::{Display, Formatter};

pub(crate) trait Categorise {
	/// Returns the category of the key for error reporting
	fn categorise(&self) -> Category;
}

#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum Category {
	/// crate::key::root::all                /
	Root,
	/// crate::key::root::ac                 /!ac{ac}
	Access,
	/// crate::key::root::nd                 /!nd{nd}
	Node,
	/// crate::key::root::ni                 /!ni
	NamespaceIdentifier,
	/// crate::key::root::ns                 /!ns{ns}
	Namespace,
	/// crate::key::root::us                 /!us{us}
	User,
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
	/// crate::key::namespace::ac            /*{ns}!ac{ac}
	NamespaceAccess,
	/// crate::key::namespace::us            /*{ns}!us{us}
	NamespaceUser,
	///
	/// ------------------------------
	///
	/// crate::key::database::all            /*{ns}*{db}
	DatabaseRoot,
	/// crate::key::database::ac             /*{ns}*{db}!ac{ac}
	DatabaseAccess,
	/// crate::key::database::az             /*{ns}*{db}!az{az}
	DatabaseAnalyzer,
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
	/// crate::key::graph                    /*{ns}*{db}*{tb}~{id}{eg}{fk}
	Graph,
}

impl Display for Category {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let name = match self {
			Category::Root => "Root",
			Category::Access => "Access",
			Category::Node => "Node",
			Category::NamespaceIdentifier => "NamespaceIdentifier",
			Category::Namespace => "Namespace",
			Category::User => "User",
			Category::NodeRoot => "NodeRoot",
			Category::NodeLiveQuery => "NodeLiveQuery",
			Category::NamespaceRoot => "NamespaceRoot",
			Category::DatabaseAlias => "DatabaseAlias",
			Category::DatabaseIdentifier => "DatabaseIdentifier",
			Category::NamespaceAccess => "NamespaceAccess",
			Category::NamespaceUser => "NamespaceUser",
			Category::DatabaseRoot => "DatabaseRoot",
			Category::DatabaseAccess => "DatabaseAccess",
			Category::DatabaseAnalyzer => "DatabaseAnalyzer",
			Category::DatabaseFunction => "DatabaseFunction",
			Category::DatabaseModel => "DatabaseModel",
			Category::DatabaseParameter => "DatabaseParameter",
			Category::DatabaseTable => "DatabaseTable",
			Category::DatabaseTableIdentifier => "DatabaseTableIdentifier",
			Category::DatabaseTimestamp => "DatabaseTimestamp",
			Category::DatabaseUser => "DatabaseUser",
			Category::DatabaseVersionstamp => "DatabaseVersionstamp",
			Category::TableRoot => "TableRoot",
			Category::TableEvent => "TableEvent",
			Category::TableField => "TableField",
			Category::TableView => "TableView",
			Category::IndexDefinition => "IndexDefinition",
			Category::TableLiveQuery => "TableLiveQuery",
			Category::IndexRoot => "IndexRoot",
			Category::IndexTermDocList => "IndexTermDocList",
			Category::IndexBTreeNode => "IndexBTreeNode",
			Category::IndexTermDocFrequency => "IndexTermDocFrequency",
			Category::IndexDocKeys => "IndexDocKeys",
			Category::IndexTermList => "IndexTermList",
			Category::IndexBTreeNodeDocLengths => "IndexBTreeNodeDocLengths",
			Category::IndexOffset => "IndexOffset",
			Category::IndexBTreeNodePostings => "IndexBTreeNodePostings",
			Category::IndexFullTextState => "IndexFullTextState",
			Category::IndexBTreeNodeTerms => "IndexBTreeNodeTerms",
			Category::IndexTerms => "IndexTerms",
			Category::Index => "Index",
			Category::ChangeFeed => "ChangeFeed",
			Category::Thing => "Thing",
			Category::Graph => "Graph",
		};
		write!(f, "{}", name)
	}
}
