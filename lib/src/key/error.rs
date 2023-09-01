pub(crate) enum KeyError {
	/// crate::key::root::all                /
	Root,
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
	/// crate::key::namespace::tk            /*{ns}!tk{tk}
	NamespaceToken,
	/// crate::key::namespace::us            /*{ns}!us{us}
	NamespaceUser,
	///
	/// crate::key::database::all            /*{ns}*{db}
	DatabaseRoot,
	/// crate::key::database::az             /*{ns}*{db}!az{az}
	DatabaseAuthorization,
	/// crate::key::database::fc             /*{ns}*{db}!fn{fc}
	DatabaseFunction,
	/// crate::key::database::lg             /*{ns}*{db}!lg{lg}
	DatabaseLog,
	/// crate::key::database::pa             /*{ns}*{db}!pa{pa}
	DatabasePartition,
	/// crate::key::database::sc             /*{ns}*{db}!sc{sc}
	DatabaseScope,
	/// crate::key::database::tb             /*{ns}*{db}!tb{tb}
	DatabaseTable,
	/// crate::key::database::ti             /+{ns id}*{db id}!ti
	DatabaseTableIdentifier,
	/// crate::key::database::tk             /*{ns}*{db}!tk{tk}
	DatabaseToken,
	/// crate::key::database::ts             /*{ns}*{db}!ts{ts}
	DatabaseTimestamp,
	/// crate::key::database::vs             /*{ns}*{db}!vs
	DatabaseVersionstamp,
	///
	/// crate::key::scope::all               /*{ns}*{db}±{sc}
	ScopeRoot,
	/// crate::key::scope::tk                /*{ns}*{db}±{sc}!tk{tk}
	ScopeToken,
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
	IndexBloom,
	/// crate::key::index::bd                /*{ns}*{db}*{tb}+{ix}!bd{id}
	IndexData,
	/// crate::key::index::bf                /*{ns}*{db}*{tb}+{ix}!bf{id}
	IndexFilter,
	/// crate::key::index::bi                /*{ns}*{db}*{tb}+{ix}!bi{id}
	IndexBloomInfo,
	/// crate::key::index::bk                /*{ns}*{db}*{tb}+{ix}!bk{id}
	IndexBloomKey,
	/// crate::key::index::bl                /*{ns}*{db}*{tb}+{ix}!bl{id}
	IndexBloomLock,
	/// crate::key::index::bo                /*{ns}*{db}*{tb}+{ix}!bo{id}
	IndexBloomOffset,
	/// crate::key::index::bp                /*{ns}*{db}*{tb}+{ix}!bp{id}
	IndexBloomPartition,
	/// crate::key::index::bs                /*{ns}*{db}*{tb}+{ix}!bs
	IndexBloomSize,
	/// crate::key::index::bt                /*{ns}*{db}*{tb}+{ix}!bt{id}
	IndexBloomTimestamp,
	/// crate::key::index::bu                /*{ns}*{db}*{tb}+{ix}!bu{id}
	IndexBloomUuid,
	/// crate::key::index                    /*{ns}*{db}*{tb}+{ix}*{fd}{id}
	Index,
	///
	/// crate::key::change                   /*{ns}*{db}#{ts}
	Change,
	///
	/// crate::key::thing                    /*{ns}*{db}*{tb}*{id}
	Thing,
	///
	/// crate::key::graph                    /*{ns}*{db}*{tb}~{id}{eg}{fk}
	Graph,
}
