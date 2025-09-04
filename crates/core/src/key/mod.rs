//! This crate defines the key structure for the key value store.
//!
//! Key struct naming conventions:
//! `XxKey` - A specific key type. eg. `/*{ns}*{db}*{tb}*{id}`
//! `XxRoot` - A root key which prefixes other keys. eg. `/*{ns}*{db}`
//! `XxRange` - Represents a start and end key. eg. `/*{ns}*{db}#\x00` or
//! `/*{ns}*{db}#\xff`
//!
//!
//!
//! Terminology:
//! - `/`: Root identifier
//! - `*`: Path separator
//! - `!`: Catalog identifier
//!
//! - {ns}: NamespaceId
//! - {db}: DatabaseId
//! - {ns_name}: &str
//! - {db_name}: &str
//!
//! crate::key::version                  !v -> Version
//!
//! crate::key::root::all                /
//! crate::key::root::ac                 /!ac{ac}
//! crate::key::root::nd                 /!nd{nd}
//! crate::key::root::ni                 /!ni
//! crate::key::root::ns                 /!ns{ns} -> NamespaceDefinition
//! crate::key::root::us                 /!us{us}
//! crate::key::root::tl                 /!tl{tl}
//!
//! crate::key::node::all                /${nd}
//! crate::key::node::lq                 /${nd}!lq{lq}{ns}{db}
//!
//! crate::key::root::access::all        /&{ac}
//! crate::key::root::access::gr         /&{ac}!gr{gr}
//!
//! crate::key::namespace::all           /*{ns}
//! crate::key::namespace::ac            /*{ns}!ac{ac}
//! crate::key::namespace::db            /*{ns}!db{db_name} -> DatabaseDefinition
//! crate::key::namespace::di            /+{ns}!di
//! crate::key::namespace::lg            /*{ns}!lg{lg}
//! crate::key::namespace::us            /*{ns}!us{us}
//!
//! crate::key::namespace::access::all   /*{ns}&{ac}
//! crate::key::namespace::access::gr    /*{ns}&{ac}!gr{gr}
//!
//! crate::key::database::all            /*{ns}*{db}
//! crate::key::database::ac             /*{ns}*{db}!ac{ac}
//! crate::key::database::az             /*{ns}*{db}!az{az}
//! crate::key::database::bu             /*{ns}*{db}!bu{bu}
//! crate::key::database::fc             /*{ns}*{db}!fn{fc}
//! crate::key::database::ml             /*{ns}*{db}!ml{ml}{vn}
//! crate::key::database::pa             /*{ns}*{db}!pa{pa}
//! crate::key::database::sq             /*{ns}*{db}!sq{sq}
//! crate::key::database::tb             /*{ns}*{db}!tb{tb_name} -> TableDefinition
//! crate::key::database::ti             /+{ns}*{db}!ti
//! crate::key::database::ts             /*{ns}*{db}!ts{ts}
//! crate::key::database::us             /*{ns}*{db}!us{us}
//! crate::key::database::vs             /*{ns}*{db}!vs
//! crate::key::database::cg             /*{ns}*{db}!cg{ty}
//!
//! crate::key::database::access::all    /*{ns}*{db}&{ac}
//! crate::key::database::access::gr     /*{ns}*{db}&{ac}!gr{gr}
//!
//! crate::key::table::all               /*{ns}*{db}*{tb}
//! crate::key::table::ev                /*{ns}*{db}*{tb}!ev{ev}
//! crate::key::table::fd                /*{ns}*{db}*{tb}!fd{fd}
//! crate::key::table::ft                /*{ns}*{db}*{tb}!ft{ft}
//! crate::key::table::ix                /*{ns}*{db}*{tb}!ix{ix}
//! crate::key::table::lq                /*{ns}*{db}*{tb}!lq{lq}
//!
//! crate::key::index::all               /*{ns}*{db}*{tb}+{ix}
//! crate::key::index::bc                /*{ns}*{db}*{tb}+{ix}!bc{id}
//! crate::key::index::bd                /*{ns}*{db}*{tb}+{ix}!bd{id}
//! crate::key::index::bf                /*{ns}*{db}*{tb}+{ix}!bf{id}
//! crate::key::index::bi                /*{ns}*{db}*{tb}+{ix}!bi{id}
//! crate::key::index::bk                /*{ns}*{db}*{tb}+{ix}!bk{id}
//! crate::key::index::bl                /*{ns}*{db}*{tb}+{ix}!bl{id}
//! crate::key::index::bo                /*{ns}*{db}*{tb}+{ix}!bo{id}
//! crate::key::index::bp                /*{ns}*{db}*{tb}+{ix}!bp{id}
//! crate::key::index::bs                /*{ns}*{db}*{tb}+{ix}!bs
//! crate::key::index::bt                /*{ns}*{db}*{tb}+{ix}!bt{id}
//! crate::key::index::bu                /*{ns}*{db}*{tb}+{ix}!bu{id}
//! crate::key::index::dl                /*{ns}*{db}*{tb}+{ix}!dl{id}
//! crate::key::index::tf                /*{ns}*{db}*{tb}+{ix}!tf{term}{id}
//! crate::key::index                    /*{ns}*{db}*{tb}+{ix}*{fd}{id}
//!
//! crate::key::change::vs_key_prefix    /*{ns}*{db}#
//! crate::key::change::vs_key_suffix                *{tb}\00
//! crate::key::change::prefix           /*{ns}*{db}#
//! crate::key::change::prefix_ts        /*{ns}*{db}#{ts}
//! crate::key::change::suffix           /*{ns}*{db}#\ff
//! crate::key::change::cf               /*{ns}*{db}#{ts}*{tb}
//! crate::key::change::vs               /*{ns}*{db}#{ts}/*{ns}/*/{db}!vs*{tb}\0
//! crate::key::change::suffix_vs        /*{ns}*{db}#{ts}/*{ns}/*/{db}!vs
//!
//! crate::key::thing                    /*{ns}*{db}*{tb}*{id}
//!
//! crate::key::graph                    /*{ns}*{db}*{tb}~{id}{eg}{ft}{fk}
//! crate::key::ref                      /*{ns}*{db}*{tb}&{id}{ft}{fk}{ff}
//!
//! crate::key::sequence::st             /*{ns}*{db}*{tb}*{sq}!st{id}
//! crate::key::sequence::ba             /*{ns}*{db}*{tb}*{sq}!ba{start}
pub(crate) mod category;
pub(crate) mod change;
pub(crate) mod database;
pub(crate) mod debug;
pub(crate) mod graph;
pub(crate) mod index;
pub(crate) mod namespace;
pub(crate) mod node;
pub(crate) mod r#ref;
pub(crate) mod root;
pub(crate) mod sequence;
pub(crate) mod table;
pub(crate) mod thing;
pub(crate) mod value;
pub(crate) mod version;
