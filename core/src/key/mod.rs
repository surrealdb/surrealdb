//! How the keys are structured in the key value store
///
/// crate::key::root::all                /
/// crate::key::root::hb                 /!hb{ts}/{nd}
/// crate::key::root::nd                 /!nd{nd}
/// crate::key::root::ni                 /!ni
/// crate::key::root::ns                 /!ns{ns}
/// crate::key::root::us                 /!us{us}
///
/// crate::key::node::all                /${nd}
/// crate::key::node::lq                 /${nd}!lq{lq}{ns}{db}
/// crate::key::node::se                 /${nd}!se{se}
///
/// crate::key::namespace::all           /*{ns}
/// crate::key::namespace::db            /*{ns}!db{db}
/// crate::key::namespace::di            /+{ns id}!di
/// crate::key::namespace::lg            /*{ns}!lg{lg}
/// crate::key::namespace::tk            /*{ns}!tk{tk}
/// crate::key::namespace::us            /*{ns}!us{us}
///
/// crate::key::database::all            /*{ns}*{db}
/// crate::key::database::az             /*{ns}*{db}!az{az}
/// crate::key::database::fc             /*{ns}*{db}!fn{fc}
/// crate::key::database::lg             /*{ns}*{db}!lg{lg}
/// crate::key::database::pa             /*{ns}*{db}!pa{pa}
/// crate::key::database::sc             /*{ns}*{db}!sc{sc}
/// crate::key::database::tb             /*{ns}*{db}!tb{tb}
/// crate::key::database::ti             /+{ns id}*{db id}!ti
/// crate::key::database::tk             /*{ns}*{db}!tk{tk}
/// crate::key::database::ts             /*{ns}*{db}!ts{ts}
/// crate::key::database::us             /*{ns}*{db}!us{us}
/// crate::key::database::vs             /*{ns}*{db}!vs
///
/// crate::key::scope::all               /*{ns}*{db}±{sc}
/// crate::key::scope::tk                /*{ns}*{db}±{sc}!tk{tk}
///
/// crate::key::table::all               /*{ns}*{db}*{tb}
/// crate::key::table::ev                /*{ns}*{db}*{tb}!ev{ev}
/// crate::key::table::fd                /*{ns}*{db}*{tb}!fd{fd}
/// crate::key::table::ft                /*{ns}*{db}*{tb}!ft{ft}
/// crate::key::table::ix                /*{ns}*{db}*{tb}!ix{ix}
/// crate::key::table::lq                /*{ns}*{db}*{tb}!lq{lq}
///
/// crate::key::index::all               /*{ns}*{db}*{tb}+{ix}
/// crate::key::index::bc                /*{ns}*{db}*{tb}+{ix}!bc{id}
/// crate::key::index::bd                /*{ns}*{db}*{tb}+{ix}!bd{id}
/// crate::key::index::bf                /*{ns}*{db}*{tb}+{ix}!bf{id}
/// crate::key::index::bi                /*{ns}*{db}*{tb}+{ix}!bi{id}
/// crate::key::index::bk                /*{ns}*{db}*{tb}+{ix}!bk{id}
/// crate::key::index::bl                /*{ns}*{db}*{tb}+{ix}!bl{id}
/// crate::key::index::bo                /*{ns}*{db}*{tb}+{ix}!bo{id}
/// crate::key::index::bp                /*{ns}*{db}*{tb}+{ix}!bp{id}
/// crate::key::index::bs                /*{ns}*{db}*{tb}+{ix}!bs
/// crate::key::index::bt                /*{ns}*{db}*{tb}+{ix}!bt{id}
/// crate::key::index::bu                /*{ns}*{db}*{tb}+{ix}!bu{id}
/// crate::key::index                    /*{ns}*{db}*{tb}+{ix}*{fd}{id}
///
/// crate::key::change                   /*{ns}*{db}#{ts}
///
/// crate::key::thing                    /*{ns}*{db}*{tb}*{id}
///
/// crate::key::graph                    /*{ns}*{db}*{tb}~{id}{eg}{fk}
///
pub mod change;
pub mod database;
pub mod debug;
pub(crate) mod error;
pub mod graph;
pub mod index;
pub(crate) mod key_req;
pub mod namespace;
pub mod node;
pub mod root;
pub mod scope;
pub mod table;
pub mod thing;
