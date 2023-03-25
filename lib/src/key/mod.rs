///
/// KV              /
/// NS              /!ns{ns}
///
/// Namespace       /*{ns}
/// NL              /*{ns}!nl{us}
/// NT              /*{ns}!nt{tk}
/// DB              /*{ns}!db{db}
///
/// Database        /*{ns}*{db}
/// DL              /*{ns}*{db}!dl{us}
/// DT              /*{ns}*{db}!dt{tk}
/// PA              /*{ns}*{db}!pa{pa}
/// SC              /*{ns}*{db}!sc{sc}
/// TB              /*{ns}*{db}!tb{tb}
/// LQ              /*{ns}*{db}!lq{lq}
///
/// Scope           /*{ns}*{db}±{sc}
/// ST              /*{ns}*{db}±{sc}!st{tk}
///
/// Table           /*{ns}*{db}*{tb}
/// EV              /*{ns}*{db}*{tb}!ev{ev}
/// FD              /*{ns}*{db}*{tb}!fd{fd}
/// FT              /*{ns}*{db}*{tb}!ft{ft}
/// IX              /*{ns}*{db}*{tb}!ix{ix}
/// LV              /*{ns}*{db}*{tb}!lv{lv}
///
/// Thing           /*{ns}*{db}*{tb}*{id}
///
/// Graph           /*{ns}*{db}*{tb}~{id}{eg}{fk}
///
/// Index           /*{ns}*{db}*{tb}¤{ix}{fd}{id}
///
pub mod database; // Stores the key prefix for all keys under a database
pub mod db; // Stores a DEFINE DATABASE config definition
pub mod dl; // Stores a DEFINE LOGIN ON DATABASE config definition
pub mod dt; // Stores a DEFINE LOGIN ON DATABASE config definition
pub mod ev; // Stores a DEFINE EVENT config definition
pub mod fc; // Stores a DEFINE FUNCTION config definition
pub mod fd; // Stores a DEFINE FIELD config definition
pub mod ft; // Stores a DEFINE TABLE AS config definition
pub mod graph; // Stores a graph edge pointer
pub mod index; // Stores an index entry
pub mod ix; // Stores a DEFINE INDEX config definition
pub mod kv; // Stores the key prefix for all keys
pub mod lq; // Stores a LIVE SELECT query definition on the database
pub mod lv; // Stores a LIVE SELECT query definition on the table
pub mod namespace; // Stores the key prefix for all keys under a namespace
pub mod nl; // Stores a DEFINE LOGIN ON NAMESPACE config definition
pub mod ns; // Stores a DEFINE NAMESPACE config definition
pub mod nt; // Stores a DEFINE TOKEN ON NAMESPACE config definition
pub mod pa; // Stores a DEFINE PARAM config definition
pub mod sc; // Stores a DEFINE SCOPE config definition
pub mod scope; // Stores the key prefix for all keys under a scope
pub mod st; // Stores a DEFINE TOKEN ON SCOPE config definition
pub mod table; // Stores the key prefix for all keys under a table
pub mod tb; // Stores a DEFINE TABLE config definition
pub mod thing; // Stores a record id
