//! How the keys are structured in the key value store
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
/// AZ              /*{ns}*{db}!az{az}
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
/// AZ              /*{ns}*{db}!az{az}
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
/// BC              /*{ns}*{db}*{tb}!bc{ix}*{id}
/// BD              /*{ns}*{db}*{tb}!bd{ix}*{id}
/// BF              /*{ns}*{db}*{tb}!bf{ix}*{id}
/// BI              /*{ns}*{db}*{tb}!bi{ix}*{id}
/// BK              /*{ns}*{db}*{tb}!bk{ix}*{id}
/// BL              /*{ns}*{db}*{tb}!bl{ix}*{id}
/// BP              /*{ns}*{db}*{tb}!bp{ix}*{id}
/// BS              /*{ns}*{db}*{tb}!bs{ix}
/// BT              /*{ns}*{db}*{tb}!bt{ix}*{id}
/// BU              /*{ns}*{db}*{tb}!bu{ix}*{id}
pub mod az; // Stores a DEFINE ANALYZER config definition
pub mod bc; // Stores Doc list for each term
pub mod bd; // Stores BTree nodes for doc ids
pub mod bf; // Stores Term/Doc frequency
pub mod bi; // Stores doc keys for doc_ids
pub mod bk; // Stores the term list for doc_ids
pub mod bl; // Stores BTree nodes for doc lengths
pub mod bp; // Stores BTree nodes for postings
pub mod bs; // Stores FullText index states
pub mod bt; // Stores BTree nodes for terms
pub mod bu; // Stores terms for term_ids
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
pub mod thing;

const CHAR_PATH: u8 = 0xb1; // ±
const CHAR_INDEX: u8 = 0xa4; // ¤
