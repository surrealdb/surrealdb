//! How the keys are structured in the key value store
//! When adding to this list, please add alphabetically
///
/// KV              /
///
/// HB              /!hb{ts}/{nd}
///
/// ND              /!nd{nd}
/// NQ              /!nd{nd}*{ns}*{db}!lq{lq}
///
/// NS              /!ns{ns}
///
/// Namespace       /*{ns}
/// NL              /*{ns}!nl{us}
/// NT              /*{ns}!nt{tk}
/// DB              /*{ns}!db{db}
///
/// Database        /*{ns}*{db}
/// AZ              /*{ns}*{db}!az{az}
/// CF              /*{ns}*{db}!cf{ts}
/// DL              /*{ns}*{db}!dl{us}
/// DT              /*{ns}*{db}!dt{tk}
/// PA              /*{ns}*{db}!pa{pa}
/// SC              /*{ns}*{db}!sc{sc}
/// TB              /*{ns}*{db}!tb{tb}
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
/// BO              /*{ns}*{db}*{tb}!bo{ix}*
/// BP              /*{ns}*{db}*{tb}!bp{ix}*{id}
/// BS              /*{ns}*{db}*{tb}!bs{ix}
/// BT              /*{ns}*{db}*{tb}!bt{ix}*{id}
/// BU              /*{ns}*{db}*{tb}!bu{ix}*{id}
/// DV              /*{ns}*{db}!tt
/// FC              /*{ns}*{db}!fn{fc}
pub mod cf;
pub mod debug;
pub mod hb;
pub mod kv;
pub mod nd;
pub mod ns;

const CHAR_PATH: u8 = 0xb1; // ±
const CHAR_INDEX: u8 = 0xa4; // ¤
