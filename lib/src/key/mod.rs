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
/// SC              /*{ns}*{db}!sc{sc}
/// ST              /*{ns}*{db}!st{sc}!tk{tk}
/// TB              /*{ns}*{db}!tb{tb}
/// LQ              /*{ns}*{db}!lq{lq}
///
/// Table           /*{ns}*{db}*{tb}
/// FT              /*{ns}*{db}*{tb}!ft{ft}
/// FD              /*{ns}*{db}*{tb}!fd{fd}
/// EV              /*{ns}*{db}*{tb}!ev{ev}
/// IX              /*{ns}*{db}*{tb}!ix{ix}
/// LV              /*{ns}*{db}*{tb}!lv{lv}
///
/// Thing           /*{ns}*{db}*{tb}*{id}
///
/// Graph           /*{ns}*{db}*{tb}~{id}{eg}{fk}
///
/// Index           /*{ns}*{db}*{tb}¤{ix}{fd}{id}
///
pub mod database;
pub mod db;
pub mod dl;
pub mod dt;
pub mod ev;
pub mod fd;
pub mod ft;
pub mod graph;
pub mod index;
pub mod ix;
pub mod kv;
pub mod lq;
pub mod lv;
pub mod namespace;
pub mod nl;
pub mod ns;
pub mod nt;
pub mod sc;
pub mod st;
pub mod table;
pub mod tb;
pub mod thing;
