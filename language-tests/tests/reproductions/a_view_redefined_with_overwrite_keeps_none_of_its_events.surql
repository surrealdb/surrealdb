/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
`DEFINE TABLE OVERWRITE … AS` clears the view table's whole key range, its
events with it, before it fills the view again: `INFO FOR TABLE` lists no
event, the fill fires none, and a later write fires only the events defined
after the redefinition. This holds for a plain view and for an aggregated one.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: src:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ a: 1, id: v:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, id: src:2 }]"

[[test.results]]
value = "[{ af: { c: 2 }, bf: { c: 1 }, ev: 'UPDATE', name: 'g.e2' }]"

[[test.results]]
value = "[{ c: 2, id: g:[] }]"
*/

DEFINE TABLE src;
CREATE src:1 SET a = 1;
DEFINE TABLE log;
DEFINE TABLE v AS SELECT * FROM src;
DEFINE EVENT e ON v THEN { CREATE log SET name = 'v.e', ev = $event, af = $after };
DEFINE TABLE OVERWRITE v AS SELECT * FROM src WHERE a = 1;
INFO FOR TABLE v;
SELECT * FROM v;
DEFINE TABLE g AS SELECT count() AS c FROM src GROUP ALL;
DEFINE EVENT e ON g THEN { CREATE log SET name = 'g.e', ev = $event, af = $after };
DEFINE TABLE OVERWRITE g AS SELECT count() AS c FROM src GROUP ALL;
INFO FOR TABLE g;
DEFINE EVENT e2 ON g THEN { CREATE log SET name = 'g.e2', ev = $event, af = $after, bf = $before };
CREATE src:2 SET a = 5;
SELECT name, ev, af, bf FROM log ORDER BY name;
SELECT * FROM g;
