/**
[env]
namespace = true
database = true

[test]
reason = """
A `COMPUTED` field is stripped before the record is stored, so everything that
observes a write has to be handed the derived value rather than reading it back
out of the record. Events are covered here; live-query notifications and
changefeed entries read the same two views and so are covered by the same
mechanism, but their payloads carry per-run identifiers that make them awkward to
assert on directly.

The event records both sides of an update, so it pins that the pre-write view is
derived from the pre-write values and the post-write view from the new ones: `a`
goes 5 -> 7, so `dbl` goes 10 -> 14.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: ev:1 }]"

[[test.results]]
value = "[{ a: 7, dbl: 14, id: ev:1 }]"

[[test.results]]
value = "[{ after_dbl: 14, before_dbl: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: cv:1 }]"

[[test.results]]
value = "[{ after_dbl: 10, before_dbl: NONE }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: dv:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ after_dbl: NONE, before_dbl: 10 }]"
*/

DEFINE TABLE elog SCHEMALESS;
DEFINE TABLE ev SCHEMALESS;
DEFINE FIELD a   ON ev TYPE number;
DEFINE FIELD dbl ON ev COMPUTED a * 2;
DEFINE EVENT e ON ev WHEN $event = "UPDATE"
	THEN (CREATE elog SET before_dbl = $before.dbl, after_dbl = $after.dbl);

CREATE ev:1 SET a = 5;
UPDATE ev:1 SET a = 7;
SELECT before_dbl, after_dbl FROM elog;

-- A create has no earlier version of the record, so an event on it has no
-- pre-write computed value to read rather than an erroneous one.
DEFINE TABLE clog SCHEMALESS;
DEFINE TABLE cv SCHEMALESS;
DEFINE FIELD a   ON cv TYPE number;
DEFINE FIELD dbl ON cv COMPUTED a * 2;
DEFINE EVENT c ON cv WHEN $event = "CREATE"
	THEN (CREATE clog SET before_dbl = $before.dbl, after_dbl = $after.dbl);
CREATE cv:1 SET a = 5;
SELECT before_dbl, after_dbl FROM clog;

-- A delete has no later version, and the event still sees the record it removed.
DEFINE TABLE dlog SCHEMALESS;
DEFINE TABLE dv SCHEMALESS;
DEFINE FIELD a   ON dv TYPE number;
DEFINE FIELD dbl ON dv COMPUTED a * 2;
DEFINE EVENT d ON dv WHEN $event = "DELETE"
	THEN (CREATE dlog SET before_dbl = $before.dbl, after_dbl = $after.dbl);
CREATE dv:1 SET a = 5;
DELETE dv:1;
SELECT before_dbl, after_dbl FROM dlog;
