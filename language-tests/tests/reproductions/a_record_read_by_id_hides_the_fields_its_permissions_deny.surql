/**
[env]
imports = ["reproductions/a_record_read_by_id_hides_the_fields_its_permissions_deny_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = """
SECURITY: a record user reading a record by id — a record id's `.*`, an
element of an array of record ids, FETCH, a graph hop's `.*` and a link
read inside a selection —
sees the fields its field permissions deny hidden, top-level, nested, in an
array's elements and computed, as a scan of the table hides them, and so
does a read of a record a call or a parameter names.
"""

[[test.results]]
value = "{ id: t:1, items: [{ open: 1 }], obj: { open: 1 }, shown: 1 }"

[[test.results]]
value = "{ id: t:1, items: [{ open: 1 }], obj: { open: 1 }, shown: 1 }"

[[test.results]]
value = "[{ id: holder:1, link: { id: t:1, items: [{ open: 1 }], obj: { open: 1 }, shown: 1 } }]"

[[test.results]]
value = "[{ id: t:1, items: [{ open: 1 }], obj: { open: 1 }, shown: 1 }]"

[[test.results]]
value = "[[{ id: t:1, items: [{ open: 1 }], obj: { open: 1 }, shown: 1 }]]"

[[test.results]]
value = "[{ id: t:1, items: [{ open: 1 }], obj: { open: 1 }, shown: 1 }]"

[[test.results]]
value = "[{ id: t:1, items: [{ open: 1 }], obj: { open: 1 }, shown: 1 }]"

[[test.results]]
value = "[{ id: t:1, items: [{ open: 1 }], obj: { open: 1 }, shown: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, items: [{ open: 1 }], obj: { open: 1 }, shown: 1 }]"

[[test.results]]
value = "{ id: t:1, items: [{ open: 1 }], obj: { open: 1 }, shown: 1 }"
*/

RETURN t:1.*;
RETURN [t:1][0].*;
SELECT * FROM holder:1 FETCH link;
SELECT VALUE link.* FROM holder:1;
SELECT VALUE ->rel->t.* FROM holder:1;
SELECT * FROM t:1;
SELECT * FROM t;
SELECT * FROM type::record('t', 1);
LET $r = t:1;
SELECT * FROM $r;
RETURN $r.*;
