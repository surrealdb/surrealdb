/**
[env]
imports = ["./return_none_error_order_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
When a statement fails on more than one record, the failure it reports is the
first the records raise, in the order they are written. Record two raises in
its select permission and record three fails an ASSERT, so the permission's
error is the one reported under `RETURN NONE`, which shows no record, and under
`RETURN AFTER`, which does, and the statement leaves nothing stored. The
table's select permission is a predicate, so the ASSERT failure redacts the
rejected value.
"""

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ id: ordered_state:1, n: 1 }, { id: ordered_state:2, n: 2 }, { id: ordered_state:3, n: 3 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ id: ordered_state:1, n: 1 }, { id: ordered_state:2, n: 2 }, { id: ordered_state:3, n: 3 }]"

[[test.results]]
error = "Found [REDACTED] for field `n`, with record `ordered:3`, but field must conform to: $value != 99"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ id: ordered_state:1, n: 1 }, { id: ordered_state:2, n: 2 }, { id: ordered_state:3, n: 3 }]"
*/

UPDATE ordered SET n = IF id = ordered:3 { 99 } ELSE { n + 10 } RETURN NONE;
SELECT * FROM ordered_state ORDER BY id;
UPDATE ordered SET n = IF id = ordered:3 { 99 } ELSE { n + 10 } RETURN AFTER;
SELECT * FROM ordered_state ORDER BY id;
UPDATE ordered:3 SET n = 99 RETURN NONE;
UPDATE ordered:2 SET n = 20 RETURN NONE;
SELECT * FROM ordered_state ORDER BY id;
