/**
[env]
imports = ["./return_none_vs_null_refusing_gate_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
`RETURN NULL` emits a row for each record the select permission admits, so under
a permission that refuses every record the statement answers no row, as `RETURN
NONE` does, and `ONLY` has no row to answer; where the permission admits the
records the same statements answer a row each, and `RETURN NONE` none. The select
gate therefore decides whether a row exists under `RETURN NULL`, and decides
nothing under `RETURN NONE`. The writes stand under every output.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: sealed_state:1, n: 61 }, { id: sealed_state:2, n: 52 }, { id: sealed_state:3, n: 3 }, { id: sealed_state:4, n: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NULL, NULL]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NULL"

[[test.results]]
value = "[NULL]"

[[test.results]]
value = "[{ id: open_tb:1, n: 41 }, { id: open_tb:2, n: 22 }, { id: open_tb:3, n: 3 }]"
*/

UPDATE sealed SET n = n + 10 RETURN NONE;
UPDATE sealed SET n = n + 10 RETURN NULL;
UPDATE ONLY sealed:1 SET n = n + 10 RETURN NONE;
UPDATE ONLY sealed:1 SET n = n + 10 RETURN NULL;
UPDATE ONLY sealed:2 SET n = n + 10;
UPDATE sealed SET n = n + 10;
UPDATE sealed SET n = n + 10 RETURN AFTER;
CREATE sealed:3 SET n = 3 RETURN NONE;
CREATE sealed:4 SET n = 4 RETURN NULL;
SELECT * FROM sealed_state ORDER BY id;
UPDATE open_tb SET n = n + 10 RETURN NONE;
UPDATE open_tb SET n = n + 10 RETURN NULL;
UPDATE ONLY open_tb:1 SET n = n + 10 RETURN NONE;
UPDATE ONLY open_tb:1 SET n = n + 10 RETURN NULL;
CREATE open_tb:3 SET n = 3 RETURN NULL;
SELECT * FROM open_tb ORDER BY id;
