/**
[env]
imports = ["./return_none_select_gate_effects_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
A select permission that draws randomness or makes an external read is evaluated
even under `RETURN NONE`: an external read the capabilities deny is refused as it
is for any other statement. The answers and the stored rows are those of the
statement that returns the record.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: effect_state:1, n: 2 }]"

[[test.results]]
error = "Remote HTTP request functions are not enabled"

[[test.results]]
value = "[{ id: fetched_state:1, n: 1 }]"

[[test.results]]
value = "[{ id: drawn:1, n: 3 }]"

[[test.results]]
value = "[{ id: effect_state:1, n: 3 }]"
*/

UPDATE drawn SET n = n + 1 RETURN NONE;
SELECT * FROM effect_state;
UPDATE fetched SET n = n + 1 RETURN NONE;
SELECT * FROM fetched_state;
UPDATE drawn SET n = n + 1 RETURN AFTER;
SELECT * FROM effect_state;
