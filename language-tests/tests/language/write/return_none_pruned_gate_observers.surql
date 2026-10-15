/**
[env]
imports = ["./return_none_pruned_gate_observers_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }
timeout = 30000

[test]
reason = """
A select permission the stages can prove inert is not judged under `RETURN NONE`,
and nothing a write leaves behind depends on it: twin tables, one written with
`RETURN NONE` and one with `RETURN AFTER`, each with an event that audits every
write and a change feed that keeps the earlier image, record the same audit rows
and store the same rows. The change feed itself is not read: `SHOW CHANGES` is
refused to a record user, and the stage that records it runs before the select
gate in every order (see the `nothing_but_the_output_follows_the_select_gate`
unit test), so the gate cannot change it.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ob_full_b:1, n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ob_full_b:2, n: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ob_full_b:1, n: 11 }, { id: ob_full_b:2, n: 12 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ action: 'DELETE', after: NONE, before: 11 }, { action: 'CREATE', after: 1, before: NONE }, { action: 'CREATE', after: 2, before: NONE }, { action: 'UPDATE', after: 11, before: 1 }, { action: 'UPDATE', after: 12, before: 2 }]"

[[test.results]]
value = "[{ action: 'DELETE', after: NONE, before: 11 }, { action: 'CREATE', after: 1, before: NONE }, { action: 'CREATE', after: 2, before: NONE }, { action: 'UPDATE', after: 11, before: 1 }, { action: 'UPDATE', after: 12, before: 2 }]"

[[test.results]]
value = "[{ id: ob_full_a_state:2, n: 12 }]"

[[test.results]]
value = "[{ id: ob_full_b_state:2, n: 12 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ action: 'DELETE', after: NONE, before: 11 }, { action: 'CREATE', after: 1, before: NONE }, { action: 'CREATE', after: 2, before: NONE }, { action: 'UPDATE', after: 11, before: 1 }, { action: 'UPDATE', after: 12, before: 2 }]"

[[test.results]]
value = "[{ action: 'DELETE', after: NONE, before: 11 }, { action: 'CREATE', after: 1, before: NONE }, { action: 'CREATE', after: 2, before: NONE }, { action: 'UPDATE', after: 11, before: 1 }, { action: 'UPDATE', after: 12, before: 2 }]"

[[test.results]]
value = "[{ id: ob_none_a_state:2, n: 12 }]"

[[test.results]]
value = "[{ id: ob_none_b_state:2, n: 12 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ob_auth_b:1, n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ob_auth_b:2, n: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ob_auth_b:1, n: 11 }, { id: ob_auth_b:2, n: 12 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ action: 'DELETE', after: NONE, before: 11 }, { action: 'CREATE', after: 1, before: NONE }, { action: 'CREATE', after: 2, before: NONE }, { action: 'UPDATE', after: 11, before: 1 }, { action: 'UPDATE', after: 12, before: 2 }]"

[[test.results]]
value = "[{ action: 'DELETE', after: NONE, before: 11 }, { action: 'CREATE', after: 1, before: NONE }, { action: 'CREATE', after: 2, before: NONE }, { action: 'UPDATE', after: 11, before: 1 }, { action: 'UPDATE', after: 12, before: 2 }]"

[[test.results]]
value = "[{ id: ob_auth_a_state:2, n: 12 }]"

[[test.results]]
value = "[{ id: ob_auth_b_state:2, n: 12 }]"
*/
CREATE ob_full_a:1 SET n = 1 RETURN NONE;
CREATE ob_full_b:1 SET n = 1 RETURN AFTER;
CREATE ob_full_a:2 SET n = 2 RETURN NONE;
CREATE ob_full_b:2 SET n = 2 RETURN AFTER;
UPDATE ob_full_a SET n = n + 10 RETURN NONE;
UPDATE ob_full_b SET n = n + 10 RETURN AFTER;
DELETE ob_full_a:1 RETURN NONE;
DELETE ob_full_b:1 RETURN AFTER;
SELECT action, before, after FROM ob_full_a_audit ORDER BY after, action;
SELECT action, before, after FROM ob_full_b_audit ORDER BY after, action;
SELECT id, n FROM ob_full_a_state ORDER BY id;
SELECT id, n FROM ob_full_b_state ORDER BY id;
CREATE ob_none_a:1 SET n = 1 RETURN NONE;
CREATE ob_none_b:1 SET n = 1 RETURN AFTER;
CREATE ob_none_a:2 SET n = 2 RETURN NONE;
CREATE ob_none_b:2 SET n = 2 RETURN AFTER;
UPDATE ob_none_a SET n = n + 10 RETURN NONE;
UPDATE ob_none_b SET n = n + 10 RETURN AFTER;
DELETE ob_none_a:1 RETURN NONE;
DELETE ob_none_b:1 RETURN AFTER;
SELECT action, before, after FROM ob_none_a_audit ORDER BY after, action;
SELECT action, before, after FROM ob_none_b_audit ORDER BY after, action;
SELECT id, n FROM ob_none_a_state ORDER BY id;
SELECT id, n FROM ob_none_b_state ORDER BY id;
CREATE ob_auth_a:1 SET n = 1 RETURN NONE;
CREATE ob_auth_b:1 SET n = 1 RETURN AFTER;
CREATE ob_auth_a:2 SET n = 2 RETURN NONE;
CREATE ob_auth_b:2 SET n = 2 RETURN AFTER;
UPDATE ob_auth_a SET n = n + 10 RETURN NONE;
UPDATE ob_auth_b SET n = n + 10 RETURN AFTER;
DELETE ob_auth_a:1 RETURN NONE;
DELETE ob_auth_b:1 RETURN AFTER;
SELECT action, before, after FROM ob_auth_a_audit ORDER BY after, action;
SELECT action, before, after FROM ob_auth_b_audit ORDER BY after, action;
SELECT id, n FROM ob_auth_a_state ORDER BY id;
SELECT id, n FROM ob_auth_b_state ORDER BY id;
