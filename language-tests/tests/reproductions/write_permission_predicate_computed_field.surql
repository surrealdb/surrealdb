/**
[env]
imports = ["reproductions/write_permission_predicate_computed_field_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A table-level write `PERMISSIONS` predicate that names a COMPUTED field must observe the value that field computes to. Computed fields are stripped before storage, so the document the gate is evaluated against carried `NONE` for them and a deny-intended predicate such as `WHERE blocked != true` was satisfied by `NONE != true`, letting every caller write. The plain-field table is the control: the identical predicate over a stored field always rejected correctly."

# The COMPUTED-gated UPDATE must be rejected: `blocked` computes to true, so
# `blocked != true` is false. A rejected record is filtered out of the result.
[[test.results]]
value = "[]"

# Control: the identical predicate over a plain stored field also rejects, so
# the two now agree rather than diverging on how the field is defined.
[[test.results]]
value = "[]"

# The delete gate shares the evaluation path and must reject too.
[[test.results]]
value = "[]"

# Nothing was modified or removed: the guard held rather than merely being
# reported.
[[test.results]]
value = "[{ blocked: true, id: person:1, name: 'x' }]"

[[test.results]]
value = "[{ blocked: true, id: doomed:1, name: 'x' }]"

*/

UPDATE person:1 SET name = 'y';
UPDATE plain:1 SET name = 'z';
DELETE doomed:1;
SELECT * FROM person:1;
SELECT * FROM doomed:1;
