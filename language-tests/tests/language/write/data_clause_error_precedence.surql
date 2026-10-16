/**
[env]
imports = ["language/write/data_clause_error_precedence_import.surql"]

[test]
reason = "Pins which error a caller sees when a data clause both fails to apply and names an `id` the statement contradicts. The two failures come from `check_data_fields` and from applying the data, and the dispatchers do not agree on which runs first: a create applies the data first and reports the application failure, an update checks the special fields first and reports the mismatch. UPSERT reads the record it names, so it answers as a create for a record that is not stored and as an update for one that is. INSERT is outside the split — it takes its record id from the content, so the special-field check stands down and only the application can fail. Every statement below is wrong in both ways at once; only the order decides what it is told."

[[test.results]]
error = "Cannot perform subtraction with 'int' and 'string'"

[[test.results]]
error = "Cannot perform subtraction with 'int' and 'string'"

[[test.results]]
error = "Cannot perform subtraction with 'int' and 'string'"

[[test.results]]
error = "Found t:9 for the `id` field, but a specific record has been specified"

[[test.results]]
error = "Found t:9 for the `id` field, but a specific record has been specified"

[[test.results]]
error = "Found e:zz for the `id` field, but a specific record has been specified"

[[test.results]]
error = "Cannot perform subtraction with 'int' and 'string'"
*/

-- The create shapes apply the data first, so the application failure is what
-- the caller hears about, and the contradicted `id` is never reached.
CREATE t:2 SET id = t:9, n = 1, n -= 'abc';
RELATE t:1->e:x2->t:1 SET id = e:zz, n = 1, n -= 'abc';

-- UPSERT reads the record it names first. `t:5` is not stored, so it answers
-- as a create; `t:1` is, so it answers as the update shapes below do.
UPSERT t:5 SET id = t:9, n = 1, n -= 'abc';
UPSERT t:1 SET id = t:9, n = 1, n -= 'abc';

-- The update shapes check the special fields first, and report the mismatch
-- without ever applying the data.
UPDATE t:1 SET id = t:9, n = 1, n -= 'abc';
RELATE t:1->e:x1->t:1 SET id = e:zz, n = 1, n -= 'abc';

-- INSERT derives its record id from the content it was handed, which stands the
-- special-field check down entirely; the duplicate-key clause is evaluated only
-- by the update half, against the stored record, and its contradicted `id` is
-- never judged.
INSERT INTO t { id: t:1, n: 1 } ON DUPLICATE KEY UPDATE id = t:9, n = 1, n -= 'abc';
