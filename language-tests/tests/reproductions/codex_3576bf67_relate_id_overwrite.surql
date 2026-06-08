/**
[env]
namespace = true
database = true

[test]
reason = "RELATE with SET id = <existing edge> must not silently overwrite the existing record under create permissions (Codex finding 3576bf67)"

[[test.results]]
value = "[{ id: foo:1 }, { id: foo:2 }, { id: bar:1 }, { id: bar:2 }]"

[[test.results]]
value = "[{ id: edge:existing, marker: 'original', in: foo:1, out: bar:1 }]"

[[test.results]]
match = "$error = /Database record .*edge:existing.* already exists/"
*/

CREATE foo:1, foo:2, bar:1, bar:2;
RELATE foo:1 -> edge:existing -> bar:1 SET marker = 'original';
-- This second RELATE picks a different edge target but tries to overwrite
-- edge:existing via SET id. Before the fix, store_record_data used
-- set_record (overwrite), so the existing edge silently lost its
-- 'marker' field and was replaced. Now we hit put_record's
-- already-exists path and return RecordExists.
RELATE foo:2 -> edge -> bar:2 SET id = edge:existing, marker = 'attacker';
