/**
[env]
imports = ["reproductions/type_field_projection_perm_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "notes:high" }

[test]
reason = "Selective projection (`SELECT VALUE secret`) must still compute the `flag` computed field when a field permission reads it via the `type::field` projection function. Without classifying `type::field` as a projection, the static dep-extractor sees a function call over a string literal and returns no deps, so `flag` is skipped and the permission decision is made against a row missing the dependency."

[[test.results]]
value = "[{ flag: true, id: notes:high, score: 50, secret: 'visible_when_high' }, { flag: false, id: notes:low, score: 1 }]"

[[test.results]]
value = "['visible_when_high', NONE]"

[[test.results]]
value = "['visible_when_high', NONE]"

*/

-- Sanity: full SELECT shows the computed `flag` and the permission
-- decision on `secret`.
SELECT * FROM notes ORDER BY id ASC;

-- Selective projection. `flag` is not in the projection but the
-- permission on `secret` reads it via `type::field("flag")`, so it must
-- still be computed before the permission is checked.
SELECT VALUE secret FROM notes ORDER BY id ASC;

-- Repeat to guard against cache regressions.
SELECT VALUE secret FROM notes ORDER BY id ASC;
