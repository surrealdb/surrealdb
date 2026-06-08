/**
[env]
imports = ["reproductions/selective_computed_field_perm_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "notes:high" }

[test]
reason = "Selective projection (`SELECT secret FROM ...`) must still evaluate computed fields referenced by field permissions. `secret` is gated by `WHERE flag` where `flag` is a computed field; if `flag` isn't computed before the permission is checked, the decision is made against a row missing the dependency."

[[test.results]]
value = "[{ flag: true, id: notes:high, score: 50, secret: 'visible_when_high' }, { flag: false, id: notes:low, score: 1 }]"

[[test.results]]
value = "['visible_when_high', NONE]"

[[test.results]]
value = "['visible_when_high', NONE]"

*/

-- Sanity: SELECT * shows the computed flag and the permission decision
-- (secret present on notes:high where flag = true, absent on notes:low
-- where flag = false).
SELECT * FROM notes ORDER BY id ASC;

-- Selective projection that does NOT explicitly request `flag`. The field
-- permission on `secret` references `flag`, so `flag` must be computed
-- before the permission is checked. Use SELECT VALUE so missing/cut fields
-- consistently surface as NONE across legacy and streaming paths.
--   notes:high (flag = true)  → 'visible_when_high'
--   notes:low  (flag = false) → NONE
SELECT VALUE secret FROM notes ORDER BY id ASC;

-- Run again to make sure caching / repeated evaluation gives the same
-- answer (regression guard for the cached FieldState path).
SELECT VALUE secret FROM notes ORDER BY id ASC;
