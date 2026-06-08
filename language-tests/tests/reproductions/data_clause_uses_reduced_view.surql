/**
[env]
imports = ["reproductions/data_clause_uses_reduced_view_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "actors:writer" }

[test]
reason = "User-supplied data-clause expressions (SET, MERGE, CONTENT, PATCH) must evaluate against the field-permission-reduced view of `current`. A viewer with FOR select NONE on a field must not be able to read it indirectly by binding it into another field through the data clause."

# 1: SELECT * — `secret` is cut by FOR select NONE and `copy` is NONE
#    so a whole-record projection drops both, leaving just the id.
[[test.results]]
value = "[{ id: vault:1 }]"

# 2: UPDATE … SET copy = secret. `secret` was 'classified' on disk but
#    the reduced view the data clause sees has secret = NONE, so the
#    new value of `copy` is NONE — not 'classified'. NONE drops from
#    the whole-record output; the explicit `SELECT copy` below shows
#    the actual stored value.
[[test.results]]
value = "[{ id: vault:1 }]"

# 3: SELECT copy — explicit projection preserves the field; confirms
#    `copy` was set to NONE, not the on-disk value of `secret`.
[[test.results]]
value = "[{ copy: NONE }]"

# 4: UPDATE … MERGE { copy: secret } — same shape via MERGE.
[[test.results]]
value = "[{ id: vault:1 }]"

# 5: UPDATE … CONTENT { secret: 'redacted', copy: secret } — CONTENT
#    evaluates `secret` against the reduced view too. The post-write
#    SELECT cut also drops `secret` from the projection.
[[test.results]]
value = "[{ id: vault:1 }]"

# 6: Sanity: SELECT copy after a series of leak-attempts shows it is
#    still NONE on disk (the secret value never made it into copy).
[[test.results]]
value = "[{ copy: NONE }]"

*/

-- The hidden field is cut on SELECT.
SELECT * FROM vault:1;

-- Attempt to copy via SET. Result `copy` must be NONE, not 'classified'.
UPDATE vault:1 SET copy = secret;
SELECT copy FROM vault:1;

-- Attempt to copy via MERGE.
UPDATE vault:1 MERGE { copy: secret };

-- Attempt to copy via CONTENT (which also rewrites secret).
UPDATE vault:1 CONTENT { secret: 'redacted', copy: secret };

-- Final state: copy is still NONE.
SELECT copy FROM vault:1;
