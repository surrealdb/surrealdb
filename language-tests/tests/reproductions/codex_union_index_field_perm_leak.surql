/**
[env]
imports = ["reproductions/codex_union_index_field_perm_leak_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:1" }

[test]
reason = "Indexed `... WHERE field CONTAINSANY [literal]` over a SELECT-denied array field must not leak which rows contain the hidden value. The vulnerable behaviour: the analyser picks `secret_tags_idx` (array-element flatten), `try_containment_expansion` emits a `UnionIndexScan`, and `strip_union_index_conditions` removes the CONTAINSANY leaf because every literal value is covered by some branch's prefix. The union sub-operators then run a `ScanPipeline` with no predicate, so the leaf is never re-evaluated after field-level SELECT permissions wipe `secret_tags` — turning the index entries into a membership oracle. `WITH NOINDEX` correctly returns []; the indexed and ANYINSIDE forms must agree."

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
value = "[item:1, item:2]"

[[test.results]]
value = "[item:1, item:2]"
*/

-- Baseline: forcing no index applies field permissions before the WHERE,
-- so `secret_tags = NONE` and `NONE CONTAINSANY ['admin']` is false.
SELECT VALUE id FROM item WITH NOINDEX WHERE secret_tags CONTAINSANY ['admin'];

-- Vulnerability: forcing the array-element index plans a UnionIndexScan.
-- Without the fix this returned `[item:1]` and leaked that item:1's
-- hidden `secret_tags` contains 'admin'.
SELECT VALUE id FROM item WITH INDEX secret_tags_idx WHERE secret_tags CONTAINSANY ['admin'];

-- Symmetric ANYINSIDE form goes through the same stripping path.
SELECT VALUE id FROM item WITH INDEX secret_tags_idx WHERE ['admin'] ANYINSIDE secret_tags;

-- Multi-value CONTAINSANY exercises the full branch-value set; without
-- the fix this also leaked the matching ids.
SELECT VALUE id FROM item WITH INDEX secret_tags_idx WHERE secret_tags CONTAINSANY ['admin', 'user'];

-- Mixed predicate: a public AND-leaf coexists with the restricted
-- CONTAINSANY. The whole WHERE is left in the residual filter (the
-- guard disables stripping for the entire clause as soon as any
-- restricted field is touched), so the post-permission recheck must
-- still drop rows where the restricted leaf fails — even when the
-- public leaf would match. Expected `[]` for the record user.
SELECT VALUE id FROM item WITH INDEX secret_tags_idx
  WHERE secret_tags CONTAINSANY ['admin'] AND tags CONTAINSANY ['public'];

-- Sanity: the same query shape against an UNRESTRICTED field must keep
-- using the indexed union — the fix must not over-trigger and break the
-- optimisation when no restricted field is referenced.
SELECT VALUE id FROM item WITH NOINDEX WHERE tags CONTAINSANY ['public'];
SELECT VALUE id FROM item WITH INDEX tags_idx WHERE tags CONTAINSANY ['public'];
