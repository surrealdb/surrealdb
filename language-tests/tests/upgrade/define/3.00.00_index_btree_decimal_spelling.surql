/**
[env]
imports = ["upgrade/define/3.00.00_index_btree_decimal_spelling_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
Two decimals that compare equal are stored at two scales when one is computed:
`1.25dec * 2` is 2.50, the literal `2.5dec` is 2.5. A B-tree index spelled each
at its own key, so an indexed read of one value missed the record holding the
other. An index built on this checkout spells both as 2.5; one an older binary
built keeps its two keys until it is rebuilt, and an indexed equality or range
returns fewer rows than the same read without the index, on a UNIQUE index as on
a plain one. The rebuild is the operator's step, and after it every read agrees
with its WITH NOINDEX twin.

The upgrade target is bounded to the current checkout: every released binary
spells the pair at two keys, rebuilt or not."""

[[test.repair]]
since = ">=3.4.0-a"
script = "upgrade/define/3.00.00_index_btree_decimal_spelling_rebuild.surql"
unrepaired = "degraded"
reason = """
A B-tree or UNIQUE index an older binary built has one key per spelling of a
decimal, so an indexed read of 2.5 does not find a record holding 2.50 and the
rows it returns differ from the same read without the index. `REBUILD INDEX`
rewrites the entries at the respelled keys. Rebuilding a UNIQUE index over a
table that holds both spellings of one value fails with a duplicate, which has to
be resolved first."""

[[test.results]]
value = "[dec_idx:1, dec_idx:2]"

[[test.results]]
value = "[dec_idx:1, dec_idx:2]"

[[test.results]]
value = "[dec_idx:1, dec_idx:2, dec_idx:3]"

[[test.results]]
value = "[dec_idx:1, dec_idx:2, dec_idx:4]"

[[test.results]]
value = "[dec_idx:3]"

[[test.results]]
value = "[dec_idx:4]"

[[test.results]]
value = "[dec_uniq:1]"

[[test.results]]
value = "[dec_uniq:1]"

[[test.results]]
value = "[dec_uniq:1, dec_uniq:2]"

*/
-- Equality, indexed and not.
SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c = 2.5dec ORDER BY id;
SELECT VALUE id FROM dec_idx WITH NOINDEX WHERE c = 2.5dec ORDER BY id;
-- A bound that sits on the pair, inclusive and exclusive.
SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c >= 2.5dec ORDER BY id;
SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c <= 2.5dec ORDER BY id;
SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c > 2.5dec ORDER BY id;
SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c < 2.5dec ORDER BY id;
-- The UNIQUE index finds its one record by the other spelling.
SELECT VALUE id FROM dec_uniq WITH INDEX dec_uniq_c WHERE c = 2.5dec;
SELECT VALUE id FROM dec_uniq WITH NOINDEX WHERE c = 2.5dec;
SELECT VALUE id FROM dec_uniq WITH INDEX dec_uniq_c WHERE c >= 2.5dec ORDER BY id;
