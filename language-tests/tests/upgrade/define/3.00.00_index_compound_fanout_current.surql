/**
[env]
# `EXPLAIN` is rejected by the legacy compute-only engine, and the ordinary
# language-test runner replays this body under every strategy.
planner-strategy = ["all-ro", "best-effort-ro"]
imports = ["upgrade/define/3.00.00_index_compound_fanout_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.3.1"
reason = """
An index that silently stops being used falls back to a table scan and returns
the same rows, so the read assertions in 3.00.00_index_compound_fanout.surql
cannot tell a working index from a missing one. This asserts the plan, and then
writes, so the new binary has to extend the entry set the old one built.

Both live here rather than in the sibling because neither can span the 3.x
line: `EXPLAIN` output is not stable across it, and the write cannot go in a
body that a `[[test.repair]]` would run twice. Gating the upgrade target to the
current checkout costs no coverage that matters, because the seed axis sends
every released version straight to it.

Every assertion here has to hold whichever binary wrote the import, because
the ordinary language-test runner replays this file too and then the import is
written by this checkout. So the entry set of a record the OLD writer produced
is deliberately not asserted: it recorded m + n - 1 of a record's m * n element
combinations rather than the product, which leaves a combination it skipped
free for a second record to claim, and no read reveals it — reads deduplicate
by record id and no access path pins both fanning columns. Anyone relying on
uniqueness over an index whose columns both fan out needs
`REBUILD INDEX tagged_pair ON tagged` after upgrading. That is release-note
material, not something an axis of this suite can observe."""

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "tagged_pair")'''

[[test.results]]
value = "[{ id: tagged:fresh, nums: [7, 8], tags: ['m', 'n'] }]"

[[test.results]]
error = "Database index `tagged_pair` already contains ['n', 7], with record `tagged:fresh`"

[[test.results]]
error = "Database index `tagged_pair` already contains ['m', 8], with record `tagged:fresh`"

[[test.results]]
value = "[{ id: tagged:base }]"

*/
-- The index is what serves the read, not a table scan.
EXPLAIN SELECT id FROM tagged WHERE tags CONTAINS 'p';

-- A record THIS binary writes owns every combination of its elements, so each
-- of the four is claimed and a probe for any one of them collides. ['m', 8] is
-- the combination a walk down one column and along the next would have
-- skipped, so the pair of probes distinguishes the product from that walk.
CREATE tagged:fresh SET tags = ['m', 'n'], nums = [7, 8];
CREATE tagged:probe_n7 SET tags = ['n'], nums = [7];
CREATE tagged:probe_m8 SET tags = ['m'], nums = [8];

-- Reading across both writers' entries returns each record once.
SELECT id FROM tagged WHERE tags CONTAINS 'p';
