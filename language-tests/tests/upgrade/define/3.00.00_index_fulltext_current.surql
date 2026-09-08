/**
[env]
# `EXPLAIN` is rejected by the legacy compute-only engine, and the ordinary
# language-test runner replays this body under every strategy.
planner-strategy = ["all-ro", "best-effort-ro"]
imports = ["upgrade/define/3.00.00_index_fulltext_import.surql"]

[test]
upgrade = true
reason = """
An index that silently becomes unusable falls back to a table scan and returns
exactly the same rows, so the read assertions in index_fulltext.surql cannot tell a
working index from a missing one. This asserts the plan, and then writes a
record so the new binary has to maintain the structure the old one built.

Both live here rather than in index_fulltext.surql because neither can span the whole
3.x line. `EXPLAIN` output is not stable across it — older releases return an
array of plan objects with entirely different operator names — and the write
cannot go in a body carrying a `[[test.repair]]`, since that body is run twice
against one store and the first pass's write would be visible to the second
pass's reads. Gating to the current checkout as the upgrade target costs no
coverage that matters: the seed axis runs every released version straight to it."""
version = ">=3.4.0-a"

[[test.results]]
match = '''string::contains($result, "FullTextScan") AND string::contains($result, "blog_title")'''

[[test.results]]
value = "[{ id: blog:5, label: 'test', title: 'another dog appeared' }]"

[[test.results]]
value = "[{ id: blog:1 }, { id: blog:2 }, { id: blog:4 }, { id: blog:5 }]"

*/
EXPLAIN SELECT id FROM blog WHERE title @1@ 'dog';
-- The write has to land in the layout the old binary established, not the one
-- this binary would have chosen.
CREATE blog:5 SET title = 'another dog appeared', label = 'test';
SELECT id FROM blog WHERE title @1@ 'dog' ORDER BY id;
