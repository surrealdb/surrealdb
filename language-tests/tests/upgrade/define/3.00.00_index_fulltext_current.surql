/**
[env]
imports = ["upgrade/define/3.00.00_index_fulltext_import.surql"]

[test]
upgrade = true
reason = """
An index that silently becomes unusable falls back to a table scan and returns
exactly the same rows, so the read assertions in index_fulltext.surql cannot tell a
working index from a missing one. This asserts the plan, and then writes and
updates records so the new binary has to maintain the structure the old one
built.

Both live here rather than in index_fulltext.surql because neither can span the whole
3.x line. `EXPLAIN` output is not stable across it — older releases return an
array of plan objects with entirely different operator names — and the write
cannot go in a body carrying a `[[test.repair]]`, since that body is run twice
against one store and the first pass's write would be visible to the second
pass's reads. Gating to the current checkout as the upgrade target costs no
coverage that matters: the seed axis runs every released version straight to it.

The scores the current binary reads from the old index are checked the same
way, before and after it writes to it: each document's score equals the score
the statement gives with no index, and a ranked top k, whose scan rejects hits
by the sort's score threshold, holds positive scores in descending order and
equals the ranking with no index. Comparing the two answers rather than fixed
numbers keeps the check true whatever scores a release's BM25 computes."""
version = ">=3.4.0-a"

[[test.results]]
match = '''string::contains($result, "FullTextScan") AND string::contains($result, "blog_title")'''

[[test.results]]
value = "true"

[[test.results]]
match = '''$result.len() == 2 AND $result[0].s >= $result[1].s AND $result[1].s > 0'''

[[test.results]]
value = "true"

[[test.results]]
value = "[{ id: blog:5, label: 'test', title: 'another dog appeared' }]"

[[test.results]]
value = "[{ id: blog:1 }, { id: blog:2 }, { id: blog:4 }, { id: blog:5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: blog:1 }, { id: blog:2 }, { id: blog:4 }, { id: blog:5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: blog:4 }]"

[[test.results]]
value = "[{ id: blog:2, label: 'test', title: 'the dog chased the other dog' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
match = '''$result.len() == 2 AND $result[0].s >= $result[1].s AND $result[1].s > 0'''

[[test.results]]
value = "true"

[[test.results]]
value = "[{ h: 'the quick brown fox jumped over the lazy <em>dog</em>', id: blog:1 }, { h: 'the <em>dog</em> chased the other <em>dog</em>', id: blog:2 }, { h: 'another <em>dog</em> appeared', id: blog:5 }]"

*/
EXPLAIN SELECT id FROM blog WHERE title @1@ 'dog';
-- The scores of the index the old binary built: as the statement gives them
-- with no index, and ranked through the scan's score threshold.
RETURN (SELECT id, search::score(1) AS s FROM blog WHERE title @1@ 'dog' ORDER BY id)
	== (SELECT id, search::score(1) AS s FROM blog WITH NOINDEX WHERE title @1@ 'dog' ORDER BY id);
SELECT id, search::score(1) AS s FROM blog WHERE title @1@ 'dog' ORDER BY s DESC LIMIT 2;
RETURN (SELECT VALUE s FROM (SELECT id, search::score(1) AS s FROM blog WHERE title @1@ 'dog' ORDER BY s DESC LIMIT 2))
	== (SELECT VALUE s FROM (SELECT id, search::score(1) AS s FROM blog WITH NOINDEX WHERE title @1@ 'dog' ORDER BY s DESC LIMIT 2));
-- The write has to land in the layout the old binary established, not the one
-- this binary would have chosen.
CREATE blog:5 SET title = 'another dog appeared', label = 'test';
SELECT id FROM blog WHERE title @1@ 'dog' ORDER BY id;
-- An update of a document the old binary indexed: its removal has to name
-- exactly the terms that binary stored, or it cancels the re-addition of a
-- term the document keeps and the document drops out of `dog`.
UPDATE blog:4 SET title = 'the dog sat there and did something' RETURN NONE;
SELECT id FROM blog WHERE title @1@ 'dog' ORDER BY id;
SELECT id FROM blog WHERE title @1@ 'nothing';
SELECT id FROM blog WHERE title @1@ 'something';
UPDATE blog:2 SET title = 'the dog chased the other dog';
DELETE blog:4;
-- The same, over the old index this binary has written to.
RETURN (SELECT id, search::score(1) AS s FROM blog WHERE title @1@ 'dog' ORDER BY id)
	== (SELECT id, search::score(1) AS s FROM blog WITH NOINDEX WHERE title @1@ 'dog' ORDER BY id);
SELECT id, search::score(1) AS s FROM blog WHERE title @1@ 'dog' ORDER BY s DESC LIMIT 2;
RETURN (SELECT VALUE s FROM (SELECT id, search::score(1) AS s FROM blog WHERE title @1@ 'dog' ORDER BY s DESC LIMIT 2))
	== (SELECT VALUE s FROM (SELECT id, search::score(1) AS s FROM blog WITH NOINDEX WHERE title @1@ 'dog' ORDER BY s DESC LIMIT 2));
SELECT id, search::highlight('<em>', '</em>', 1) AS h FROM blog WHERE title @1@ 'dog' ORDER BY id;
