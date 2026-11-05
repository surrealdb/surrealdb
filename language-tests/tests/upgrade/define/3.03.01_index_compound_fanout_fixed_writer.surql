/**
[env]
imports = ["upgrade/define/3.00.00_index_compound_fanout_import.surql"]

[test]
upgrade = true
introduced = ">=3.3.1"
reason = """
An index over two columns that each fan out to one entry per element holds one
entry per combination of their elements when 3.3.1 or later wrote it, and the
current binary reads that entry set whole, with no repair. A seek pinning both
columns addresses one such combination, so it is the read that tells a whole
entry set from a partial one.

The releases before 3.3.1 recorded only some combinations; their stores need
`REBUILD INDEX`, which 3.00.00_index_compound_fanout.surql declares and checks.
`introduced` is 3.3.1, the first release that writes the whole set, so no
pairwise run sends this test a store with combinations missing. The ordinary
language-test runner replays it too, with this checkout as the writer, and the
same reads hold."""

[[test.results]]
value = "[{ id: tagged:base, nums: [1, 2], tags: ['p', 'q'] }, { id: tagged:other, nums: [9], tags: ['z'] }]"

[[test.results]]
value = "[{ id: tagged:base }]"

[[test.results]]
value = "[{ id: tagged:base }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ id: tagged:other }]"

[[test.results]]
value = "[{ id: tagged:base }]"

[[test.results]]
value = "[{ id: tagged:base }]"

*/
-- Each containment spells the flatten (`tags.*`): only then is a stored scalar
-- ruled out, so only then may the element index answer it.

-- The records themselves survive the hop.
SELECT * FROM tagged ORDER BY id;
-- Served by the old binary's index entries. `tagged:base` owns several of them
-- under the 'p' prefix, and must still arrive once.
SELECT id FROM tagged WHERE tags.* CONTAINS 'p';
-- The oracle: a table scan cannot consult the index at all.
SELECT id FROM tagged WITH NOINDEX WHERE tags.* CONTAINS 'p';
-- A count over the same predicate counts records, not entries.
SELECT count() FROM tagged WHERE tags.* CONTAINS 'p' GROUP ALL;
SELECT count() FROM tagged WITH NOINDEX WHERE tags.* CONTAINS 'p' GROUP ALL;
-- A single-element row is the same under either writer's expansion.
SELECT id FROM tagged WHERE tags.* CONTAINS 'z';
-- Pinning BOTH fanning columns addresses one combination of the record's
-- elements, which is the read that a partially recorded entry set cannot
-- answer. The oracle is a table scan and never consults the index.
SELECT id FROM tagged WHERE tags.* CONTAINS 'p' AND nums.* CONTAINS 2;
SELECT id FROM tagged WITH NOINDEX WHERE tags.* CONTAINS 'p' AND nums.* CONTAINS 2;
