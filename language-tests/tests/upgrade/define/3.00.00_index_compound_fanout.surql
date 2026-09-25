/**
[env]
imports = ["upgrade/define/3.00.00_index_compound_fanout_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
reason = """
An index over two columns that each fan out to one entry per element is
maintained by whichever binary performed the write, and a writer that recorded
one entry per element of each column in turn rather than one per combination
left combinations unrecorded. A seek that pins both columns addresses exactly
such a combination, so the record is reachable by a table scan and not by the
index until the index is rebuilt.

`REBUILD INDEX tagged_pair ON tagged` is that step, declared below so the
runner checks it in both directions: the body must show the damage before the
repair runs, and must be clean after it. A declaration that stops reproducing
fails just as loudly as an undeclared requirement.

The reverse direction needs the same step and no axis of this suite can reach
it, so it is recorded here rather than tested. While a rolling upgrade has both
binaries live, a writer that records every combination and a deleter that
records one entry per element of each column in turn disagree about how many
entries a record owns: the deleter removes the ones it would have written and
strands the rest. A stranded UNIQUE entry then collides with a later write and
names the deleted record as the holder, and it does not clear when the upgrade
completes. Rebuilding is the remedy there too, which is why the step belongs in
the release note and not only in an upgrade path."""

[[test.repair]]
since = ">=3.3.1"
script = "upgrade/define/3.00.00_index_compound_fanout_rebuild.surql"
unrepaired = "degraded"
reason = """
An index whose columns both fan out and which was written before every element
combination was recorded holds only some of them. A seek pinning both columns
addresses a combination that may never have been written, so the row is missing
from an indexed read until `REBUILD INDEX` restores the entry set."""

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
-- The records themselves survive the hop.
SELECT * FROM tagged ORDER BY id;
-- Served by the old binary's index entries. `tagged:base` owns several of them
-- under the 'p' prefix, and must still arrive once.
SELECT id FROM tagged WHERE tags CONTAINS 'p';
-- The oracle: a table scan cannot consult the index at all.
SELECT id FROM tagged WITH NOINDEX WHERE tags CONTAINS 'p';
-- A count over the same predicate counts records, not entries.
SELECT count() FROM tagged WHERE tags CONTAINS 'p' GROUP ALL;
SELECT count() FROM tagged WITH NOINDEX WHERE tags CONTAINS 'p' GROUP ALL;
-- A single-element row is the same under either writer's expansion.
SELECT id FROM tagged WHERE tags CONTAINS 'z';
-- Pinning BOTH fanning columns addresses one combination of the record's
-- elements, which is the read that a partially recorded entry set cannot
-- answer. The oracle is a table scan and never consults the index.
SELECT id FROM tagged WHERE tags CONTAINS 'p' AND nums CONTAINS 2;
SELECT id FROM tagged WITH NOINDEX WHERE tags CONTAINS 'p' AND nums CONTAINS 2;
