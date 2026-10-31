/**
[test]
reason = """
A blocking DEFINE INDEX of a kind whose writes queue work for compaction
returns once its index is online and then materialised: what the build left to
fold is folded before the statement returns, so INFO FOR INDEX reports
`compacting: false` straight after it. The language-test provisioner runs no
background compaction, so only the statement's own wait can have folded it.
Covered for full-text, count and HNSW indexes, each over rows that exist
before the index, and for a REBUILD INDEX of each.
"""
issue = 1397

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, text: 'alpha beta', vec: [1f, 0f] }]"

[[test.results]]
value = "[{ id: t:2, text: 'beta gamma', vec: [0f, 1f] }]"

[[test.results]]
value = "[{ id: t:3, text: 'gamma delta', vec: [1f, 1f] }]"

[[test.results]]
value = "NONE"

# `building.queued` and `building.active` follow the background compaction drain,
# which a server runs and the in-process harness does not, so they are not pinned.
[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 3, pending: 0, status: 'ready', updated: 0 }"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 3, pending: 0, status: 'ready', updated: 0 }"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 3, pending: 0, status: 'ready', updated: 0 }"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 3, pending: 0, status: 'ready', updated: 0 }"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 3, pending: 0, status: 'ready', updated: 0 }"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 3, pending: 0, status: 'ready', updated: 0 }"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:3 }]"

*/

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
CREATE t:1 SET text = 'alpha beta', vec = [1.0, 0.0];
CREATE t:2 SET text = 'beta gamma', vec = [0.0, 1.0];
CREATE t:3 SET text = 'gamma delta', vec = [1.0, 1.0];

DEFINE INDEX ft ON t FIELDS text FULLTEXT ANALYZER simple BM25;
INFO FOR INDEX ft ON t;
DEFINE INDEX cx ON t COUNT;
INFO FOR INDEX cx ON t;
DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
INFO FOR INDEX hx ON t;

REBUILD INDEX ft ON t;
INFO FOR INDEX ft ON t;
REBUILD INDEX cx ON t;
INFO FOR INDEX cx ON t;
REBUILD INDEX hx ON t;
INFO FOR INDEX hx ON t;

SELECT id FROM t WHERE text @@ 'beta' ORDER BY id;
SELECT count() FROM t GROUP ALL;
SELECT id FROM t WHERE vec <|2,10|> [1.0, 0.0] ORDER BY id;
