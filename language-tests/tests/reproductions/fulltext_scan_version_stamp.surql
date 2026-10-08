/**
[env]
namespace = true
database = true
auth = { level = "owner" }
backend = ["rocksdb", "surrealkv"]
versioned = true
timeout = 5000
planner-strategy = ["all-ro"]

[test]
reason = """
	A `VERSION` clause is evaluated once, by the `VersionScope` the planner
	wraps the statement in, against the unversioned outer context. A scan
	beneath it reads that stamp rather than evaluating the clause again,
	since the second evaluation would run at the stamp the first produced,
	where a `DEFINE PARAM` stored after it cannot be read. The full-text scan
	resolves it that way too.

	A `VERSION` datetime truncates to the HLC millisecond, so the definitions
	the read must resolve are separated from the stamp by a sleep.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'hello', id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $hist]
    SelectProject [ctx: Db] [projections: id]
        FullTextScan [ctx: Db] [table: t, index: body_ft, query: hello]
''''

[[test.results]]
value = "[{ id: t:1 }]"
*/

DEFINE ANALYZER blank TOKENIZERS blank;
DEFINE TABLE t SCHEMALESS;
DEFINE INDEX body_ft ON t FIELDS body FULLTEXT ANALYZER blank BM25;
CREATE t:1 SET body = 'hello';

-- The stamp must name a later millisecond than the definitions above.
SLEEP 50ms;

-- Stored after the record, and strictly before the revision the parameter
-- itself is stored at, so a read of `$hist` at `$hist` finds no definition.
DEFINE PARAM $hist VALUE time::now() PERMISSIONS FULL;

EXPLAIN SELECT id FROM t WHERE body @@ 'hello' VERSION $hist;
SELECT id FROM t WHERE body @@ 'hello' VERSION $hist;
