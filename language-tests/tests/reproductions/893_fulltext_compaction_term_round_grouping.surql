/**
[env]
namespace = true
database = true

[test]
reason = """
A document repeatedly gaining and losing a term must resolve to the term set its
last write left, whatever the compactor did with the changes in between.

Uncompacted changes to a term are signed counts folded onto a compacted bitmap.
The bitmap holds a document once, so a partial fold can arrive at a count it
cannot represent — a document seen added while the removal cancelling it is in
the part of the log the round did not reach. Discarding what does not fit makes
the compacted set depend on where the round stopped, and the document keeps a
term it lost or gains one it does not carry.

Where a compaction round stops is not reachable from SurrealQL, so what this
covers end to end is the arithmetic either side of it: each write's own
transaction, the reads that fold what is uncompacted, and the compaction the
writes trigger. The round boundaries themselves are pinned in the full-text
index's own tests.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: doc:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: doc:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: doc:1 }]"

[[test.results]]
value = "[{ id: doc:1 }, { id: doc:2 }]"

[[test.results]]
value = "[{ id: doc:2 }]"
*/

DEFINE ANALYZER az TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_title ON doc COLUMNS title FULLTEXT ANALYZER az BM25;

-- A second document that keeps the term throughout, so the term's compacted set
-- is never empty and the first document's changes always land on an existing
-- bitmap rather than on nothing.
CREATE doc:2 SET title = 'alpha bravo' RETURN NONE;

-- Each update is its own transaction, so each contributes its own delta entry:
-- the term leaves the document, comes back, and leaves again.
CREATE doc:1 SET title = 'alpha charlie' RETURN NONE;
SELECT id FROM doc WHERE title @@ 'charlie';

UPDATE doc:1 SET title = 'alpha delta' RETURN NONE;
SELECT id FROM doc WHERE title @@ 'charlie';

UPDATE doc:1 SET title = 'alpha charlie' RETURN NONE;
SELECT id FROM doc WHERE title @@ 'charlie';

UPDATE doc:1 SET title = 'alpha delta' RETURN NONE;
SELECT id FROM doc WHERE title @@ 'charlie';

UPDATE doc:1 SET title = 'alpha charlie' RETURN NONE;
SELECT id FROM doc WHERE title @@ 'charlie';

-- The terms neither document churned are unaffected by the one that did.
SELECT id FROM doc WHERE title @@ 'alpha' ORDER BY id;
SELECT id FROM doc WHERE title @@ 'bravo';
