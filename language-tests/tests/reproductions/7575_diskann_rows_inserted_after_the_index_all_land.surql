/**
[test]
requires = ["diskann"]
reason = "Public issue surrealdb/surrealdb#7575: `surreal export` writes a table's `DEFINE INDEX … DISKANN` ahead of its data, and on import an `INSERT` could fail with a retryable write conflict against background index compaction: the compactor stepped the shard's `!dy` pending-state guard that the `INSERT` had read or written. On a store the datastore is the sole writer of, the writing transaction never touches the guard; its commit arms the guard in a transaction of its own, under a fence the compactor's guard steps also take. This pins the export-ordered shape end to end: every batch lands and KNN reaches the rows. The block at the end pins what the writing transaction sees before its commit arms the guard: its own pending row in a KNN query, and `compacting` reported for it. The interleaving itself is pinned by the DiskANN unit tests in `surrealdb-idx`."
issue = 7575

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ distance: 0f, id: seg:33 }, { distance: 1f, id: seg:34 }, { distance: 25f, id: seg:8 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: seg:40 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 11 }]"
*/

OPTION IMPORT;
DEFINE TABLE seg TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD text ON seg TYPE string;
DEFINE FIELD embedding ON seg TYPE option<array<float>>;
DEFINE INDEX seg_emb ON seg FIELDS embedding DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
INSERT [ { embedding: [1f, 0f], id: seg:1, text: 'row 1' }, { embedding: [2f, 0f], id: seg:2, text: 'row 2' } ];
INSERT [ { embedding: [3f, 0f], id: seg:3, text: 'row 3' }, { embedding: [4f, 0f], id: seg:4, text: 'row 4' } ];
INSERT [ { embedding: [5f, 0f], id: seg:5, text: 'row 5' }, { embedding: [6f, 0f], id: seg:6, text: 'row 6' } ];
INSERT [ { embedding: [7f, 0f], id: seg:7, text: 'row 7' }, { embedding: [8f, 0f], id: seg:8, text: 'row 8' } ];
INSERT [ { embedding: [33f, 0f], id: seg:33, text: 'row 33' }, { embedding: [34f, 0f], id: seg:34, text: 'row 34' } ];
SELECT count() FROM seg GROUP ALL;
SELECT id, vector::distance::knn() AS distance FROM seg WHERE embedding <|3,40|> [33f, 0f] ORDER BY distance;
BEGIN;
CREATE seg:40 SET embedding = [40f, 0f], text = 'row 40';
SELECT id FROM seg WHERE embedding <|1,40|> [40f, 0f];
(INFO FOR INDEX seg_emb ON seg).building.compacting;
COMMIT;
SELECT count() FROM seg GROUP ALL;
