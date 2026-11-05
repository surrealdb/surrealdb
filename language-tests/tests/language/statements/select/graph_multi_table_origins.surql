/**
[test]
reason = "Graph lookups whose origins sit in tables with different edge inline-cache settings, one origin built from data, answer the same whichever table each origin is in"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: cached:1 }, { id: cached:2 }, { id: plain:1 }, { id: plain:2 }]"

[[test.results]]
value = "[{ id: knows:a, in: cached:1, out: plain:1 }]"

[[test.results]]
value = "[{ id: knows:b, in: cached:1, out: cached:2 }]"

[[test.results]]
value = "[{ id: knows:c, in: plain:1, out: cached:1 }]"

[[test.results]]
value = "[{ id: knows:d, in: plain:1, out: plain:2 }]"

[[test.results]]
value = "[{ id: knows:e, in: plain:2, out: cached:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[plain:1, cached:2], [cached:1, plain:2], [], [cached:2]]"

[[test.results]]
value = "[[plain:1, cached:2, cached:2], [cached:1, plain:2]]"

[[test.results]]
value = "[[cached:1, plain:2], [plain:1], [cached:1], [plain:1]]"

[[test.results]]
value = "[[cached:1, plain:2], [plain:1, cached:2]]"

[[test.results]]
value = "[[cached:1, plain:2]]"

[[test.results]]
value = "[[cached:1, plain:2], [plain:1, cached:2]]"
*/

DEFINE TABLE cached SCHEMALESS INLINE EDGES 4;
DEFINE TABLE plain SCHEMALESS;
DEFINE TABLE knows SCHEMALESS;
CREATE cached:1, cached:2, plain:1, plain:2;
RELATE cached:1->knows:a->plain:1;
RELATE cached:1->knows:b->cached:2;
RELATE plain:1->knows:c->cached:1;
RELATE plain:1->knows:d->plain:2;
RELATE plain:2->knows:e->cached:2;
LET $t = 'plain';
LET $k = 1;
SELECT VALUE ->knows->? FROM [cached:1, plain:1, cached:2, plain:2];
SELECT VALUE ->knows->?->knows->? FROM [plain:1, cached:1];
SELECT VALUE <-knows<-? FROM [cached:2, plain:2, plain:1, cached:1];
SELECT VALUE ->knows->? FROM [type::record($t, $k), cached:1];
SELECT VALUE ->knows->? FROM type::record($t, $k);
SELECT VALUE ->knows->? FROM [plain:1, cached:1, plain:2] WHERE id != plain:2;
