/**
[env]

[test]
reason = """
A record id holding a nested number must stay reachable through an ANN index.

An ANN index queues its writes under a record-keyed pending key and recovered
the id back out of that key at compaction, to resolve the record's doc-ID. That
key pins `IndexFormat` for its indexed values, which omits a number's
`NumberKind` — an index key stands for its whole equality class, so a lookup for
`1` finds a row stored as `1f`. A record id is not matched that way; it
addresses a record, and erasing its type is what breaks it.

So the id is carried in the revisioned pending value instead, which keeps it
exactly, and read in preference to the key's copy.

Recovered from the key, an id holding a number came back decimalised,
compaction recorded the doc-ID under a spelling no record is stored under, and
the KNN hit was dropped at the record fetch — after it had consumed a top-k
slot, so the query yielded fewer rows than the k it asked for and k=1 yielded
none. A nested `Decimal` is the fixed point of that round trip and the only
numeric spelling it leaves intact, which is why the table pairs one with the
rest. An inexact float is the converse case and is covered too: `0.1f` encodes
at a longer scale than it decodes back at, so it is not a fixed point either
way, and it is the spelling that keeps the DiskANN dual-layout fold from
matching its counterpart in one pass. A nested string, bool, uuid or datetime is
never at risk. The decimal here
is deliberately not numerically equal to the integer beside it: two ids that
differ only in numeric variant collide on the forward doc-ID key, which is a
separate defect.
"""
issue = 7437

[[test.results]]
value = "[{ id: t:[1], v: [1f, 0f, 0f, 0f], w: 'array-int' }]"

[[test.results]]
value = "[{ id: t:[2dec], v: [0f, 1f, 0f, 0f], w: 'array-decimal' }]"

[[test.results]]
value = "[{ id: t:['s'], v: [0f, 0f, 1f, 0f], w: 'array-string' }]"

[[test.results]]
value = "[{ id: t:{ k: 2 }, v: [0f, 0f, 0f, 1f], w: 'object-int' }]"

[[test.results]]
value = "[{ id: t:plain, v: [1f, 1f, 0f, 0f], w: 'plain-string' }]"

[[test.results]]
value = "[{ id: t:[0.1f], v: [0f, 1f, 1f, 0f], w: 'array-float' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ w: 'array-decimal' }, { w: 'array-float' }, { w: 'array-int' }, { w: 'array-string' }, { w: 'object-int' }, { w: 'plain-string' }]"

[[test.results]]
value = "[{ w: 'array-decimal' }, { w: 'array-float' }, { w: 'array-int' }, { w: 'array-string' }, { w: 'object-int' }, { w: 'plain-string' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: n:[7], v: [1f, 0f, 0f, 0f] }]"

[[test.results]]
value = "[{ id: n:[7] }]"

[[test.results]]
value = "[true]"
[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: d:[1], v: [1f, 0f, 0f, 0f], w: 'array-int' }]"

[[test.results]]
value = "[{ id: d:[2dec], v: [0f, 1f, 0f, 0f], w: 'array-decimal' }]"

[[test.results]]
value = "[{ id: d:['s'], v: [0f, 0f, 1f, 0f], w: 'array-string' }]"

[[test.results]]
value = "[{ id: d:{ k: 2 }, v: [0f, 0f, 0f, 1f], w: 'object-int' }]"

[[test.results]]
value = "[{ id: d:plain, v: [1f, 1f, 0f, 0f], w: 'plain-string' }]"

[[test.results]]
value = "[{ id: d:[0.1f], v: [0f, 1f, 1f, 0f], w: 'array-float' }]"

[[test.results]]
value = "[{ w: 'array-decimal' }, { w: 'array-float' }, { w: 'array-int' }, { w: 'array-string' }, { w: 'object-int' }, { w: 'plain-string' }]"

[[test.results]]
value = "[{ w: 'array-decimal' }, { w: 'array-float' }, { w: 'array-int' }, { w: 'array-string' }, { w: 'object-int' }, { w: 'plain-string' }]"
*/

-- Four compound ids, one per kind of value they can hold, plus a plain id as
-- the control. Each carries a marker so the assertions do not depend on how
-- two numerically equal ids order against each other.
CREATE t:[1]    SET v = [1.0f, 0.0f, 0.0f, 0.0f], w = 'array-int';
CREATE t:[2dec] SET v = [0.0f, 1.0f, 0.0f, 0.0f], w = 'array-decimal';
CREATE t:['s']  SET v = [0.0f, 0.0f, 1.0f, 0.0f], w = 'array-string';
CREATE t:{k: 2} SET v = [0.0f, 0.0f, 0.0f, 1.0f], w = 'object-int';
CREATE t:plain  SET v = [1.0f, 1.0f, 0.0f, 0.0f], w = 'plain-string';
CREATE t:[0.1f] SET v = [0.0f, 1.0f, 1.0f, 0.0f], w = 'array-float';

DEFINE INDEX t_hnsw ON t FIELDS v
    HNSW DIMENSION 4 DIST COSINE TYPE F32 EFC 150 M 12 M0 24;

LET $q = [1.0f, 0.0f, 0.0f, 0.0f];

-- Every record is readable.
SELECT w FROM t ORDER BY w;
-- The indexed query must agree: k exceeds the row count, so nothing is cut off
-- by k and every row missing here is one the index dropped.
SELECT w FROM t WHERE v <|6, 40|> $q ORDER BY w;

-- A dropped row consumes its top-k slot on the way out, so a lone record whose
-- id holds a number answers k=1 with an empty result instead of itself.
DEFINE INDEX n_hnsw ON n FIELDS v
    HNSW DIMENSION 4 DIST COSINE TYPE F32 EFC 150 M 12 M0 24;
CREATE n:[7] SET v = [1.0f, 0.0f, 0.0f, 0.0f];
SELECT id FROM n WHERE v <|1, 40|> $q;

-- An expected id cannot pin the spelling the defect changes: numeric
-- expectations are type-blind, so `n:[7]` is satisfied by `n:[7dec]`. Presence
-- is the real signal above -- the defect drops the row outright -- and this
-- asserts the variant the row comes back with.
SELECT VALUE type::is_int(id[0]) FROM n WHERE v <|1, 40|> $q;

-- The same shapes through the other ANN family: both queue writes under a
-- record-keyed pending entry and both recover the id from it, so a fix to one
-- that misses the other leaves half the defect in place.
DEFINE INDEX d_ann ON d FIELDS v DISKANN DIMENSION 4 DIST COSINE TYPE F32;
CREATE d:[1]    SET v = [1.0f, 0.0f, 0.0f, 0.0f], w = 'array-int';
CREATE d:[2dec] SET v = [0.0f, 1.0f, 0.0f, 0.0f], w = 'array-decimal';
CREATE d:['s']  SET v = [0.0f, 0.0f, 1.0f, 0.0f], w = 'array-string';
CREATE d:{k: 2} SET v = [0.0f, 0.0f, 0.0f, 1.0f], w = 'object-int';
CREATE d:plain  SET v = [1.0f, 1.0f, 0.0f, 0.0f], w = 'plain-string';
CREATE d:[0.1f] SET v = [0.0f, 1.0f, 1.0f, 0.0f], w = 'array-float';
SELECT w FROM d ORDER BY w;
SELECT w FROM d WHERE v <|6, 40|> $q ORDER BY w;
