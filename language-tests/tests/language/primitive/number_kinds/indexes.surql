/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: idx_t:1, v: 10u8 }]"

[[test.results]]
value = "[{ id: idx_t:2, v: -20i8 }]"

[[test.results]]
value = "[{ id: idx_t:3, v: 300u16 }]"

[[test.results]]
value = "[{ id: idx_t:4, v: 1.5f16 }]"

[[test.results]]
value = "[{ id: idx_t:5, v: 10000000000000000000u64 }]"

[[test.results]]
error = "Database index `idx_v` already contains 10, with record `idx_t:1`"

[[test.results]]
error = "Database index `idx_v` already contains 10f32, with record `idx_t:1`"

[[test.results]]
error = "Database index `idx_v` already contains 1.5dec, with record `idx_t:4`"

[[test.results]]
value = "[{ id: idx_t:1, v: 10u8 }]"

[[test.results]]
value = "[{ id: idx_t:1, v: 10u8 }]"

[[test.results]]
value = "[{ id: idx_t:4, v: 1.5f16 }]"

[[test.results]]
value = "[{ id: idx_t:2, v: -20i8 }, { id: idx_t:4, v: 1.5f16 }, { id: idx_t:1, v: 10u8 }, { id: idx_t:3, v: 300u16 }]"

[[test.results]]
value = "[{ id: idx_t:3, v: 300u16 }, { id: idx_t:5, v: 10000000000000000000u64 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, v]
    IndexScan [ctx: Db] [table: idx_t, index: idx_v, access: = 10u8, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, v]
    IndexScan [ctx: Db] [table: idx_t, index: idx_v, access: >=0u8 <=500u16, direction: Forward]
''''
*/

DEFINE TABLE idx_t SCHEMALESS;
DEFINE INDEX idx_v ON idx_t FIELDS v UNIQUE;

CREATE idx_t:1 SET v = 10u8;
CREATE idx_t:2 SET v = -20i8;
CREATE idx_t:3 SET v = 300u16;
CREATE idx_t:4 SET v = 1.5f16;
CREATE idx_t:5 SET v = 10000000000000000000u64;

-- UNIQUE index treats equal numeric values across kinds as duplicates
CREATE idx_t:dup_int SET v = 10;
CREATE idx_t:dup_f32 SET v = 10.0f32;
CREATE idx_t:dup_dec SET v = 1.5dec;

-- Point and range lookups on B-tree index match across kinds and use the index plan
SELECT id, v FROM idx_t WHERE v = 10 ORDER BY id;
SELECT id, v FROM idx_t WHERE v = 10u8 ORDER BY id;
SELECT id, v FROM idx_t WHERE v = 1.5f ORDER BY id;
SELECT id, v FROM idx_t WHERE v >= -20 AND v <= 300 ORDER BY v ASC;
SELECT id, v FROM idx_t WHERE v > 100u8 ORDER BY v ASC;

EXPLAIN SELECT id, v FROM idx_t WHERE v = 10u8;
EXPLAIN SELECT id, v FROM idx_t WHERE v >= 0u8 AND v <= 500u16;
