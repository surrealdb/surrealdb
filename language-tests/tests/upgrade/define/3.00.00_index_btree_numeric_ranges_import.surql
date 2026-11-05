/**
[test]
reason = """
Setup for 3.00.00_index_btree_numeric_ranges.surql and its _current sibling:
the old binary writes a non-unique and a UNIQUE index over a column declared
`number`, holding ints, floats and decimals of either sign, zero in every kind
and spelling, and integers past the precision of a float, so the new binary
seeks and ranges over entries whose key bytes the old writer chose."""
run = false
*/
DEFINE TABLE num SCHEMALESS;
-- A declared scalar kind holds no array, so the index can deliver order and
-- answer a count from its keys alone.
DEFINE FIELD n ON num TYPE number;
DEFINE INDEX num_n ON num FIELDS n;
CREATE num:1 SET n = -9007199254740993;
CREATE num:2 SET n = -1000.5dec;
CREATE num:3 SET n = -2.5f;
CREATE num:4 SET n = -1;
CREATE num:5 SET n = -0.0f;
CREATE num:6 SET n = 0;
CREATE num:7 SET n = 0dec;
CREATE num:8 SET n = 0.25f;
CREATE num:9 SET n = 1;
CREATE num:10 SET n = 1f;
CREATE num:11 SET n = 1dec;
CREATE num:12 SET n = 1.5dec;
CREATE num:13 SET n = 2;
-- 2^53 and 2^53 + 1 are one float apart and two distinct integers.
CREATE num:14 SET n = 9007199254740992;
CREATE num:15 SET n = 9007199254740993;
CREATE num:16 SET n = 100000000000000000000dec;

DEFINE TABLE numu SCHEMALESS;
DEFINE FIELD n ON numu TYPE number;
DEFINE INDEX numu_n ON numu FIELDS n UNIQUE;
CREATE numu:1 SET n = -9007199254740993;
CREATE numu:2 SET n = -1000.5dec;
CREATE numu:3 SET n = -2.5f;
CREATE numu:4 SET n = -1;
CREATE numu:5 SET n = -0.0f;
CREATE numu:8 SET n = 0.25f;
CREATE numu:11 SET n = 1dec;
CREATE numu:12 SET n = 1.5f;
CREATE numu:13 SET n = 2;
CREATE numu:14 SET n = 9007199254740992;
CREATE numu:15 SET n = 9007199254740993;
CREATE numu:16 SET n = 100000000000000000000dec;
