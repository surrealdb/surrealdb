/**
[test]
reason = """
Setup for 3.00.00_index_btree_compound_ranges.surql and its _current sibling:
the old binary writes a non-unique and a UNIQUE index over (string, number),
with several numbers of mixed kinds under one leading value and neighbours on
either side of it, so the new binary reads a prefix-plus-range over entries the
old writer laid out."""
run = false
*/
DEFINE TABLE cmp SCHEMALESS;
DEFINE FIELD a ON cmp TYPE string;
DEFINE FIELD b ON cmp TYPE number;
DEFINE INDEX cmp_ab ON cmp FIELDS a, b;
CREATE cmp:1 SET a = 'w', b = 5;
CREATE cmp:2 SET a = 'x', b = -1;
CREATE cmp:3 SET a = 'x', b = 0;
CREATE cmp:4 SET a = 'x', b = 0.5f;
CREATE cmp:5 SET a = 'x', b = 1;
CREATE cmp:6 SET a = 'x', b = 1dec;
CREATE cmp:7 SET a = 'x', b = 2;
CREATE cmp:8 SET a = 'x', b = 3.5dec;
CREATE cmp:9 SET a = 'y', b = -5;
-- A leading value that extends 'x': a prefix range over 'x' must end before it.
CREATE cmp:10 SET a = 'xx', b = 1;

DEFINE TABLE cmpu SCHEMALESS;
DEFINE FIELD a ON cmpu TYPE string;
DEFINE FIELD b ON cmpu TYPE number;
DEFINE INDEX cmpu_ab ON cmpu FIELDS a, b UNIQUE;
CREATE cmpu:1 SET a = 'w', b = 5;
CREATE cmpu:2 SET a = 'x', b = -1;
CREATE cmpu:3 SET a = 'x', b = 0;
CREATE cmpu:4 SET a = 'x', b = 0.5f;
CREATE cmpu:5 SET a = 'x', b = 1dec;
CREATE cmpu:7 SET a = 'x', b = 2;
CREATE cmpu:8 SET a = 'x', b = 3.5dec;
CREATE cmpu:9 SET a = 'y', b = -5;
CREATE cmpu:10 SET a = 'xx', b = 1;
