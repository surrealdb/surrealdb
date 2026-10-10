/**
[test]
reason = """
Setup for 3.00.00_index_unique_nullish_single.surql and its _current sibling:
the old binary writes a single-column UNIQUE index holding several NULL values
and a NONE beside complete ones."""
run = false
*/
DEFINE TABLE solo SCHEMALESS;
DEFINE FIELD v ON solo TYPE option<string | null>;
DEFINE INDEX solo_v ON solo FIELDS v UNIQUE;
CREATE solo:1 SET v = 'a';
CREATE solo:2 SET v = NULL;
CREATE solo:3 SET v = NULL;
CREATE solo:4 SET v = 'b';
CREATE solo:5;
