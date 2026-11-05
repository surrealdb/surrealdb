/**
[test]
reason = """
Setup for 3.00.00_index_unique_nullish_slot.surql and its _current sibling: the old
binary writes a UNIQUE index over two columns holding tuples with a NONE or a
NULL value beside complete ones. Each nullish tuple is written once under its
leading value, which a writer storing such a tuple at its unique slot
accepts."""
run = false
*/
DEFINE TABLE pair SCHEMALESS;
DEFINE FIELD a ON pair TYPE string;
DEFINE FIELD b ON pair TYPE option<string | null>;
DEFINE INDEX pair_ab ON pair FIELDS a, b UNIQUE;
CREATE pair:1 SET a = 'k', b = 'v';
CREATE pair:2 SET a = 'k';
CREATE pair:3 SET a = 'k', b = NULL;
CREATE pair:4 SET a = 'j', b = 'v';
CREATE pair:5 SET a = 'j';
