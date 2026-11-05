/**
[test]
reason = """
Setup for 3.00.02_index_unique_nullish_coexist.surql: the old binary writes
several tuples sharing a NONE or a NULL value on a UNIQUE index, which coexist
because such a tuple equals no other for uniqueness."""
run = false
*/
DEFINE TABLE nul SCHEMALESS;
DEFINE FIELD a ON nul TYPE string;
DEFINE FIELD b ON nul TYPE option<string | null>;
DEFINE INDEX nul_ab ON nul FIELDS a, b UNIQUE;
CREATE nul:1 SET a = 'k', b = 'v';
CREATE nul:2 SET a = 'k';
CREATE nul:3 SET a = 'k';
CREATE nul:4 SET a = 'k', b = NULL;
CREATE nul:5 SET a = 'k', b = NULL;
CREATE nul:6 SET a = 'j', b = 'v';
