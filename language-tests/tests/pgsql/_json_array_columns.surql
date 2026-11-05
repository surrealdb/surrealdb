/**
[test]
run = false
reason = "Setup only: the tables CREATE TABLE ja_nullable (id int PRIMARY KEY, docs jsonb[]), ja_required (id int PRIMARY KEY, docs json[] NOT NULL) and ja_grid (id int PRIMARY KEY, docs jsonb[][]) define, each written with arrays of JSON documents, read by json_array_columns_hold_documents"
*/

DEFINE TABLE ja_nullable SCHEMAFULL;
DEFINE FIELD id ON ja_nullable TYPE int SQL TYPE POSTGRESQL 'integer';
DEFINE FIELD docs ON ja_nullable TYPE none | null | array<any> SQL TYPE POSTGRESQL 'jsonb[]';
DEFINE TABLE ja_required SCHEMAFULL;
DEFINE FIELD id ON ja_required TYPE int SQL TYPE POSTGRESQL 'integer';
DEFINE FIELD docs ON ja_required TYPE array<any> SQL TYPE POSTGRESQL 'json[]';
DEFINE TABLE ja_grid SCHEMAFULL;
DEFINE FIELD id ON ja_grid TYPE int SQL TYPE POSTGRESQL 'integer';
DEFINE FIELD docs ON ja_grid TYPE none | null | array<array<any>> SQL TYPE POSTGRESQL 'jsonb[][]';
CREATE ja_nullable:1 SET id = 1, docs = [{ a: 1 }, { b: { c: [2, { d: NULL }] } }];
CREATE ja_required:1 SET id = 1, docs = [{ a: 1 }, [1, { e: 'f' }]];
UPDATE ja_required:1 SET docs = [{ z: { y: 1 } }, { a: 1 }];
CREATE ja_grid:1 SET id = 1, docs = [[{ a: 1 }], [{ g: { h: true } }]];
