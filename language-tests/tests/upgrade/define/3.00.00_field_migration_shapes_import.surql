/**
[test]
reason = "Setup for 3.00.00_field_migration_shapes.surql: the old binary writes rows of nested, array, optional, record link and edge shapes into SCHEMAFULL and SCHEMALESS tables, under indexes it builds itself."
run = false
*/
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD age ON person TYPE string;
CREATE person:1 SET age = '41';
CREATE person:2 SET age = 'unknown';
DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD name ON doc TYPE string;
DEFINE FIELD qty ON doc TYPE string;
DEFINE FIELD meta ON doc TYPE object;
DEFINE FIELD meta.score ON doc TYPE string;
DEFINE FIELD meta.note ON doc TYPE option<string>;
DEFINE FIELD tags ON doc TYPE array<string>;
DEFINE FIELD opt ON doc TYPE option<string>;
DEFINE FIELD owner ON doc TYPE option<record<person>>;
DEFINE FIELD retired ON doc TYPE option<string>;
DEFINE INDEX by_name ON doc FIELDS name;
DEFINE INDEX by_qty ON doc FIELDS qty;
CREATE doc:1 SET name = 'a', qty = '5', meta = { score: '1.5', note: '9' }, tags = ['x', 'y'], opt = '10', owner = person:1, retired = 'r1';
CREATE doc:2 SET name = 'b', qty = 'junk', meta = { score: 'n/a' }, tags = [], owner = person:2;
CREATE doc:3 SET name = 'c', qty = '07', meta = { score: '3', note: 'none' }, tags = ['z'], opt = 'x', retired = 'r3';
DEFINE TABLE knows TYPE RELATION IN person OUT person SCHEMAFULL;
DEFINE FIELD since ON knows TYPE string;
RELATE person:1->knows->person:2 SET since = '2001';
RELATE person:2->knows->person:1 SET since = 'long ago';
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD a ON loose TYPE string;
CREATE loose:1 SET a = '1', extra = { deep: [1, '2'] };
CREATE loose:2 SET a = 'two', other = true;
