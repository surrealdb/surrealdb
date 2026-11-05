/**
[env]
namespace = true
database = true

[test]
reason = """
	A table whose `id` field is typed as a tuple without component names keeps
	the plain typed-id write behaviour: an explicit key is coerced to the
	tuple as a whole (a float or decimal element stays one), a key that does not
	fit is refused with the generic coercion error, no key is generated, fields
	named like the elements are stored like any other field, and every data
	clause, RETURN form, ON DUPLICATE KEY UPDATE, IGNORE and the events see the
	stored record. Fields are processed in dependency order.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[plain:['a', 5f], false, true]]"

[[test.results]]
value = "[plain:['a', 5]]"

[[test.results]]
value = "[[plain:['b', 5dec], false, true]]"

[[test.results]]
error = "Couldn't coerce value for field `id` of `plain:['c', 5.5f]`: Expected `[string, int]` but found `['c', 5.5f]`"

[[test.results]]
error = "Couldn't coerce value for field `id` of `plain:['e', 'x']`: Expected `[string, int]` but found `['e', 'x']`"

[[test.results]]
error = "Couldn't coerce value for field `id` of `plain:['f']`: Expected `[string, int]` but found `['f']`"

[[test.results]]
error = "Couldn't coerce value for field `id` of `plain:['g', 1, 2]`: Expected `[string, int]` but found `['g', 1, 2]`"

[[test.results]]
error = "Cannot generate a record id of type `[string, int]` for the `plain` table; specify an explicit record id, or declare the `id` field as `uuid` or `string` to auto-generate one."

[[test.results]]
value = "[{ city: 'London', id: plain:['h', 7f] }]"

[[test.results]]
value = "[{ city: 'Paris', id: plain:['i', 8], time: 1 }]"

[[test.results]]
value = "[{ id: plain:['j', 2dec], v: 1 }, { id: plain:['k', 3], v: 2 }]"

[[test.results]]
value = "[{ id: plain:['l', 4f], v: 3 }]"

[[test.results]]
value = "[{ id: plain:['k', 3], v: 9 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: plain:['m', 1], v: 1 }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/v', value: 2 }]]"

[[test.results]]
value = "[{ id: plain:['m', 1], v: 2 }]"

[[test.results]]
value = "[{ id: plain:['m', 1], time: 1, v: 2 }]"

[[test.results]]
value = "[{ city: 'y', id: plain:['m', 1], time: 1, v: 2 }]"

[[test.results]]
value = "[{ city: 'z', id: plain:['m', 1], time: 1, v: 2 }]"

[[test.results]]
value = "[{ id: plain:['m', 1], w: 1 }]"

[[test.results]]
error = "Found ['n', 1] for the `id` field, but a specific record has been specified"

[[test.results]]
value = "[{ id: plain:['a', 5] }, { id: plain:['a', 5f] }, { id: plain:['b', 5dec] }, { city: 'London', id: plain:['h', 7f] }, { city: 'Paris', id: plain:['i', 8], time: 1 }, { id: plain:['j', 2dec], v: 1 }, { id: plain:['k', 3], v: 9 }, { id: plain:['l', 4f], v: 3 }, { id: plain:['m', 1], w: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: plain2:['x', d'2024-01-01T00:00:00Z'] }]"

[[test.results]]
error = "Database record `plain2:['x', d'2024-01-01T00:00:00Z']` already exists"

[[test.results]]
error = "Couldn't coerce value for field `id` of `plain2:['y', '2024-01-01T00:00:00Z']`: Expected `[string, datetime]` but found `['y', '2024-01-01T00:00:00Z']`"

[[test.results]]
value = "[{ id: plain2:['y', d'2024-01-01T00:00:00.500Z'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'x', id: plain3:['a', 1], v: 1 }]"

[[test.results]]
error = "Couldn't coerce value for field `id` of `plain3:['b', 'q']`: Expected `[string, int]` but found `['b', 'q']`"

[[test.results]]
error = "Couldn't coerce value for field `v` of `plain3:['c', 2]`: Expected `int` but found `'w'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[plain4:[1, 2], true]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'k', id: plain5:['a', 5f] }]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 4, b: 2, c: 1, d: 4, id: p6:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: plain:['k', 3], v: 11 }]"

[[test.results]]
value = "[{ id: plain:['o', 1], v: 1 }]"

[[test.results]]
value = "[{ after: { id: plain:['o', 1], v: 1 }, before: NONE, ev: 'CREATE' }, { after: { id: plain:['k', 3], v: 11 }, before: { id: plain:['k', 3], v: 9 }, ev: 'UPDATE' }]"
*/


DEFINE TABLE plain SCHEMALESS;
DEFINE FIELD id ON plain TYPE [string, int];
CREATE plain:['a', 5.0] RETURN VALUE [id, type::is_int(id[1]), type::is_float(id[1])];
CREATE plain:['a', 5] RETURN VALUE id;
CREATE plain:['b', 5dec] RETURN VALUE [id, type::is_int(id[1]), type::is_decimal(id[1])];
CREATE plain:['c', 5.5] RETURN VALUE id;
CREATE plain:['e', 'x'] RETURN VALUE id;
CREATE plain:['f'] RETURN VALUE id;
CREATE plain:['g', 1, 2] RETURN VALUE id;
CREATE plain SET x = 1 RETURN VALUE id;
CREATE plain SET id = ['h', 7.0], city = 'London' RETURN AFTER;
CREATE plain CONTENT { id: ['i', 8], city: 'Paris', time: 1 } RETURN AFTER;
INSERT INTO plain [{ id: ['j', 2dec], v: 1 }, { id: ['k', 3], v: 2 }] RETURN AFTER;
INSERT INTO plain (id, v) VALUES (['l', 4.0], 3) RETURN AFTER;
INSERT INTO plain [{ id: ['k', 3], v: 9 }] ON DUPLICATE KEY UPDATE v = $input.v RETURN AFTER;
INSERT IGNORE INTO plain [{ id: ['k', 3], v: 10 }] RETURN AFTER;
UPSERT plain:['m', 1] SET v = 1 RETURN AFTER;
UPSERT plain:['m', 1] SET v = 2 RETURN DIFF;
UPDATE plain:['m', 1] SET city = 'x', time = 1 RETURN BEFORE;
UPDATE plain:['m', 1] UNSET city RETURN AFTER;
UPDATE plain:['m', 1] MERGE { city: 'y' } RETURN AFTER;
UPDATE plain:['m', 1] PATCH [{ op: 'replace', path: '/city', value: 'z' }] RETURN AFTER;
UPDATE plain:['m', 1] REPLACE { w: 1 } RETURN AFTER;
UPDATE plain:['m', 1] SET id = ['n', 1];
SELECT * FROM plain ORDER BY id;

DEFINE TABLE plain2 SCHEMALESS;
DEFINE FIELD id ON plain2 TYPE [string, datetime] DEFAULT ['x', d'2024-01-01T00:00:00Z'];
CREATE plain2 SET a = 1 RETURN AFTER;
CREATE plain2 SET a = 2 RETURN AFTER;
CREATE plain2:['y', '2024-01-01T00:00:00Z'] RETURN AFTER;
CREATE plain2:['y', d'2024-01-01T00:00:00.5Z'] RETURN AFTER;

DEFINE TABLE plain3 SCHEMAFULL;
DEFINE FIELD id ON plain3 TYPE [string, int];
DEFINE FIELD v ON plain3 TYPE int;
DEFINE FIELD city ON plain3 TYPE string DEFAULT 'none';
CREATE plain3:['a', 1] SET v = 1, city = 'x' RETURN AFTER;
CREATE plain3:['b', 'q'] SET v = 'w' RETURN AFTER;
CREATE plain3:['c', 2] SET v = 'w' RETURN AFTER;

DEFINE TABLE plain4 SCHEMALESS;
DEFINE FIELD id ON plain4 TYPE array<int>;
CREATE plain4:[1.0, 2] RETURN VALUE [id, type::is_int(id[0])];
DEFINE TABLE plain5 SCHEMALESS;
CREATE plain5:['a', 5.0] SET city = 'k' RETURN AFTER;
CREATE plain5 SET city = 'k', time = d'2024-01-01T00:00:00Z' RETURN VALUE string::len(record::id(id)) > 0;

DEFINE TABLE p6 SCHEMALESS;
DEFINE FIELD b ON p6 VALUE c + 1;
DEFINE FIELD c ON p6 DEFAULT 1;
DEFINE FIELD a ON p6 COMPUTED b * 2;
DEFINE FIELD d ON p6 VALUE a;
CREATE p6:1 RETURN AFTER;
DEFINE EVENT ev ON plain THEN { CREATE log SET before = $before, after = $after, ev = $event };
UPDATE plain:['k', 3] SET v = 11;
CREATE plain:['o', 1] SET v = 1;
SELECT before, after, ev FROM log ORDER BY ev, after.v;
