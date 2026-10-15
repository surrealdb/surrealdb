/**
[test]
reason = "A migrated value must pass every definition beneath its field. The definition of an array's or a set's elements, and the definitions beneath it, are part of the type a value is tested against, so a transform whose elements break them takes the FALLBACK, and a FALLBACK that breaks them is refused. A definition no type can carry, such as the values of an object (`m.*`) or a key beneath an object that may hold others, refuses the migration and names the field to migrate instead; migrating that field is accepted."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[], []]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[{ qty: 7 }], [{ qty: 7 }]]"

[[test.results]]
error = "Cannot migrate field `items` of table `t`: the FALLBACK does not pass the field's definition"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[{ qty: 7 }, { qty: 9 }], [{ qty: 7 }]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{,}]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot migrate field `m` of table `o`: the type of the field `m.*` beneath it is not one a migrated value can be checked against: migrate that field itself"

[[test.results]]
value = "[{ a: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot migrate field `meta` of table `l`: the type of the field `meta.score` beneath it is not one a migrated value can be checked against: migrate that field itself"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ note: 'x', score: 2 }]"
*/
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD items ON t TYPE array<object>;
DEFINE FIELD items.*.qty ON t TYPE int;
CREATE t:1 SET items = [{ qty: 1 }] RETURN NONE;
CREATE t:2 SET items = [{ qty: 2 }, { qty: 3 }] RETURN NONE;
ALTER FIELD items ON t TYPE array<object> MIGRATE EXISTING ROWS AS items_bad USING [{ qty: 'bad' }] FALLBACK [];
SELECT VALUE items FROM t ORDER BY id;
ALTER FIELD items ON t TYPE array<object> MIGRATE EXISTING ROWS AS items_good USING [{ qty: 7 }] FALLBACK [];
SELECT VALUE items FROM t ORDER BY id;
ALTER FIELD items ON t TYPE array<object> MIGRATE EXISTING ROWS AS items_bad_fallback USING [{ qty: 1 }] FALLBACK [{ qty: 'x' }];
UPDATE t:1 SET items += { qty: 9 } RETURN NONE;
SELECT VALUE items FROM t ORDER BY id;

DEFINE TABLE s SCHEMAFULL;
DEFINE FIELD tags ON s TYPE set<object>;
DEFINE FIELD tags.*.n ON s TYPE int;
CREATE s:1 SET tags = [{ n: 1 }] RETURN NONE;
ALTER FIELD tags ON s TYPE set<object> MIGRATE EXISTING ROWS AS tags_bad USING [{ n: 'bad' }] FALLBACK [];
SELECT VALUE tags FROM s;

DEFINE TABLE o SCHEMAFULL;
DEFINE FIELD m ON o TYPE object;
DEFINE FIELD m.* ON o TYPE int;
CREATE o:1 SET m = { a: 1 } RETURN NONE;
ALTER FIELD m ON o TYPE object MIGRATE EXISTING ROWS AS m_bad USING { a: 'bad' } FALLBACK {};
SELECT VALUE m FROM o;

DEFINE TABLE l SCHEMALESS;
DEFINE FIELD meta ON l TYPE object;
DEFINE FIELD meta.score ON l TYPE int;
CREATE l:1 SET meta = { score: 1, note: 'x' } RETURN NONE;
ALTER FIELD meta ON l TYPE object MIGRATE EXISTING ROWS AS meta_bad USING { score: 'bad' } FALLBACK {};
ALTER FIELD meta.score ON l TYPE int MIGRATE EXISTING ROWS AS score_plus USING $value + 1 FALLBACK 0;
SELECT VALUE meta FROM l;
