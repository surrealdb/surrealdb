/**
[test]
reason = "A migration is refused while an index reads a value it changes: an index over the migrated path, over a field it is nested in, or over a path that meets it at an array element. An index over a sibling path of the same object reads none of them, so it does not refuse the migration, and it answers as before once the migration has run."

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
value = "[{ id: box:1, list: ['1', 'x'], meta: { score: 10, tags: ['a'] } }]"

[[test.results]]
value = "[{ id: box:2, list: ['2'], meta: { score: 20, tags: ['b'] } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ score: 10f, tags: ['a'] }, { score: 20f, tags: ['b'] }]"

[[test.results]]
value = "[box:2]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot migrate field `meta.score` of table `box`: the index `by_meta` reads it: remove the index, migrate, then define it again"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot migrate field `list.*` of table `box`: the index `by_first` reads it: remove the index, migrate, then define it again"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 0], [2]]"

*/
DEFINE TABLE box SCHEMAFULL;
DEFINE FIELD meta ON box TYPE object;
DEFINE FIELD meta.score ON box TYPE option<int>;
DEFINE FIELD meta.tags ON box TYPE option<array<string>>;
DEFINE FIELD list ON box TYPE option<array>;
CREATE box:1 SET meta = { score: 10, tags: ['a'] }, list = ['1', 'x'];
CREATE box:2 SET meta = { score: 20, tags: ['b'] }, list = ['2'];

DEFINE INDEX by_tag ON box FIELDS meta.tags;
ALTER FIELD meta.score ON box TYPE option<float> MIGRATE EXISTING ROWS AS score_f USING <float>$value FALLBACK 0.5;
SELECT VALUE meta FROM box ORDER BY id;
SELECT VALUE id FROM box WHERE meta.tags CONTAINS 'b';

DEFINE INDEX by_meta ON box FIELDS meta;
ALTER FIELD meta.score ON box TYPE option<decimal> MIGRATE EXISTING ROWS AS score_d USING <decimal>$value FALLBACK 0.5dec;
REMOVE INDEX by_meta ON box;

DEFINE INDEX by_first ON box FIELDS list[0];
ALTER FIELD list.* ON box TYPE int MIGRATE EXISTING ROWS AS list_int USING <int>$value FALLBACK 0;
REMOVE INDEX by_first ON box;
ALTER FIELD list.* ON box TYPE int MIGRATE EXISTING ROWS AS list_int USING <int>$value FALLBACK 0;
SELECT VALUE list FROM box ORDER BY id;
