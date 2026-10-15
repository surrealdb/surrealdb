/**
[test]
reason = "A migration of a field is refused while an index of any kind reads it, in a database at either compatibility level, with nothing minted, and is accepted once the index is removed; an index on another field, and a COUNT index with no guard, are no obstacle."

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: the index `plain` reads it: remove the index, migrate, then define it again"

[[test.results]]
error = "Cannot migrate field `c` of table `tb`: the index `uniq` reads it: remove the index, migrate, then define it again"

[[test.results]]
error = "Cannot migrate field `d` of table `tb`: the index `comp` reads it: remove the index, migrate, then define it again"

[[test.results]]
error = "Cannot migrate field `e` of table `tb`: the index `comp` reads it: remove the index, migrate, then define it again"

[[test.results]]
error = "Cannot migrate field `f` of table `tb`: the index `ft` reads it: remove the index, migrate, then define it again"

[[test.results]]
error = "Cannot migrate field `o.n` of table `tb`: the index `nested` reads it: remove the index, migrate, then define it again"

[[test.results]]
error = "Cannot migrate field `o` of table `tb`: the index `nested` reads it: remove the index, migrate, then define it again"

[[test.results]]
error = "Cannot migrate field `t.*` of table `tb`: the index `arr` reads it: remove the index, migrate, then define it again"

[[test.results]]
value = "{ generation: 11, guaranteed: [], migrations: [] }"

[[test.results]]
value = "[{ a: '1', c: '1', d: '1', e: '1', f: 'Hello', h: '1', k: '1', o: { n: '1' }, t: ['1'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ field: 'h', generation: 12, name: 'h_int' }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[tb:1]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'plain'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[1]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[{ field: 'h', generation: 12, name: 'h_int' }, { field: 'a', generation: 14, name: 'a_int' }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot migrate field `k` of table `tb`: the index `by_k` reads it: remove the index, migrate, then define it again"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['1']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'lvl3', namespace: 'test' }"

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
error = "Cannot migrate field `a` of table `tb`: the index `plain` reads it: remove the index, migrate, then define it again"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '1', b: 1 }]"
*/
DEFINE ANALYZER an TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
DEFINE FIELD c ON tb TYPE string;
DEFINE FIELD d ON tb TYPE string;
DEFINE FIELD e ON tb TYPE string;
DEFINE FIELD f ON tb TYPE string;
DEFINE FIELD h ON tb TYPE string;
DEFINE FIELD k ON tb TYPE string;
DEFINE FIELD o ON tb TYPE option<object>;
DEFINE FIELD o.n ON tb TYPE string;
DEFINE FIELD t ON tb TYPE option<array>;
CREATE tb:1 SET a = '1', c = '1', d = '1', e = '1', h = '1', k = '1', f = 'Hello', o = { n: '1' }, t = ['1'] RETURN NONE;
DEFINE INDEX plain ON tb FIELDS a;
DEFINE INDEX uniq ON tb FIELDS c UNIQUE;
DEFINE INDEX comp ON tb FIELDS e, d;
DEFINE INDEX ft ON tb FIELDS f FULLTEXT ANALYZER an BM25;
DEFINE INDEX nested ON tb FIELDS o.n;
DEFINE INDEX arr ON tb FIELDS t.*;
DEFINE INDEX every ON tb COUNT;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD c ON tb TYPE int MIGRATE EXISTING ROWS AS c_int USING <int>$value FALLBACK 0;
ALTER FIELD d ON tb TYPE int MIGRATE EXISTING ROWS AS d_int USING <int>$value FALLBACK 0;
ALTER FIELD e ON tb TYPE int MIGRATE EXISTING ROWS AS e_int USING <int>$value FALLBACK 0;
ALTER FIELD f ON tb TYPE string MIGRATE EXISTING ROWS AS f_low USING string::lowercase($value) FALLBACK '';
ALTER FIELD o.n ON tb TYPE int MIGRATE EXISTING ROWS AS on_int USING <int>$value FALLBACK 0;
ALTER FIELD o ON tb TYPE option<object> MIGRATE EXISTING ROWS AS o_id USING $value;
ALTER FIELD t.* ON tb TYPE int MIGRATE EXISTING ROWS AS t_int USING <int>$value FALLBACK 0;
-- Nothing was minted.
(INFO FOR TABLE tb).migrations;
SELECT a, c, d, e, f, h, k, o, t FROM tb;
-- An index on another field, and a COUNT index with no guard, are no obstacle.
ALTER FIELD h ON tb TYPE int MIGRATE EXISTING ROWS AS h_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE tb).migrations.migrations;
SELECT VALUE h FROM tb;
-- Removing the index lets the migration through, and the index defined again holds the migrated values.
REMOVE INDEX plain ON tb;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX plain ON tb FIELDS a;
SELECT VALUE id FROM tb WHERE a = 1;
SELECT VALUE id FROM tb WHERE a = 1 EXPLAIN;
SELECT VALUE a FROM tb;
SELECT VALUE type::is_int(a) FROM tb;
(INFO FOR TABLE tb).migrations.migrations;
-- In a transaction the refusal fails the whole transaction.
BEGIN;
DEFINE INDEX by_k ON tb FIELDS k;
ALTER FIELD k ON tb TYPE int MIGRATE EXISTING ROWS AS k_int USING <int>$value FALLBACK 0;
COMMIT;
(INFO FOR TABLE tb).indexes.by_k;
SELECT VALUE k FROM tb;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
DEFINE FIELD b ON tb TYPE string;
CREATE tb:1 SET a = '1', b = '1' RETURN NONE;
DEFINE INDEX plain ON tb FIELDS a UNIQUE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON tb TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
SELECT a, b FROM tb;
