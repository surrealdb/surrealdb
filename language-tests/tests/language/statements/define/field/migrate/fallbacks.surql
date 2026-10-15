/**
[test]
reason = "A transform that raises, a result that does not coerce, and a lossy coercion all take the FALLBACK."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 'x', b: 1000000000000000000000000000000f, id: tb:1 }]"

[[test.results]]
value = "[{ a: '12', b: 9007199254740993, id: tb:2 }]"

[[test.results]]
value = "[{ a: 7, b: 2.5f, id: tb:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: -1, b: -1 }, { a: 12, b: 9007199254740993 }, { a: 7, b: -1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: fl:1, n: 9007199254740993 }]"

[[test.results]]
value = "[{ id: fl:2, n: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ n: -1f }, { n: 3f }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: s:1, v: 'a' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ v: 42 }]"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE any;
DEFINE FIELD b ON tb TYPE any;
CREATE tb:1 SET a = 'x', b = 1e30;
CREATE tb:2 SET a = '12', b = 9007199254740993;
CREATE tb:3 SET a = 7, b = 2.5;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER FIELD b ON tb TYPE int MIGRATE EXISTING ROWS AS b_int USING $value FALLBACK -1;
SELECT a, b FROM tb ORDER BY id;
DEFINE TABLE fl SCHEMAFULL;
DEFINE FIELD n ON fl TYPE any;
CREATE fl:1 SET n = 9007199254740993;
CREATE fl:2 SET n = 3;
ALTER FIELD n ON fl TYPE float MIGRATE EXISTING ROWS AS n_float USING $value FALLBACK -1f;
SELECT n FROM fl ORDER BY id;
-- A result that does not satisfy the type.
DEFINE TABLE s SCHEMAFULL;
DEFINE FIELD v ON s TYPE string;
CREATE s:1 SET v = 'a';
ALTER FIELD v ON s TYPE int MIGRATE EXISTING ROWS AS v_int USING $value FALLBACK 42;
SELECT v FROM s;
