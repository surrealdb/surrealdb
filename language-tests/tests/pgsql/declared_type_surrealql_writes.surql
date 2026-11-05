/**
[test]
reason = "A SurrealQL write to a column a PostgreSQL CREATE TABLE declared is refused or canonicalised as the SQL write is: an over-long varchar, a numeric(5,2) overflow, a json column given a record id or a value nested in one, with char(n), numeric and timestamp(0) values canonicalised"

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
value = "[{ c: 'ab', id: dt:1, j: [1, 2], jb: 5, n: 123.46dec, ts: ts'2026-10-27T12:00:01', v: 'abc' }]"

[[test.results]]
error = "value too long for type character varying(3) (field `v` of `dt:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "numeric field overflow (field `n` of `dt:1`, SQL TYPE numeric(5,2))"

[[test.results]]
error = "numeric field overflow (field `n` of `dt:1`, SQL TYPE numeric(5,2))"

[[test.results]]
error = "JSON cannot hold a record (field `j` of `dt:1`, SQL TYPE json)"

[[test.results]]
error = "JSON cannot hold a record (field `jb` of `dt:1`, SQL TYPE jsonb)"

[[test.results]]
value = "[{ c: 'ab', id: dt:1, j: { a: 'x', b: 1 }, jb: 'text', n: 123.46dec, ts: ts'2026-10-27T12:00:01', v: 'abc' }]"

[[test.results]]
value = "[{ c: 'ab', id: dt:1, j: { a: 'x', b: 1 }, jb: 'text', n: 123.46dec, ts: ts'2026-10-27T12:00:01', v: 'abc' }]"
*/

DEFINE TABLE dt SCHEMAFULL;
DEFINE FIELD v ON dt TYPE option<string> SQL TYPE POSTGRESQL 'varchar(3)';
DEFINE FIELD c ON dt TYPE option<string> SQL TYPE POSTGRESQL 'char(4)';
DEFINE FIELD n ON dt TYPE option<decimal> SQL TYPE POSTGRESQL 'numeric(5,2)';
DEFINE FIELD ts ON dt TYPE option<timestamp> SQL TYPE POSTGRESQL 'timestamp(0)';
DEFINE FIELD j ON dt TYPE any SQL TYPE POSTGRESQL 'json';
DEFINE FIELD jb ON dt TYPE any SQL TYPE POSTGRESQL 'jsonb';
CREATE dt:1 SET v = 'abc', c = 'ab      ', n = 123.455dec, ts = ts'2026-10-27T12:00:00.5', j = [1, 2], jb = 5;
UPDATE dt:1 SET v = 'abcd';
UPDATE dt:1 SET n = 1000dec;
UPDATE dt:1 SET n = 999.995dec;
UPDATE dt:1 SET j = person:1;
UPDATE dt:1 SET jb = { a: [1, person:1] };
UPDATE dt:1 SET j = { b: 1, a: 'x' }, jb = 'text';
SELECT * FROM dt;
