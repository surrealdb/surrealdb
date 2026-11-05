/**
[test]
reason = "A SurrealQL write to a column a MySQL CREATE TABLE declared is refused or canonicalised as the SQL write is: an over-long VARCHAR, a DECIMAL(5,2) overflow, a BINARY(4) overflow, a JSON column given a record id, with CHAR(n), DECIMAL, DATETIME(0) and BINARY(4) values canonicalised"

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
value = """[{ b: b"61620000", c: 'ab', id: dt:1, j: [1, 2], n: 123.46dec, ts: d'2026-10-27T12:00:01Z', v: 'abc' }]"""

[[test.results]]
error = "Data too long for column 'v' at row 1 (field `v` of `dt:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "Out of range value for column 'n' at row 1 (field `n` of `dt:1`, SQL TYPE decimal(5,2))"

[[test.results]]
error = "Data too long for column 'b' at row 1 (field `b` of `dt:1`, SQL TYPE binary(4))"

[[test.results]]
error = "JSON cannot hold a record (field `j` of `dt:1`, SQL TYPE json)"

[[test.results]]
value = """[{ b: b"61620000", c: 'ab', id: dt:1, j: 'text', n: 123.46dec, ts: d'2026-10-27T12:00:01Z', v: 'abc' }]"""

[[test.results]]
value = """[{ b: b"61620000", c: 'ab', id: dt:1, j: 'text', n: 123.46dec, ts: d'2026-10-27T12:00:01Z', v: 'abc' }]"""
*/

DEFINE TABLE dt SCHEMAFULL;
DEFINE FIELD v ON dt TYPE option<string> SQL TYPE MYSQL 'VARCHAR(3)';
DEFINE FIELD c ON dt TYPE option<string> SQL TYPE MYSQL 'CHAR(4)';
DEFINE FIELD n ON dt TYPE option<decimal> SQL TYPE MYSQL 'DECIMAL(5,2)';
DEFINE FIELD ts ON dt TYPE option<datetime> SQL TYPE MYSQL 'DATETIME(0)';
DEFINE FIELD b ON dt TYPE option<bytes> SQL TYPE MYSQL 'BINARY(4)';
DEFINE FIELD j ON dt TYPE any SQL TYPE MYSQL 'JSON';
CREATE dt:1 SET v = 'abc', c = 'ab      ', n = 123.455dec, ts = d'2026-10-27T12:00:00.5Z', b = <bytes>'ab', j = [1, 2];
UPDATE dt:1 SET v = 'abcd';
UPDATE dt:1 SET n = 1000dec;
UPDATE dt:1 SET b = <bytes>'abcde';
UPDATE dt:1 SET j = person:1;
UPDATE dt:1 SET j = 'text';
SELECT * FROM dt;
