/**
[test]
reason = "A field's declared SQL type judges every SurrealQL write of the field, whatever statement makes it: CREATE, UPDATE, UPSERT, INSERT and its ON DUPLICATE KEY UPDATE, RELATE, INSERT RELATION, CONTENT, MERGE, REPLACE and PATCH. Each over-long varchar(3) is refused with PostgreSQL's message, and a char(3) has its trailing spaces trimmed."

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
error = "value too long for type character varying(3) (field `s` of `t:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `t:1`, SQL TYPE varchar(3))"

[[test.results]]
value = "[{ c: 'ab', id: t:1, s: 'abc' }]"

[[test.results]]
value = "[{ c: 'ab', id: t:1, s: 'abc' }]"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `t:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `t:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `t:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `t:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `t:2`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `t:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `t:3`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `t:4`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `t:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `e:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character varying(3) (field `s` of `e:2`, SQL TYPE varchar(3))"

[[test.results]]
value = "[{ c: 'ab', id: t:1, s: 'abc' }]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD s ON t TYPE option<string> SQL TYPE POSTGRESQL 'varchar(3)';
DEFINE FIELD c ON t TYPE option<string> SQL TYPE POSTGRESQL 'char(3)';
DEFINE TABLE e TYPE RELATION SCHEMAFULL;
DEFINE FIELD s ON e TYPE option<string> SQL TYPE POSTGRESQL 'varchar(3)';

CREATE t:1 SET s = 'abcd';
CREATE t:1 CONTENT { s: 'abcd' };
CREATE t:1 SET s = 'abc', c = 'ab     ';
SELECT * FROM t:1;
UPDATE t:1 SET s = 'abcd';
UPDATE t:1 MERGE { s: 'abcd' };
UPDATE t:1 REPLACE { s: 'abcd' };
UPDATE t:1 PATCH [{ op: 'replace', path: '/s', value: 'abcd' }];
UPSERT t:2 SET s = 'abcd';
UPSERT t:1 SET s = 'abcd';
INSERT INTO t { id: t:3, s: 'abcd' };
INSERT INTO t [{ id: t:3, s: 'ab' }, { id: t:4, s: 'abcd' }];
INSERT INTO t { id: t:1, s: 'ab' } ON DUPLICATE KEY UPDATE s = 'abcd';
RELATE t:1->e:1->t:1 SET s = 'abcd';
INSERT RELATION INTO e { id: e:2, in: t:1, out: t:1, s: 'abcd' };
SELECT * FROM t ORDER BY id;
SELECT * FROM e;
