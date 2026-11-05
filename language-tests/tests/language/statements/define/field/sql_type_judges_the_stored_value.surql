/**
[test]
reason = "A field's declared SQL type judges the value the field stores, after its VALUE clause: a written text longer than varchar(3) is accepted when VALUE shortens it, and a short text VALUE lengthens is refused."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, v: 'abc' }]"

[[test.results]]
error = "value too long for type character varying(3) (field `w` of `t:2`, SQL TYPE varchar(3))"

[[test.results]]
value = "[{ id: t:1, v: 'abc' }]"
*/

DEFINE FIELD v ON t TYPE string VALUE string::slice($value, 0, 3) SQL TYPE POSTGRESQL 'varchar(3)';
DEFINE FIELD w ON t TYPE option<string> VALUE IF $value THEN $value + 'xyz' END SQL TYPE POSTGRESQL 'varchar(3)';
CREATE t:1 SET v = 'abcdef';
CREATE t:2 SET v = 'a', w = 'ab';
SELECT * FROM t ORDER BY id;
