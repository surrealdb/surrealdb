/**
[env]
namespace = true
database = true

[test]
reason = "A field of kind bigdecimal coerces every finite number to it and refuses anything else; the definition's stored text round-trips"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:int, o: 1.50bigdec, v: 1bigdec }]"

[[test.results]]
value = "[{ id: t:dec, o: 1.50bigdec, v: 1.25bigdec }]"

[[test.results]]
value = "[{ id: t:float, o: 1.50bigdec, v: 0.1bigdec }]"

[[test.results]]
value = "[{ id: t:big, o: 1.50bigdec, s: {1bigdec, 2bigdec}, v: 12345678901234567890123456789012345.678901234567890123456789012345bigdec }]"

[[test.results]]
error = "Couldn't coerce value for field `v` of `t:text`: Expected `bigdecimal` but found `'1.5'`"

[[test.results]]
error = "Couldn't coerce value for field `v` of `t:nan`: Expected `bigdecimal` but found `NaN`"

[[test.results]]
value = "[{ id: t:big, kind: 'bigdecimal', o: 1.50bigdec, v: 12345678901234567890123456789012345.678901234567890123456789012345bigdec }, { id: t:dec, kind: 'bigdecimal', o: 1.50bigdec, v: 1.25bigdec }, { id: t:float, kind: 'bigdecimal', o: 1.50bigdec, v: 0.1bigdec }, { id: t:int, kind: 'bigdecimal', o: 1.50bigdec, v: 1bigdec }]"

[[test.results]]
value = "[{1bigdec, 2bigdec}]"

[[test.results]]
value = """{ events: {  }, fields: { o: 'DEFINE FIELD o ON t TYPE none | bigdecimal DEFAULT 1.50bigdec PERMISSIONS FULL', s: 'DEFINE FIELD s ON t TYPE none | set<bigdecimal> PERMISSIONS FULL', "s.*": 'DEFINE FIELD s.* ON t TYPE bigdecimal PERMISSIONS FULL', v: 'DEFINE FIELD v ON t TYPE bigdecimal PERMISSIONS FULL' }, indexes: {  }, lives: {  }, migrations: { generation: 4, guaranteed: [], migrations: [] }, tables: {  } }"""

*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD v ON t TYPE bigdecimal;
DEFINE FIELD o ON t TYPE option<bigdecimal> DEFAULT 1.50bigdec;
DEFINE FIELD s ON t TYPE option<set<bigdecimal>>;

CREATE t:int SET v = 1;
CREATE t:dec SET v = 1.25dec;
CREATE t:float SET v = 0.1f;
CREATE t:big SET v = 12345678901234567890123456789012345.678901234567890123456789012345bigdec, o = NONE, s = [1bigdec, 1.0bigdec, 2];
CREATE t:text SET v = "1.5";
CREATE t:nan SET v = NaN;

SELECT id, v, type::of(v) AS kind, o FROM t ORDER BY id;
SELECT VALUE s FROM t:big;

-- The stored text of the definitions names the kind and spells the default as written.
INFO FOR TABLE t;
