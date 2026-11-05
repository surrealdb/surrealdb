/**
[test]
requires = ["changefeed"]
reason = "A changefeed records, for a write to a row behind a migration step, the migrated values: the row it stores, and with INCLUDE ORIGINAL the original it replaces, are what a read of the row returned before the write. An entry written before the step keeps the values that were written then."

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
match = '''$result[1..].map(|$r| $r.changes) == [[{ update: { a: '5', id: cf:1, o: { n: '1' } } }], [{ update: { a: 'x', id: cf:2 } }], [{ update: { a: '9', id: cf:3 } }], [{ update: { a: 6, id: cf:1, o: { n: 1 } } }], [{ delete: { id: cf:2 } }], [{ update: { a: 9, id: cf:3, o: { n: 7 } } }], [{ update: { a: 4, id: cf:4 } }]] && type::is_string($result[3].changes[0].update.a) && type::is_int($result[6].changes[0].update.a) && type::is_int($result[6].changes[0].update.o.n)'''

[[test.results]]
match = '''$result[1..].map(|$r| $r.changes) == [[{ update: { a: '5', id: cfo:1 } }], [{ update: { a: 'x', id: cfo:2 } }], [{ current: { a: 6, id: cfo:1 }, update: [{ op: 'replace', path: '/a', value: 5 }] }], [{ delete: { id: cfo:2, original: { a: -1, id: cfo:2 } } }], [{ current: { a: 7, id: cfo:1 }, update: [{ op: 'replace', path: '/a', value: 6 }] }]] && type::is_int($result[3].changes[0].update[0].value) && type::is_int($result[4].changes[0].delete.original.a) && type::is_int($result[5].changes[0].update[0].value) && type::is_int($result[3].changes[0].current.a)'''

[[test.results]]
value = "[{ a: 6, id: cf:1, o: { n: 1 } }, { a: 9, id: cf:3, o: { n: 7 } }, { a: 4, id: cf:4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'lvl3'"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
match = '''$result[1..].map(|$r| $r.changes) == [[{ update: { a: '5', id: cf:1 } }], [{ update: { a: 'x', id: cf:2 } }], [{ current: { a: 6, id: cf:1 }, update: [{ op: 'replace', path: '/a', value: 5 }] }], [{ delete: { id: cf:2, original: { a: -1, id: cf:2 } } }]] && type::is_int($result[3].changes[0].update[0].value) && type::is_int($result[4].changes[0].delete.original.a)'''
*/
DEFINE TABLE cf SCHEMAFULL CHANGEFEED 1h;
DEFINE FIELD a ON cf TYPE string;
DEFINE FIELD o ON cf TYPE option<object>;
DEFINE FIELD o.n ON cf TYPE string;
DEFINE TABLE cfo SCHEMAFULL CHANGEFEED 1h INCLUDE ORIGINAL;
DEFINE FIELD a ON cfo TYPE string;
CREATE cf:1 SET a = '5', o = { n: '1' } RETURN NONE;
CREATE cf:2 SET a = 'x' RETURN NONE;
CREATE cf:3 SET a = '9' RETURN NONE;
CREATE cfo:1 SET a = '5' RETURN NONE;
CREATE cfo:2 SET a = 'x' RETURN NONE;
ALTER FIELD a ON cf TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER FIELD o.n ON cf TYPE int MIGRATE EXISTING ROWS AS on_int USING <int>$value FALLBACK -1;
ALTER FIELD a ON cfo TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
UPDATE cf:1 SET a = 6 RETURN NONE;
DELETE cf:2;
UPDATE cf:3 SET o = { n: 7 } RETURN NONE;
CREATE cf:4 SET a = 4 RETURN NONE;
UPDATE cfo:1 SET a = 6 RETURN NONE;
DELETE cfo:2;
UPSERT cfo:1 SET a = 7 RETURN NONE;
SHOW CHANGES FOR TABLE cf SINCE 0 LIMIT 100;
SHOW CHANGES FOR TABLE cfo SINCE 0 LIMIT 100;
-- Reads agree with what the entries recorded after the step.
SELECT * FROM cf ORDER BY id;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE cf SCHEMAFULL CHANGEFEED 1h INCLUDE ORIGINAL;
DEFINE FIELD a ON cf TYPE string;
CREATE cf:1 SET a = '5' RETURN NONE;
CREATE cf:2 SET a = 'x' RETURN NONE;
ALTER FIELD a ON cf TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
UPDATE cf:1 SET a = 6 RETURN NONE;
DELETE cf:2;
SHOW CHANGES FOR TABLE cf SINCE 0 LIMIT 100;
