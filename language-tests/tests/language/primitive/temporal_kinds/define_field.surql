/**
[test]
reason = "Each new kind as a field type, in option, array, set and union positions, and in function arguments and returns"

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
value = "[{ d: date'2026-10-27', def: date'2026-01-01', id: t:1, iv: {interval'P1D',}, ts: [timestamp'2026-10-27T12:00:00'], u: interval'P2D' }]"

[[test.results]]
value = "[{ d: date'2026-10-27', def: date'2026-01-01', id: t:1, iv: {interval'P1D',}, ts: [timestamp'2026-10-27T12:00:00'], u: interval'P2D' }]"

[[test.results]]
error = "Couldn't coerce value for field `u` of `t:2`: Expected `none | date | interval` but found `time'12:00:00'`"

[[test.results]]
value = "[{ d: date'2026-10-27', def: date'2026-01-01', id: t:3, ts: [timestamp'2026-10-27T00:00:00'] }]"

[[test.results]]
error = "Couldn't coerce value for field `d` of `t:4`: Expected `date (a datetime that is not midnight UTC needs the cast `<date>`)` but found `d'2026-10-27T12:00:00Z'`"

[[test.results]]
value = """{ events: {  }, fields: { d: 'DEFINE FIELD d ON t TYPE date PERMISSIONS FULL', def: "DEFINE FIELD def ON t TYPE date DEFAULT date'2026-01-01' PERMISSIONS FULL", iv: 'DEFINE FIELD iv ON t TYPE none | set<interval> PERMISSIONS FULL', "iv.*": 'DEFINE FIELD iv.* ON t TYPE interval PERMISSIONS FULL', tm: 'DEFINE FIELD tm ON t TYPE none | time PERMISSIONS FULL', ts: 'DEFINE FIELD ts ON t TYPE none | array<timestamp> PERMISSIONS FULL', "ts.*": 'DEFINE FIELD ts.* ON t TYPE timestamp PERMISSIONS FULL', u: 'DEFINE FIELD u ON t TYPE none | date | interval PERMISSIONS FULL' }, indexes: {  }, lives: {  }, migrations: { generation: 7, guaranteed: [], migrations: [] }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "date'2026-10-28'"

[[test.results]]
value = "date'2026-10-28'"

[[test.results]]
error = "Incorrect arguments for function fn::next(). Failed to coerce argument `$d`: Expected `date (a datetime that is not midnight UTC needs the cast `<date>`)` but found `d'2026-10-27T01:00:00Z'`"
*/
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD d ON t TYPE date;
DEFINE FIELD tm ON t TYPE option<time>;
DEFINE FIELD ts ON t TYPE option<array<timestamp>>;
DEFINE FIELD iv ON t TYPE option<set<interval>>;
DEFINE FIELD u ON t TYPE option<date | interval>;
DEFINE FIELD def ON t TYPE date DEFAULT date'2026-01-01';
CREATE t:1 SET d = date'2026-10-27', ts = [timestamp'2026-10-27T12:00:00'], iv = {interval'1 day', interval'24 hours'}, u = interval'2 days';
SELECT * FROM t:1;
CREATE t:2 SET d = date'2026-10-27', u = time'12:00:00';
CREATE t:3 SET d = date'2026-10-27', ts = [date'2026-10-27'];
CREATE t:4 SET d = d'2026-10-27T12:00:00Z';
INFO FOR TABLE t;
DEFINE FUNCTION fn::next($d: date) -> date { RETURN $d + 1; };
fn::next(date'2026-10-27');
fn::next(d'2026-10-27T00:00:00Z');
fn::next(d'2026-10-27T01:00:00Z');
