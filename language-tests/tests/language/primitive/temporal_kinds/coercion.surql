/**
[test]
reason = "Coercion into typed fields and function arguments widens losslessly, narrows only exactly, and never takes a string"

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
value = "[{ d: pd'2026-10-27', k: 'date' }]"

[[test.results]]
value = "[{ d: pd'2026-10-27', k: 'date' }]"

[[test.results]]
value = "[{ k: 'timestamp', ts: ts'2026-10-27T00:00:00' }]"

[[test.results]]
value = "[{ k: 'timestamp', ts: ts'2026-10-27T12:30:45' }]"

[[test.results]]
value = "[{ dt: d'2026-10-27T00:00:00Z', k: 'datetime' }]"

[[test.results]]
value = "[{ dt: d'2026-10-27T12:30:45Z', k: 'datetime' }]"

[[test.results]]
error = "Couldn't coerce value for field `d` of `t:7`: Expected `none | date` but found `d'2026-10-27T12:30:45Z'`"

[[test.results]]
error = "Couldn't coerce value for field `d` of `t:8`: Expected `none | date` but found `'2026-10-27'`"

[[test.results]]
error = "Couldn't coerce value for field `ts` of `t:9`: Expected `none | timestamp` but found `'2026-10-27T12:30:45'`"

[[test.results]]
error = "Couldn't coerce value for field `tm` of `t:10`: Expected `none | time` but found `d'2026-10-27T12:30:45Z'`"

[[test.results]]
error = "Couldn't coerce value for field `iv` of `t:11`: Expected `none | interval` but found `1h`"

[[test.results]]
error = "Couldn't coerce value for field `d` of `t:12`: Expected `none | date` but found `ts'2026-10-27T00:00:00'`"

[[test.results]]
value = "[{ iv: ivl'P1D', tm: pt'12:30:00' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "pd'2026-10-27'"

[[test.results]]
value = "pd'2026-10-27'"

[[test.results]]
error = "Incorrect arguments for function fn::as_date(). Failed to coerce argument `$d`: Expected `date (a datetime that is not midnight UTC needs the cast `<date>`)` but found `d'2026-10-27T01:00:00Z'`"

[[test.results]]
value = "ts'2026-10-27T00:00:00'"

[[test.results]]
value = "ts'2026-10-27T01:00:00'"

[[test.results]]
error = "Incorrect arguments for function fn::as_timestamp(). Failed to coerce argument `$d`: Expected `timestamp` but found `'2026-10-27'`"
*/
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD d ON t TYPE option<date>;
DEFINE FIELD ts ON t TYPE option<timestamp>;
DEFINE FIELD dt ON t TYPE option<datetime>;
DEFINE FIELD tm ON t TYPE option<time>;
DEFINE FIELD iv ON t TYPE option<interval>;
CREATE t:1 SET d = d'2026-10-27T00:00:00Z' RETURN d, type::of(d) AS k;
CREATE t:2 SET d = pd'2026-10-27' RETURN d, type::of(d) AS k;
CREATE t:3 SET ts = pd'2026-10-27' RETURN ts, type::of(ts) AS k;
CREATE t:4 SET ts = d'2026-10-27T12:30:45Z' RETURN ts, type::of(ts) AS k;
CREATE t:5 SET dt = pd'2026-10-27' RETURN dt, type::of(dt) AS k;
CREATE t:6 SET dt = ts'2026-10-27T12:30:45' RETURN dt, type::of(dt) AS k;
CREATE t:7 SET d = d'2026-10-27T12:30:45Z';
CREATE t:8 SET d = '2026-10-27';
CREATE t:9 SET ts = '2026-10-27T12:30:45';
CREATE t:10 SET tm = d'2026-10-27T12:30:45Z';
CREATE t:11 SET iv = 1h;
CREATE t:12 SET d = ts'2026-10-27T00:00:00';
CREATE t:13 SET tm = pt'12:30:00', iv = ivl'1 day' RETURN tm, iv;
DEFINE FUNCTION fn::as_date($d: date) { RETURN $d; };
DEFINE FUNCTION fn::as_timestamp($d: timestamp) { RETURN $d; };
fn::as_date(pd'2026-10-27');
fn::as_date(d'2026-10-27T00:00:00Z');
fn::as_date(d'2026-10-27T01:00:00Z');
fn::as_timestamp(pd'2026-10-27');
fn::as_timestamp(d'2026-10-27T01:00:00Z');
fn::as_timestamp('2026-10-27');
