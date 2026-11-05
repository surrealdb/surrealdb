/**
[test]
reason = "Equality and ORDER BY over indexed date, time, timestamp and interval fields return the same rows as a scan"

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
value = "[{ d: pd'2026-10-28', id: t:1, iv: ivl'P1M', tm: pt'12:00:00', ts: ts'2026-10-28T00:00:00' }]"

[[test.results]]
value = "[{ d: pd'-0001-01-01', id: t:2, iv: ivl'P-1D', tm: pt'00:00:00', ts: ts'1969-12-31T23:59:59.500' }]"

[[test.results]]
value = "[{ d: pd'2026-10-27', id: t:3, iv: ivl'P29D', tm: pt'24:00:00', ts: ts'2026-10-27T00:00:00' }]"

[[test.results]]
value = "[t:2, t:3, t:1]"

[[test.results]]
value = "[t:2, t:1, t:3]"

[[test.results]]
value = "[t:1, t:3, t:2]"

[[test.results]]
value = "[t:2, t:3, t:1]"

[[test.results]]
value = "[t:3]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database index `iv_u` already contains ivl'P1M', with record `t:1`"

[[test.results]]
error = "Database index `iv_u` already contains ivl'P30D', with record `t:1`"
*/
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD d ON t TYPE date;
DEFINE FIELD tm ON t TYPE time;
DEFINE FIELD ts ON t TYPE timestamp;
DEFINE FIELD iv ON t TYPE interval;
DEFINE INDEX d_idx ON t FIELDS d;
DEFINE INDEX tm_idx ON t FIELDS tm;
DEFINE INDEX ts_idx ON t FIELDS ts;
DEFINE INDEX iv_idx ON t FIELDS iv;
CREATE t:1 SET d = pd'2026-10-28', tm = pt'12:00:00', ts = ts'2026-10-28T00:00:00', iv = ivl'1 mon';
CREATE t:2 SET d = pd'-0001-01-01', tm = pt'00:00:00', ts = ts'1969-12-31T23:59:59.5', iv = ivl'-1 day';
CREATE t:3 SET d = pd'2026-10-27', tm = pt'24:00:00', ts = ts'2026-10-27T00:00:00', iv = ivl'29 days';
SELECT VALUE id FROM t ORDER BY d;
SELECT VALUE id FROM t ORDER BY tm;
SELECT VALUE id FROM t ORDER BY ts DESC;
SELECT VALUE id FROM t ORDER BY iv;
SELECT VALUE id FROM t WHERE d = pd'2026-10-27';
SELECT VALUE id FROM t WHERE tm = pt'12:00:00';
SELECT VALUE id FROM t WHERE ts = ts'1969-12-31T23:59:59.5';
SELECT VALUE id FROM t WHERE iv = ivl'1 mon';
SELECT VALUE id FROM t WHERE iv = ivl'30 days';
SELECT VALUE id FROM t WHERE d > pd'2026-10-27';
DEFINE INDEX iv_u ON t FIELDS iv UNIQUE;
CREATE t:4 SET d = pd'2026-10-27', tm = pt'01:00:00', ts = ts'2026-10-27T00:00:00', iv = ivl'1 mon';
CREATE t:5 SET d = pd'2026-10-27', tm = pt'01:00:00', ts = ts'2026-10-27T00:00:00', iv = ivl'30 days';
