/**
[test]
reason = "An indexed date, time, timestamp or interval field is served by an index scan for equality, IN and ranges, with the rows a table scan returns"

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
value = "[]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: d_idx, access: = date'2000-02-29', direction: Forward]
"'''

[[test.results]]
value = "[t:5]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: tm_idx, access: = time'24:00:00', direction: Forward]
"'''

[[test.results]]
value = "[t:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: ts_idx, access: = timestamp'1969-12-31T23:59:59.500', direction: Forward]
"'''

[[test.results]]
value = "[t:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: iv_idx, access: = interval'P30D', direction: Forward]
"'''

[[test.results]]
value = "[t:1, t:4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: d INSIDE [date'2026-10-27', date'1970-01-01']]
        UnionIndexScan [ctx: Db] [table: t, branches: 2]
            IndexScan [ctx: Db] [table: t, index: d_idx, access: = date'2026-10-27', direction: Forward]
            IndexScan [ctx: Db] [table: t, index: d_idx, access: = date'1970-01-01', direction: Forward]
"'''

[[test.results]]
value = "[t:3, t:4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: iv INSIDE [interval'P29D', interval'P-1D']]
        UnionIndexScan [ctx: Db] [table: t, branches: 2]
            IndexScan [ctx: Db] [table: t, index: iv_idx, access: = interval'P29D', direction: Forward]
            IndexScan [ctx: Db] [table: t, index: iv_idx, access: = interval'P-1D', direction: Forward]
"'''

[[test.results]]
value = "[t:2, t:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: d_idx, access: >date'2000-02-29', direction: Forward]
"'''

[[test.results]]
value = "[t:3, t:1]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: d_idx, access: >=date'1970-01-01' <date'2026-10-28', direction: Forward]
"'''

[[test.results]]
value = "[t:4, t:5, t:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: tm_idx, access: <time'12:00:00', direction: Forward]
"'''

[[test.results]]
value = "[t:2, t:4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: tm_idx, access: >=time'12:00:00' <=time'24:00:00', direction: Forward]
"'''

[[test.results]]
value = "[t:1, t:5, t:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: ts_idx, access: <timestamp'1970-01-01T00:00:00', direction: Forward]
"'''

[[test.results]]
value = "[t:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: ts_idx, access: >timestamp'2000-02-29T12:00:00' <=timestamp'2026-10-27T00:00:00', direction: Forward]
"'''

[[test.results]]
value = "[t:5, t:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: iv_idx, access: >interval'P29D', direction: Forward]
"'''

[[test.results]]
value = "[t:1, t:4]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: iv_idx, access: >=interval'PT0S' <interval'P30D', direction: Forward]
"'''

[[test.results]]
value = "[t:5, t:3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: d_idx, direction: Forward]
''''

[[test.results]]
value = "[t:2, t:4, t:5, t:3, t:1]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: tm_idx, direction: Backward]
''''

[[test.results]]
value = "[t:3, t:5, t:1, t:4, t:2]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: ts_idx, direction: Forward]
''''

[[test.results]]
value = "[t:2, t:4, t:5, t:3, t:1]"

[[test.results]]
value = "[t:2, t:5, t:3, t:1, t:4]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[interval'P1M', interval'P30D']"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1, t:2, t:3, t:4, t:5]"

[[test.results]]
value = "[u:1, u:2, u:3, u:4, u:5]"

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
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: d_idx, access: >d'2026-10-28T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: t, index: ts_idx, access: = d'2026-10-28T00:00:00Z', direction: Forward]
"'''

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
value = "[]"

[[test.results]]
error = "Database index `d_u` already contains date'2026-10-28', with record `k:1`"

[[test.results]]
error = "Database index `tm_u` already contains time'12:00:00', with record `k:1`"

[[test.results]]
error = "Database index `ts_u` already contains timestamp'2026-10-28T00:00:00', with record `k:1`"

[[test.results]]
error = "Database index `iv_u` already contains interval'P30D', with record `k:1`"

[[test.results]]
value = "[k:1]"

[[test.results]]
value = "[k:1]"
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
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD d ON u TYPE date;
DEFINE FIELD tm ON u TYPE time;
DEFINE FIELD ts ON u TYPE timestamp;
DEFINE FIELD iv ON u TYPE interval;
CREATE t:1 SET d = date'2026-10-28', tm = time'12:00:00', ts = timestamp'2026-10-28T00:00:00', iv = interval'1 mon' RETURN NONE;
CREATE t:2 SET d = date'-0001-01-01', tm = time'00:00:00', ts = timestamp'1969-12-31T23:59:59.5', iv = interval'-1 day' RETURN NONE;
CREATE t:3 SET d = date'2026-10-27', tm = time'24:00:00', ts = timestamp'2026-10-27T00:00:00', iv = interval'29 days' RETURN NONE;
CREATE t:4 SET d = date'1970-01-01', tm = time'06:30:00', ts = timestamp'1970-01-01T00:00:00', iv = interval'30 days' RETURN NONE;
CREATE t:5 SET d = date'2000-02-29', tm = time'23:59:59.999999999', ts = timestamp'2000-02-29T12:00:00.000000001', iv = interval'0 seconds' RETURN NONE;
INSERT INTO u (SELECT * FROM t) RETURN NONE;
-- Equality.
EXPLAIN SELECT VALUE id FROM t WHERE d = date'2000-02-29';
SELECT VALUE id FROM t WHERE d = date'2000-02-29';
EXPLAIN SELECT VALUE id FROM t WHERE tm = time'24:00:00';
SELECT VALUE id FROM t WHERE tm = time'24:00:00';
EXPLAIN SELECT VALUE id FROM t WHERE ts = timestamp'1969-12-31T23:59:59.5';
SELECT VALUE id FROM t WHERE ts = timestamp'1969-12-31T23:59:59.5';
EXPLAIN SELECT VALUE id FROM t WHERE iv = interval'30 days';
SELECT VALUE id FROM t WHERE iv = interval'1 mon' ORDER BY id;
-- IN.
EXPLAIN SELECT VALUE id FROM t WHERE d IN [date'2026-10-27', date'1970-01-01'];
SELECT VALUE id FROM t WHERE d IN [date'2026-10-27', date'1970-01-01'] ORDER BY id;
EXPLAIN SELECT VALUE id FROM t WHERE iv IN [interval'29 days', interval'-1 day'];
SELECT VALUE id FROM t WHERE iv IN [interval'29 days', interval'-1 day'] ORDER BY id;
-- Ranges.
EXPLAIN SELECT VALUE id FROM t WHERE d > date'2000-02-29';
SELECT VALUE id FROM t WHERE d > date'2000-02-29' ORDER BY d;
EXPLAIN SELECT VALUE id FROM t WHERE d >= date'1970-01-01' AND d < date'2026-10-28';
SELECT VALUE id FROM t WHERE d >= date'1970-01-01' AND d < date'2026-10-28' ORDER BY d;
EXPLAIN SELECT VALUE id FROM t WHERE tm < time'12:00:00';
SELECT VALUE id FROM t WHERE tm < time'12:00:00' ORDER BY tm;
EXPLAIN SELECT VALUE id FROM t WHERE tm >= time'12:00:00' AND tm <= time'24:00:00';
SELECT VALUE id FROM t WHERE tm >= time'12:00:00' AND tm <= time'24:00:00' ORDER BY tm;
EXPLAIN SELECT VALUE id FROM t WHERE ts < timestamp'1970-01-01T00:00:00';
SELECT VALUE id FROM t WHERE ts < timestamp'1970-01-01T00:00:00' ORDER BY ts;
EXPLAIN SELECT VALUE id FROM t WHERE ts > timestamp'2000-02-29T12:00:00' AND ts <= timestamp'2026-10-27T00:00:00';
SELECT VALUE id FROM t WHERE ts > timestamp'2000-02-29T12:00:00' AND ts <= timestamp'2026-10-27T00:00:00' ORDER BY ts;
EXPLAIN SELECT VALUE id FROM t WHERE iv > interval'29 days';
SELECT VALUE id FROM t WHERE iv > interval'29 days' ORDER BY iv, id;
EXPLAIN SELECT VALUE id FROM t WHERE iv >= interval'0 seconds' AND iv < interval'30 days';
SELECT VALUE id FROM t WHERE iv >= interval'0 seconds' AND iv < interval'30 days' ORDER BY iv, id;
-- ORDER BY.
EXPLAIN SELECT VALUE id FROM t ORDER BY d;
SELECT VALUE id FROM t ORDER BY d;
EXPLAIN SELECT VALUE id FROM t ORDER BY tm DESC;
SELECT VALUE id FROM t ORDER BY tm DESC;
EXPLAIN SELECT VALUE id FROM t ORDER BY ts;
SELECT VALUE id FROM t ORDER BY ts;
SELECT VALUE id FROM t ORDER BY iv, id;
-- Counts.
SELECT count() FROM t WHERE d > date'1970-01-01' GROUP ALL;
SELECT count() FROM t WHERE iv = interval'720 hours' GROUP ALL;
-- The row is read: an interval keeps its written triple.
SELECT VALUE iv FROM t WHERE iv = interval'30 days' ORDER BY id;
-- The same predicates over a table with no index return the same rows.
(SELECT VALUE record::id(id) FROM u WHERE iv >= interval'0 seconds' AND iv < interval'30 days' ORDER BY iv, id) = (SELECT VALUE record::id(id) FROM t WHERE iv >= interval'0 seconds' AND iv < interval'30 days' ORDER BY iv, id);
(SELECT VALUE record::id(id) FROM u WHERE ts > timestamp'2000-02-29T12:00:00' ORDER BY ts) = (SELECT VALUE record::id(id) FROM t WHERE ts > timestamp'2000-02-29T12:00:00' ORDER BY ts);
-- A literal of another kind than the column compares by kind, as on a table scan.
SELECT VALUE id FROM t WHERE ts = d'2026-10-28T00:00:00Z';
SELECT VALUE id FROM u WHERE ts = d'2026-10-28T00:00:00Z';
SELECT VALUE id FROM t WHERE d > d'2026-10-28T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM u WHERE d > d'2026-10-28T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM t WHERE ts < d'2026-10-28T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM u WHERE ts < d'2026-10-28T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM t WHERE iv = 30d;
SELECT VALUE id FROM u WHERE iv = 30d;
SELECT VALUE id FROM t WHERE tm = 12h;
SELECT VALUE id FROM u WHERE tm = 12h;
EXPLAIN SELECT VALUE id FROM t WHERE d > d'2026-10-28T00:00:00Z';
EXPLAIN SELECT VALUE id FROM t WHERE ts = d'2026-10-28T00:00:00Z';
-- UNIQUE indexes enforce uniqueness per kind, and an equality reads through them.
DEFINE TABLE k SCHEMAFULL;
DEFINE FIELD d ON k TYPE date;
DEFINE FIELD tm ON k TYPE time;
DEFINE FIELD ts ON k TYPE timestamp;
DEFINE FIELD iv ON k TYPE interval;
DEFINE INDEX d_u ON k FIELDS d UNIQUE;
DEFINE INDEX tm_u ON k FIELDS tm UNIQUE;
DEFINE INDEX ts_u ON k FIELDS ts UNIQUE;
DEFINE INDEX iv_u ON k FIELDS iv UNIQUE;
CREATE k:1 SET d = date'2026-10-28', tm = time'12:00:00', ts = timestamp'2026-10-28T00:00:00', iv = interval'1 mon' RETURN NONE;
CREATE k:2 SET d = date'2026-10-28', tm = time'01:00:00', ts = timestamp'2026-10-27T00:00:00', iv = interval'1 day';
CREATE k:3 SET d = date'2026-10-27', tm = time'12:00:00', ts = timestamp'2026-10-27T00:00:00', iv = interval'1 day';
CREATE k:4 SET d = date'2026-10-27', tm = time'01:00:00', ts = timestamp'2026-10-28T00:00:00', iv = interval'1 day';
CREATE k:5 SET d = date'2026-10-27', tm = time'01:00:00', ts = timestamp'2026-10-27T00:00:00', iv = interval'30 days';
SELECT VALUE id FROM k WHERE iv = interval'30 days';
SELECT VALUE id FROM k WHERE ts = timestamp'2026-10-28T00:00:00';
