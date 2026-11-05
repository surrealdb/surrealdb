-- String fields retyped to datetime, duration and uuid through fallible casts,
-- indexed on the new type, then retyped back to string. A cast fails on some
-- rows and the FALLBACK must be a value of the new type; a datetime, duration or
-- uuid cast back to string is not proven total and needs a FALLBACK too. Probes
-- what the step writes into the NULL and absent rows, and that index ranges over
-- datetime and duration agree with a scan. Indexes are removed before the
-- migrations that read them.
DEFINE TABLE ev SCHEMAFULL;
DEFINE FIELD ts ON ev TYPE any;
DEFINE FIELD span ON ev TYPE any;
DEFINE FIELD ref ON ev TYPE any;
DEFINE FIELD label ON ev TYPE string;
CREATE ev:1 SET ts = '2024-01-02T03:04:05Z', span = '1h', ref = '3f2b4c1e-0000-4000-8000-000000000001', label = 'a';
CREATE ev:2 SET ts = 'not a date', span = '1d2h', ref = 'nope', label = 'b';
CREATE ev:3 SET ts = '2024-02-30T00:00:00Z', span = 'x', ref = '3F2B4C1E-0000-4000-8000-000000000003', label = 'c';
CREATE ev:4 SET ts = '2024-01-02', span = '', ref = NULL, label = 'd';
CREATE ev:5 SET ts = NULL, span = '30s', ref = '3f2b4c1e-0000-4000-8000-000000000005', label = 'e';
CREATE ev:6 SET ts = '2024-01-02T03:04:05+02:00', span = '1.5h', ref = 'urn:uuid:3f2b4c1e-0000-4000-8000-000000000006', label = 'f';
CREATE ev:7 SET span = '2w', label = 'g';
CREATE ev:8 SET ts = '1999-12-31T23:59:59Z', span = '5m', ref = 7, label = 'h';
SELECT VALUE ts FROM ev ORDER BY id;
SELECT VALUE span FROM ev ORDER BY id;
SELECT VALUE ref FROM ev ORDER BY id;
-- @upgrade
ALTER FIELD ts ON ev TYPE datetime MIGRATE EXISTING ROWS AS ts_dt USING <datetime>$value;
ALTER FIELD ts ON ev TYPE datetime MIGRATE EXISTING ROWS AS ts_dt USING <datetime>$value FALLBACK d"1970-01-01T00:00:00Z";
ALTER FIELD span ON ev TYPE duration MIGRATE EXISTING ROWS AS span_dur USING <duration>$value FALLBACK 1s;
ALTER FIELD ref ON ev TYPE uuid MIGRATE EXISTING ROWS AS ref_uuid USING <uuid>$value FALLBACK u"00000000-0000-4000-8000-000000000000";
SELECT VALUE ts FROM ev ORDER BY id;
SELECT VALUE span FROM ev ORDER BY id;
SELECT VALUE ref FROM ev ORDER BY id;
SELECT VALUE ts FROM ONLY ev:5;
SELECT VALUE ts FROM ONLY ev:7;
(INFO FOR TABLE ev).migrations.guaranteed;
SELECT VALUE ts = d"1970-01-01T00:00:00Z" FROM ev ORDER BY id;
SELECT VALUE ts = d"2024-01-02T01:04:05Z" FROM ev ORDER BY id;
SELECT VALUE ref = u"00000000-0000-4000-8000-000000000000" FROM ev ORDER BY id;
SELECT VALUE span = 1s FROM ev ORDER BY id;
DEFINE INDEX by_ts ON ev FIELDS ts;
DEFINE INDEX by_span ON ev FIELDS span;
-- @check
SELECT VALUE label FROM ev WHERE ts > d"2024-01-01T00:00:00Z" ORDER BY id;
SELECT VALUE label FROM ev WHERE ts = d"1970-01-01T00:00:00Z" ORDER BY id;
SELECT VALUE label FROM ev WHERE span >= 1h ORDER BY id;
SELECT VALUE label FROM ev WHERE span < 1h ORDER BY id;
-- @drain
SELECT VALUE ts FROM ev ORDER BY id;
(INFO FOR TABLE ev).migrations.generation;
-- Rows written after the change hold the new types.
CREATE ev:9 SET ts = d"2025-06-01T00:00:00Z", span = 3h, ref = u"3f2b4c1e-0000-4000-8000-000000000009", label = 'i';
CREATE ev:10 SET ts = '2025-06-01T00:00:00Z', span = 3h, ref = u"3f2b4c1e-0000-4000-8000-000000000010", label = 'j';
SELECT VALUE ts FROM ev ORDER BY id;
-- @check
-- The same fields back to string. The indexes on ts and span are removed first;
-- each cast takes a FALLBACK of its own.
REMOVE INDEX by_ts ON ev;
REMOVE INDEX by_span ON ev;
ALTER FIELD ts ON ev TYPE string MIGRATE EXISTING ROWS AS ts_str USING <string>$value;
ALTER FIELD ts ON ev TYPE string MIGRATE EXISTING ROWS AS ts_str USING <string>$value FALLBACK 'none';
ALTER FIELD span ON ev TYPE string MIGRATE EXISTING ROWS AS span_str USING <string>$value FALLBACK 'x';
ALTER FIELD ref ON ev TYPE string MIGRATE EXISTING ROWS AS ref_str USING <string>$value FALLBACK 'y';
SELECT VALUE ts FROM ev ORDER BY id;
SELECT VALUE span FROM ev ORDER BY id;
SELECT VALUE ref FROM ev ORDER BY id;
(INFO FOR TABLE ev).migrations.generation;
-- @drain
SELECT VALUE ts FROM ev ORDER BY id;
-- A datetime is refused in a field that is now a string.
CREATE ev:11 SET ts = d"2025-06-01T00:00:00Z", span = '1h', ref = 'z', label = 'k';
SELECT VALUE ts FROM ev ORDER BY id;
-- @check
