/**
[test]
reason = """
Setup for 3.00.00_field_temporal_retype_timestamp.surql: the old binary writes
`datetime` and `option<datetime>` fields holding whole-second, fractional and
pre-epoch values, so the new binary can retype them to `timestamp`."""
run = false
*/
DEFINE TABLE rt SCHEMAFULL;
DEFINE FIELD f ON rt TYPE datetime;
DEFINE FIELD o ON rt TYPE option<datetime>;
CREATE rt:1 SET f = d'2026-10-27T12:00:00Z', o = d'2026-10-28T00:00:00Z';
CREATE rt:2 SET f = d'2026-10-27T12:00:00.123456Z';
CREATE rt:3 SET f = d'1969-12-31T23:59:59.5Z', o = d'1970-01-01T00:00:00Z';
CREATE rt:4 SET f = d'2026-10-27T00:00:00Z';
