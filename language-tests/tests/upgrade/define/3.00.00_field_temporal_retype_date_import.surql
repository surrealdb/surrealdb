/**
[test]
reason = """
Setup for 3.00.00_field_temporal_retype_date.surql: the old binary writes
`datetime` and `option<datetime>` fields holding values at exactly midnight UTC
and values that are not, so the new binary can retype them to `date`."""
run = false
*/
DEFINE TABLE rd SCHEMAFULL;
DEFINE FIELD f ON rd TYPE datetime;
DEFINE FIELD o ON rd TYPE option<datetime>;
CREATE rd:1 SET f = d'2026-10-27T00:00:00Z', o = d'2026-10-28T00:00:00Z';
CREATE rd:2 SET f = d'2026-10-28T00:00:00Z';
CREATE rd:3 SET f = d'2026-10-28T12:30:00Z', o = d'2026-10-29T00:00:00Z';
CREATE rd:4 SET f = d'2026-10-29T00:00:00.5Z';
