/**
[test]
reason = "Within a kind comparison is chronological, interval comparison is normalised, and kinds order by rank across kinds"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[date'-0001-01-01', date'2026-10-27', date'2026-10-28']"

[[test.results]]
value = "[time'00:00:00', time'12:00:00', time'24:00:00']"

[[test.results]]
value = "[timestamp'1969-12-31T23:59:59.500', timestamp'2026-10-27T00:00:00', timestamp'2026-10-28T00:00:00']"

[[test.results]]
value = "[interval'P-1D', interval'PT1H', interval'P29D', interval'P1M']"

[[test.results]]
value = "[interval'P1M']"

[[test.results]]
value = "[interval'P1M']"

[[test.results]]
value = "[d'2026-10-27T00:00:00Z', date'2026-10-27', time'01:00:00', timestamp'2026-10-27T00:00:00', interval'P1D']"
*/
date'2026-10-27' < date'2026-10-28';
date'2026-10-27' = date'2026-10-27';
date'2026-10-27' = d'2026-10-27';
time'01:00:00' < time'02:00:00';
time'24:00:00' > time'23:59:59.999999999';
timestamp'2026-10-27T00:00:00' < timestamp'2026-10-27T00:00:00.000000001';
interval'1 mon' = interval'30 days';
interval'1 day' = interval'24 hours';
interval'1 mon' > interval'29 days';
interval'-1 day' < interval'00:00:00';
array::sort([date'2026-10-28', date'-0001-01-01', date'2026-10-27']);
array::sort([time'12:00:00', time'00:00:00', time'24:00:00']);
array::sort([timestamp'2026-10-28T00:00:00', timestamp'1969-12-31T23:59:59.5', timestamp'2026-10-27T00:00:00']);
array::sort([interval'1 mon', interval'-1 day', interval'29 days', interval'1 hour']);
array::sort([interval'1 mon', interval'30 days'].distinct());
array::distinct([interval'1 mon', interval'30 days']);
array::sort([date'2026-10-27', d'2026-10-27', interval'1 day', time'01:00:00', timestamp'2026-10-27T00:00:00']);
