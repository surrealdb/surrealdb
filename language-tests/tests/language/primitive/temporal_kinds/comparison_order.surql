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
value = "[pd'-0001-01-01', pd'2026-10-27', pd'2026-10-28']"

[[test.results]]
value = "[pt'00:00:00', pt'12:00:00', pt'24:00:00']"

[[test.results]]
value = "[ts'1969-12-31T23:59:59.500', ts'2026-10-27T00:00:00', ts'2026-10-28T00:00:00']"

[[test.results]]
value = "[ivl'P-1D', ivl'PT1H', ivl'P29D', ivl'P1M']"

[[test.results]]
value = "[ivl'P1M']"

[[test.results]]
value = "[ivl'P1M']"

[[test.results]]
value = "[d'2026-10-27T00:00:00Z', pd'2026-10-27', pt'01:00:00', ts'2026-10-27T00:00:00', ivl'P1D']"
*/
pd'2026-10-27' < pd'2026-10-28';
pd'2026-10-27' = pd'2026-10-27';
pd'2026-10-27' = d'2026-10-27';
pt'01:00:00' < pt'02:00:00';
pt'24:00:00' > pt'23:59:59.999999999';
ts'2026-10-27T00:00:00' < ts'2026-10-27T00:00:00.000000001';
ivl'1 mon' = ivl'30 days';
ivl'1 day' = ivl'24 hours';
ivl'1 mon' > ivl'29 days';
ivl'-1 day' < ivl'00:00:00';
array::sort([pd'2026-10-28', pd'-0001-01-01', pd'2026-10-27']);
array::sort([pt'12:00:00', pt'00:00:00', pt'24:00:00']);
array::sort([ts'2026-10-28T00:00:00', ts'1969-12-31T23:59:59.5', ts'2026-10-27T00:00:00']);
array::sort([ivl'1 mon', ivl'-1 day', ivl'29 days', ivl'1 hour']);
array::sort([ivl'1 mon', ivl'30 days'].distinct());
array::distinct([ivl'1 mon', ivl'30 days']);
array::sort([pd'2026-10-27', d'2026-10-27', ivl'1 day', pt'01:00:00', ts'2026-10-27T00:00:00']);
