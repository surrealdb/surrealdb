/**
[test]
reason = "Keyword-prefixed date, time, timestamp and interval literals render back as the same literal, and d'..' stays a datetime"

[[test.results]]
value = "pd'2026-10-27'"

[[test.results]]
value = "pd'2026-10-27'"

[[test.results]]
value = "pt'12:30:00'"

[[test.results]]
value = "pt'12:30:00.123456789'"

[[test.results]]
value = "pt'24:00:00'"

[[test.results]]
value = "ts'2026-10-27T12:30:00'"

[[test.results]]
value = "ts'2026-10-27T12:30:00.500'"

[[test.results]]
value = "ivl'P1Y2M3DT4H5M6.5S'"

[[test.results]]
value = "ivl'P1Y2M3DT4H5M6.5S'"

[[test.results]]
value = "ivl'P-1D'"

[[test.results]]
value = "d'2026-10-27T00:00:00Z'"

[[test.results]]
value = "'datetime'"

[[test.results]]
value = "[pd'2026-10-27', pt'12:30:00', ts'2026-10-27T12:30:00', ivl'P1D']"

[[test.results]]
value = "{ a: pd'2026-10-27', b: ivl'P1M' }"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "'interval'"
*/
pd'2026-10-27';
pd"2026-10-27";
pt'12:30:00';
pt'12:30:00.123456789';
pt'24:00:00';
ts'2026-10-27T12:30:00';
ts'2026-10-27 12:30:00.5';
ivl'1 year 2 mons 3 days 04:05:06.5';
ivl'P1Y2M3DT4H5M6.5S';
ivl'-1 day';
d'2026-10-27';
type::of(d'2026-10-27');
[pd'2026-10-27', pt'12:30:00', ts'2026-10-27T12:30:00', ivl'1 day'];
{ a: pd'2026-10-27', b: ivl'1 month' };
pd'2026-10-27' = pd'2026-10-27';
ivl'1 mon' = ivl'30 days';
type::of(ivl'1 mon');
