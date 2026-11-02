/**
[test]
reason = "Keyword-prefixed date, time, timestamp and interval literals render back as the same literal, and d'..' stays a datetime"

[[test.results]]
value = "date'2026-10-27'"

[[test.results]]
value = "date'2026-10-27'"

[[test.results]]
value = "time'12:30:00'"

[[test.results]]
value = "time'12:30:00.123456789'"

[[test.results]]
value = "time'24:00:00'"

[[test.results]]
value = "timestamp'2026-10-27T12:30:00'"

[[test.results]]
value = "timestamp'2026-10-27T12:30:00.500'"

[[test.results]]
value = "interval'P1Y2M3DT4H5M6.5S'"

[[test.results]]
value = "interval'P1Y2M3DT4H5M6.5S'"

[[test.results]]
value = "interval'P-1D'"

[[test.results]]
value = "d'2026-10-27T00:00:00Z'"

[[test.results]]
value = "'datetime'"

[[test.results]]
value = "[date'2026-10-27', time'12:30:00', timestamp'2026-10-27T12:30:00', interval'P1D']"

[[test.results]]
value = "{ a: date'2026-10-27', b: interval'P1M' }"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "'interval'"
*/
date'2026-10-27';
date"2026-10-27";
time'12:30:00';
time'12:30:00.123456789';
time'24:00:00';
timestamp'2026-10-27T12:30:00';
timestamp'2026-10-27 12:30:00.5';
interval'1 year 2 mons 3 days 04:05:06.5';
interval'P1Y2M3DT4H5M6.5S';
interval'-1 day';
d'2026-10-27';
type::of(d'2026-10-27');
[date'2026-10-27', time'12:30:00', timestamp'2026-10-27T12:30:00', interval'1 day'];
{ a: date'2026-10-27', b: interval'1 month' };
date'2026-10-27' = date'2026-10-27';
interval'1 mon' = interval'30 days';
type::of(interval'1 mon');
