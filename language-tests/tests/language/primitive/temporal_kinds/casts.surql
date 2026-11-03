/**
[test]
reason = "Casts between the temporal kinds and from strings, including <date> dropping the time of a datetime"

[[test.results]]
value = "pd'2026-10-27'"

[[test.results]]
value = "pd'2026-10-27'"

[[test.results]]
value = "pd'2026-10-27'"

[[test.results]]
value = "pd'2026-10-27'"

[[test.results]]
value = "pt'12:30:45.500'"

[[test.results]]
value = "pt'12:30:45'"

[[test.results]]
value = "pt'12:30:45'"

[[test.results]]
value = "ts'2026-10-27T12:30:45'"

[[test.results]]
value = "ts'2026-10-27T00:00:00'"

[[test.results]]
value = "ts'2026-10-27T12:30:45'"

[[test.results]]
value = "d'2026-10-27T12:30:45Z'"

[[test.results]]
value = "d'2026-10-27T00:00:00Z'"

[[test.results]]
value = "ivl'PT1H2M3S'"

[[test.results]]
value = "ivl'PT1H30M'"

[[test.results]]
value = "ivl'P1Y2D'"

[[test.results]]
value = "1h2m3s"

[[test.results]]
value = "4w3d1s"

[[test.results]]
value = "'2026-10-27'"

[[test.results]]
value = "'12:30:00'"

[[test.results]]
value = "'2026-10-27T12:30:00'"

[[test.results]]
value = "'P1Y2D'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pd'2026-01-01', pd'2026-01-02']"

[[test.results]]
value = "pd'2026-10-27'"

[[test.results]]
value = "pt'12:30:00'"

[[test.results]]
value = "ts'2026-10-27T12:30:00'"

[[test.results]]
value = "ivl'P2D'"

[[test.results]]
error = "Could not cast into `date` using input `'not a date'`"

[[test.results]]
error = "Could not cast into `date` using input `1`"

[[test.results]]
error = "Could not cast into `interval` using input `1`"

[[test.results]]
error = "Could not cast into `duration` using input `ivl'P-1D'`"

[[test.results]]
error = "Could not cast into `interval` using input `1ns`"

[[test.results]]
error = "Could not cast into `time` using input `ivl'P1D'`"
*/
<date> d'2026-10-27T12:30:45Z';
<date> '2026-10-27';
<date> '2026-10-27T12:30:45Z';
<date> ts'2026-10-27T12:30:45';
<time> d'2026-10-27T12:30:45.5Z';
<time> '12:30:45';
<time> ts'2026-10-27T12:30:45';
<timestamp> d'2026-10-27T12:30:45Z';
<timestamp> pd'2026-10-27';
<timestamp> '2026-10-27 12:30:45';
<datetime> ts'2026-10-27T12:30:45';
<datetime> pd'2026-10-27';
<interval> pt'01:02:03';
<interval> 1h30m;
<interval> '1 year 2 days';
<duration> pt'01:02:03';
<duration> ivl'1 mon 1 day 00:00:01';
<string> pd'2026-10-27';
<string> pt'12:30:00';
<string> ts'2026-10-27T12:30:00';
<string> ivl'1 year 2 days';
<option<date>> NONE;
<array<date>> ['2026-01-01', '2026-01-02'];
type::date('2026-10-27');
type::time('12:30:00');
type::timestamp('2026-10-27T12:30:00');
type::interval('2 days');
<date> 'not a date';
<date> 1;
<interval> 1;
<duration> ivl'-1 day';
<interval> 1ns;
<time> ivl'1 day';
