/**
[test]
reason = "Arithmetic on the temporal kinds per ADR 0013 section 3, with existing datetime and duration results unchanged"

[[test.results]]
value = "pd'2026-11-01'"

[[test.results]]
value = "pd'2026-09-27'"

[[test.results]]
value = "pd'2026-11-01'"

[[test.results]]
value = "299"

[[test.results]]
value = "ts'2026-11-29T00:00:00'"

[[test.results]]
value = "ts'2025-10-27T00:00:00'"

[[test.results]]
value = "ts'2026-10-28T00:00:00'"

[[test.results]]
value = "ts'2026-10-27T12:30:00'"

[[test.results]]
value = "pt'13:30:00'"

[[test.results]]
value = "pt'00:30:00'"

[[test.results]]
value = "pt'23:30:00'"

[[test.results]]
value = "pt'14:30:00'"

[[test.results]]
value = "ivl'PT2H30M'"

[[test.results]]
value = "ivl'PT-2H-30M'"

[[test.results]]
value = "ts'2026-02-28T10:00:00'"

[[test.results]]
value = "ts'2026-02-28T10:00:00'"

[[test.results]]
value = "ts'2025-02-28T10:00:00'"

[[test.results]]
value = "ts'2026-10-28T12:00:00'"

[[test.results]]
value = "ts'2026-10-27T11:30:00'"

[[test.results]]
value = "ts'2026-10-27T08:30:00'"

[[test.results]]
value = "ivl'P7DT1H30M'"

[[test.results]]
value = "ivl'P-7DT-1H-30M'"

[[test.results]]
value = "d'2026-02-28T10:00:00Z'"

[[test.results]]
value = "d'2026-10-26T08:00:00Z'"

[[test.results]]
value = "1d"

[[test.results]]
value = "d'2026-10-28T10:00:00Z'"

[[test.results]]
value = "ivl'P1DT2H'"

[[test.results]]
value = "ivl'P1M-1D'"

[[test.results]]
value = "ivl'P-1M-2DT-3H'"

[[test.results]]
value = "ivl'P2M4DT6H'"

[[test.results]]
value = "ivl'P1M15D'"

[[test.results]]
value = "ivl'PT12H'"

[[test.results]]
value = "ivl'PT2H'"

[[test.results]]
value = "ivl'P15D'"

[[test.results]]
value = "ivl'P1DT12H'"

[[test.results]]
value = "ivl'PT2H30M'"

[[test.results]]
error = "An interval cannot be divided by zero."

[[test.results]]
error = 'Failed to compute: "interval", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "interval", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "interval", as the operation results in an arithmetic overflow.'

[[test.results]]
value = "pd'infinity'"

[[test.results]]
error = 'Failed to compute: "date", as the operation results in an arithmetic overflow.'

[[test.results]]
error = "Cannot perform addition with 'date' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'date' and 'date'"

[[test.results]]
error = "Cannot perform multiplication with 'time' and 'int'"

[[test.results]]
value = "10"

[[test.results]]
value = "2h"

[[test.results]]
value = "d'2026-10-27T11:00:00Z'"
*/
pd'2026-10-27' + 5;
pd'2026-10-27' - 30;
5 + pd'2026-10-27';
pd'2026-10-27' - pd'2026-01-01';
pd'2026-10-27' + ivl'1 month 2 days';
pd'2026-10-27' - ivl'1 year';
pd'2026-10-27' + 1d;
pd'2026-10-27' + pt'12:30:00';
pt'12:30:00' + ivl'1 hour';
pt'23:30:00' + ivl'1 hour';
pt'00:30:00' - ivl'1 hour';
pt'12:30:00' + 2h;
pt'12:30:00' - pt'10:00:00';
pt'10:00:00' - pt'12:30:00';
ts'2026-01-31T10:00:00' + ivl'1 month';
ts'2026-03-31T10:00:00' - ivl'1 month';
ts'2024-02-29T10:00:00' + ivl'1 year';
ts'2026-10-27T10:00:00' + ivl'1 day 2 hours';
ts'2026-10-27T10:00:00' + 90m;
ts'2026-10-27T10:00:00' - 90m;
ts'2026-10-27T10:00:00' - ts'2026-10-20T08:30:00';
ts'2026-10-20T08:30:00' - ts'2026-10-27T10:00:00';
d'2026-01-31T10:00:00Z' + ivl'1 month';
d'2026-10-27T10:00:00Z' - ivl'1 day 2 hours';
d'2026-10-27T10:00:00Z' - d'2026-10-26T10:00:00Z';
d'2026-10-27T10:00:00Z' + 1d;
ivl'1 day' + ivl'2 hours';
ivl'1 mon' - ivl'1 day';
-ivl'1 mon 2 days 03:00:00';
ivl'1 mon 2 days 03:00:00' * 2;
ivl'1 mon' * 1.5;
ivl'1 day' * 0.5;
2 * ivl'1 hour';
ivl'1 mon' / 2;
ivl'3 days' / 2;
ivl'10:00:00' / 4;
ivl'1 day' / 0;
ivl'2147483647 mons' + ivl'1 mon';
ivl'-2147483648 days' - ivl'1 day';
ivl'1 mon' * 99999999999;
pd'infinity' + 1;
pd'+262142-12-30' + 1;
pd'2026-10-27' + 'a';
pd'2026-10-27' + pd'2026-10-27';
pt'12:00:00' * 2;
5 + 5;
1h + 1h;
d'2026-10-27T10:00:00Z' + 1h;
