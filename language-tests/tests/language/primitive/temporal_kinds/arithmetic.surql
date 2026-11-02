/**
[test]
reason = "Arithmetic on the temporal kinds per ADR 0013 section 3, with existing datetime and duration results unchanged"

[[test.results]]
value = "date'2026-11-01'"

[[test.results]]
value = "date'2026-09-27'"

[[test.results]]
value = "date'2026-11-01'"

[[test.results]]
value = "299"

[[test.results]]
value = "timestamp'2026-11-29T00:00:00'"

[[test.results]]
value = "timestamp'2025-10-27T00:00:00'"

[[test.results]]
value = "timestamp'2026-10-28T00:00:00'"

[[test.results]]
value = "timestamp'2026-10-27T12:30:00'"

[[test.results]]
value = "time'13:30:00'"

[[test.results]]
value = "time'00:30:00'"

[[test.results]]
value = "time'23:30:00'"

[[test.results]]
value = "time'14:30:00'"

[[test.results]]
value = "interval'PT2H30M'"

[[test.results]]
value = "interval'PT-2H-30M'"

[[test.results]]
value = "timestamp'2026-02-28T10:00:00'"

[[test.results]]
value = "timestamp'2026-02-28T10:00:00'"

[[test.results]]
value = "timestamp'2025-02-28T10:00:00'"

[[test.results]]
value = "timestamp'2026-10-28T12:00:00'"

[[test.results]]
value = "timestamp'2026-10-27T11:30:00'"

[[test.results]]
value = "timestamp'2026-10-27T08:30:00'"

[[test.results]]
value = "interval'P7DT1H30M'"

[[test.results]]
value = "interval'P-7DT-1H-30M'"

[[test.results]]
value = "d'2026-02-28T10:00:00Z'"

[[test.results]]
value = "d'2026-10-26T08:00:00Z'"

[[test.results]]
value = "1d"

[[test.results]]
value = "d'2026-10-28T10:00:00Z'"

[[test.results]]
value = "interval'P1DT2H'"

[[test.results]]
value = "interval'P1M-1D'"

[[test.results]]
value = "interval'P-1M-2DT-3H'"

[[test.results]]
value = "interval'P2M4DT6H'"

[[test.results]]
value = "interval'P1M15D'"

[[test.results]]
value = "interval'PT12H'"

[[test.results]]
value = "interval'PT2H'"

[[test.results]]
value = "interval'P15D'"

[[test.results]]
value = "interval'P1DT12H'"

[[test.results]]
value = "interval'PT2H30M'"

[[test.results]]
error = "An interval cannot be divided by zero."

[[test.results]]
error = 'Failed to compute: "interval", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "interval", as the operation results in an arithmetic overflow.'

[[test.results]]
error = 'Failed to compute: "interval", as the operation results in an arithmetic overflow.'

[[test.results]]
value = "date'infinity'"

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
date'2026-10-27' + 5;
date'2026-10-27' - 30;
5 + date'2026-10-27';
date'2026-10-27' - date'2026-01-01';
date'2026-10-27' + interval'1 month 2 days';
date'2026-10-27' - interval'1 year';
date'2026-10-27' + 1d;
date'2026-10-27' + time'12:30:00';
time'12:30:00' + interval'1 hour';
time'23:30:00' + interval'1 hour';
time'00:30:00' - interval'1 hour';
time'12:30:00' + 2h;
time'12:30:00' - time'10:00:00';
time'10:00:00' - time'12:30:00';
timestamp'2026-01-31T10:00:00' + interval'1 month';
timestamp'2026-03-31T10:00:00' - interval'1 month';
timestamp'2024-02-29T10:00:00' + interval'1 year';
timestamp'2026-10-27T10:00:00' + interval'1 day 2 hours';
timestamp'2026-10-27T10:00:00' + 90m;
timestamp'2026-10-27T10:00:00' - 90m;
timestamp'2026-10-27T10:00:00' - timestamp'2026-10-20T08:30:00';
timestamp'2026-10-20T08:30:00' - timestamp'2026-10-27T10:00:00';
d'2026-01-31T10:00:00Z' + interval'1 month';
d'2026-10-27T10:00:00Z' - interval'1 day 2 hours';
d'2026-10-27T10:00:00Z' - d'2026-10-26T10:00:00Z';
d'2026-10-27T10:00:00Z' + 1d;
interval'1 day' + interval'2 hours';
interval'1 mon' - interval'1 day';
-interval'1 mon 2 days 03:00:00';
interval'1 mon 2 days 03:00:00' * 2;
interval'1 mon' * 1.5;
interval'1 day' * 0.5;
2 * interval'1 hour';
interval'1 mon' / 2;
interval'3 days' / 2;
interval'10:00:00' / 4;
interval'1 day' / 0;
interval'2147483647 mons' + interval'1 mon';
interval'-2147483648 days' - interval'1 day';
interval'1 mon' * 99999999999;
date'infinity' + 1;
date'+262142-12-30' + 1;
date'2026-10-27' + 'a';
date'2026-10-27' + date'2026-10-27';
time'12:00:00' * 2;
5 + 5;
1h + 1h;
d'2026-10-27T10:00:00Z' + 1h;
