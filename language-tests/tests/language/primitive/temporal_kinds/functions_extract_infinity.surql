/**
[test]
reason = "time::extract of infinity or -infinity is that infinity for a field that only grows with time and NULL for any other, and time::trunc keeps it, as in PostgreSQL"

[[test.results]]
value = "Infinity"

[[test.results]]
value = "-Infinity"

[[test.results]]
value = "NULL"

[[test.results]]
value = "NULL"

[[test.results]]
value = "Infinity"

[[test.results]]
value = "NULL"

[[test.results]]
error = "Incorrect arguments for function time::extract(). `hour` cannot be read from a date."

[[test.results]]
value = "Infinity"

[[test.results]]
value = "2026"

[[test.results]]
value = "timestamp'infinity'"

[[test.results]]
value = "date'-infinity'"

[[test.results]]
value = "d'+262142-12-31T23:59:59.999999999Z'"

*/

time::extract(timestamp'infinity', 'year');
time::extract(timestamp'-infinity', 'epoch');
time::extract(timestamp'infinity', 'month');
time::extract(timestamp'infinity', 'hour');
time::extract(date'infinity', 'century');
time::extract(date'-infinity', 'dow');
time::extract(date'infinity', 'hour');
time::extract(<datetime> 'infinity', 'year');
time::extract(timestamp'2026-10-27T12:30:45.5', 'year');
time::trunc(timestamp'infinity', 'year');
time::trunc(date'-infinity', 'month');
time::trunc(<datetime> 'infinity', 'day');
