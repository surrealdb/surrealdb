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
value = "ts'infinity'"

[[test.results]]
value = "pd'-infinity'"

[[test.results]]
value = "d'+262142-12-31T23:59:59.999999999Z'"

*/

time::extract(ts'infinity', 'year');
time::extract(ts'-infinity', 'epoch');
time::extract(ts'infinity', 'month');
time::extract(ts'infinity', 'hour');
time::extract(pd'infinity', 'century');
time::extract(pd'-infinity', 'dow');
time::extract(pd'infinity', 'hour');
time::extract(<datetime> 'infinity', 'year');
time::extract(ts'2026-10-27T12:30:45.5', 'year');
time::trunc(ts'infinity', 'year');
time::trunc(pd'-infinity', 'month');
time::trunc(<datetime> 'infinity', 'day');
