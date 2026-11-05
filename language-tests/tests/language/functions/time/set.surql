/**
[test]
reason = "Tests the time::set_year, time::set_month, time::set_day, time::set_hour, time::set_minute, time::set_second and time::set_nanosecond functions"

[[test.results]]
value = "d'1914-02-26T07:29:23.346200Z'"

[[test.results]]
value = "d'-0044-02-26T07:29:23Z'"

[[test.results]]
error = "Unable to set datetime to year 2021"

[[test.results]]
error = "Unable to set datetime to year 2020.5"

[[test.results]]
value = "d'2026-06-26T07:29:23.346200Z'"

[[test.results]]
error = "Unable to set datetime to month 2"

[[test.results]]
error = "Unable to set datetime to month 13"

[[test.results]]
error = "Unable to set datetime to month 0"

[[test.results]]
value = "d'2026-02-28T07:29:23.346200Z'"

[[test.results]]
error = "Unable to set datetime to day 30"

[[test.results]]
error = "Unable to set datetime to day 0"

[[test.results]]
value = "d'2026-02-26T10:29:23.346200Z'"

[[test.results]]
value = "d'2026-02-26T00:29:23Z'"

[[test.results]]
error = "Unable to set datetime to hour 24"

[[test.results]]
value = "d'2026-02-26T07:45:23.346200Z'"

[[test.results]]
error = "Unable to set datetime to 60 minutes"

[[test.results]]
error = "Incorrect arguments for function time::set_minute(). Argument 2 was the wrong type. Expected `number` but found `'30'`"

[[test.results]]
value = "d'2026-02-26T07:29:30.346200Z'"

[[test.results]]
error = "Unable to set datetime to 60 seconds"

[[test.results]]
value = "d'2026-02-26T07:29:23Z'"

[[test.results]]
value = "d'2026-02-26T07:29:23.999999999Z'"

[[test.results]]
error = "out of range integral type conversion attempted"

[[test.results]]
value = "d'1914-06-28T07:29:23Z'"

[[test.results]]
error = "Incorrect arguments for function time::set_hour(). Argument 2 was the wrong type. Expected `number` but found `NONE`"
*/

-- time::set_year
time::set_year(d'2026-02-26T07:29:23.346200Z', 1914);
time::set_year(d'2026-02-26T07:29:23Z', -44);
time::set_year(d'2020-02-29T12:00:00Z', 2021);
time::set_year(d'2026-02-26T07:29:23Z', 2020.5);

-- time::set_month
time::set_month(d'2026-02-26T07:29:23.346200Z', 6);
time::set_month(d'2026-01-31T00:00:00Z', 2);
time::set_month(d'2026-02-26T07:29:23Z', 13);
time::set_month(d'2026-02-26T07:29:23Z', 0);

-- time::set_day
time::set_day(d'2026-02-26T07:29:23.346200Z', 28);
time::set_day(d'2026-02-01T00:00:00Z', 30);
time::set_day(d'2026-02-26T07:29:23Z', 0);

-- time::set_hour
time::set_hour(d'2026-02-26T07:29:23.346200Z', 10);
time::set_hour(d'2026-02-26T07:29:23Z', 0);
time::set_hour(d'2026-02-26T07:29:23Z', 24);

-- time::set_minute
time::set_minute(d'2026-02-26T07:29:23.346200Z', 45);
time::set_minute(d'2026-02-26T07:29:23Z', 60);
time::set_minute(d'2026-02-26T07:29:23Z', "30");

-- time::set_second
time::set_second(d'2026-02-26T07:29:23.346200Z', 30);
time::set_second(d'2026-02-26T07:29:23Z', 60);

-- time::set_nanosecond
time::set_nanosecond(d'2026-02-26T07:29:23.346200Z', 0);
time::set_nanosecond(d'2026-02-26T07:29:23Z', 999999999);
time::set_nanosecond(d'2026-02-26T07:29:23Z', -1);

-- Nested composition
time::set_year(time::set_month(time::set_day(d'2026-02-26T07:29:23Z', 28), 6), 1914);

-- NONE as the component argument
time::set_hour(d'2026-02-26T07:29:23Z', NONE);
