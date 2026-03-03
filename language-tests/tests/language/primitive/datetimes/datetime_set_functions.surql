/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "d'1914-02-26T07:29:23.346200Z'"

[[test.results]]
value = "d'2026-06-26T07:29:23.346200Z'"

[[test.results]]
value = "d'2026-02-28T07:29:23.346200Z'"

[[test.results]]
value = "d'2026-02-26T10:29:23.346200Z'"

[[test.results]]
value = "d'2026-02-26T07:45:23.346200Z'"

[[test.results]]
value = "d'2026-02-26T07:29:30.346200Z'"

[[test.results]]
value = "d'2026-02-26T07:29:23Z'"

[[test.results]]
value = "d'1914-06-28T10:45:30Z'"

[[test.results]]
value = "d'-1000-02-26T07:29:23.346200Z'"

[[test.results]]
value = "d'+262142-02-26T07:29:23.346200Z'"

[[test.results]]
value = "d'-262143-02-26T07:29:23.346200Z'"

[[test.results]]
error = "Unable to set datetime to year 262143"

[[test.results]]
error = "out of range integral type conversion attempted"

[[test.results]]
error = "out of range integral type conversion attempted"

[[test.results]]
error = "out of range integral type conversion attempted"

[[test.results]]
error = "out of range integral type conversion attempted"

[[test.results]]
error = "out of range integral type conversion attempted"

[[test.results]]
error = "out of range integral type conversion attempted"

*/

LET $now = d'2026-02-26T07:29:23.346200Z';

-- Test each function
$now.set_year(1914);
$now.set_month(6);
$now.set_day(28);
$now.set_hour(10);
$now.set_minute(45);
$now.set_second(30);
$now.set_nanosecond(0);

-- Test chaining
$now
    .set_year(1914)
    .set_month(6)
    .set_day(28)
    .set_hour(10)
    .set_minute(45)
    .set_second(30)
    .set_nanosecond(0);

-- Negative years and up to greatest positive+negative are fine
$now.set_year(-1000);
$now.set_year(time::maximum.year());
$now.set_year(time::minimum.year());
-- One greater than max = error
$now.set_year(262143);

-- Negative numbers = error for anything but set_year
$now.set_month(-1);
$now.set_day(-1);
$now.set_hour(-1);
$now.set_minute(-1);
$now.set_second(-1);
$now.set_nanosecond(-1);