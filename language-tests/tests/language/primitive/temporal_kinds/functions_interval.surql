/**
[test]
reason = "The interval:: namespace builds intervals, extracts their fields, and justifies them"

[[test.results]]
value = "ivl'P1Y2M10DT4H5M6.5S'"

[[test.results]]
value = "ivl'PT0S'"

[[test.results]]
value = "ivl'P1Y'"

[[test.results]]
error = "Incorrect arguments for function interval::make(). The years must be an integer."

[[test.results]]
value = "ivl'PT1H30M'"

[[test.results]]
value = "4"

[[test.results]]
value = "2"

[[test.results]]
value = "5"

[[test.results]]
value = "50"

[[test.results]]
value = "30"

[[test.results]]
value = "15.5dec"

[[test.results]]
value = "15500000"

[[test.results]]
value = "ivl'P3DT2H'"

[[test.results]]
value = "ivl'P1M15DT2H'"

[[test.results]]
value = "ivl'P1M17DT2H'"

[[test.results]]
value = "ivl'P29D'"

[[test.results]]
value = "ivl'PT22H'"

[[test.results]]
value = "ivl'P3DT2H'"

[[test.results]]
value = "3"

[[test.results]]
error = "Incorrect arguments for function interval::years(). Argument 1 was the wrong type. Expected `interval` but found `1`"
*/
interval::make(1, 2, 1, 3, 4, 5, 6.5);
interval::make();
interval::make(1);
interval::make(0.5);
interval::make(0, 0, 0, 0, 1.5);
interval::years(ivl'3 years 14 mons');
interval::months(ivl'3 years 14 mons');
interval::days(ivl'1 mon 5 days');
interval::hours(ivl'50:30:15.5');
interval::mins(ivl'50:30:15.5');
interval::secs(ivl'50:30:15.5');
interval::micros(ivl'50:30:15.5');
interval::justify_hours(ivl'1 day 50:00:00');
interval::justify_days(ivl'45 days 02:00:00');
interval::justify_interval(ivl'45 days 50:00:00');
interval::justify_interval(ivl'1 mon -1 day');
interval::justify_interval(ivl'1 day -02:00:00');
ivl'1 day 50:00:00'.justify_hours();
ivl'3 years'.years();
interval::years(1);
