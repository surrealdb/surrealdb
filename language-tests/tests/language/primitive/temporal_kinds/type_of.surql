/**
[test]
reason = "type::of and the type::is_* checks answer each new kind and none other"

[[test.results]]
value = "['date', 'time', 'timestamp', 'interval', 'datetime']"

[[test.results]]
value = "[true, false, true, false, true, false, true, false]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "date'2026-10-27'"

[[test.results]]
value = "time'12:30:00'"
*/
[type::of(date'2026-10-27'), type::of(time'12:30:00'), type::of(timestamp'2026-10-27T12:30:00'), type::of(interval'1 day'), type::of(d'2026-10-27')];
[type::is_date(date'2026-10-27'), type::is_date(d'2026-10-27'), type::is_time(time'12:30:00'), type::is_time(d'2026-10-27'), type::is_timestamp(timestamp'2026-10-27T12:30:00'), type::is_timestamp(date'2026-10-27'), type::is_interval(interval'1 day'), type::is_interval(1d)];
date'2026-10-27'.is_date();
interval'1 day'.is_interval();
'2026-10-27'.to_date();
'12:30:00'.to_time();
