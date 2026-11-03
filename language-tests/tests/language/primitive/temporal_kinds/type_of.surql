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
value = "pd'2026-10-27'"

[[test.results]]
value = "pt'12:30:00'"
*/
[type::of(pd'2026-10-27'), type::of(pt'12:30:00'), type::of(ts'2026-10-27T12:30:00'), type::of(ivl'1 day'), type::of(d'2026-10-27')];
[type::is_date(pd'2026-10-27'), type::is_date(d'2026-10-27'), type::is_time(pt'12:30:00'), type::is_time(d'2026-10-27'), type::is_timestamp(ts'2026-10-27T12:30:00'), type::is_timestamp(pd'2026-10-27'), type::is_interval(ivl'1 day'), type::is_interval(1d)];
pd'2026-10-27'.is_date();
ivl'1 day'.is_interval();
'2026-10-27'.to_date();
'12:30:00'.to_time();
