/**
[test]
reason = "time:: accessors, rewriters, min and max accept the new kinds, return the argument's kind, and the new constructors and field functions follow PostgreSQL"

[[test.results]]
value = "2026"

[[test.results]]
value = "10"

[[test.results]]
value = "27"

[[test.results]]
value = "2"

[[test.results]]
value = "300"

[[test.results]]
value = "44"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "12"

[[test.results]]
value = "30"

[[test.results]]
value = "45"

[[test.results]]
value = "1000000000"

[[test.results]]
value = "1000000"

[[test.results]]
value = "1000"

[[test.results]]
value = "24"

[[test.results]]
error = "Incorrect arguments for function time::hour(). The argument must be a time, timestamp or datetime, found date."

[[test.results]]
error = "Incorrect arguments for function time::year(). The argument must be a date, timestamp or datetime, found time."

[[test.results]]
value = "2026"

[[test.results]]
value = "12"

[[test.results]]
value = "'2026/10/27'"

[[test.results]]
value = "'12:30'"

[[test.results]]
value = "date'2026-10-01'"

[[test.results]]
value = "'date'"

[[test.results]]
value = "timestamp'2026-10-27T12:00:00'"

[[test.results]]
value = "'timestamp'"

[[test.results]]
value = "d'2026-10-27T12:00:00Z'"

[[test.results]]
value = "timestamp'2026-10-27T12:00:00'"

[[test.results]]
value = "timestamp'2026-10-27T13:00:00'"

[[test.results]]
value = "date'2026-10-27'"

[[test.results]]
value = "date'2030-10-27'"

[[test.results]]
value = "timestamp'2026-10-27T03:30:45'"

[[test.results]]
error = "Incorrect arguments for function time::set_hour(). The result is not a whole date, so the argument must be cast to a timestamp first."

[[test.results]]
value = "date'2026-10-27'"

[[test.results]]
value = "date'2026-10-28'"

[[test.results]]
value = "'time'"

[[test.results]]
value = "timestamp'2026-10-28T00:00:00'"

[[test.results]]
value = "interval'P1M'"

[[test.results]]
error = "Incorrect arguments for function time::max(). The elements must all be of one kind."

[[test.results]]
value = "NONE"

[[test.results]]
value = "date'2026-10-27'"

[[test.results]]
error = "Incorrect arguments for function time::make_date(). 2026-2-30 is not a valid date."

[[test.results]]
value = "time'12:30:45.500'"

[[test.results]]
value = "time'24:00:00'"

[[test.results]]
error = "Incorrect arguments for function time::make_time(). The time fields are out of range."

[[test.results]]
value = "timestamp'2026-10-27T12:30:45.250'"

[[test.results]]
value = "interval'P6Y7M27DT5H30M'"

[[test.results]]
value = "interval'P-6Y-7M-27DT-5H-30M'"

[[test.results]]
value = "interval'P1M1D'"

[[test.results]]
value = "interval'PT12H'"

[[test.results]]
value = "2026"

[[test.results]]
value = "4"

[[test.results]]
value = "21"

[[test.results]]
value = "202"

[[test.results]]
value = "3"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "300"

[[test.results]]
value = "2026"

[[test.results]]
value = "44"

[[test.results]]
value = "45.5dec"

[[test.results]]
value = "45500dec"

[[test.results]]
value = "45500000"

[[test.results]]
value = "1793104245.5dec"

[[test.results]]
value = "2461341"

[[test.results]]
value = "0"

[[test.results]]
value = "12"

[[test.results]]
error = "Incorrect arguments for function time::extract(). `year` cannot be read from a time."

[[test.results]]
error = "Incorrect arguments for function time::extract(). The field must be one of century, day, decade, dow, doy, epoch, hour, isodow, isoyear, julian, microseconds, millennium, milliseconds, minute, month, quarter, second, week or year."

[[test.results]]
value = "timestamp'2026-10-27T12:30:00'"

[[test.results]]
value = "timestamp'2026-10-26T00:00:00'"

[[test.results]]
value = "timestamp'2026-10-01T00:00:00'"

[[test.results]]
value = "date'2020-01-01'"

[[test.results]]
value = "date'2001-01-01'"

[[test.results]]
value = "date'2001-01-01'"

[[test.results]]
value = "time'12:00:00'"

[[test.results]]
value = "date'2026-10-27'"

[[test.results]]
value = "'date'"

[[test.results]]
value = "2461341.5dec"
*/
time::year(date'2026-10-27');
time::month(timestamp'2026-10-27T12:30:45');
time::day(d'2026-10-27T12:30:45Z');
time::wday(date'2026-10-27');
time::yday(date'2026-10-27');
time::week(date'2026-10-27');
time::is_leap_year(date'2024-01-01');
time::is_leap_year(date'2026-01-01');
time::hour(time'12:30:45.123456789');
time::minute(timestamp'2026-10-27T12:30:45');
time::second(time'12:30:45.5');
time::nano(time'00:00:01');
time::micros(time'00:00:01');
time::millis(time'00:00:01');
time::hour(time'24:00:00');
time::hour(date'2026-10-27');
time::year(time'12:00:00');
date'2026-10-27'.year();
timestamp'2026-10-27T12:30:45'.hour();
time::format(date'2026-10-27', '%Y/%m/%d');
time::format(timestamp'2026-10-27T12:30:45', '%H:%M');
time::group(date'2026-10-27', 'month');
type::of(time::group(date'2026-10-27', 'month'));
time::group(timestamp'2026-10-27T12:30:45', 'hour');
type::of(time::group(timestamp'2026-10-27T12:30:45', 'hour'));
time::group(d'2026-10-27T12:30:45Z', 'hour');
time::floor(timestamp'2026-10-27T12:30:45', 1h);
time::round(timestamp'2026-10-27T12:30:45', 1h);
time::ceil(date'2026-10-27', 1d);
time::set_year(date'2026-10-27', 2030);
time::set_hour(timestamp'2026-10-27T12:30:45', 3);
time::set_hour(date'2026-10-27', 3);
time::min([date'2026-10-28', date'2026-10-27']);
time::max([date'2026-10-28', date'2026-10-27']);
type::of(time::max([time'01:00:00', time'02:00:00']));
time::max([timestamp'2026-10-28T00:00:00', timestamp'2026-10-27T00:00:00']);
time::max([interval'1 mon', interval'29 days']);
time::max([date'2026-10-28', time'01:00:00']);
time::max([]);
time::make_date(2026, 10, 27);
time::make_date(2026, 2, 30);
time::make_time(12, 30, 45.5);
time::make_time(24, 0, 0);
time::make_time(24, 1, 0);
time::make_timestamp(2026, 10, 27, 12, 30, 45.25);
time::age(timestamp'2026-10-27T12:00:00', timestamp'2020-02-29T06:30:00');
time::age(timestamp'2020-02-29T06:30:00', timestamp'2026-10-27T12:00:00');
time::age(date'2026-03-01', date'2026-01-31');
time::age(timestamp'2026-10-27T00:00:00', timestamp'2026-10-26T12:00:00');
time::extract(timestamp'2026-10-27T12:30:45.5', 'year');
time::extract(timestamp'2026-10-27T12:30:45.5', 'quarter');
time::extract(timestamp'2026-10-27T12:30:45.5', 'century');
time::extract(timestamp'2026-10-27T12:30:45.5', 'decade');
time::extract(timestamp'2026-10-27T12:30:45.5', 'millennium');
time::extract(timestamp'2026-10-27T12:30:45.5', 'dow');
time::extract(timestamp'2026-10-27T12:30:45.5', 'isodow');
time::extract(timestamp'2026-10-27T12:30:45.5', 'doy');
time::extract(timestamp'2026-10-27T12:30:45.5', 'isoyear');
time::extract(timestamp'2026-10-27T12:30:45.5', 'week');
time::extract(timestamp'2026-10-27T12:30:45.5', 'second');
time::extract(timestamp'2026-10-27T12:30:45.5', 'milliseconds');
time::extract(timestamp'2026-10-27T12:30:45.5', 'microseconds');
time::extract(timestamp'2026-10-27T12:30:45.5', 'epoch');
time::extract(date'2026-10-27', 'julian');
time::extract(date'1970-01-01', 'epoch');
time::extract(time'12:30:45', 'hour');
time::extract(time'12:30:45', 'year');
time::extract(date'2026-10-27', 'fortnight');
time::trunc(timestamp'2026-10-27T12:30:45.123456', 'minute');
time::trunc(timestamp'2026-10-27T12:30:45.123456', 'week');
time::trunc(timestamp'2026-10-27T12:30:45', 'quarter');
time::trunc(date'2026-10-27', 'decade');
time::trunc(date'2026-10-27', 'century');
time::trunc(date'2026-10-27', 'millennium');
time::trunc(time'12:30:45', 'hour');
time::trunc(date'2026-10-27', 'hour');
type::of(time::trunc(date'2026-10-27', 'year'));
time::extract(timestamp'2026-10-27T12:00:00', 'julian');
