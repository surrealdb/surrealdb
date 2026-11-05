/**
[test]
reason = "Test time::minute extracting the minute from a datetime"

[[test.results]]
value = "34"

[[test.results]]
value = "0"

[[test.results]]
value = "59"

[[test.results]]
value = "4"

[[test.results]]
error = "Incorrect arguments for function time::minute(). Argument 1 was the wrong type. Expected `datetime` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function time::minute(). Argument 1 was the wrong type. Expected `datetime` but found `'2024-05-05T12:34:56Z'`"
*/

time::minute(d"2024-05-05T12:34:56Z");
time::minute(d"2024-05-05T12:00:56Z");
time::minute(d"2024-05-05T12:59:56Z");
time::minute(d"2024-05-05T12:34:56+05:30");
time::minute(NONE);
time::minute("2024-05-05T12:34:56Z");
