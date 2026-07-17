/**
[test]
reason = "Test time::hour extracting the hour from a datetime"

[[test.results]]
value = "12"

[[test.results]]
value = "0"

[[test.results]]
value = "23"

[[test.results]]
value = "10"

[[test.results]]
error = "Incorrect arguments for function time::hour(). Argument 1 was the wrong type. Expected `datetime` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function time::hour(). Argument 1 was the wrong type. Expected `datetime` but found `'2024-05-05T12:34:56Z'`"
*/

time::hour(d"2024-05-05T12:34:56Z");
time::hour(d"2024-05-05T00:00:00Z");
time::hour(d"2024-05-05T23:59:59Z");
time::hour(d"2024-05-05T12:34:56+02:00");
time::hour(NONE);
time::hour("2024-05-05T12:34:56Z");
