/**
[test]
reason = "Test time::second extracting the second from a datetime"

[[test.results]]
value = "56"

[[test.results]]
value = "0"

[[test.results]]
value = "59"

[[test.results]]
value = "56"

[[test.results]]
error = "Incorrect arguments for function time::second(). Argument 1 was the wrong type. Expected `datetime` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function time::second(). Argument 1 was the wrong type. Expected `datetime` but found `'2024-05-05T12:34:56Z'`"
*/

time::second(d"2024-05-05T12:34:56Z");
time::second(d"2024-05-05T12:34:00Z");
time::second(d"2024-05-05T12:34:59Z");
time::second(d"2024-05-05T12:34:56.999999999Z");
time::second(NONE);
time::second("2024-05-05T12:34:56Z");
