/**
[test]
reason = "Ensure sleep is callable as a builtin function as well as a statement"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

sleep(0ms);
RETURN sleep(1ms);
SLEEP 1ms;
