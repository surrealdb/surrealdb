/**
[test]
reason = "Test time::timezone() returns a valid UTC-offset string"

[[test.results]]
match = "$result = /^[+-]\\d{2}:\\d{2}$/"
error = false

*/
time::timezone();
