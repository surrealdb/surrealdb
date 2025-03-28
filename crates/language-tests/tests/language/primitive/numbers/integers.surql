/**
[test]
reason = "testing varies integer literals"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "-1"

[[test.results]]
value = "-9223372036854775808"

[[test.results]]
value = "9223372036854775807"

[[test.results]]
value = "-9223372036854775808"

[[test.results]]
value = "9223372036854775807"

*/
1;
0;
-1;
-9_223_372_036_854_775_808;
9_223_372_036_854_775_807;
-000009223372036854775808;
00009223372036854775_____807;
