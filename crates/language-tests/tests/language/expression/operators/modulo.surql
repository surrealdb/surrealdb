/**
[test]

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "-2"

[[test.results]]
value = "-2"

[[test.results]]
value = "0"

[[test.results]]
error = "Cannot perform remainder with '3' and '0'"

[[test.results]]
error = "Cannot perform remainder with '0' and '0'"

[[test.results]]
value = "2dec"

[[test.results]]
value = "2dec"

[[test.results]]
value = "2f"

[[test.results]]
value = "2f"

[[test.results]]
value = "2f"

[[test.results]]
value = "1.333333333333333f"

[[test.results]]
error = "Cannot perform remainder with '8' and '3'"

*/
8 % 3;
8 % -3;
-8 % -3;
-8 % 3;
0 % 3;
3 % 0;
0 % 0;
8 % 3dec;
8dec % 3;
8.0 % 3.0;
8.0 % 3;
8 % 3.0;
8 % 3.3333333333333333;
"8" % "3";
