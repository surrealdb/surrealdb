/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

*/
/a/ = "a";
/a/ = /a/;
/a/ = /b/;
/a/ = "b";
/a|b/ = /b/;
/a|b/ = "b";
