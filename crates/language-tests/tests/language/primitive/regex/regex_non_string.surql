/**
[test]
wip = true

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

*/
/[0-9]/ = 1;
/[0-9.]/ = 1.0;
/.*/ = time::now();
/.*/ = 1h;
/.*/ = NULL;
/.*/ = NONE;
/.*/ = true;
/.*/ = 1dec;
/.*/ = u"00000000-0000-0000-0000-000000000000";
/.*/ = <future> { 1 + 1 };
/.*/ = [];
/.*/ = {};
/.*/ =  (1.0,1.0);
