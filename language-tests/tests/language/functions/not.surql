/**
[test]

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

*/

not(true);
not(not(true));
not(not(false));
not(false);
not(1);
not(0);
not("hello");
not("");
not([1]);
not([]);
not(1s);
not(0s);
not({a: 1});
not({});
not(d"0000-01-01T00:00:00Z");
not(/ /);
not(u"00000000-0000-0000-0000-000000000000");
not(u"FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF");
