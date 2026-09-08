/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ct:1, b: true }]"

[[test.results]]
value = "[{ id: ct:2, b: true }]"

[[test.results]]
value = "[{ id: ct:3, b: false }]"

[[test.results]]
value = "[{ id: ct:4, b: false }]"

*/
DEFINE TABLE ct;
DEFINE INDEX ct_idx ON ct COUNT;
CREATE ct:1 SET b = true;
CREATE ct:2 SET b = true;
CREATE ct:3 SET b = false;
CREATE ct:4 SET b = false;
