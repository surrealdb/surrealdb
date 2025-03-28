/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bar: 1, id: foo:1 }]"

*/
DEFINE TABLE foo;
DEFINE FIELD bar ON foo READONLY;

create foo:1 content { bar: 1 };
