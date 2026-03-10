/**
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bar: 1, id: foo:1 }]"

[[test.results]]
value = "[{ bar: 1, id: foo:1 }]"

[[test.results]]
value = "[{ bar: 1 }]"

*/

DEFINE TABLE foo;
DEFINE FIELD bar ON foo COMPUTED { 1 };

CREATE foo:1;

SELECT * FROM foo;
SELECT bar FROM foo;
