/**
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bar: 4, id: foo:1 }]"

[[test.results]]
value = "[{ bar: 4, id: foo:1 }]"

[[test.results]]
value = "[{ bar: 4 }]"
*/

DEFINE TABLE foo;
DEFINE FIELD bar ON foo COMPUTED { (1 + 1) * 2 };

CREATE foo:1;

SELECT * FROM foo;
SELECT bar FROM foo;
