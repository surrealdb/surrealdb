/**
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ baz: 1, id: faz:1 }]"

[[test.results]]
value = "[{ baz: 2, id: faz:2 }]"

[[test.results]]
value = "[{ baz: 3, id: faz:3 }]"

[[test.results]]
value = "[{ baz: 1, id: faz:1 }, { baz: 2, id: faz:2 }, { baz: 3, id: faz:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bar: [{ baz: 1, id: faz:1 }, { baz: 2, id: faz:2 }, { baz: 3, id: faz:3 }], id: foo:1 }]"

[[test.results]]
value = "[{ bar: [{ baz: 1, id: faz:1 }, { baz: 2, id: faz:2 }, { baz: 3, id: faz:3 }], id: foo:1 }]"

[[test.results]]
value = "[{ bar: [{ baz: 1, id: faz:1 }, { baz: 2, id: faz:2 }, { baz: 3, id: faz:3 }] }]"


*/
DEFINE TABLE faz;
DEFINE FIELD baz on faz TYPE number;

CREATE faz:1 CONTENT { baz: 1 };
CREATE faz:2 CONTENT { baz: 2 };
CREATE faz:3 CONTENT { baz: 3 };

SELECT * FROM faz;

DEFINE TABLE foo;
DEFINE FIELD bar ON foo COMPUTED { RETURN SELECT * FROM faz; };

CREATE foo:1;

SELECT * FROM foo;
SELECT bar FROM foo;
