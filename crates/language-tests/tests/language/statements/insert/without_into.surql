/**
[test]

[[test.results]]
value = "[{ id: test:1 }]"

[[test.results]]
value = "[{ id: test:2 }]"

[[test.results]]
value = "[{ id: test:3 }]"

[[test.results]]
error = "Can not execute INSERT statement where property 'id' is: NONE"

*/
INSERT [
	{ id: test:1 }
];

INSERT { id: test:2 };
INSERT (id) VALUES (test:3);

INSERT {};
