/**
[env]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:{ val: 123 }, val: 456 }]"

[[test.results]]
value = "[{ id: test:{ val: 123 }, val: 456 }]"

[[test.results]]
value = "[{ id: test:{ val: 123 }, val: 456 }]"

[[test.results]]
value = "[{ id: test:{ val: 123 }, val: 456 }]"

[[test.results]]
value = "[{ id: test:{ val: 123 }, val: 456 }]"

*/
DEFINE INDEX doc_val ON test FIELDS val;
DEFINE INDEX rid_val ON test FIELDS id.id().val;

CREATE test:{ val: 123 } SET val = 456;

SELECT * FROM test WHERE val = 456;
SELECT * FROM test WHERE id.val = 123;
SELECT * FROM test WHERE id.id().val = 123;
SELECT * FROM test WHERE record::id(id).val = 123;
