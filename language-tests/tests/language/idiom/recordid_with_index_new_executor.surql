/**
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

[[test.results]]
value = '''"Project [ctx: Db]\n└────> Filter [ctx: Db] [predicate: val = 456]\n       └────> Scan [ctx: Db] [source: test]\n"'''

[[test.results]]
value = '''"Project [ctx: Db]\n└────> Filter [ctx: Db] [predicate: id.val = 456]\n       └────> Scan [ctx: Db] [source: test]\n"'''

[[test.results]]
value = '''"Project [ctx: Db]\n└────> Filter [ctx: Db] [predicate: id.id().val = 456]\n       └────> Scan [ctx: Db] [source: test]\n"'''

[[test.results]]
value = '''"Project [ctx: Db]\n└────> Filter [ctx: Db] [predicate: record::id(id).val = 456]\n       └────> Scan [ctx: Db] [source: test]\n"'''

*/

DEFINE INDEX doc_val ON test FIELDS val;
DEFINE INDEX rid_val ON test FIELDS id.id().val;

CREATE test:{ val: 123 } SET val = 456;

SELECT * FROM test WHERE val = 456;
SELECT * FROM test WHERE id.val = 456;
SELECT * FROM test WHERE id.id().val = 123;
SELECT * FROM test WHERE record::id(id).val = 123;

EXPLAIN SELECT * FROM test WHERE val = 456;
EXPLAIN SELECT * FROM test WHERE id.val = 456;
EXPLAIN SELECT * FROM test WHERE id.id().val = 456;
EXPLAIN SELECT * FROM test WHERE record::id(id).val = 456;
