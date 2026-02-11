/**
[test]
reason = "Test that EXPLAIN can be used inside expression context (e.g. object literals)"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:2, name: 'Bob' }]"

[[test.results]]
value = '''{
	"agg": "Aggregate [ctx: Db] [mode: GROUP ALL]
└────> Scan [ctx: Db] [source: person]
",
	"select": "Project [ctx: Db]
└────> Scan [ctx: Db] [source: person]
" }'''

*/

CREATE person:1 SET name = 'Alice';

CREATE person:2 SET name = 'Bob';

RETURN {
	"select": (EXPLAIN SELECT * FROM person),
	"agg": (EXPLAIN SELECT count() as total FROM person GROUP ALL),
};
