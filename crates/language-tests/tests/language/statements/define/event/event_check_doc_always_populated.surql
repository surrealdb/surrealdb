/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1, num: 1 }]"

[[test.results]]
value = "[{ id: test:1, num: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = """[
	{
			after: { id: test:1, num: 1 },
			id: log:CREATE,
			this: { id: test:1, num: 1 },
			value: { id: test:1, num: 1 }
	},
	{
			before: { id: test:1, num: 2 },
			id: log:DELETE,
			this: { id: test:1, num: 2 },
			value: { id: test:1, num: 2 }
	},
	{
			after: { id: test:1, num: 2 },
			before: { id: test:1, num: 1 },
			id: log:UPDATE,
			this: { id: test:1, num: 2 },
			value: { id: test:1, num: 2 }
	}
]"""

*/

DEFINE EVENT test ON test WHEN true THEN {
    LET $doc = $this;
    CREATE type::thing('log', $event) SET this = $doc, value = $value, before = $before, after = $after;
};
CREATE test:1 SET num = 1;
UPSERT test:1 set num = 2;
DELETE test:1;
SELECT * FROM log;
