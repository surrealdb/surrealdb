/**
[test]
reason = "Test EXPLAIN FORMAT JSON with simple SELECT statement"

[[test.results]]
value = """[
	{
		attributes: {
			table: 'person'
		},
		children: [
			{
				attributes: {
					table: 'person'
				},
				context: 'Db',
				operator: 'Scan'
			}
		],
		context: 'Db',
		operator: 'ComputeFields'
	}
]"""

*/

-- FORMAT JSON
EXPLAIN FORMAT JSON SELECT * FROM person;
