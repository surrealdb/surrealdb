/**
[test]

[[test.results]]
value = """
[
	{
		op: 'remove',
		path: '/a'
	},
	{
		op: 'add',
		path: '/c',
		value: 3
	}
]
"""

*/

RETURN value::diff({ a: 1, b: 2 }, { c: 3, b: 2 });
