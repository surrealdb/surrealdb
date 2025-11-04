/**
[test]

[[test.results]]
value = "{ b: 2, c: 3 }"

*/


RETURN value::patch({ a: 1, b: 2 }, [
	{
		op: 'remove',
		path: '/a'
	},
	{
		op: 'add',
		path: '/c',
		value: 3
	}
]);
