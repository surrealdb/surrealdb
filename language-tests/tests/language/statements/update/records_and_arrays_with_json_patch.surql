/**
[test]

[[test.results]]
value = "[{ biscuits: [{ name: 'Digestive' }, { name: 'Choco Leibniz' }], bugs: [], id: person:test, username: 'parsley' }]"

[[test.results]]
value = "[{ biscuits: [{ name: 'Ginger Nut' }, { name: 'Digestive' }, { name: 'Choco Leibniz' }], bugs: ['rfc6902'], id: person:test, test: true, username: 'parsley' }]"

[[test.results]]
value = "[{ biscuits: [{ name: 'Ginger Nut' }, { name: 'Digestive' }, { name: 'Choco Leibniz' }], bugs: ['rfc6902', 'rfc6903'], id: person:test, test: true, username: 'parsley' }]"

*/
UPSERT person:test CONTENT {
	username: 'parsley',
	bugs: [],
	biscuits: [
		{ name: 'Digestive' },
		{ name: 'Choco Leibniz' }
	]
};
UPDATE person:test PATCH [
	{
		op: 'add',
		path: '/bugs',
		value: 'rfc6902'
	},
	{
		op: 'add',
		path: '/biscuits/0',
		value: { name: 'Ginger Nut' }
	},
	{
		op: 'add',
		path: '/test',
		value: true,
	}
];
UPSERT person:test PATCH [
	{
		op: 'add',
		path: '/bugs/-',
		value: 'rfc6903'
	}
];
