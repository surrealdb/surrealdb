/**
[test]

[[test.results]]
value = "{ type: 'MultiLineString', coordinates: [[[1f, 2f], [3f, 4f]], [[5f, 6f], [7f, 8f]]] }"

*/
RETURN function() {
	return {
		type: "MultiLineString",
		coordinates: [
			[
				[1.0,2.0],
				[3.0,4.0],
			],
			[
				[5.0,6.0],
				[7.0,8.0],
			]
		]
	}
}
