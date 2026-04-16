/**
[test]

[[test.results]]
value = "{ type: 'LineString', coordinates: [[1f, 2f], [3f, 4f]] }"

*/
RETURN function() {
	return {
		type: "LineString",
		coordinates: [
			[1.0,2.0],
			[3.0,4.0],
		]
	}
}
