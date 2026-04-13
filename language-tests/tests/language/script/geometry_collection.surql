/**
[test]

[[test.results]]
value = "{ type: 'GeometryCollection', geometries: [(1f, 2f), { type: 'LineString', coordinates: [[3f, 4f], [5f, 6f]] }] }"

*/
RETURN function() {
	return {
		type: "GeometryCollection",
		geometries: [{
			type: "Point",
			coordinates: [1.0,2.0]
		},{
			 "type": "LineString",
			 "coordinates": [
				 [3.0, 4.0],
				 [5.0, 6.0]
			 ]
		 }]
	}
}
