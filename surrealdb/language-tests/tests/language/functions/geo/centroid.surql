/**
[test]

[[test.results]]
value = "(-0.10231019499999999, 51.49576478)"

[[test.results]]
value = "(-1, 1)"

[[test.results]]
error = "Incorrect arguments for function geo::centroid(). Argument 1 was the wrong type. Expected `geometry` but found `1`"

*/
geo::centroid({
	type: 'Polygon',
	coordinates: [[
		[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
		[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
		[-0.38314819, 51.37692386]
	]]
});
geo::centroid((-1.0,1.0));
geo::centroid(1);
