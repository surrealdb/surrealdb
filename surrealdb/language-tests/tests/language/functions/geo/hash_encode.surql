/**
[test]

[[test.results]]
value = "'gcpvhchdswz9'"

[[test.results]]
error = "Incorrect arguments for function geo::hash::encode(). Argument 1 was the wrong type. Expected `geometry` but found `''`"

[[test.results]]
value = "NONE"

*/
geo::hash::encode({
	type: 'Point',
	coordinates: [-0.136439, 51.509865]
});
geo::hash::encode("");
geo::hash::encode({
	type: 'Polygon',
	coordinates: [[
		[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
		[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
		[-0.38314819, 51.37692386]
	]]
})
