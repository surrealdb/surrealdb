/**
[test]

[[test.results]]
value = "1029944667.4192368f"

[[test.results]]
value = "0f"

[[test.results]]
error = "Incorrect arguments for function geo::area(). Argument 1 was the wrong type. Expected `geometry` but found `''`"

*/
geo::area({
	type: 'Polygon',
	coordinates: [[
		[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
		[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
		[-0.38314819, 51.37692386]
	]]
});
geo::area((1,1));
geo::area("");
