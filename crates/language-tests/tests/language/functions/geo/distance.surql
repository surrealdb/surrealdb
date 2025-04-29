/**
[test]

[[test.results]]
value = "5562851.11270021f"

[[test.results]]
value = "5562851.11270021f"

[[test.results]]
error = "Incorrect arguments for function geo::distance(). Argument 1 was the wrong type. Expected `geometry` but found `1`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
geo::distance(
	{
		type: 'Point',
		coordinates: [-0.136439, 51.509865]
	},
	{
		type: 'Point',
		coordinates: [ -73.971321, 40.776676]
	}
);
geo::distance((-0.136439,51.509865), ( -73.971321, 40.776676));
geo::distance(1,1);
let $tmp_area = {
	type: 'Polygon',
	coordinates: [[
		[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
		[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
		[-0.38314819, 51.37692386]
	]]
};
geo::distance($tmp_area,$tmp_area);
