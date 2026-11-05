/**
[test]
reason = "Setup for 3.00.00_record_id_numeric_components.surql: record ids whose key holds a float, decimal or integer component, which the current release must decode on a scan"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
LET $keys = [
	['a', 1e30],
	['b', -1e30],
	['c', 1.7976931348623157e308],
	['d', 1.6330027767028172],
	['e', 0.30000000000000004],
	['f', 5.6527459748369866e-8],
	['g', 1e-20],
	['h', 5e-324],
	['i', 0.0000001dec],
	['j', 0.00000000000000000000000001dec],
	['k', 79228162514264337593543950335dec],
	['l', 1.5],
	['m', 1000],
	['n', 1.50dec],
];
FOR $k IN $keys { CREATE type::record('numkey', $k); };
