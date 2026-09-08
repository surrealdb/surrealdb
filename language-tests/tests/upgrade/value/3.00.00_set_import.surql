/**
[test]

[[test.results]]
value = "[{ big: {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, id: test:set_values, single: {42,}, small: {1, 2, 3} }]"

*/

CREATE test:set_values SET
	small = {1, 2, 3},
	single = {42,},
	big = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
