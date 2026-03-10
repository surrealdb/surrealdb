/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ five: 5, four: 4, id: test:1, one: 1, three: 3, two: 2 }]"

[[test.results]]
value = "[{ five: 5, four: 4, id: test:1, three: 3, two: 2 }]"

*/

LET $single = "two";
LET $multiple = ["three", "four"];

CREATE test:1 SET one = 1, two = 2, three = 3, four = 4, five = 5;

SELECT * OMIT one, type::field($single), type::fields($multiple) FROM test;