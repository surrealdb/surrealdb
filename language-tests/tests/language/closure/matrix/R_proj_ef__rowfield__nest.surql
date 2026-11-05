/**
[test]
reason = "Position: an element filter in a projected column; the body reads the field `lim` of the row through a closure nested in the body (`array::any([E], |$q| $q >= $m)`). A bare field in an element filter reads the element, including in a closure a builtin invokes within the filter: the invoking frame is the element filter, whose row is the element."

[[test.results]]
value = "[{ id: t:1, lim: 8, n: 1 }]"

[[test.results]]
value = "[{ id: t:2, lim: 8, n: 5 }]"

[[test.results]]
value = "[{ id: t:3, lim: 8, n: 10 }]"

[[test.results]]
value = "[{ id: t:4, lim: 8, n: 15 }]"

[[test.results]]
value = "[{ id: t:5, lim: 8, n: 20 }]"

[[test.results]]
value = "[{ id: u:1, m: 1 }]"

[[test.results]]
value = "[{ id: u:2, m: 5 }]"

[[test.results]]
value = "[{ id: u:3, m: 10 }]"

[[test.results]]
value = "[{ id: u:4, m: 15 }]"

[[test.results]]
value = "[{ id: u:5, m: 20 }]"

[[test.results]]
value = "[{ n: 1, r: [1, 5, 10, 15, 20] }]"
*/
CREATE t:1 SET n = 1, lim = 8;
CREATE t:2 SET n = 5, lim = 8;
CREATE t:3 SET n = 10, lim = 8;
CREATE t:4 SET n = 15, lim = 8;
CREATE t:5 SET n = 20, lim = 8;
CREATE u:1 SET m = 1;
CREATE u:2 SET m = 5;
CREATE u:3 SET m = 10;
CREATE u:4 SET m = 15;
CREATE u:5 SET m = 20;
SELECT n, [1, 5, 10, 15, 20][WHERE array::any([$this], |$q| $q >= lim)] AS r FROM t:1;
