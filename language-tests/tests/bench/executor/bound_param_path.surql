/**
[bench]
reason = "A path into a large LET-bound value, read once per loop iteration and once per projected row. Each read reaches one 768-float row of a 100-row array, so its cost must track the row it reads rather than the whole bound value."
run = true
rebuild = false
warmup = "1s"
sample-size = 20
measurement-time = "10s"

[test]
run = false
*/

LET $rows = array::map(array::range(0, 100), |$i| array::repeat(<float> $i, 768));

FOR $i IN array::range(0, 2000) {
	LET $row = $rows[$i % 100];
};

RETURN array::len((SELECT VALUE $rows[$this % 100][0] FROM array::range(0, 200)));
