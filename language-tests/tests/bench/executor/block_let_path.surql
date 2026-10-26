/**
[bench]
reason = "A path into a large value a block binds with LET, read once per loop iteration. Each read reaches one 768-float row of a 100-row array, so its cost must track the row it reads rather than the whole bound value."
run = true
rebuild = false
warmup = "1s"
sample-size = 20
measurement-time = "10s"

[test]
run = false
*/

{
	LET $rows = array::map(array::range(0, 100), |$i| array::repeat(<float> $i, 768));
	FOR $i IN array::range(0, 2000) {
		LET $row = $rows[$i % 100];
	};
	RETURN array::len($rows);
};
