/**
[bench]
reason = "Per-element closure invocation: array::map/filter/fold each call a closure once per element, so this isolates invocation cost from the surrounding query. The nearest existing coverage is crud/create_1000, where the same work is swamped by writing 1000 records."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

LET $xs = array::range(0, 4000);

RETURN {
	mapped: array::len(array::map($xs, |$v| $v * 2)),
	filtered: array::len(array::filter($xs, |$v| $v % 3 = 0)),
	folded: array::fold($xs, 0, |$acc, $v| $acc + $v),
};
