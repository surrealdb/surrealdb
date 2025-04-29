/**
[test]

[[test.results]]
value = "15"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5]"

[[test.results]]
value = "[[[[[0, 1], 2], 3], 4], 5]"

[[test.results]]
value = "'Just a string'"

[[test.results]]
value = "100"

[[test.results]]
value = "10"

[[test.results]]
value = "19"

*/
array::fold([1,2,3,4,5], 0, |$n, $acc| $n + $acc);
array::fold([1,2,3,4,5], [0], |$n, $acc| array::concat($n,[$acc]));
array::fold([1,2,3,4,5], 0,|$n, $i| {
	array::concat([$n], [$i])
});

"gnirts a tsuJ".split("").fold("", |$one, $two| $two + $one);

[10, 9, 8, 7, 6, 5, 4, 3, 2, 1].fold(0, |$one, $two, $three| $one + $two + $three);

[].fold(10, |$x, $y| $x + $y);

[9].fold(10, |$x, $y| $x + $y);
