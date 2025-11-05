/**
[test]

[[test.results]]
value = "15"

[[test.results]]
error = "Incorrect arguments for function array::concat(). Argument 1 was the wrong type. Expected `array` but found `1`"

[[test.results]]
value = "[[[[1, 2], 3], 4], 5]"

[[test.results]]
value = "'Just a string'"

[[test.results]]
value = "91"

[[test.results]]
value = "NONE"

[[test.results]]
value = "9"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

*/
array::reduce([1,2,3,4,5],  |$n, $acc| $n + $acc);
array::reduce([1,2,3,4,5],  |$n, $acc| array::concat($n,[$acc]));
array::reduce([1,2,3,4,5], |$n, $i| {
	array::concat([$n], [$i])
});

"gnirts a tsuJ".split("").reduce(|$one, $two| $two + $one);

[10, 9, 8, 7, 6, 5, 4, 3, 2, 1].reduce(|$one, $two, $three| $one + $two + $three);

[].reduce(|$x, $y| $x + $y);

[9].reduce(|$x, $y| $x + $y);

[1,2].reduce(|$x, $y, $idx| $idx);

[1,2,3].reduce(|$x, $y, $idx| $idx);
