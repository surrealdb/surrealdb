/**
[test]
reason = "The body is written in `RETURN array::map([[1, 5], [10, 15, 20]], |$o| array::filter($o, |$i| math::max([$i, $o]) = $i))`. The statement raises the error the closure or the clause it stands in gives."

[[test.results]]
error = "Incorrect arguments for function math::max(). Argument 1 was the wrong type. Expected `number` but found `[1, 5]` when coercing element at index 1 of `array<number>`"
*/
RETURN array::map([[1, 5], [10, 15, 20]], |$o| array::filter($o, |$i| math::max([$i, $o]) = $i));
