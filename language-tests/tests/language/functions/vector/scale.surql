/**
[test]
reason = "Tests scaling a vector by a scalar with int, float, decimal, zero, and negative factors, plus empty vectors and invalid scalar types"

[[test.results]]
value = "[2, 4, 6]"

[[test.results]]
value = "[1.5f, 3f, 4.5f]"

[[test.results]]
value = "[1.5dec, 3.0dec]"

[[test.results]]
value = "[0, 0]"

[[test.results]]
value = "[-1, -2]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function vector::scale(). Argument 2 was the wrong type. Expected `number` but found `'2'`"

[[test.results]]
error = "Incorrect arguments for function vector::scale(). Argument 2 was the wrong type. Expected `number` but found `NONE`"
*/
vector::scale([1, 2, 3], 2);
vector::scale([1, 2, 3], 1.5);
vector::scale([1, 2], 1.5dec);
vector::scale([1, 2], 0);
vector::scale([1, 2], -1);
vector::scale([], 5);
vector::scale([1, 2], "2");
vector::scale([1, 2], NONE);
