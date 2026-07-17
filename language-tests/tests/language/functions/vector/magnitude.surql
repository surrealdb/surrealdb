/**
[test]
reason = "Tests vector magnitude (Euclidean norm), including empty vectors, negative components, and invalid argument types"

[[test.results]]
value = "5f"

[[test.results]]
value = "5f"

[[test.results]]
value = "0f"

[[test.results]]
value = "1f"

[[test.results]]
value = "1f"

[[test.results]]
error = "Incorrect arguments for function vector::magnitude(). Argument 1 was the wrong type. Expected `array<number>` but found `'not a vector'`"

[[test.results]]
error = "Incorrect arguments for function vector::magnitude(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"
*/
vector::magnitude([3, 4]);
vector::magnitude([-3, 4]);
vector::magnitude([0, 0, 0]);
vector::magnitude([1]);
vector::magnitude([0.5, 0.5, 0.5, 0.5]);
vector::magnitude("not a vector");
vector::magnitude(NONE);
