/**
[test]
reason = "ensuring right precedence"

[[test.results]]
value = "4"

[[test.results]]
value = "5"

[[test.results]]
value = "8"

[[test.results]]
value = "4"

[[test.results]]
value = "4"

[[test.results]]
value = "1"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

*/
1 + 1 * 3;
1 + 2 ** 2;
2 * 2 ** 2;
2 + 1 ?: true + 1;
2 + 1 ?? true + 1;
1??1..1;
2 == 1 + 1;
2 = 1 + 1;
2 is 1 + 1;
1 < 1 + 1;
3 > 1 + 1;
4 == 2 * 2;
4 == 2 ** 2;
