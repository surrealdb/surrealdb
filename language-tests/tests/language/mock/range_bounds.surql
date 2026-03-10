/**
[test]

[[test.results]]
value = "[test:2, test:3]"

[[test.results]]
value = "[test:1, test:2, test:3, test:4]"

[[test.results]]
value = "[test:-9223372036854775808, test:-9223372036854775807, test:-9223372036854775806]"

[[test.results]]
value = "[test:9223372036854775805, test:9223372036854775806, test:9223372036854775807]"

[[test.results]]
error = "Mock range exceeds allocation limit"

*/

|test:1>..4|;
|test:1..=4|;
|test:..=-9223372036854775806|;
|test:9223372036854775805..|;
|test:..|;
