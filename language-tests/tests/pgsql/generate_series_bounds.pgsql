/**
[env]
namespace = true
database = true

[test]
reason = "generate_series includes its stop, is empty when stop is before start, and does not overflow at the largest integer"

[[test.results]]
value = "[{ generate_series: 1 }, { generate_series: 2 }, { generate_series: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ generate_series: 9223372036854775807 }]"

[[test.results]]
value = "[{ generate_series: 4 }]"
*/

SELECT generate_series(1, 3);

SELECT generate_series(3, 1);

SELECT generate_series(9223372036854775807, 9223372036854775807);

SELECT generate_series(4, 4);
