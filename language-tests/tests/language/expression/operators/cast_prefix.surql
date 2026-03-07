/**
[test]
reason = "Test cast prefix operators for type coercion"

[[test.results]]
value = "42"

[[test.results]]
value = "3.14f"

[[test.results]]
value = "'42'"

[[test.results]]
value = "true"

[[test.results]]
value = "d'2024-06-15T12:00:00Z'"

[[test.results]]
value = "1h30m"

[[test.results]]
value = "99.99dec"

*/

<int> "42";
<float> "3.14";
<string> 42;
<bool> "true";
<datetime> "2024-06-15T12:00:00Z";
<duration> "1h30m";
<decimal> "99.99";
