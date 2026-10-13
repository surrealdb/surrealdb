/**
[test]
reason = "Tests string::slug converting strings to lowercase ascii slugs"

[[test.results]]
value = "'hello-world'"

[[test.results]]
value = "'hello-world-this-is-surrealdb'"

[[test.results]]
value = "'multiple-hyphens'"

[[test.results]]
value = "'hello-world'"

[[test.results]]
value = "'snake_case_is_kept'"

[[test.results]]
value = "''"

[[test.results]]
value = "''"

[[test.results]]
value = "'aeroskobing'"
*/

string::slug("Hello World");
string::slug("Hello, World! This is SurrealDB.");
string::slug("  --Multiple---Hyphens--  ");
string::slug("Héllo Wörld");
string::slug("snake_case_is_kept");
string::slug("");
string::slug("!!!");
string::slug("ÆrøSKØbing");
