/**
[test]
reason = "'for method expect' / 'for function expect' output differs by planner strategy so have isolated each into its own file"

[env]
planner-strategy = ["best-effort-ro"]

[[test.results]]
error = "Incorrect arguments for method expect(). Expected 2 to 3 arguments"
*/

9.expect(|$n| $n = 9, "a", "b");