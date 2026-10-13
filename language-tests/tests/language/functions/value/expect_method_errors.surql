/**
[test]
reason = "Under method syntax the receiver is the asserted value and is returned unchanged on success. A thrown assertion failure keeps the `value::expect` name on every planner strategy, but argument-validation errors are relabelled: the compute executor reports them against the function while the streaming executors rewrite them to the bare method name, so those expectations are matched by pattern"

[[test.results]]
value = "10"

[[test.results]]
value = "'abc'"

[[test.results]]
error = "An error occurred: value::expect assertion failed"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: 1"

[[test.results]]
match = "$error = /Incorrect arguments for (?:function|method) (?:value::)?expect\\(\\)\\. Assertion closure must return a bool, got int/"
error = true

[[test.results]]
match = "$error = /Incorrect arguments for (?:function|method) (?:value::)?expect\\(\\)\\. Argument 2 was the wrong type\\. Expected `function` but found `'not a closure'`/"
error = true

[[test.results]]
match = "$error = /Incorrect arguments for (?:function|method) (?:value::)?expect\\(\\)\\. Expected 2 to 3 arguments/"
error = true
*/

-- The receiver is passed through untouched when the assertion holds
RETURN (10).expect(|$v| $v > 5);
RETURN "abc".expect(|$v| $v.len() = 3);

-- A thrown assertion failure is not relabelled by the method dispatch
RETURN (10).expect(|$v| $v > 50);
RETURN (10).expect(|$v| $v > 50, 1);

-- Argument-validation errors name the function or the method by strategy
RETURN (10).expect(|$v| $v);
RETURN (10).expect("not a closure");
RETURN (10).expect();
