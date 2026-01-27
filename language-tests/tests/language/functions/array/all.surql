/**
[env]
imports = ["harness/assert.surql"]

[test]

# 0: Empty array = true
[[test.results]]
value = "true"

# 1: Single value = true
[[test.results]]
value = "true"

# 2: Multiple values = true
[[test.results]]
value = "true"

# 3: Successfully boolean values = true
[[test.results]]
value = "true"

# 4: Successfully string values = true
[[test.results]]
value = "true"

# 5: False value = false
[[test.results]]
value = "false"

# 6: Mixed values = false
[[test.results]]
value = "false"

# 7: String value matching = true
[[test.results]]
value = "true"

# 8: String value not matching = false
[[test.results]]
value = "false"

# 9: String value not matching = false
[[test.results]]
value = "false"

# 10: String value `==` regex = false
[[test.results]]
value = "false"

# 11: String value `==` regex = false
[[test.results]]
value = "false"

# 12: String value `=` regex = true
[[test.results]]
value = "true"

# 13: Array value matching = false
[[test.results]]
value = "false"

# 14: Array value matching = true
[[test.results]]
value = "true"

# 15: String value = error
[[test.results]]
error = "Incorrect arguments for function array::all(). Argument 1 was the wrong type. Expected `array` but found `'some text'`"

# 16: Object value = error
[[test.results]]
error = "Incorrect arguments for function array::all(). Argument 1 was the wrong type. Expected `array` but found `{ bar: 2, foo: 1 }`"

*/

// 0: Empty array = true
array::all([]);
// 1: Single value = true
array::all([1]);
// 2: Multiple values
array::all([1,2]);
// 3: Successfully boolean values
array::all([true]);
// 4: Successfully string values
array::all(["a"]);
// 5: False value = false
array::all([false]);
// 6: Mixed values = false
array::all([1,2,"text",3,NONE,3,4]);
// 7: String value matching = true
array::all(["foo","foo"], "foo");
// 8: String value not matching = false
array::all(["foo","foo"], "bar");
// 9: String value not matching = false
array::all(["bar","foo"], "bar");
// 10: String value `==` regex = false
array::all(["bar","foo"], /foo|bar/);
// 11: String value `==` regex = false
array::all(["bar","foo"], /foo|bar/);
// 12: String value `=` regex = true
array::all(["bar","foo"], |$v| $v = /foo|bar/);

// 13: Array value matching = false
array::all([1,2],[1,2]);
// 14: Array value matching = true
array::all([[1,2]],[1,2]);
// 15: String value = error
array::all("some text");
// 16: Object value = error
array::all({ foo: 1, bar: 2});
