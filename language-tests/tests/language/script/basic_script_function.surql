/**
[test]
reason = "Test basic embedded JavaScript function execution"

[[test.results]]
value = "10"

[[test.results]]
value = "'hello world'"

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = "{ a: 1, b: 2 }"

[env]
namespace = "test"
database = "test"

*/

-- Basic arithmetic in JavaScript
RETURN function() {
	return 5 + 5;
};

-- String manipulation
RETURN function() {
	return "hello " + "world";
};

-- Array operations
RETURN function() {
	return [1, 2, 3, 4, 5];
};

-- Object creation
RETURN function() {
	return { a: 1, b: 2 };
};
