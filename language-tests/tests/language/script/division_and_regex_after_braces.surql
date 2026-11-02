/**
[test]
requires = ["scripting"]
reason = "A `/` after an object literal's `}` divides; after a block's `}` it can start a regex literal"

[[test.results]]
value = "0.5f"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[env]
namespace = "test"
database = "test"

*/

-- Division after a member of an object literal
RETURN function() {
	return { a: 1 }.a / 2;
};

-- Division of an object literal itself
RETURN function() {
	const ratio = {} / 2;
	return Number.isNaN(ratio);
};

-- A regex literal holding `}` after a block
RETURN function() {
	if (true) { }
	/}/.test("a}b");
	return /}/.test("a}b");
};

-- A regex literal after an arrow function's block body
RETURN function() {
	const f = () => { return "}"; };
	return /}/.test(f());
};

-- Division after a function expression's body
RETURN function() {
	const ratio = function() { return 1; } / 2;
	return Number.isNaN(ratio);
};
