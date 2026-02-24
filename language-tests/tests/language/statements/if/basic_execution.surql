/**
[test]
reason = "Test IF/ELSE statement execution behavior"

[[test.results]]
# Simple IF with truthy condition
value = "'yes'"

[[test.results]]
# Simple IF with falsy condition (returns NONE)
value = "NONE"

[[test.results]]
# IF/ELSE with truthy condition
value = "'if-branch'"

[[test.results]]
# IF/ELSE with falsy condition
value = "'else-branch'"

[[test.results]]
# IF/ELSE IF with first condition true
value = "'first'"

[[test.results]]
# IF/ELSE IF with second condition true
value = "'second'"

[[test.results]]
# IF/ELSE IF/ELSE with else branch
value = "'else'"

[[test.results]]
# Nested IF statements
value = "'inner-if'"

[[test.results]]
# IF with expression evaluation in condition
value = "'sum-is-five'"

[[test.results]]
# IF with complex body (multiple statements via block)
value = "{ greeting: 'Hello', result: 'yes' }"

*/

-- Simple IF with truthy condition
IF true { "yes" };

-- Simple IF with falsy condition (returns NONE)
IF false { "yes" };

-- IF/ELSE with truthy condition
IF true { "if-branch" } ELSE { "else-branch" };

-- IF/ELSE with falsy condition
IF false { "if-branch" } ELSE { "else-branch" };

-- IF/ELSE IF with first condition true
IF true { "first" } ELSE IF true { "second" };

-- IF/ELSE IF with second condition true
IF false { "first" } ELSE IF true { "second" };

-- IF/ELSE IF/ELSE with else branch
IF false { "first" } ELSE IF false { "second" } ELSE { "else" };

-- Nested IF statements
IF true { IF true { "inner-if" } ELSE { "inner-else" } };

-- IF with expression evaluation in condition
IF 2 + 3 == 5 { "sum-is-five" } ELSE { "sum-is-not-five" };

-- IF with complex body (multiple statements via block)
IF true {
    LET $greeting = "Hello";
    { greeting: $greeting, result: "yes" }
};
