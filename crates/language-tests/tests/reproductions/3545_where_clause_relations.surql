/**
[env]
namespace = true
database = true

[test]
reason = "Test WHERE clause with relation traversal - verify bug #3545 is fixed"
issue = 3545

[[test.results]]
value = "[{ id: task:one }, { id: task:two }]"

[[test.results]]
value = "[{ id: child_of:test, in: task:two, out: task:one }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: task:two, parents: [task:one] }]"

[[test.results]]
error = "Incorrect arguments for function array::len(). Argument 1 was the wrong type. Expected `array` but found `NONE`"

[[test.results]]
value = "[{ id: task:two, parents: [task:one] }]"
*/

-- Set up test data with parent-child relationship
CREATE task:one, task:two;
RELATE task:two->child_of:test->task:one;

-- Test 1: WHERE clause with relation alias using count()
SELECT id, ->child_of->task AS parents
FROM task
WHERE count(parents) >= 1;

-- Test 2: WHERE clause with direct relation traversal using count()
SELECT id, ->child_of->task AS parents
FROM task
WHERE count(->child_of->task) >= 1;

-- Test 3: WHERE clause with relation alias using array::len()
SELECT id, ->child_of->task AS parents
FROM task
WHERE array::len(parents) >= 1;

-- Test 4: WHERE clause with direct relation traversal using array::len()
SELECT id, ->child_of->task AS parents
FROM task
WHERE array::len(->child_of->task) >= 1;
