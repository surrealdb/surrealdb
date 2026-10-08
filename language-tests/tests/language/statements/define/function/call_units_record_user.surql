/**
[env]
imports = ["language/statements/define/function/call_units_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "SECURITY: a call of a user-defined function by a record user enters the function as the definer's auth caps it, judges the `PERMISSIONS` predicate for that caller, and runs a body that reads data as the caller reads it; a parameter read judges the parameter's own predicate. A table permission that calls a function evaluates it where reads are not checked."

[[test.results]]
value = "'bob'"

[[test.results]]
error = "You don't have permission to run the fn::alice_only function"

[[test.results]]
error = "You don't have permission to run the fn::nobody function"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "10"

[[test.results]]
value = "['bob', 'for bob', 2]"

[[test.results]]
error = "You don't have permission to view the $alice_param parameter"

[[test.results]]
error = "You don't have permission to view the $hidden_param parameter"

[[test.results]]
value = "'for all'"

[[test.results]]
value = "[['for bob', 1], ['for bob', 3]]"

[[test.results]]
value = "[6, 6]"
*/

-- A predicate that admits the caller, one that does not, and a clause that admits no one.
fn::bob_only();
fn::alice_only();
fn::nobody();
-- A body that reads data sees the rows a record user may see.
fn::counts();
fn::counted_by_bob();
-- A table permission that calls a function reads unchecked.
SELECT VALUE id FROM doc;
-- A call whose arguments and body call functions.
fn::chain(4);
-- A parameter whose predicate admits the caller, among values.
[fn::bob_only(), $bob_param, fn::double(1)];
$alice_param;
$hidden_param;
$open_param;
-- Per row, a parameter and a function are judged on every row.
SELECT VALUE [$bob_param, n] FROM item WHERE n != 2 ORDER BY n;
[fn::double(3), fn::chain(0) + 4];
