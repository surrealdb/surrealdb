/**
[test]
reason = "Test casting strings to table type"

[[test.results]]
match = "$result == type::table('users')"

[[test.results]]
match = "$result == type::table('users')"

[[test.results]]
match = "$result == type::table('users')"

[[test.results]]
match = "$result == type::table('posts')"

[[test.results]]
error = "Could not cast into `table<users|posts>` using input `'comments'`"

[[test.results]]
match = "$result == type::table('')"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result == type::table('users')"

[[test.results]]
match = "$result == type::table('user_accounts')"

[[test.results]]
match = "$result == type::table('user_accounts')"

[env]
namespace = "test"
database = "test"
*/

-- Test basic table casting from string
RETURN <table> "users";

-- Test table casting with specific table name
RETURN <table<users>> "users";

-- Test table casting with union of table names (should accept either)
RETURN <table<users | posts>> "users";
RETURN <table<users | posts>> "posts";

-- Test that casting wrong table name fails
-- This should fail because "comments" is not in the allowed list
RETURN <table<users | posts>> "comments";

-- Test with empty string (should work for generic table)
RETURN <table> "";

-- Test table type in variable assignment
LET $my_table = <table> "users";
RETURN $my_table;

-- Test table type with complex table name
RETURN <table> "user_accounts";
RETURN <table<user_accounts>> "user_accounts";

