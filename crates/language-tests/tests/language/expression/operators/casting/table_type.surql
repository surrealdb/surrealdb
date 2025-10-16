/**
[test]

[[test.results]]
value = "'users'"

[[test.results]]
value = "'posts'"

[[test.results]]
error = "Expected `table<users>` but found a `'posts'`"

[[test.results]]
value = "'users'"

[[test.results]]
value = "'posts'"

[[test.results]]
value = "'admin'"

*/
-- Cast string to generic table
<table> "users";

-- Cast string to table (works because it's any table)
<table> "posts";

-- Cast string to specific table type (should fail)
<table<users>> "posts";

-- Cast string to specific table type (should succeed)
<table<users>> "users";

-- Cast string to union of table types
<table<users | posts>> "posts";

-- Cast string to union of table types
<table<users | posts | admin>> "admin";

