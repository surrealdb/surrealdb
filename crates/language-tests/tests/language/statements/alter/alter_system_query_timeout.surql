/**

[test]

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.config.QUERY_TIMEOUT == 200ms"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.config.QUERY_TIMEOUT == NONE"

*/

-- Set a global query timeout and verify it's applied
ALTER SYSTEM QUERY_TIMEOUT 200ms;
INFO FOR ROOT;

-- Drop the global query timeout and verify it's cleared
ALTER SYSTEM DROP QUERY_TIMEOUT;
INFO FOR ROOT;
