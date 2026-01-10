/**
[env]
imports = ["datasets/graph.surql"]
# This is set higher just to ensure that the TiKV backend doesn't exceed the timeout.
timeout = 5000

[test]
reason = "Test TIMEOUT on graph expressions"

# 0
[[test.results]]
value = "[{ knows: ['Alice Zhang', 'Charlie Brown', 'Dana Lee', 'Bob Martinez', 'Elena Rodriguez', 'Charlie Brown', 'Alice Zhang', 'Dana Lee', 'Bob Martinez', 'Elena Rodriguez', 'Alice Zhang', 'Dana Lee', 'Charlie Brown', 'Bob Martinez', 'Elena Rodriguez', 'Charlie Brown', 'Alice Zhang', 'Charlie Brown', 'Dana Lee', 'Alice Zhang', 'Dana Lee'], name: 'Alice Zhang' }]"

# 1
[[test.results]]
error = "The query was not executed because it exceeded the timeout: 1s"

*/

-- This will succeed because we set an upper bound
SELECT name, @.{..6}(->knows->person).name AS knows FROM person:alice TIMEOUT 4s;

-- This will timeout because we did not set an upper bound on a cyclic graph
SELECT name, @.{..}(->knows->person).name AS knows FROM person:alice TIMEOUT 1s;


