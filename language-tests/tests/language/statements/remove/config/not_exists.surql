/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Test REMOVE CONFIG IF EXISTS returns NONE when config doesn't exist, and REMOVE CONFIG errors"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The config for graphql does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The config for api does not exist"

*/

-- 0: IF EXISTS returns NONE when no GraphQL config exists
REMOVE CONFIG IF EXISTS GRAPHQL;

-- 1: Without IF EXISTS, errors
REMOVE CONFIG GRAPHQL;

-- 2: IF EXISTS returns NONE when no API config exists
REMOVE CONFIG IF EXISTS API;

-- 3: Without IF EXISTS, errors
REMOVE CONFIG API;
