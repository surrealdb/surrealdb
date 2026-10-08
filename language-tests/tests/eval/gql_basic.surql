/**
[env]
namespace = true
database = true
imports = ["gql/_schema.surql"]
# `eval::gql` runs GQL `MATCH` as a query plan, as top-level GQL does.

[env.capabilities]
allow-eval-query = ["system"]
allow-experimental = ["gql"]

[test]
reason = "eval::gql runs an ISO GQL query from within SurrealQL, in the same transaction"

[[test.results]]
value = "[{ name: 'A' }, { name: 'B' }, { name: 'C' }]"
*/

RETURN eval::gql("MATCH (n:person) RETURN n.name AS name ORDER BY name");
