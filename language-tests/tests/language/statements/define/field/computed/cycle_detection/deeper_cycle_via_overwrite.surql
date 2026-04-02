/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Deeper cycle introduced by OVERWRITE: a -> d -> b -> a"

[[test.results]]
value = "'OK'"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> d -> b -> a"

*/

{
    DEFINE TABLE t_deep SCHEMAFULL;
    DEFINE FIELD a ON t_deep TYPE number DEFAULT 1;
    DEFINE FIELD b ON t_deep COMPUTED a + 1;
    DEFINE FIELD c ON t_deep COMPUTED a + b;
    DEFINE FIELD d ON t_deep COMPUTED c * b;
    RETURN "OK";
};
DEFINE FIELD OVERWRITE a ON t_deep COMPUTED d * 2;
