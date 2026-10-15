/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "Every new extension needs a name: a silo package defined without one is refused at parse time in either keyword spelling. An unnamed definition already in the catalog keeps loading, and only an import reads the unnamed form back."

[test.results]
parsing-error = """
An extension needs a name
  --> [24:18]
   |
24 | DEFINE EXTENSION silo::acme::pkg::<1.0.0> UNSIGNED;
   |                  ^^^^ Name the package and call it through that name: `DEFINE EXTENSION ext::name FROM silo::…`, then `ext::name::fn()`
"""

*/

DEFINE EXTENSION silo::acme::pkg::<1.0.0> UNSIGNED;
