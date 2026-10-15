/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "The unnamed silo form is refused under the deprecated MODULE keyword too, so the alias cannot be used to create a definition the new keyword refuses."

[test.results]
parsing-error = """
An extension needs a name
  --> [24:15]
   |
24 | DEFINE MODULE silo::acme::pkg::<1.0.0> UNSIGNED;
   |               ^^^^ Name the package and call it through that name: `DEFINE EXTENSION ext::name FROM silo::…`, then `ext::name::fn()`
"""

*/

DEFINE MODULE silo::acme::pkg::<1.0.0> UNSIGNED;
