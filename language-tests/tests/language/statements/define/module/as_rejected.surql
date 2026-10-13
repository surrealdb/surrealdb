/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "`AS` is no longer the keyword before a module executable. Everywhere else in SurrealQL `AS` introduces an alias, with the new name on the right, whereas here the right-hand side is the module's source and the name is on the left -- so the statement takes `FROM` and rejects `AS` outright rather than accepting both. DEFINE MODULE sits behind the `surrealism` experimental capability and is not stable, so replacing the keyword is a language break we can still take."

[test.results]
parsing-error = """
Unexpected token `AS`, expected FROM
  --> [23:25]
   |
23 | DEFINE MODULE mod::test AS f"bucket:/module.wasm" UNSIGNED;
   |                         ^^
"""
*/

DEFINE MODULE mod::test AS f"bucket:/module.wasm" UNSIGNED;
