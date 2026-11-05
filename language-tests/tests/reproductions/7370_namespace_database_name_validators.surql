/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Feature request: a regex validator on namespace and database names (DEFINE NS ... OPTIONS). There is no such clause; DEFINE NAMESPACE accepts any name."
issue = 7370

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected Eof
  --> [20:25]
   |
20 | DEFINE NAMESPACE foobar OPTIONS { databaseNameFormat: /[a-z]+/ };
   |                         ^^^^^^^
"""
*/
DEFINE NAMESPACE foobar OPTIONS { databaseNameFormat: /[a-z]+/ };
