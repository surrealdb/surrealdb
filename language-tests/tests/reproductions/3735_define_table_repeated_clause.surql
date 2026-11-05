/**
[test]
reason = "A table definition takes each clause once, so a later clause cannot silently replace an earlier one."
issue = 3735

[test.results]
parsing-error = """
Conflicting clause on this table definition
  --> [20:75]
   |
20 | ... = "user" PERMISSIONS FOR update, delete, create WHERE $scope = "user";
   |              ^^^^^^^^^^^ A table definition takes each clause once
  --> [20:30]
   |
20 | DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE $scope = "user" PER...
   |                              ^^^^^^^^^^^ First given here
"""

*/
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE $scope = "user" PERMISSIONS FOR update, delete, create WHERE $scope = "user";
