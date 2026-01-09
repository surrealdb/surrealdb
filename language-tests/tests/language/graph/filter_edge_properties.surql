/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test edge property filtering with WHERE clauses on edges"

[[test.results]]
value = "[project:database]"

[[test.results]]
value = "[project:database]"

[[test.results]]
value = "[person:bob]"

[[test.results]]
value = "[skill:postgresql, skill:rust]"

[[test.results]]
value = "[person:ceo]"

[[test.results]]
value = "[project:database]"

[[test.results]]
value = "[project:auth]"
*/

-- Works on projects with > 20 hours
person:alice->(works_on WHERE hours > 20)->project;

-- Works on projects with >= 25 hours
person:alice->(works_on WHERE hours >= 25)->project;

-- Strong connections only
person:alice->(knows WHERE strength = "strong")->person;

-- Expert-level skills
person:alice->(has_skill WHERE level = "expert")->skill;

-- Recent relationships (since 2021)
person:alice->(knows WHERE since > d"2021-01-01")->person;

-- Lead role with significant hours
person:alice->(works_on WHERE role = "lead" AND hours >= 25)->project;

-- Contributor role
person:alice->(works_on WHERE role = "contributor")->project;

