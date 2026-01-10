/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test edge property filtering with WHERE clauses on edges"

# 0
[[test.results]]
value = "[project:database]"

# 1
[[test.results]]
value = "[project:database]"

# 2
[[test.results]]
value = "[person:bob]"

# 3
[[test.results]]
value = "[skill:postgresql, skill:rust]"

# 4
[[test.results]]
value = "[person:ceo]"

# 5
[[test.results]]
value = "[project:database]"

# 6
[[test.results]]
value = "[project:auth]"
*/

-- 0: Works on projects with > 20 hours
person:alice->(works_on WHERE hours > 20)->project;

-- 1: Works on projects with >= 25 hours
person:alice->(works_on WHERE hours >= 25)->project;

-- 2: Strong connections only
person:alice->(knows WHERE strength = "strong")->person;

-- 3: Expert-level skills
person:alice->(has_skill WHERE level = "expert")->skill;

-- 4: Recent relationships (since 2021)
person:alice->(knows WHERE since > d"2021-01-01")->person;

-- 5: Lead role with significant hours
person:alice->(works_on WHERE role = "lead" AND hours >= 25)->project;

-- 6: Contributor role
person:alice->(works_on WHERE role = "contributor")->project;

